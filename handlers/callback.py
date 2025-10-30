# tg_zov/handlers/callback.py
import asyncio
import time
import logging
import os
import json
from aiogram import Router, types, F
from aiogram.types import Message

from config import ADMIN_IDS
from services.flop_pair import run_flop_pair, find_flop_pairs
from services.castle_machine import run_castle_machine
from services.thanksgiving_event import run_thanksgiving_event
from services.promo_code import run_promo_code, load_promo_history, save_promo_history
from services.accounts_manager import get_active_account
from services.event_manager import run_full_event_cycle

router = Router()
logger = logging.getLogger("callback")

# 🔐 Глобальная блокировка, чтобы не запускать автосбор дважды
_RUN_LOCK = asyncio.Lock()
PROMO_HISTORY_FILE = "data/promo_history.json"


# ---------------------------------------
# 🔄 Обновить cookies (асинхронно, в фоне)
# ---------------------------------------
@router.message(F.text == "🔄 Обновить cookies")
async def handle_update_cookies(message: Message):
    """Асинхронное массовое обновление cookies в фоне (не блокирует бота)."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 Только для админов.")
        return

    await message.answer("♻️ Запускаю обновление cookies в фоне...")

    async def background_update():
        from services.accounts_manager import get_all_users_accounts
        from services.castle_api import refresh_cookies_mvp

        accounts_by_user = get_all_users_accounts()
        logger.info(f"[COOKIES] Всего пользователей найдено: {len(accounts_by_user)}")

        total_success = 0
        failures = []
        skipped = []

        total_accounts = sum(len(v) for v in accounts_by_user.values())
        done = 0

        for user_id, accounts in accounts_by_user.items():
            for account in accounts:
                done += 1
                uid = account.get("uid", "").strip()
                mvp_url = account.get("mvp_url", "").strip()
                username = account.get("username", "Игрок")

                if not uid:
                    skipped.append(f"{user_id}: отсутствует UID")
                    continue

                if not mvp_url:
                    skipped.append(f"{user_id}:{uid} — нет MVP ссылки")
                    continue

                progress = f"{done}/{total_accounts}"
                await message.answer(
                    f"🔁 <b>{progress}</b> — обновляю cookies для <b>{username}</b> (<code>{uid}</code>)...",
                    parse_mode="HTML"
                )

                try:
                    result = await refresh_cookies_mvp(user_id, uid)
                    if result.get("success"):
                        total_success += 1
                        await message.answer(f"✅ Успешно обновлено: <code>{uid}</code>", parse_mode="HTML")
                    else:
                        error_text = result.get("error", "неизвестная ошибка")
                        failures.append(f"{user_id}:{uid} — {error_text}")
                        await message.answer(
                            f"⚠️ Не удалось обновить <b>{username}</b> (<code>{uid}</code>): <i>{error_text}</i>",
                            parse_mode="HTML"
                        )
                except Exception as exc:
                    logger.exception(f"[COOKIES] ❌ Ошибка при обновлении cookies {user_id}:{uid}: {exc}")
                    failures.append(f"{user_id}:{uid} — исключение: {exc}")
                    await message.answer(
                        f"❌ Ошибка при обработке <code>{uid}</code>: {exc}",
                        parse_mode="HTML"
                    )

                await asyncio.sleep(2)

        summary_lines = [
            "📊 <b>Итоги обновления cookies:</b>",
            f"• ✅ Успешно: <b>{total_success}</b>",
            f"• ❌ Ошибки: <b>{len(failures)}</b>",
            f"• ⚠️ Пропущено: <b>{len(skipped)}</b>",
        ]

        if failures:
            summary_lines.append("\n❌ <b>Ошибки:</b>")
            summary_lines.extend(f" - {item}" for item in failures)

        if skipped:
            summary_lines.append("\n⚠️ <b>Пропущенные аккаунты:</b>")
            summary_lines.extend(f" - {item}" for item in skipped)

        await message.answer("\n".join(summary_lines), parse_mode="HTML")

    # 🔥 Запуск в фоне — бот не блокируется
    asyncio.create_task(background_update())


# ---------------------------------------
# 🔍 Проверить пары (админская команда)
# ---------------------------------------
@router.message(F.text == "🔍 Проверить пары")
async def handle_find_pairs(message: Message):
    """Сканирует страницу акции 'Найди пару' и сохраняет найденные пары."""
    user_id = str(message.from_user.id)

    if int(user_id) not in ADMIN_IDS:
        await message.answer("🚫 Эта команда доступна только администраторам.")
        return

    await message.answer("🔍 Начинаю сканирование карт и поиск пар...")

    result = await find_flop_pairs(user_id)

    msg = result.get("message", "⚠️ Ошибка при поиске пар.")
    await message.answer(msg)


# ---------------------------------------
# 🃏 Найди пару
# ---------------------------------------
@router.message(F.text == "🃏 Найди пару")
async def handle_flop_pair(message: Message):
    """Запускает 'Найди пару' для всех аккаунтов пользователя."""
    user_id = str(message.from_user.id)
    from services.accounts_manager import get_all_accounts
    accounts = get_all_accounts(user_id)

    if not accounts:
        await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
        return

    await message.answer(f"🃏 Запускаю акцию 'Найди пару' для всех твоих аккаунтов ({len(accounts)} шт)...")

    total_success = 0
    total_fail = 0
    messages = []

    for acc in accounts:
        uid = acc.get("uid")
        username = acc.get("username", "Игрок")
        if not uid:
            continue

        await message.answer(f"🎯 Запуск для <b>{username}</b> (<code>{uid}</code>)...", parse_mode="HTML")

        try:
            result = await run_flop_pair(user_id, uid=uid)
            if result.get("success"):
                total_success += 1
            else:
                total_fail += 1
            messages.append(result.get("message", "⚠️ Неизвестный ответ"))
        except Exception as e:
            total_fail += 1
            messages.append(f"❌ Ошибка при обработке {uid}: {e}")
        await asyncio.sleep(2)

    summary = (
        f"✅ Завершено!\n"
        f"Всего аккаунтов: {len(accounts)}\n"
        f"Успешно: {total_success}\n"
        f"Ошибки: {total_fail}\n\n"
        "📜 Результаты:\n\n" + "\n\n".join(messages)
    )

    await message.answer(summary, parse_mode="HTML")


# ---------------------------------------
# 🧪 Тест
# ---------------------------------------
@router.message(F.text == "🧪 Тест")
async def handle_test(message: Message):
    """Проверка, что бот работает и активный аккаунт выбран."""
    user_id = str(message.from_user.id)
    active_acc = get_active_account(user_id)

    if active_acc:
        await message.answer(
            f"✅ Бот работает!\nАктивный аккаунт: <code>{active_acc['uid']}</code>",
            parse_mode="HTML"
        )
    else:
        await message.answer("✅ Бот работает! Но активный аккаунт не выбран.")


# ---------------------------------------
# 🔁 Автосбор наград
# ---------------------------------------
@router.message(F.text == "🔁 Автосбор наград")
async def handle_manual_autocollect(message: Message):
    """Ручной запуск автосбора для всех пользователей и аккаунтов (в фоне)."""
    user_id = message.from_user.id

    if user_id not in ADMIN_IDS:
        await message.answer("🚫 Эта команда доступна только администраторам.")
        return

    if _RUN_LOCK.locked():
        await message.answer("⏳ Автосбор уже выполняется. Дождитесь завершения текущего запуска.")
        return

    await message.answer(
        "🚀 Запускаю ручной автосбор по всем пользователям и аккаунтам…\n"
        "Я пришлю подробный отчёт по завершении."
    )

    async def _run():
        start_ts = time.perf_counter()
        async with _RUN_LOCK:
            try:
                result = await run_full_event_cycle(bot=message.bot, manual=True)
                took = time.perf_counter() - start_ts
                summary = (
                    f"✅ <b>Ручной автосбор завершён!</b>\n"
                    f"🕒 Время выполнения: <b>{took:.1f} сек</b>\n"
                    f"📄 Сообщение: {result.get('message', '—')}"
                )
                await message.answer(summary, parse_mode="HTML")
            except Exception as e:
                logger.exception(f"❌ Ошибка во время ручного автосбора: {e}")
                await message.answer(f"❌ Автосбор завершился с ошибкой: {e}", parse_mode="HTML")

    asyncio.create_task(_run())


# ---------------------------------------
# 🧩 Маленькая помощь (GAS) — асинхронно
# ---------------------------------------
from services.gas_event import run_gas_event

@router.message(F.text == "🧩 Маленькая помощь")
async def handle_gas_event(message: Message):
    """🧩 Асинхронный запуск события 'Маленькая помощь' для всех аккаунтов пользователя."""
    user_id = str(message.from_user.id)
    await message.answer("🧩 Запускаю событие <b>Маленькая помощь</b> в фоне... ⏳", parse_mode="HTML")

    async def background_gas():
        from services.accounts_manager import get_all_accounts
        accounts = get_all_accounts(user_id)
        if not accounts:
            await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
            return

        total = len(accounts)
        success_count = 0
        fail_count = 0

        # 🚀 Запускаем все аккаунты параллельно
        tasks = [run_gas_event(user_id, acc["uid"]) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for acc, res in zip(accounts, results):
            uid = acc.get("uid")
            username = acc.get("username", "Игрок")

            if isinstance(res, Exception):
                fail_count += 1
                await message.answer(f"❌ {username} ({uid}) — ошибка: {res}", parse_mode="HTML")
                continue

            msg = res.get("message", "⚠️ Неизвестный ответ")
            await message.answer(msg, parse_mode="HTML")

            if res.get("success"):
                success_count += 1
            else:
                fail_count += 1

        report = (
            f"🧩 <b>Маленькая помощь завершена!</b>\n\n"
            f"👥 Аккаунтов: <b>{total}</b>\n"
            f"✅ Успешно: <b>{success_count}</b>\n"
            f"⚠️ Ошибок: <b>{fail_count}</b>"
        )
        await message.answer(report, parse_mode="HTML")

    asyncio.create_task(background_gas())
# ---------------------------------------
# ⚙️ Создающая машина — асинхронно
# ---------------------------------------
@router.message(F.text == "⚙️ Создающая машина")
async def handle_castle_machine(message: types.Message):
    """⚙️ Асинхронный запуск акции 'Создающая машина'."""
    from html import escape

    user_id = str(message.from_user.id)
    await message.answer("⚙️ Запускаю акцию <b>Создающая машина</b> в фоне... ⏳", parse_mode="HTML")

    async def background_castle_machine():
        from services.accounts_manager import get_all_accounts
        from services.castle_machine import run_castle_machine

        accounts = get_all_accounts(user_id)
        if not accounts:
            await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
            return

        total = len(accounts)
        success_count = 0
        fail_count = 0

        # 🚀 Параллельный запуск всех аккаунтов
        tasks = [run_castle_machine(user_id, acc["uid"]) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for acc, res in zip(accounts, results):
            uid = acc.get("uid")
            username = acc.get("username", "Игрок")

            if isinstance(res, Exception):
                fail_count += 1
                safe_err = escape(str(res))
                await message.answer(f"❌ {username} ({uid}) — ошибка: <code>{safe_err}</code>", parse_mode="HTML")
                continue

            msg = res.get("message", "❓ Неизвестный ответ")

            # 🧹 Безопасная отправка, экранируем HTML
            try:
                await message.answer(msg, parse_mode="HTML")
            except Exception:
                # если Telegram ругается на теги, экранируем полностью
                safe_msg = escape(msg)
                await message.answer(safe_msg, parse_mode="HTML")

            if res.get("success"):
                success_count += 1
            else:
                fail_count += 1

        summary = (
            f"⚙️ <b>Создающая машина завершена!</b>\n\n"
            f"👥 Аккаунтов: <b>{total}</b>\n"
            f"✅ Успешно: <b>{success_count}</b>\n"
            f"⚠️ Ошибок: <b>{fail_count}</b>"
        )
        await message.answer(summary, parse_mode="HTML")

    asyncio.create_task(background_castle_machine())

# ---------------------------------------
# 🎁 10 дней призов (асинхронно, в фоне)
# ---------------------------------------
@router.message(F.text == "🎁 10 дней призов")
async def handle_thanksgiving_event(message: types.Message):
    """Ручной запуск акции '10 дней призов' (не блокирует бота)."""
    user_id = str(message.from_user.id)
    await message.answer("🎁 Запускаю акцию <b>10 дней призов</b> в фоне... ⏳", parse_mode="HTML")

    async def background_thanksgiving():
        try:
            from services.accounts_manager import get_all_accounts
            accounts = get_all_accounts(user_id)

            if not accounts:
                await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
                return

            total = len(accounts)
            success_count = 0
            fail_count = 0

            # Запускаем все аккаунты параллельно
            tasks = [run_thanksgiving_event(user_id, acc.get("uid")) for acc in accounts if acc.get("uid")]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Соответствие accounts/results по порядку uid
            seq_accounts = [acc for acc in accounts if acc.get("uid")]

            for acc, res in zip(seq_accounts, results):
                uid = acc.get("uid")
                username = acc.get("username", "Игрок")

                if isinstance(res, Exception):
                    fail_count += 1
                    await message.answer(f"❌ <b>{username}</b> ({uid}) — ошибка: {res}", parse_mode="HTML")
                else:
                    msg = res.get("message", "❓ Неизвестный ответ")
                    await message.answer(msg, parse_mode="HTML")
                    if res.get("success"):
                        success_count += 1
                    else:
                        fail_count += 1

            summary = (
                f"✅ <b>Проверка акции '10 дней призов' завершена!</b>\n\n"
                f"👥 Всего аккаунтов: <b>{total}</b>\n"
                f"✅ Успешно: <b>{success_count}</b>\n"
                f"⚠️ Ошибки: <b>{fail_count}</b>"
            )
            await message.answer(summary, parse_mode="HTML")

        except Exception as e:
            await message.answer(f"❌ Ошибка при запуске акции: <code>{e}</code>", parse_mode="HTML")

    # 🚀 Запуск в фоне — бот остаётся отзывчивым
    asyncio.create_task(background_thanksgiving())


# ------------------------------------------
# 🎟 Ввод промокода
# ------------------------------------------
@router.message(F.text == "🎁 Ввод промокода")
async def ask_promo_code(message: types.Message):
    await message.answer("🎁 Отправь промокод, я попробую активировать его на всех аккаунтах в базе.")


@router.message(F.text.regexp(r"^[A-Za-z0-9]+$"))
async def apply_promo_code(message: types.Message):
    code = message.text.strip().upper()
    history = load_promo_history()

    # Проверка на дубликаты
    if code in history:
        await message.answer(f"⚠️ Промокод <b>{code}</b> уже использовался ранее.", parse_mode="HTML")
        return

    await message.answer(f"🚀 Пробую активировать промокод <b>{code}</b> на всех аккаунтах...", parse_mode="HTML")

    results = await run_promo_code(code)

    # Сохраняем в историю
    history.append(code)
    save_promo_history(history)

    # Рассылаем пользователям отчёты
    for user_id, msgs in results.items():
        if not msgs:
            continue
        text = f"🎟 Результат промокода <b>{code}</b>:\n\n" + "\n".join(msgs)
        try:
            await message.bot.send_message(user_id, text, parse_mode="HTML")
        except Exception:
            pass

    await message.answer("✅ Промокод обработан по всем аккаунтам!", parse_mode="HTML")
