from aiogram import Bot, Dispatcher
from config import BOT_TOKEN

# Создаём экземпляр бота
bot = Bot(token=BOT_TOKEN)

# В aiogram 3.x Dispatcher создаётся без аргументов
dp = Dispatcher()
