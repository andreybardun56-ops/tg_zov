"""Утилиты для решения капч с несколькими стратегиями OCR."""
"""Captcha solving utilities."""

from __future__ import annotations

import io
import math
import os
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional, Tuple

import numpy as np
import pytesseract
from PIL import Image, ImageChops, ImageFilter, ImageOps

# optional: easyocr fallback (install if you want)
try:  # pragma: no cover - зависящие от окружения зависимости не критичны
    import easyocr
except Exception:  # pragma: no cover - optional dependency
    easyocr = None

try:  # pragma: no cover - используется, если доступно OpenCV
    import cv2
except Exception:  # pragma: no cover - не делаем hard dependency
    cv2 = None

__all__ = ["solve_captcha"]


def _configure_tesseract_cmd() -> None:
    """Configure pytesseract binary path via env variable if provided."""

    cmd = os.getenv("TESSERACT_CMD")
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd


_configure_tesseract_cmd()

# ---------- Image preprocessing helpers ----------

def pil_to_gray(img: Image.Image) -> Image.Image:
    return img.convert("L")

def resize_for_ocr(img: Image.Image, scale: int = 3) -> Image.Image:
    w, h = img.size
    return img.resize((w * scale, h * scale), Image.LANCZOS)

def contrast_stretch(img: Image.Image) -> Image.Image:
    return ImageOps.autocontrast(img, cutoff=0)

def adaptive_binarize(img: Image.Image, block_size: int = 15, offset: int = 10) -> Image.Image:
    """Адаптивное пороговое преобразование с использованием OpenCV, если доступно."""

    block_size = max(3, block_size | 1)  # OpenCV требует нечётный размер окна
    arr = np.array(img, dtype=np.uint8)

    if cv2 is not None:
        # cv2.adaptiveThreshold автоматически приводит изображение к uint8
        thresh = cv2.adaptiveThreshold(
            arr,
            255,
            cv2.ADAPTIVE_THRESH_MEAN_C,
            cv2.THRESH_BINARY,
            block_size,
            offset,
        )
        return Image.fromarray(thresh)

    # fallback: векторизованный расчёт среднего через интегральное изображение
    pad = block_size // 2
    padded = np.pad(arr, pad, mode="edge")
    integral = padded.cumsum(axis=0).cumsum(axis=1)

    y1 = np.arange(arr.shape[0])[:, None]
    x1 = np.arange(arr.shape[1])[None, :]
    y2 = y1 + block_size
    x2 = x1 + block_size

    area = block_size * block_size
    total = (
        integral[y2, x2]
        - integral[y1, x2]
        - integral[y2, x1]
        + integral[y1, x1]
    )
    mean = total / area
    out = np.where(arr > (mean - offset), 255, 0).astype(np.uint8)
    return Image.fromarray(out)

def remove_background_tophat(img: Image.Image, kernel_size: int = 15) -> Image.Image:
    """Apply a simple morphological top-hat filter to remove background."""

    opened = img.filter(ImageFilter.MinFilter(kernel_size)).filter(ImageFilter.MaxFilter(kernel_size))
    return ImageChops.subtract(img, opened)

def denoise(img: Image.Image) -> Image.Image:
    return img.filter(ImageFilter.MedianFilter(size=3))


def auto_invert(img: Image.Image) -> Image.Image:
    """Инвертирует изображение, если фон явно тёмный."""

    arr = np.array(img, dtype=np.uint8)
    # Если средняя яркость ниже середины диапазона, вероятно фон тёмный
    if arr.mean() < 127:
        return ImageOps.invert(img)
    return img


def trim_border(img: Image.Image, threshold: int = 250) -> Image.Image:
    """Обрезает почти однотонную рамку по краям."""

    if img.mode != "L":
        gray = img.convert("L")
    else:
        gray = img

    arr = np.array(gray)
    mask = arr < threshold
    if not mask.any():
        return img

    ys, xs = np.where(mask)
    top, bottom = ys.min(), ys.max() + 1
    left, right = xs.min(), xs.max() + 1
    return img.crop((left, top, right, bottom))

def deskew(img: Image.Image) -> Image.Image:
    # simple deskew via projection/profile - approximate
    bw = np.array(img.convert("L"))
    coords = np.column_stack(np.where(bw < 128))
    if coords.size == 0:
        return img
    angle = -skew_angle_from_coords(coords)
    if abs(angle) < 0.1:
        return img
    return img.rotate(angle, expand=True, fillcolor=255)

def skew_angle_from_coords(coords: np.ndarray) -> float:
    # fit line to coordinates and compute angle
    try:
        import numpy as np
        xs = coords[:, 1].astype(np.float32)
        ys = coords[:, 0].astype(np.float32)
        A = np.vstack([xs, np.ones_like(xs)]).T
        m, c = np.linalg.lstsq(A, ys, rcond=None)[0]
        angle = math.degrees(math.atan(m))
        return angle
    except Exception:
        return 0.0

# ---------- OCR pipeline ----------

def ocr_tesseract(img: Image.Image, whitelist: Optional[str] = None) -> str:
    config = "--psm 8"  # treat image as a single word/line (tweak if needed)
    if whitelist:
        config += f" -c tessedit_char_whitelist={whitelist}"
    try:
        txt = pytesseract.image_to_string(img, config=config)
        return txt.strip()
    except Exception:
        return ""


@lru_cache(maxsize=1)
def _get_easyocr_reader() -> Optional["easyocr.Reader"]:
    if easyocr is None:
        return None
    try:
        # gpu=False by default to avoid GPU requirement on servers
        return easyocr.Reader(["en"], gpu=False)
    except Exception:
        return None


def ocr_easyocr(img: Image.Image) -> str:
    reader = _get_easyocr_reader()
    if reader is None:
        return ""
    arr = np.array(img)
    try:
        res = reader.readtext(arr, detail=0)
        return "".join(res).strip()
    except Exception:
        return ""

def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """Подготавливает изображение капчи к распознаванию."""

    img = pil_to_gray(img)
    img = trim_border(img)
    img = contrast_stretch(img)
    img = auto_invert(img)
    img = remove_background_tophat(img, kernel_size=15)
    img = denoise(img)
    img = adaptive_binarize(img, block_size=17, offset=12)
    img = resize_for_ocr(img, scale=3)
    img = img.filter(ImageFilter.MedianFilter(size=3))
    return img

def _load_image(path_or_bytes: "Path | str | bytes | bytearray") -> Image.Image:
    if isinstance(path_or_bytes, (bytes, bytearray)):
        return Image.open(io.BytesIO(path_or_bytes))
    return Image.open(path_or_bytes)


def _collect_attempts(img: Image.Image, whitelist: Optional[str]) -> Iterable[Tuple[str, str]]:
    attempts: list[Tuple[str, str]] = []



def _collect_attempts(img: Image.Image, whitelist: Optional[str]) -> Iterable[Tuple[str, str]]:
    attempts: list[Tuple[str, str]] = []

    attempts.append(("tess_raw", ocr_tesseract(img, whitelist)))

    pre = preprocess_for_ocr(img)
    attempts.append(("tess_prep", ocr_tesseract(pre, whitelist)))

    desk = deskew(pre)
    attempts.append(("tess_prep_deskew", ocr_tesseract(desk, whitelist)))

    reader = _get_easyocr_reader()
    if reader is not None:
        attempts.append(("easy_prep", ocr_easyocr(pre)))
        attempts.append(("easy_raw", ocr_easyocr(img)))

    return attempts


def _select_candidate(attempts: Iterable[Tuple[str, str]]) -> str:
    candidates = [txt for _, txt in attempts if txt and txt.strip()]
    if not candidates:
        return ""

    candidates = ["".join(ch for ch in c if ch.isalnum()) for c in candidates]
    candidates = [c for c in candidates if c]
    if not candidates:
        return ""

    return max(candidates, key=len)


def solve_captcha(
    path_or_bytes: "Path | str | bytes | bytearray",
    whitelist: Optional[str] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
) -> str:
    """Recognise captcha text from an image path or bytes."""

    img = _load_image(path_or_bytes)
    attempts = list(_collect_attempts(img, whitelist))
    return _select_candidate(attempts)


if __name__ == "__main__":
    import numpy as np
    print("✅ NumPy работает:", np.arange(5))
    import pytesseract
    print("✅ Pytesseract работает, версия:", pytesseract.get_tesseract_version())
