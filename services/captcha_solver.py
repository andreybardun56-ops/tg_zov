# captcha_solver.py
import io
import math
from typing import Optional
from PIL import Image, ImageFilter, ImageOps, ImageChops
import numpy as np
import pytesseract

# optional: easyocr fallback (install if you want)
try:
    import easyocr
except Exception:
    easyocr = None

# If tesseract not in PATH on Windows, укажи путь:
pytesseract.pytesseract.tesseract_cmd = r"C:\скаченное\tesseract.exe"

# ---------- Image preprocessing helpers ----------

def pil_to_gray(img: Image.Image) -> Image.Image:
    return img.convert("L")

def resize_for_ocr(img: Image.Image, scale: int = 3) -> Image.Image:
    w, h = img.size
    return img.resize((w * scale, h * scale), Image.LANCZOS)

def contrast_stretch(img: Image.Image) -> Image.Image:
    return ImageOps.autocontrast(img, cutoff=0)

def adaptive_binarize(img: Image.Image, block_size: int = 15, offset: int = 10) -> Image.Image:
    # simple adaptive threshold implementation using numpy
    arr = np.array(img, dtype=np.uint8)
    pad = block_size // 2
    # integral image for fast mean filter
    integral = arr.cumsum(axis=0).cumsum(axis=1)
    H, W = arr.shape
    out = np.zeros_like(arr)
    for y in range(H):
        y1 = max(0, y - pad)
        y2 = min(H - 1, y + pad)
        for x in range(W):
            x1 = max(0, x - pad)
            x2 = min(W - 1, x + pad)
            area = (y2 - y1 + 1) * (x2 - x1 + 1)
            s = integral[y2, x2]
            if x1 > 0: s -= integral[y2, x1 - 1]
            if y1 > 0: s -= integral[y1 - 1, x2]
            if x1 > 0 and y1 > 0: s += integral[y1 - 1, x1 - 1]
            mean = s // area
            out[y, x] = 255 if arr[y, x] > (mean - offset) else 0
    return Image.fromarray(out)

def remove_background_tophat(img: Image.Image, kernel_size: int = 15) -> Image.Image:
    # morphological top-hat: img - opening(img)
    from PIL import ImageFilter
    opened = img.filter(ImageFilter.MinFilter(kernel_size)).filter(ImageFilter.MaxFilter(kernel_size))
    # top-hat:
    th = ImageChops.subtract(img, opened)
    return th

def denoise(img: Image.Image) -> Image.Image:
    return img.filter(ImageFilter.MedianFilter(size=3))

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

def ocr_easyocr(img: Image.Image) -> str:
    if easyocr is None:
        return ""
    reader = easyocr.Reader(['en'], gpu=False)  # set gpu=True if available
    arr = np.array(img)
    try:
        res = reader.readtext(arr, detail=0)
        return "".join(res).strip()
    except Exception:
        return ""

def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    # pipeline tuned for small, noisy captchas
    img = pil_to_gray(img)
    img = contrast_stretch(img)
    img = remove_background_tophat(img, kernel_size=15)
    img = denoise(img)
    img = adaptive_binarize(img, block_size=15, offset=12)
    img = resize_for_ocr(img, scale=3)
    img = img.filter(ImageFilter.MedianFilter(size=3))
    return img

def solve_captcha(path_or_bytes, whitelist: Optional[str] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") -> str:
    """
    Main entry.
    path_or_bytes: path to image file or bytes-like object.
    returns: recognized string (possibly empty)
    """
    if isinstance(path_or_bytes, (bytes, bytearray)):
        img = Image.open(io.BytesIO(path_or_bytes))
    else:
        img = Image.open(path_or_bytes)

    # try a few preprocess variants and heuristics
    attempts = []

    # raw -> tesseract
    attempts.append(("tess_raw", ocr_tesseract(img, whitelist)))

    # preprocessed -> tesseract
    pre = preprocess_for_ocr(img)
    attempts.append(("tess_prep", ocr_tesseract(pre, whitelist)))

    # deskew + preprocessed -> tesseract
    desk = deskew(pre)
    attempts.append(("tess_prep_deskew", ocr_tesseract(desk, whitelist)))

    # easyocr fallback
    if easyocr is not None:
        attempts.append(("easy_prep", ocr_easyocr(pre)))
        attempts.append(("easy_raw", ocr_easyocr(img)))

    # pick the longest non-empty result (heuristic)
    candidates = [txt for _, txt in attempts if txt and txt.strip()]
    if not candidates:
        return ""
    # prefer alphanumeric only and strip spaces
    candidates = [ "".join(ch for ch in c if ch.isalnum()) for c in candidates ]
    candidates = [c for c in candidates if c]
    if not candidates:
        return ""
    # choose candidate with max length (likely full code)
    chosen = max(candidates, key=len)
    return chosen
if __name__ == "__main__":
    import numpy as np
    print("✅ NumPy работает:", np.arange(5))
    import pytesseract
    print("✅ Pytesseract работает, версия:", pytesseract.get_tesseract_version())
