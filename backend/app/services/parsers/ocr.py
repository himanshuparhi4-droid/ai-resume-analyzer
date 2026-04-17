from __future__ import annotations

import io

from PIL import Image

from app.core.config import settings


class OCRService:
    def __init__(self) -> None:
        self._enabled = settings.enable_ocr
        self._pytesseract = None
        self._convert_from_bytes = None
        self._poppler_path = settings.poppler_path
        if not self._enabled:
            return
        try:
            import pytesseract  # type: ignore
            from pdf2image import convert_from_bytes  # type: ignore

            self._pytesseract = pytesseract
            self._convert_from_bytes = convert_from_bytes
            if settings.tesseract_cmd:
                self._pytesseract.pytesseract.tesseract_cmd = settings.tesseract_cmd
        except Exception:
            self._enabled = False

    def extract_text_from_pdf(self, file_bytes: bytes) -> str:
        if not self._enabled or self._pytesseract is None or self._convert_from_bytes is None:
            return ""
        try:
            convert_kwargs = {"poppler_path": self._poppler_path} if self._poppler_path else {}
            images = self._convert_from_bytes(file_bytes, **convert_kwargs)
            return "\n".join(self._pytesseract.image_to_string(image) for image in images)
        except Exception:
            return ""

    def extract_text_from_image(self, file_bytes: bytes) -> str:
        if not self._enabled or self._pytesseract is None:
            return ""
        try:
            return self._pytesseract.image_to_string(Image.open(io.BytesIO(file_bytes)))
        except Exception:
            return ""
