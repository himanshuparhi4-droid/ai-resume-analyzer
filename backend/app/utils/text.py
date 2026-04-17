import re
from html import unescape

TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")


def strip_html(value: str) -> str:
    cleaned = TAG_RE.sub(" ", value or "")
    cleaned = unescape(cleaned)
    return SPACE_RE.sub(" ", cleaned).strip()


def normalize_whitespace(value: str) -> str:
    return SPACE_RE.sub(" ", value or "").strip()


def truncate(value: str, limit: int = 280) -> str:
    text = normalize_whitespace(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
