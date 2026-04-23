import re
from html import unescape

TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")


def strip_html(value: str) -> str:
    cleaned = unescape(value or "")
    cleaned = re.sub(r"<script[\s\S]*?</script>", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<style[\s\S]*?</style>", " ", cleaned, flags=re.IGNORECASE)
    cleaned = TAG_RE.sub(" ", cleaned)
    return SPACE_RE.sub(" ", cleaned).strip()


def normalize_whitespace(value: str) -> str:
    return SPACE_RE.sub(" ", value or "").strip()


def truncate(value: str, limit: int = 280) -> str:
    text = normalize_whitespace(value)
    if len(text) <= limit:
        return text
    clipped = text[: limit - 3]
    sentence_boundary = max(clipped.rfind(". "), clipped.rfind("! "), clipped.rfind("? "))
    if sentence_boundary >= max(32, int(limit * 0.35)):
        clipped = clipped[: sentence_boundary + 1]
    else:
        space_boundary = clipped.rfind(" ")
        if space_boundary >= max(24, int(limit * 0.3)):
            clipped = clipped[:space_boundary]
    return clipped.rstrip(" ,;:-") + "..."
