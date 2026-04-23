from __future__ import annotations

import os
from functools import lru_cache
from math import sqrt

from app.core.config import settings


@lru_cache(maxsize=1)
def _load_sentence_transformer():
    from sentence_transformers import SentenceTransformer  # type: ignore

    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    return SentenceTransformer(settings.embedding_model_name, local_files_only=True)


class EmbeddingService:
    def __init__(self) -> None:
        self._enabled = settings.enable_embeddings and (
            settings.environment != "production" or settings.enable_production_embeddings
        )

    def _get_model(self):
        if not self._enabled:
            return None
        try:
            return _load_sentence_transformer()
        except Exception:
            return None

    def similarity(self, left: str, right: str) -> float:
        model = self._get_model()
        if model is not None:
            try:
                embeddings = model.encode([left, right], show_progress_bar=False)
                return round(self._cosine(embeddings[0], embeddings[1]) * 100, 2)
            except Exception:
                pass
        return round(self._token_overlap(left, right) * 100, 2)

    def similarities_to_many(self, left: str, rights: list[str]) -> list[float]:
        if not rights:
            return []

        model = self._get_model()
        if model is not None:
            try:
                embeddings = model.encode([left, *rights], show_progress_bar=False)
                left_embedding = embeddings[0]
                return [round(self._cosine(left_embedding, embedding) * 100, 2) for embedding in embeddings[1:]]
            except Exception:
                pass

        return [round(self._token_overlap(left, right) * 100, 2) for right in rights]

    def _cosine(self, left, right) -> float:
        numerator = sum(a * b for a, b in zip(left, right))
        left_norm = sqrt(sum(a * a for a in left))
        right_norm = sqrt(sum(b * b for b in right))
        if not left_norm or not right_norm:
            return 0.0
        return numerator / (left_norm * right_norm)

    def _token_overlap(self, left: str, right: str) -> float:
        left_tokens = {token.lower() for token in left.split() if len(token) > 2}
        right_tokens = {token.lower() for token in right.split() if len(token) > 2}
        if not left_tokens or not right_tokens:
            return 0.0
        overlap = len(left_tokens & right_tokens)
        return overlap / len(right_tokens)
