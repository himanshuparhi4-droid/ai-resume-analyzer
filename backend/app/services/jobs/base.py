from __future__ import annotations

from typing import Protocol


class JobProvider(Protocol):
    source_name: str
    supports_query_variations: bool
    supports_location_variations: bool

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        ...
