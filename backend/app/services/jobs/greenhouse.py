from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

_GREENHOUSE_BOARD_CACHE: dict[str, dict] = {}


class GreenhouseProvider:
    source_name = "greenhouse"
    supports_query_variations = False
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_greenhouse_boards:
            return []

        jobs: list[dict] = []
        seen_links: set[str] = set()
        for board in settings.greenhouse_board_tokens:
            for item in await self._fetch_board_jobs(board):
                link = str(item.get("url") or item.get("external_id") or "").strip()
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                jobs.append(item)

        target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
        positively_aligned = [
            item
            for item in jobs
            if role_title_alignment_score(
                query,
                str(item.get("title", "")),
                description=str(item.get("description", "")),
                tags=item.get("tags") or [],
            )
            > 0
        ]
        ranked_pool = positively_aligned if len(positively_aligned) >= max(limit * 2, 12) else jobs
        ranked = sorted(
            ranked_pool,
            key=lambda item: (
                role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description=str(item.get("description", "")),
                    tags=item.get("tags") or [],
                ),
                role_fit_score(query, item),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    async def _fetch_board_jobs(self, board: str) -> list[dict]:
        cached = _GREENHOUSE_BOARD_CACHE.get(board)
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        if cached and now - cached["stored_at"] < ttl:
            return cached["jobs"]

        endpoint = f"{settings.greenhouse_base_url}/{board}/jobs"
        params = {"content": "true"}
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            payload = response.json()
        raw_jobs = payload.get("jobs") or []

        jobs: list[dict] = []
        for item in raw_jobs:
            description = strip_html(str(item.get("content") or ""))
            title = str(item.get("title") or "Unknown Role")
            location = str((item.get("location") or {}).get("name") or "Unknown")
            metadata = item.get("metadata") or []
            tags = [str(entry.get("name") or "").strip() for entry in metadata if entry.get("name")]
            if board:
                tags.append(board)
            requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("absolute_url") or ""),
                    "title": title,
                    "company": self._company_from_board(board),
                    "location": location,
                    "remote": "remote" in location.lower() or "remote" in description.lower(),
                    "url": str(item.get("absolute_url") or ""),
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": [tag for tag in tags if tag],
                    "normalized_data": requirement_profile,
                    "posted_at": self._parse_datetime(item.get("updated_at") or item.get("updatedAt")),
                }
            )

        _GREENHOUSE_BOARD_CACHE[board] = {"stored_at": now, "jobs": jobs}
        return jobs

    def _company_from_board(self, board: str) -> str:
        cleaned = board.replace("-", " ").replace("_", " ").strip()
        return cleaned.title() if cleaned else "Greenhouse Company"

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
