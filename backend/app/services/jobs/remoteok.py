from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.fast_profile import build_fast_requirement_profile
from app.services.jobs.taxonomy import role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class RemoteOKProvider:
    source_name = "remoteok"
    supports_query_variations = False
    supports_location_variations = False
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://remoteok.com/",
    }

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=self.request_headers) as client:
            response = await client.get(settings.remoteok_base_url)
            response.raise_for_status()
            payload = response.json()

        jobs: list[dict] = []
        for item in payload:
            if not isinstance(item, dict) or not item.get("position"):
                continue

            title = item.get("position", "Unknown Role")
            tags = [str(tag).strip() for tag in (item.get("tags") or []) if str(tag).strip()]
            description = strip_html(item.get("description", "") or "")
            if settings.environment == "production":
                requirement_profile = build_fast_requirement_profile(
                    query=query,
                    title=title,
                    description=description[:500],
                    tags=tags,
                    source=self.source_name,
                )
            else:
                requirement_profile = extract_job_requirement_profile(
                    title=title,
                    description=description,
                    tags=tags,
                )
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("slug") or item.get("url") or title),
                    "title": title,
                    "company": item.get("company", "Unknown Company"),
                    "location": item.get("location") or location or "Remote",
                    "remote": True,
                    "url": item.get("url") or "https://remoteok.com",
                    "description": description,
                    "tags": tags,
                    "normalized_data": {
                        "salary_min": item.get("salary_min"),
                        "salary_max": item.get("salary_max"),
                        "benefits": item.get("benefits") or [],
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(item.get("date") or item.get("epoch")),
                }
            )

        ranked = sorted(
            jobs,
            key=lambda item: (
                role_fit_score(query, item),
                self._tag_overlap(query, item.get("tags", [])),
                self._location_score(location, item.get("location", "")),
            ),
            reverse=True,
        )
        return ranked[: max(limit * 4, 36)]

    def _parse_datetime(self, value: str | int | float | None) -> datetime | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value)
            except (OverflowError, OSError, ValueError):
                return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _location_score(self, requested_location: str, job_location: str) -> float:
        requested = (requested_location or "").strip().lower()
        job = (job_location or "").strip().lower()
        if not requested:
            return 0.0
        if requested in {"remote", "worldwide", "global"}:
            return 1.0 if not job or "remote" in job or "worldwide" in job else 0.25
        if requested in job:
            return 1.0
        if "remote" in job or "worldwide" in job:
            return 0.5
        return 0.0

    def _tag_overlap(self, query: str, tags: list[str]) -> float:
        lowered = " ".join(str(tag).lower() for tag in tags)
        if not lowered:
            return 0.0
        score = 0.0
        for token in query.lower().split():
            if len(token) > 2 and token in lowered:
                score += 0.5
        return score
