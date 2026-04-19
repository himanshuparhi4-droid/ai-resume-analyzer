from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

_LEVER_BOARD_CACHE: dict[str, dict] = {}


class LeverProvider:
    source_name = "lever"
    supports_query_variations = False
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_lever_companies:
            return []

        jobs: list[dict] = []
        seen_links: set[str] = set()
        for company in settings.lever_company_tokens:
            for item in await self._fetch_company_jobs(company):
                link = str(item.get("url") or item.get("external_id") or "").strip()
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                jobs.append(item)

        target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
        ranked = sorted(
            jobs,
            key=lambda item: (
                role_fit_score(query, item),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    async def _fetch_company_jobs(self, company: str) -> list[dict]:
        cached = _LEVER_BOARD_CACHE.get(company)
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        if cached and now - cached["stored_at"] < ttl:
            return cached["jobs"]

        endpoint = f"{settings.lever_base_url}/{company}"
        params = {"mode": "json"}
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            raw_jobs = response.json()

        jobs: list[dict] = []
        for item in raw_jobs:
            description = strip_html(
                str(item.get("descriptionPlain") or item.get("description") or "")
            )
            title = str(item.get("text") or "Unknown Role")
            categories = item.get("categories") or {}
            location = str(categories.get("location") or "Unknown")
            team = str(categories.get("team") or "").strip()
            commitment = str(categories.get("commitment") or "").strip()
            tags = [tag for tag in [team, commitment, company] if tag]
            requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("hostedUrl") or ""),
                    "title": title,
                    "company": self._company_from_token(company),
                    "location": location,
                    "remote": "remote" in location.lower() or "remote" in description.lower(),
                    "url": str(item.get("hostedUrl") or ""),
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": tags,
                    "normalized_data": requirement_profile,
                    "posted_at": self._parse_datetime(item.get("createdAt") or item.get("updatedAt")),
                }
            )

        _LEVER_BOARD_CACHE[company] = {"stored_at": now, "jobs": jobs}
        return jobs

    def _company_from_token(self, token: str) -> str:
        cleaned = token.replace("-", " ").replace("_", " ").strip()
        return cleaned.title() if cleaned else "Lever Company"

    def _parse_datetime(self, value) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            try:
                return datetime.fromtimestamp(timestamp, tz=UTC)
            except (OverflowError, OSError, ValueError):
                return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
