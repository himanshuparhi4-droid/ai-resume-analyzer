from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class ArbeitnowProvider:
    source_name = "arbeitnow"
    supports_query_variations = False
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        collected: list[dict] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=20.0) as client:
            for page in range(1, 4):
                try:
                    response = await client.get(settings.arbeitnow_base_url, params={"page": page})
                    response.raise_for_status()
                except httpx.HTTPError:
                    if collected:
                        break
                    raise
                payload = response.json()
                items = payload.get("data") or payload.get("jobs") or []
                if not items:
                    break
                for item in items:
                    external_id = str(item.get("slug") or item.get("id") or item.get("url") or "")
                    if not external_id or external_id in seen_ids:
                        continue
                    seen_ids.add(external_id)
                    description = strip_html(item.get("description", "") or "")
                    title = item.get("title", "Unknown Role")
                    requirement_profile = extract_job_requirement_profile(title=title, description=description)
                    collected.append(
                        {
                            "source": self.source_name,
                            "external_id": external_id,
                            "title": title,
                            "company": item.get("company_name") or item.get("company") or "Unknown Company",
                            "location": item.get("location") or location or "Remote",
                            "remote": bool(item.get("remote")) or "remote" in str(item.get("location", "")).lower(),
                            "url": item.get("url") or "https://www.arbeitnow.com",
                            "description": description,
                            "tags": item.get("tags") or [query],
                            "normalized_data": {
                                "job_types": item.get("job_types") or [],
                                "visa_sponsorship": item.get("visa_sponsorship"),
                                **requirement_profile,
                            },
                            "posted_at": self._parse_datetime(item.get("created_at") or item.get("published_at")),
                        }
                    )
                    if len(collected) >= limit * 8:
                        break

        ranked = sorted(
            collected,
            key=lambda item: (
                role_fit_score(query, item),
                self._location_score(location, item.get("location", "")),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[: limit * 4]

    def _parse_datetime(self, value: str | int | float | None) -> datetime | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value)
            except (OverflowError, OSError, ValueError):
                return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError):
            return None

    def _location_score(self, requested_location: str, job_location: str) -> float:
        requested = (requested_location or "").strip().lower()
        job = (job_location or "").strip().lower()
        if not requested:
            return 0.0
        if requested in {"remote", "worldwide", "global"}:
            return 1.0 if "remote" in job else 0.25
        if requested in job:
            return 1.0
        if "remote" in job:
            return 0.5
        return 0.0
