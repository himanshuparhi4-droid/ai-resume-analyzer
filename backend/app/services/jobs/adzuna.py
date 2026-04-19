from __future__ import annotations

from datetime import datetime
import re

import httpx

from app.core.config import settings
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class AdzunaProvider:
    source_name = "adzuna"
    supports_query_variations = True
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_adzuna_credentials:
            return []

        params = {
            "app_id": settings.adzuna_app_id,
            "app_key": settings.adzuna_app_key,
            "results_per_page": min(max(limit * 2, 20), 50),
            "what": query,
            "where": location,
            "content-type": "application/json",
        }
        endpoint = f"{settings.adzuna_base_url}/{settings.adzuna_country}/search/1"
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            payload = response.json()

        jobs = []
        for item in payload.get("results", [])[:limit]:
            description = strip_html(item.get("description", ""))
            title = item.get("title", "Unknown Role")
            category = item.get("category") or {}
            category_label = category.get("label", "") if isinstance(category, dict) else str(category)
            tags = [segment.strip() for segment in re.split(r"[/>]", category_label) if segment and segment.strip()]
            requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id")),
                    "title": title,
                    "company": item.get("company", {}).get("display_name", "Unknown Company"),
                    "location": item.get("location", {}).get("display_name", location),
                    "remote": "remote" in description.lower() or "remote" in title.lower(),
                    "url": item.get("redirect_url", "https://www.adzuna.com"),
                    "description": description,
                    "tags": tags or [category_label or query],
                    "normalized_data": {
                        "salary_min": item.get("salary_min"),
                        "salary_max": item.get("salary_max"),
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(item.get("created")),
                }
            )
        return jobs

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
