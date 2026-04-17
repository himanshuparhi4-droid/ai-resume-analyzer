from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class RemotiveProvider:
    source_name = "remotive"
    supports_query_variations = True
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        params = {"search": query, "limit": limit}
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(settings.remotive_base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        jobs = []
        for item in payload.get("jobs", [])[:limit]:
            description = strip_html(item.get("description", ""))
            title = item.get("title", "Unknown Role")
            tags = [str(tag).strip() for tag in (item.get("tags") or []) if str(tag).strip()]
            enriched_tags = list(dict.fromkeys(tags + [item.get("category", "General"), item.get("job_type", "Unknown")]))
            requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=enriched_tags)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id")),
                    "title": title,
                    "company": item.get("company_name", "Unknown Company"),
                    "location": item.get("candidate_required_location") or location,
                    "remote": True,
                    "url": item.get("url", "https://remotive.com"),
                    "description": description,
                    "tags": enriched_tags,
                    "normalized_data": {
                        "category": item.get("category"),
                        "salary": item.get("salary"),
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(item.get("publication_date")),
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
