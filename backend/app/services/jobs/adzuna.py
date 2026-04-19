from __future__ import annotations

from datetime import datetime
import re
from math import ceil

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class AdzunaProvider:
    source_name = "adzuna"
    supports_query_variations = True
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_adzuna_credentials:
            return []

        normalized_location = normalize_role(location)
        location_filter = ""
        if normalized_location not in {"", "india", "remote", "worldwide", "global"}:
            location_filter = location

        results_per_page = min(max(limit * 3, settings.production_live_candidate_fetch), 50)
        target_candidates = max(limit * 4, settings.production_live_candidate_fetch)
        page_count = min(3, max(1, ceil(target_candidates / results_per_page)))

        jobs = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            for page in range(1, page_count + 1):
                params = {
                    "app_id": settings.adzuna_app_id,
                    "app_key": settings.adzuna_app_key,
                    "results_per_page": results_per_page,
                    "what": query,
                    "content-type": "application/json",
                }
                if location_filter:
                    params["where"] = location_filter
                endpoint = f"{settings.adzuna_base_url}/{settings.adzuna_country}/search/{page}"
                response = await client.get(endpoint, params=params)
                response.raise_for_status()
                payload = response.json()
                results = payload.get("results", []) or []
                if not results:
                    break

                for item in results:
                    external_id = str(item.get("id") or item.get("redirect_url") or item.get("title") or "")
                    if not external_id or external_id in seen_ids:
                        continue
                    seen_ids.add(external_id)
                    description = strip_html(item.get("description", ""))
                    title = item.get("title", "Unknown Role")
                    category = item.get("category") or {}
                    category_label = category.get("label", "") if isinstance(category, dict) else str(category)
                    tags = [segment.strip() for segment in re.split(r"[/>]", category_label) if segment and segment.strip()]
                    requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
                    jobs.append(
                        {
                            "source": self.source_name,
                            "external_id": external_id,
                            "title": title,
                            "company": item.get("company", {}).get("display_name", "Unknown Company"),
                            "location": item.get("location", {}).get("display_name", location or "India"),
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
                    if len(jobs) >= target_candidates:
                        break
                if len(jobs) >= target_candidates:
                    break
        return jobs[:target_candidates]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
