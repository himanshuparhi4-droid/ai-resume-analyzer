from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate


class RemotiveProvider:
    source_name = "remotive"
    supports_query_variations = True
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if settings.environment == "production":
            request_limit = min(max(limit + 6, 12), 18)
            extraction_limit = 850
            enrichment_budget = min(max(limit // 2, 3), 4)
        else:
            request_limit = min(max(limit * 3, 18), 36)
            extraction_limit = 4000
            enrichment_budget = request_limit
        params = {"search": query, "limit": request_limit}
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(settings.remotive_base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        seed_jobs = []
        for item in payload.get("jobs", [])[:request_limit]:
            raw_description = strip_html(item.get("description", ""))
            title = item.get("title", "Unknown Role")
            tags = [str(tag).strip() for tag in (item.get("tags") or []) if str(tag).strip()]
            enriched_tags = list(dict.fromkeys(tags + [item.get("category", "General"), item.get("job_type", "Unknown")]))
            description = truncate(raw_description, 4000)
            seed_jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id")),
                    "title": title,
                    "company": item.get("company_name", "Unknown Company"),
                    "location": item.get("candidate_required_location") or location,
                    "remote": True,
                    "url": item.get("url", "https://remotive.com"),
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": enriched_tags,
                    "normalized_data": {
                        "category": item.get("category"),
                        "salary": item.get("salary"),
                    },
                    "posted_at": self._parse_datetime(item.get("publication_date")),
                }
            )
        positively_aligned = [
            item
            for item in seed_jobs
            if role_title_alignment_score(
                query,
                str(item.get("title", "")),
                description=str(item.get("description", "")),
                tags=item.get("tags") or [],
            )
            > 0
        ]
        ranked_seed_pool = positively_aligned if len(positively_aligned) >= max(4, min(limit, 6)) else seed_jobs
        ranked_seed = sorted(
            ranked_seed_pool,
            key=lambda item: (
                role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description=str(item.get("description", "")),
                    tags=item.get("tags") or [],
                ),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )[:enrichment_budget]

        jobs = []
        for item in ranked_seed:
            extraction_description = truncate(str(item.get("description", "")), extraction_limit)
            requirement_profile = extract_job_requirement_profile(
                title=str(item.get("title", "")),
                description=extraction_description,
                tags=item.get("tags") or [],
            )
            item["normalized_data"] = {
                **(item.get("normalized_data") or {}),
                **requirement_profile,
            }
            jobs.append(item)

        return sorted(
            jobs,
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
        )[:request_limit]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
