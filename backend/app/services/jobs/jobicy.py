from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

class JobicyProvider:
    source_name = "jobicy"
    supports_query_variations = True
    supports_location_variations = False
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        normalized_role = normalize_role(query)
        request_count = min(max(limit * 2, 16), 24) if settings.environment == "production" else min(max(limit * 2, settings.production_live_candidate_fetch), 50)
        params = {"count": request_count}
        role_tag = (query or normalized_role).strip()
        if role_tag:
            params["tag"] = role_tag

        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=self.request_headers) as client:
            response = await client.get(settings.jobicy_base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        jobs: list[dict] = []
        for item in payload.get("jobs", []) or []:
            title = item.get("jobTitle", "Unknown Role")
            raw_description = strip_html(item.get("jobDescription", "") or item.get("jobExcerpt", "") or "")
            industry_tags = [str(tag).replace("&amp;", "&").strip() for tag in (item.get("jobIndustry") or []) if str(tag).strip()]
            type_tags = [str(tag).strip() for tag in (item.get("jobType") or []) if str(tag).strip()]
            level = str(item.get("jobLevel", "")).strip()
            tags = [tag for tag in [*industry_tags, *type_tags, level] if tag]
            requirement_profile = extract_job_requirement_profile(
                title=title,
                description=raw_description,
                tags=tags,
            )
            description = truncate(raw_description, 4000)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("jobSlug") or item.get("url") or title),
                    "title": title,
                    "company": item.get("companyName", "Unknown Company"),
                    "location": item.get("jobGeo") or location or "Remote",
                    "remote": True,
                    "url": item.get("url") or "https://jobicy.com",
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": tags,
                    "normalized_data": {
                        "industry": industry_tags,
                        "job_type": type_tags,
                        "job_level": level,
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(item.get("pubDate")),
                }
            )

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
                self._location_score(location, item.get("location", "")),
            ),
            reverse=True,
        )
        return ranked[: max(limit * 2, 20)]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
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
        if job in {"remote", "worldwide", "global"}:
            return 0.5
        return 0.0
