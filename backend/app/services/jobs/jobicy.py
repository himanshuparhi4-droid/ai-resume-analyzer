from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.fast_profile import build_fast_requirement_profile, fast_title_alignment_score
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
        if settings.environment == "production":
            request_count = min(max(limit + 4, 12), 24)
            extraction_limit = 850
            enrichment_budget = request_count
        else:
            request_count = min(max(limit * 2, settings.production_live_candidate_fetch), 50)
            extraction_limit = 4000
            enrichment_budget = max(limit * 2, 20)
        params = {"count": request_count}
        role_tag = (query or normalized_role).strip()
        if role_tag:
            params["tag"] = role_tag

        timeout = httpx.Timeout(
            connect=min(6.5, settings.job_request_timeout_seconds),
            read=settings.job_request_timeout_seconds,
            write=10.0,
            pool=5.0,
        )
        async with httpx.AsyncClient(timeout=timeout, headers=self.request_headers) as client:
            response = await client.get(settings.jobicy_base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        seed_jobs: list[dict] = []
        for item in payload.get("jobs", []) or []:
            title = item.get("jobTitle", "Unknown Role")
            raw_description = strip_html(item.get("jobDescription", "") or item.get("jobExcerpt", "") or "")
            industry_tags = [str(tag).replace("&amp;", "&").strip() for tag in (item.get("jobIndustry") or []) if str(tag).strip()]
            type_tags = [str(tag).strip() for tag in (item.get("jobType") or []) if str(tag).strip()]
            level = str(item.get("jobLevel", "")).strip()
            tags = [tag for tag in [*industry_tags, *type_tags, level] if tag]
            description = truncate(raw_description, 4000)
            seed_jobs.append(
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
                    },
                    "posted_at": self._parse_datetime(item.get("pubDate")),
                }
            )

        if settings.environment == "production":
            ranked_seed = sorted(
                seed_jobs,
                key=lambda item: (
                    fast_title_alignment_score(
                        query,
                        str(item.get("title", "")),
                        tags=item.get("tags") or [],
                    ),
                    self._location_score(location, item.get("location", "")),
                ),
                reverse=True,
            )[:enrichment_budget]
        else:
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
            ranked_seed_pool = positively_aligned if len(positively_aligned) >= max(3, min(limit, 5)) else seed_jobs
            ranked_seed = sorted(
                ranked_seed_pool,
                key=lambda item: (
                    role_title_alignment_score(
                        query,
                        str(item.get("title", "")),
                        description=str(item.get("description", "")),
                        tags=item.get("tags") or [],
                    ),
                    self._location_score(location, item.get("location", "")),
                ),
                reverse=True,
            )[:enrichment_budget]

        jobs: list[dict] = []
        for item in ranked_seed:
            if settings.environment == "production":
                requirement_profile = build_fast_requirement_profile(
                    query=query,
                    title=str(item.get("title", "")),
                    description=truncate(str(item.get("description", "")), 240),
                    tags=item.get("tags") or [],
                    source=self.source_name,
                )
            else:
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

        if settings.environment == "production":
            ranked = sorted(
                jobs,
                key=lambda item: (
                    float((item.get("normalized_data") or {}).get("title_alignment_score") or 0.0),
                    float((item.get("normalized_data") or {}).get("role_fit_score") or 0.0),
                    self._location_score(location, item.get("location", "")),
                ),
                reverse=True,
            )
        else:
            ranked = sorted(
                jobs,
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
