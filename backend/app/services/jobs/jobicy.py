from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

ROLE_TAG_MAP = {
    "data analyst": "data",
    "data scientist": "data",
    "data engineer": "data",
    "machine learning engineer": "data",
    "software engineer": "software",
    "frontend developer": "software",
    "full stack developer": "software",
    "devops engineer": "software",
    "qa engineer": "software",
    "product manager": "product",
    "ui/ux designer": "design",
    "graphic designer": "design",
    "teacher": "teacher",
    "painter": "painter",
}


class JobicyProvider:
    source_name = "jobicy"
    supports_query_variations = False
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
        params = {"count": min(max(limit * 4, 40), 80)}
        role_tag = ROLE_TAG_MAP.get(normalized_role)
        if role_tag:
            params["tag"] = role_tag

        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=self.request_headers) as client:
            response = await client.get(settings.jobicy_base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        jobs: list[dict] = []
        for item in payload.get("jobs", []) or []:
            title = item.get("jobTitle", "Unknown Role")
            description = strip_html(item.get("jobDescription", "") or item.get("jobExcerpt", "") or "")
            industry_tags = [str(tag).replace("&amp;", "&").strip() for tag in (item.get("jobIndustry") or []) if str(tag).strip()]
            type_tags = [str(tag).strip() for tag in (item.get("jobType") or []) if str(tag).strip()]
            level = str(item.get("jobLevel", "")).strip()
            tags = [tag for tag in [*industry_tags, *type_tags, level] if tag]
            requirement_profile = extract_job_requirement_profile(
                title=title,
                description=description,
                tags=tags,
            )
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

        ranked = sorted(
            jobs,
            key=lambda item: (
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
