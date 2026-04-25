from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.fast_profile import build_fast_requirement_profile
from app.services.jobs.taxonomy import role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html


class USAJobsProvider:
    source_name = "usajobs"

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_usajobs_credentials:
            return []
        headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": settings.usajobs_user_agent,
            "Authorization-Key": settings.usajobs_api_key,
        }
        params = {"Keyword": query, "LocationName": location, "ResultsPerPage": limit}
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds) as client:
            response = await client.get(settings.usajobs_base_url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
        items = payload.get("SearchResult", {}).get("SearchResultItems", [])
        jobs: list[dict] = []
        for item in items[:limit]:
            descriptor = item.get("MatchedObjectDescriptor", {})
            description = strip_html(" ".join(descriptor.get("UserArea", {}).get("Details", {}).get("MajorDuties", [])))
            title = descriptor.get("PositionTitle", "Unknown Role")
            tags = [item.get("PositionTitle", ""), item.get("OrganizationName", "")]
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
                    "external_id": descriptor.get("PositionID"),
                    "title": title,
                    "company": descriptor.get("OrganizationName", "USAJOBS"),
                    "location": ", ".join(loc.get("LocationName", "") for loc in descriptor.get("PositionLocation", [])) or location,
                    "remote": False,
                    "url": descriptor.get("PositionURI", settings.usajobs_base_url),
                    "description": description,
                    "tags": [descriptor.get("JobCategory", [{}])[0].get("Name", query)],
                    "normalized_data": {**requirement_profile},
                    "posted_at": self._parse_datetime(descriptor.get("PublicationStartDate")),
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
        ranked_pool = positively_aligned if len(positively_aligned) >= max(limit, 8) else jobs
        return sorted(
            ranked_pool,
            key=lambda item: (
                role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description=str(item.get("description", "")),
                    tags=item.get("tags") or [],
                ),
                role_fit_score(query, item),
            ),
            reverse=True,
        )[:limit]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
