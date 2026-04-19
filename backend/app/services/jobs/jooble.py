from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate


class JoobleProvider:
    source_name = "jooble"
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
        if not settings.has_jooble_credentials:
            return []

        target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
        page_size = min(max(limit * 4, 30), 100)
        page_count = min(5, max(1, (target_candidates + page_size - 1) // page_size))
        endpoint = f"{settings.jooble_base_url}/{settings.jooble_api_key}"

        jobs: list[dict] = []
        seen_links: set[str] = set()
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=self.request_headers) as client:
            for page in range(1, page_count + 1):
                payload = {
                    "keywords": query,
                    "location": location,
                    "page": str(page),
                    "companysearch": "false",
                }
                response = await client.post(endpoint, json=payload)
                response.raise_for_status()
                data = response.json()
                results = data.get("jobs") or []
                if not results:
                    break

                for item in results:
                    link = str(item.get("link") or "").strip()
                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)
                    description = strip_html(str(item.get("snippet") or ""))
                    title = str(item.get("title") or "Unknown Role")
                    type_label = str(item.get("type") or "").strip()
                    tags = [tag for tag in [type_label, str(item.get("source") or "").strip()] if tag]
                    requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
                    jobs.append(
                        {
                            "source": self.source_name,
                            "external_id": link,
                            "title": title,
                            "company": str(item.get("company") or "Unknown Company"),
                            "location": str(item.get("location") or location or "Remote"),
                            "remote": "remote" in description.lower() or "remote" in title.lower(),
                            "url": link,
                            "description": description,
                            "preview": truncate(description, 260),
                            "tags": tags,
                            "normalized_data": {
                                "salary": item.get("salary"),
                                **requirement_profile,
                            },
                            "posted_at": self._parse_datetime(item.get("updated")),
                        }
                    )
                    if len(jobs) >= target_candidates:
                        break
                if len(jobs) >= target_candidates:
                    break

        ranked = sorted(
            jobs,
            key=lambda item: (
                role_fit_score(query, item),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
