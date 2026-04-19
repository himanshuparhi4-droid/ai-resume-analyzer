from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import role_fit_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate


class IndianAPIProvider:
    source_name = "indianapi"
    supports_query_variations = True
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        if not settings.has_indianapi_credentials:
            return []

        target_candidates = max(limit * 5, settings.production_live_candidate_fetch)
        page_size = min(max(limit * 4, 24), 100)
        jobs: list[dict] = []
        seen_links: set[str] = set()

        headers = {
            "X-Api-Key": str(settings.indianapi_api_key),
            "Accept": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
        }

        params = {
            "title": query,
            "limit": page_size,
        }
        normalized_location = (location or "").strip().lower()
        if normalized_location not in {"", "remote", "global", "worldwide"}:
            params["location"] = location

        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=headers) as client:
            response = await client.get(settings.indianapi_jobs_base_url, params=params)
            response.raise_for_status()
            payload = response.json()
            results = payload.get("jobs") or payload.get("data") or payload.get("results") or []

        for item in results:
            link = str(item.get("apply_link") or item.get("url") or item.get("link") or "").strip()
            if not link:
                title = str(item.get("title") or item.get("job_title") or "").strip()
                company = str(item.get("company_name") or item.get("company") or "").strip()
                link = f"{self.source_name}:{title}:{company}"
            if link in seen_links:
                continue
            seen_links.add(link)

            title = str(item.get("title") or item.get("job_title") or "Unknown Role")
            company = str(item.get("company_name") or item.get("company") or "Unknown Company")
            location_label = str(item.get("location") or item.get("job_location") or location or "India")
            description = strip_html(
                str(
                    item.get("description")
                    or item.get("job_description")
                    or item.get("summary")
                    or item.get("snippet")
                    or ""
                )
            )
            tags = [
                tag
                for tag in [
                    str(item.get("job_type") or "").strip(),
                    str(item.get("experience") or "").strip(),
                    str(item.get("source") or "").strip(),
                ]
                if tag
            ]
            requirement_profile = extract_job_requirement_profile(title=title, description=description, tags=tags)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": link,
                    "title": title,
                    "company": company,
                    "location": location_label,
                    "remote": "remote" in location_label.lower() or "remote" in description.lower(),
                    "url": str(item.get("apply_link") or item.get("url") or item.get("link") or ""),
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": tags,
                    "normalized_data": {
                        "salary": item.get("salary"),
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(
                        item.get("posted_at") or item.get("date") or item.get("published_at")
                    ),
                }
            )
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
        text = str(value).strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
