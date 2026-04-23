from __future__ import annotations

from datetime import datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

ROLE_CATEGORY_MAP = {
    "data analyst": ["Data and Analytics"],
    "data scientist": ["Data and Analytics"],
    "data engineer": ["Data and Analytics", "Software Engineering"],
    "machine learning engineer": ["Data and Analytics", "Software Engineering"],
    "software engineer": ["Software Engineering"],
    "frontend developer": ["Software Engineering"],
    "full stack developer": ["Software Engineering"],
    "devops engineer": ["Software Engineering"],
    "cybersecurity engineer": ["Software Engineering"],
    "qa engineer": ["Software Engineering"],
    "product manager": ["Product"],
    "ui/ux designer": ["Design and UX"],
    "graphic designer": ["Design and UX"],
    "teacher": ["Education"],
    "painter": ["Manufacturing and Warehouse"],
}


class TheMuseProvider:
    source_name = "themuse"
    supports_query_variations = False
    supports_location_variations = False
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.themuse.com/",
    }

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        normalized_role = normalize_role(query)
        categories = ROLE_CATEGORY_MAP.get(normalized_role, [])
        location_value = (location or "").strip()
        use_location = location_value.lower() not in {"", "remote", "worldwide", "global"}
        page_count = 1 if settings.environment == "production" else 4
        items_per_page = 20

        request_specs: list[dict[str, str]] = []
        if categories:
            if settings.environment == "production":
                # Keep hosted requests fast but not too narrow: two category
                # pages materially improve dense-role coverage while still
                # staying within a small request budget on Render free tier.
                request_specs.append({"category": categories[0]})
            else:
                for category in categories[:2]:
                    request_specs.append({"category": category})
                if use_location:
                    request_specs.extend({"category": category, "location": location_value} for category in categories[:1])
        else:
            request_specs.append({})
            if use_location and settings.environment != "production":
                request_specs.append({"location": location_value})

        collected: list[dict] = []
        seen_ids: set[str] = set()

        timeout = httpx.Timeout(
            connect=min(4.0, settings.job_request_timeout_seconds),
            read=settings.job_request_timeout_seconds,
            write=10.0,
            pool=5.0,
        )
        async with httpx.AsyncClient(timeout=timeout, headers=self.request_headers) as client:
            for spec in request_specs:
                for page in range(1, page_count + 1):
                    params = {"page": page, "items_per_page": items_per_page, **spec}
                    if settings.themuse_api_key:
                        params["api_key"] = settings.themuse_api_key
                    response = await client.get(settings.themuse_base_url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                    results = payload.get("results") or []
                    if not results:
                        break

                    for item in results:
                        external_id = str(item.get("id") or item.get("short_name") or "")
                        if not external_id or external_id in seen_ids:
                            continue
                        seen_ids.add(external_id)
                        raw_description = strip_html(item.get("contents", "") or "")
                        title = item.get("name", "Unknown Role")
                        category_tags = [entry.get("name", "") for entry in (item.get("categories") or []) if entry.get("name")]
                        level_tags = [entry.get("name", "") for entry in (item.get("levels") or []) if entry.get("name")]
                        tags = [tag for tag in [*category_tags, *level_tags] if tag]
                        locations = [entry.get("name", "") for entry in (item.get("locations") or []) if entry.get("name")]
                        location_label = ", ".join(locations[:2]) or location_value or "Remote"
                        job = {
                            "source": self.source_name,
                            "external_id": external_id,
                            "title": title,
                            "company": (item.get("company") or {}).get("name") or "Unknown Company",
                            "location": location_label,
                            "remote": "remote" in location_label.lower(),
                            "url": ((item.get("refs") or {}).get("landing_page")) or "https://www.themuse.com",
                            "description": truncate(raw_description, 4000),
                            "preview": truncate(raw_description, 260),
                            "tags": tags,
                            "normalized_data": {
                                "categories": category_tags,
                                "levels": level_tags,
                            },
                            "posted_at": self._parse_datetime(item.get("publication_date")),
                        }
                        collected.append(job)
                        if len(collected) >= max(limit * 4, 40):
                            break
                    if len(collected) >= max(limit * 4, 40):
                        break
                if len(collected) >= max(limit * 4, 40):
                    break

        positively_aligned = [
            item
            for item in collected
            if role_title_alignment_score(
                query,
                str(item.get("title", "")),
                description=str(item.get("description", "")),
                tags=item.get("tags") or [],
            )
            > 0
        ]
        ranked_pool = positively_aligned if len(positively_aligned) >= max(limit * 2, 10) else collected
        ranked_seed = sorted(
            ranked_pool,
            key=lambda item: (
                role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description=str(item.get("description", "")),
                    tags=item.get("tags") or [],
                ),
                role_fit_score(query, item),
                self._location_score(location_value, item.get("location", "")),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        if settings.environment == "production":
            dense_data_role = normalized_role in {
                "data analyst",
                "data scientist",
                "data engineer",
                "machine learning engineer",
            }
            enrichment_budget = min(max(limit, 6), 8) if dense_data_role else min(max(limit // 2, 3), 4)
            extraction_limit = 850
        else:
            enrichment_budget = max(limit * 3, 32)
            extraction_limit = 4000
        enriched_jobs: list[dict] = []
        for item in ranked_seed[:enrichment_budget]:
            requirement_profile = extract_job_requirement_profile(
                title=str(item.get("title", "")),
                description=truncate(str(item.get("description", "")), extraction_limit),
                tags=item.get("tags") or [],
            )
            normalized = item.get("normalized_data") or {}
            item["normalized_data"] = {
                "categories": normalized.get("categories", []),
                "levels": normalized.get("levels", []),
                **requirement_profile,
            }
            enriched_jobs.append(item)

        ranked = sorted(
            enriched_jobs,
            key=lambda item: (
                role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description=str(item.get("description", "")),
                    tags=item.get("tags") or [],
                ),
                role_fit_score(query, item),
                self._location_score(location_value, item.get("location", "")),
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[: max(limit * 2, 16 if settings.environment == "production" else 32)]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _location_score(self, requested_location: str, job_location: str) -> float:
        requested = (requested_location or "").strip().lower()
        job = (job_location or "").strip().lower()
        if not requested:
            return 0.0
        if requested in {"remote", "worldwide", "global"}:
            return 1.0 if "remote" in job else 0.2
        if requested in job:
            return 1.0
        if "remote" in job:
            return 0.45
        return 0.0
