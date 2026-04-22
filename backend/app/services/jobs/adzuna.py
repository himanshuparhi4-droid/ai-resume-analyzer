from __future__ import annotations

from datetime import datetime
import logging
import re
from math import ceil

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

logger = logging.getLogger(__name__)


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

        if settings.environment == "production":
            results_per_page = min(max(limit * 2, 16), 24)
            target_candidates = min(max(limit * 3, 24), 36)
            page_count = 1
            extraction_limit = 1800
            enrichment_budget = min(max(limit + 2, 6), 8)
        else:
            results_per_page = min(max(limit * 4, settings.production_live_candidate_fetch), 50)
            target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
            page_count = min(5, max(1, ceil(target_candidates / results_per_page)))
            extraction_limit = 4000
            enrichment_budget = target_candidates

        seed_jobs = []
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
                try:
                    response = await client.get(endpoint, params=params)
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:
                    if seed_jobs:
                        logger.warning(
                            "Adzuna page fetch failed after partial results for query=%s page=%s: %s",
                            query,
                            page,
                            exc,
                        )
                        break
                    raise
                results = payload.get("results", []) or []
                if not results:
                    break

                for item in results:
                    external_id = str(item.get("id") or item.get("redirect_url") or item.get("title") or "")
                    if not external_id or external_id in seen_ids:
                        continue
                    seen_ids.add(external_id)
                    raw_description = strip_html(item.get("description", ""))
                    title = item.get("title", "Unknown Role")
                    category = item.get("category") or {}
                    category_label = category.get("label", "") if isinstance(category, dict) else str(category)
                    tags = [segment.strip() for segment in re.split(r"[/>]", category_label) if segment and segment.strip()]
                    description = truncate(raw_description, 4000)
                    seed_jobs.append(
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
                            },
                            "posted_at": self._parse_datetime(item.get("created")),
                        }
                    )
                    if len(seed_jobs) >= target_candidates:
                        break
                if len(seed_jobs) >= target_candidates:
                    break
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
            requirement_profile = extract_job_requirement_profile(
                title=str(item.get("title", "")),
                description=truncate(str(item.get("description", "")), extraction_limit),
                tags=item.get("tags") or [],
            )
            item["normalized_data"] = {
                **(item.get("normalized_data") or {}),
                **requirement_profile,
            }
            jobs.append(item)

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
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
