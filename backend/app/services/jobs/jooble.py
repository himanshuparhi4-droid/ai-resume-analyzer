from __future__ import annotations

from datetime import datetime
import logging

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import (
    normalize_role,
    role_domain,
    role_family,
    role_fit_score,
    role_title_alignment_score,
)
from app.services.jobs.fast_profile import build_fast_requirement_profile
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

logger = logging.getLogger(__name__)


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
        normalized_query = normalize_role(query)
        normalized_location = normalize_role(location)
        location_filter = "" if normalized_location in {"", "remote", "worldwide", "global"} else location
        query_domain = role_domain(query)
        canonical_query = role_family(query)
        india_focused_location = normalized_location == "india" or "india" in normalized_location
        high_recall_role = (
            query_domain in {"marketing", "sales", "customer", "people", "finance", "operations"}
            or canonical_query
            in {
                "devops engineer",
                "support engineer",
                "technical writer",
                "ui/ux designer",
            }
        )

        if settings.environment == "production":
            analyst_style = normalized_query == "data analyst"
            target_candidates = min(max(limit * 2, 16), 20) if analyst_style else min(max(limit * 3, 24), 36)
            page_size = min(max(limit * 2, 16), 24 if analyst_style else 30)
            page_count = 1
            extraction_limit = 750 if analyst_style else 900
            enrichment_budget = target_candidates
        else:
            target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
            page_size = min(max(limit * 4, 30), 100)
            page_count = min(5, max(1, (target_candidates + page_size - 1) // page_size))
            extraction_limit = 4000
            enrichment_budget = target_candidates
        endpoint = f"{settings.jooble_base_url}/{settings.jooble_api_key}"

        seed_jobs: list[dict] = []
        seen_links: set[str] = set()
        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, headers=self.request_headers) as client:
            for page in range(1, page_count + 1):
                payload = {
                    "keywords": query,
                    "location": location_filter,
                    "page": str(page),
                    "companysearch": "false",
                }
                try:
                    response = await client.post(endpoint, json=payload)
                    response.raise_for_status()
                    data = response.json()
                except Exception as exc:
                    if seed_jobs:
                        logger.warning("Jooble page fetch failed after partial results for query=%s page=%s: %s", query, page, exc)
                        break
                    raise
                results = data.get("jobs") or []
                if not results:
                    break

                for item in results:
                    link = str(item.get("link") or "").strip()
                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)
                    raw_description = strip_html(str(item.get("snippet") or ""))
                    title = str(item.get("title") or "Unknown Role")
                    type_label = str(item.get("type") or "").strip()
                    tags = [tag for tag in [type_label, str(item.get("source") or "").strip()] if tag]
                    description = truncate(raw_description, 4000)
                    seed_jobs.append(
                        {
                            "source": self.source_name,
                            "external_id": link,
                            "title": title,
                            "company": str(item.get("company") or "Unknown Company"),
                            "location": str(item.get("location") or location_filter or "Remote"),
                            "remote": "remote" in description.lower() or "remote" in title.lower(),
                            "url": link,
                            "description": description,
                            "preview": truncate(description, 260),
                            "tags": tags,
                            "normalized_data": {
                                "salary": item.get("salary"),
                            },
                            "posted_at": self._parse_datetime(item.get("updated")),
                        }
                    )
                    if len(seed_jobs) >= target_candidates:
                        break
                if len(seed_jobs) >= target_candidates:
                    break

        if settings.environment == "production":
            ranked_seed = sorted(
                seed_jobs,
                key=lambda item: (
                    role_title_alignment_score(
                        query,
                        str(item.get("title", "")),
                        description="",
                        tags=item.get("tags") or [],
                    ),
                    1 if item.get("remote") else 0,
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
                    1 if item.get("remote") else 0,
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
