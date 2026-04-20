from __future__ import annotations

from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate


class CareerjetProvider:
    source_name = "careerjet"
    supports_query_variations = True
    supports_location_variations = False

    async def search(
        self,
        query: str,
        location: str,
        limit: int,
        request_context: dict | None = None,
    ) -> list[dict]:
        if not settings.has_careerjet_credentials:
            return []

        user_ip = str((request_context or {}).get("user_ip") or "").strip()
        user_agent = str((request_context or {}).get("user_agent") or "").strip()
        if not user_ip or not user_agent:
            return []

        target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
        page_size = min(max(limit * 4, 30), 100)
        page_count = min(4, max(1, (target_candidates + page_size - 1) // page_size))
        normalized_location = normalize_role(location)
        use_location = normalized_location not in {"", "remote", "worldwide", "global"}
        location_attempts = [location] if use_location else [""]
        if use_location:
            location_attempts.append("")

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        auth = httpx.BasicAuth(str(settings.careerjet_api_key), "")

        async with httpx.AsyncClient(timeout=settings.job_request_timeout_seconds, auth=auth) as client:
            for location_filter in list(dict.fromkeys(location_attempts)):
                for page in range(1, page_count + 1):
                    params = {
                        "locale_code": settings.careerjet_locale_code,
                        "keywords": query,
                        "page": page,
                        "page_size": page_size,
                        "fragment_size": 240,
                        "sort": "relevance",
                        "user_ip": user_ip,
                        "user_agent": user_agent,
                    }
                    if location_filter:
                        params["location"] = location_filter
                    response = await client.get(settings.careerjet_base_url, params=params)
                    response.raise_for_status()
                    payload = response.json()

                    if str(payload.get("type") or "").upper() == "LOCATIONS":
                        break

                    results = payload.get("jobs") or []
                    if not results:
                        break

                    for item in results:
                        url = str(item.get("url") or "").strip()
                        title = str(item.get("title") or "Unknown Role").strip()
                        company = str(item.get("company") or "Unknown Company").strip()
                        if not url:
                            url = f"{self.source_name}:{title}:{company}"
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                        description = strip_html(str(item.get("description") or ""))
                        location_label = str(item.get("locations") or location_filter or "Worldwide").strip()
                        site_label = str(item.get("site") or "").strip()
                        tags = [tag for tag in [site_label, str(item.get("salary_type") or "").strip()] if tag]
                        requirement_profile = extract_job_requirement_profile(
                            title=title,
                            description=description,
                            tags=tags,
                        )
                        jobs.append(
                            {
                                "source": self.source_name,
                                "external_id": url,
                                "title": title,
                                "company": company,
                                "location": location_label,
                                "remote": any(
                                    hint in f"{title} {location_label} {description}".lower()
                                    for hint in ("remote", "work from home", "hybrid")
                                ),
                                "url": url,
                                "description": description,
                                "preview": truncate(description, 260),
                                "tags": tags,
                                "normalized_data": {
                                    "site": site_label,
                                    "salary": item.get("salary"),
                                    "salary_currency_code": item.get("salary_currency_code"),
                                    "salary_max": item.get("salary_max"),
                                    "salary_min": item.get("salary_min"),
                                    "salary_type": item.get("salary_type"),
                                    **requirement_profile,
                                },
                                "posted_at": self._parse_datetime(item.get("date")),
                            }
                        )
                        if len(jobs) >= target_candidates:
                            break
                    if len(jobs) >= target_candidates or len(results) < page_size:
                        break
                if jobs:
                    break

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
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = parsedate_to_datetime(str(value).strip())
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
            return parsed
        except (TypeError, ValueError, IndexError):
            return None

    def _location_score(self, requested_location: str, job_location: str) -> float:
        requested = (requested_location or "").strip().lower()
        job = (job_location or "").strip().lower()
        if not requested:
            return 0.0
        if requested in {"remote", "worldwide", "global"}:
            return 1.0 if "remote" in job else 0.3
        if requested in job:
            return 1.0
        if "remote" in job or "worldwide" in job:
            return 0.45
        return 0.0
