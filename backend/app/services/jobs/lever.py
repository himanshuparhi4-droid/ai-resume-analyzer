from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx

from app.core.config import settings
from app.services.jobs.fast_profile import build_fast_requirement_profile
from app.services.jobs.taxonomy import normalize_role, role_domain, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
from app.utils.text import strip_html, truncate

_LEVER_BOARD_CACHE: dict[str, dict] = {}
_MAX_LEVER_BOARD_CACHE_ENTRIES = 16
logger = logging.getLogger(__name__)

_CURATED_LEVER_COMPANIES = {
    "data": [
        "plaid",
        "dnb",
        "pipedrive",
        "caseware",
        "questanalytics",
    ],
    "software": [
        "palantir",
        "plaid",
        "gohighlevel",
        "greenlight",
        "spreetail",
        "thinkahead",
        "cti-md",
    ],
    "security": [
        "palantir",
        "plaid",
        "dnb",
        "cti-md",
    ],
    "product": [
        "pipedrive",
        "greenlight",
        "highspot",
        "caseware",
        "plaid",
    ],
    "design": [
        "greenlight",
        "highspot",
        "pipedrive",
        "caseware",
    ],
}

_ROLE_SPECIFIC_LEVER_COMPANIES = {
    "data analyst": ["plaid", "dnb", "pipedrive", "caseware", "questanalytics"],
    "data scientist": ["plaid", "dnb", "palantir", "caseware"],
    "data engineer": ["plaid", "palantir", "dnb", "caseware", "pipedrive"],
    "software engineer": ["palantir", "plaid", "gohighlevel", "greenlight", "spreetail", "thinkahead"],
    "full stack developer": ["palantir", "gohighlevel", "greenlight", "spreetail", "thinkahead"],
    "frontend developer": ["palantir", "greenlight", "gohighlevel", "pipedrive", "highspot"],
    "devops engineer": ["thinkahead", "cti-md", "gohighlevel", "greenlight", "plaid"],
    "cybersecurity engineer": ["palantir", "plaid", "dnb", "cti-md"],
    "product manager": ["pipedrive", "greenlight", "highspot", "caseware", "plaid"],
    "ui/ux designer": ["greenlight", "highspot", "pipedrive", "caseware"],
}


class LeverProvider:
    source_name = "lever"
    supports_query_variations = False
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        companies = self._companies_for_query(query)
        if not companies:
            return []
        if settings.environment == "production":
            query_domain = role_domain(query)
            if query_domain == "data":
                companies = companies[:3]
            elif query_domain in {"software", "security"}:
                companies = companies[:5]

        jobs: list[dict] = []
        seen_links: set[str] = set()
        timeout = httpx.Timeout(
            connect=min(4.0, settings.job_request_timeout_seconds),
            read=settings.job_request_timeout_seconds,
            write=10.0,
            pool=5.0,
        )
        async with httpx.AsyncClient(timeout=timeout) as client:
            if settings.environment == "production":
                domain = role_domain(query)
                if domain in {"software", "security"}:
                    batch_timeout = 4.25
                elif domain == "data":
                    batch_timeout = 4.75
                else:
                    batch_timeout = 5.0
                async def fetch_company_with_timeout(company: str) -> tuple[str, list[dict] | Exception]:
                    try:
                        jobs = await asyncio.wait_for(
                            self._fetch_company_jobs(company, client=client),
                            timeout=batch_timeout,
                        )
                        return company, jobs
                    except Exception as exc:
                        return company, exc

                company_results_raw = await asyncio.gather(
                    *(fetch_company_with_timeout(company) for company in companies)
                )
            else:
                company_results_raw = list(
                    zip(
                        companies,
                        await asyncio.gather(
                            *(self._fetch_company_jobs(company, client=client) for company in companies),
                            return_exceptions=True,
                        ),
                    )
                )
        company_results: list[list[dict]] = []
        for company, company_jobs in company_results_raw:
            if isinstance(company_jobs, Exception):
                logger.warning("Lever company fetch failed for %s: %s", company, company_jobs)
                continue
            company_results.append(company_jobs)
        for company_jobs in company_results:
            for item in company_jobs:
                link = str(item.get("url") or item.get("external_id") or "").strip()
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                jobs.append(item)

        target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
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
                1 if item.get("remote") else 0,
            ),
            reverse=True,
        )
        return ranked[:target_candidates]

    def _companies_for_query(self, query: str) -> list[str]:
        normalized = normalize_role(query)
        if settings.has_lever_companies:
            return settings.lever_company_tokens
        if settings.environment != "production":
            return []
        specific = _ROLE_SPECIFIC_LEVER_COMPANIES.get(normalized)
        if specific:
            return list(specific)
        return list(_CURATED_LEVER_COMPANIES.get(role_domain(query) or "", []))

    async def _fetch_company_jobs(self, company: str, *, client: httpx.AsyncClient) -> list[dict]:
        self._prune_cache(_LEVER_BOARD_CACHE, max_entries=_MAX_LEVER_BOARD_CACHE_ENTRIES)
        cached = _LEVER_BOARD_CACHE.get(company)
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        if cached and now - cached["stored_at"] < ttl:
            return cached["jobs"]

        endpoint = f"{settings.lever_base_url}/{company}"
        params = {"mode": "json"}
        response = await client.get(endpoint, params=params)
        response.raise_for_status()
        raw_jobs = response.json()
        if settings.environment == "production":
            raw_jobs = raw_jobs[:60]

        jobs: list[dict] = []
        for item in raw_jobs:
            raw_description = strip_html(
                str(item.get("descriptionPlain") or item.get("description") or "")
            )
            title = str(item.get("text") or "Unknown Role")
            categories = item.get("categories") or {}
            location = str(categories.get("location") or "Unknown")
            team = str(categories.get("team") or "").strip()
            commitment = str(categories.get("commitment") or "").strip()
            tags = [tag for tag in [team, commitment, company] if tag]
            if settings.environment == "production":
                requirement_profile = build_fast_requirement_profile(
                    query=title,
                    title=title,
                    description=truncate(raw_description, 500),
                    tags=tags,
                    source=self.source_name,
                )
            else:
                requirement_profile = extract_job_requirement_profile(title=title, description=raw_description, tags=tags)
            description = truncate(raw_description, 4000)
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("hostedUrl") or ""),
                    "title": title,
                    "company": self._company_from_token(company),
                    "location": location,
                    "remote": "remote" in location.lower() or "remote" in description.lower(),
                    "url": str(item.get("hostedUrl") or ""),
                    "description": description,
                    "preview": truncate(description, 260),
                    "tags": tags,
                    "normalized_data": requirement_profile,
                    "posted_at": self._parse_datetime(item.get("createdAt") or item.get("updatedAt")),
                }
            )

        _LEVER_BOARD_CACHE[company] = {"stored_at": now, "jobs": jobs}
        return jobs

    def _prune_cache(self, cache: dict[str, dict], *, max_entries: int) -> None:
        if not cache:
            return
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        expired_keys = [
            key
            for key, payload in cache.items()
            if now - payload.get("stored_at", now) >= ttl
        ]
        for key in expired_keys:
            cache.pop(key, None)
        overflow = len(cache) - max_entries
        if overflow > 0:
            oldest_keys = [
                key
                for key, _ in sorted(
                    cache.items(),
                    key=lambda item: item[1].get("stored_at", now),
                )[:overflow]
            ]
            for key in oldest_keys:
                cache.pop(key, None)

    def _company_from_token(self, token: str) -> str:
        cleaned = token.replace("-", " ").replace("_", " ").strip()
        return cleaned.title() if cleaned else "Lever Company"

    def _parse_datetime(self, value) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            try:
                return datetime.fromtimestamp(timestamp, tz=UTC)
            except (OverflowError, OSError, ValueError):
                return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
