from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.jobs.arbeitnow import ArbeitnowProvider
from app.services.jobs.adzuna import AdzunaProvider
from app.services.jobs.cache import JobCacheService
from app.services.jobs.remotive import RemotiveProvider
from app.services.jobs.taxonomy import dedupe_key, normalize_role, query_variations, role_fit_score, role_market_hints, role_primary_hints
from app.services.jobs.usajobs import USAJobsProvider
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION, extract_job_requirement_profile

logger = logging.getLogger(__name__)


class JobAggregator:
    def __init__(self, db: Session | None = None) -> None:
        self.db = db
        self.providers = []
        if settings.default_job_source in {"auto", "adzuna"} and settings.has_adzuna_credentials:
            self.providers.append(AdzunaProvider())
        if settings.default_job_source in {"auto", "usajobs"} and settings.has_usajobs_credentials:
            self.providers.append(USAJobsProvider())
        if settings.default_job_source in {"auto", "remotive"}:
            self.providers.append(RemotiveProvider())
        if settings.default_job_source in {"auto", "arbeitnow"}:
            self.providers.append(ArbeitnowProvider())
        if not self.providers:
            self.providers = [RemotiveProvider(), ArbeitnowProvider()]

    def _use_cache(self) -> bool:
        if self.db is None or not settings.enable_job_cache:
            return False
        database_url = (settings.database_url or "").lower()
        if settings.environment == "production":
            return False
        if "pooler.supabase.com" in database_url:
            return False
        return True

    async def fetch_jobs(self, query: str, location: str, limit: int, force_refresh: bool = False) -> list[dict]:
        use_cache = self._use_cache()
        if use_cache and not force_refresh:
            cache = JobCacheService(self.db)
            cached = cache.get_cached_jobs(query=query, location=location, limit=limit)
            if cached:
                cached_updated = False
                for item in cached:
                    item.setdefault("normalized_data", {})
                    if self._needs_requirement_refresh(item):
                        refreshed = extract_job_requirement_profile(
                            title=item.get("title", ""),
                            description=item.get("description", ""),
                        )
                        preserved = {
                            key: value
                            for key, value in item["normalized_data"].items()
                            if key not in {
                                "skills",
                                "skill_weights",
                                "skill_evidence",
                                "skill_extraction_mode",
                                "requirement_quality",
                                "normalization_version",
                            }
                        }
                        item["normalized_data"] = {**preserved, **refreshed}
                        cached_updated = True
                    item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                    item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, item)
                filtered_cached = self._filter_relevant_jobs(query, cached)
                if cached_updated and filtered_cached:
                    filtered_cached = cache.store_jobs(jobs=filtered_cached, query=query, location=location)
                if filtered_cached:
                    return filtered_cached[:limit]

        collected: list[dict] = []
        seen: set[str] = set()

        for provider in self.providers:
            search_queries = self._search_queries(provider, query)
            search_locations = self._search_locations(provider, location)
            for search_query in search_queries:
                for search_location in search_locations:
                    try:
                        items = await provider.search(query=search_query, location=search_location, limit=max(8, limit))
                    except Exception as exc:
                        logger.warning(
                            "Job provider search failed for %s (query=%s, location=%s): %s",
                            provider.__class__.__name__,
                            search_query,
                            search_location,
                            exc,
                        )
                        items = []
                    for item in items:
                        key = dedupe_key(item)
                        if key in seen:
                            continue
                        seen.add(key)
                        item.setdefault("normalized_data", {})
                        item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                        item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, item)
                        collected.append(item)
                    if len(collected) >= limit * 2:
                        break
                if len(collected) >= limit * 2:
                    break

        collected = self._filter_relevant_jobs(query, collected)

        if use_cache and collected:
            collected = JobCacheService(self.db).store_jobs(jobs=collected[:limit], query=query, location=location)
        return collected[:limit]

    def _filter_relevant_jobs(self, query: str, jobs: list[dict]) -> list[dict]:
        ordered = sorted(
            jobs,
            key=lambda item: item.get("normalized_data", {}).get("market_quality_score", 0.0),
            reverse=True,
        )
        filtered = [item for item in ordered if self._passes_quality_gate(query, item)]
        return filtered if filtered else []

    def _market_quality_score(self, query: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        hint_overlap = len(skills & role_market_hints(query))
        primary_overlap = len(skills & role_primary_hints(query))
        return round((role_fit * 10) + requirement_quality + min(10, skill_count * 1.5) + (hint_overlap * 8) + (primary_overlap * 12), 2)

    def _passes_quality_gate(self, query: str, item: dict) -> bool:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        if role_fit < 2.0:
            return False

        normalized_query = normalize_role(query)
        normalized_title = normalize_role(item.get("title", ""))
        if (
            role_primary_hints(query)
            and role_primary_hints(normalized_title)
            and normalized_query != normalized_title
            and role_fit < 4.5
        ):
            return False

        market_hints = role_market_hints(query)
        hint_overlap = len(skills & market_hints)
        primary_overlap = len(skills & role_primary_hints(query))
        if market_hints:
            if hint_overlap == 0 and role_fit < 6.5:
                return False
            if hint_overlap == 1 and skill_count <= 2:
                return False
        if role_primary_hints(query) and primary_overlap == 0 and role_fit < 9.0:
            return False

        return requirement_quality >= 22.0 or skill_count >= 3 or role_fit >= 4.5

    def _needs_requirement_refresh(self, item: dict) -> bool:
        normalized = item.get("normalized_data", {}) or {}
        version = int(normalized.get("normalization_version", 0) or 0)
        if item.get("source") == "role-baseline":
            return False
        return (
            version < JOB_REQUIREMENT_PROFILE_VERSION
            or not normalized.get("skill_weights")
            or not normalized.get("skill_evidence")
            or not normalized.get("skills")
            or normalized.get("skill_extraction_mode") not in {"weighted-pattern", "hybrid"}
        )

    def _search_queries(self, provider: object, query: str) -> list[str]:
        if not getattr(provider, "supports_query_variations", True):
            return [query]
        return query_variations(query)

    def _search_locations(self, provider: object, location: str) -> list[str]:
        if not getattr(provider, "supports_location_variations", True):
            return [location]
        options = [location]
        lowered = (location or "").strip().lower()
        if lowered and lowered not in {"remote", "worldwide", "global"}:
            options.extend(["Remote", "Worldwide", ""])
        elif not lowered:
            options.extend(["Remote"])
        return list(dict.fromkeys(options))
