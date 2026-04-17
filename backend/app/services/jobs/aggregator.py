from __future__ import annotations

import asyncio
import logging
import re

from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.jobs.arbeitnow import ArbeitnowProvider
from app.services.jobs.adzuna import AdzunaProvider
from app.services.jobs.cache import JobCacheService
from app.services.jobs.remoteok import RemoteOKProvider
from app.services.jobs.remotive import RemotiveProvider
from app.services.jobs.themuse import TheMuseProvider
from app.services.jobs.taxonomy import (
    dedupe_key,
    normalize_role,
    production_query_variations,
    query_variations,
    role_fit_score,
    role_market_hints,
    role_primary_hints,
    role_title_hints,
)
from app.services.jobs.usajobs import USAJobsProvider
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION, extract_job_requirement_profile
from app.utils.text import truncate

logger = logging.getLogger(__name__)


class JobAggregator:
    def __init__(self, db: Session | None = None) -> None:
        self.db = db
        self.providers = []
        self.last_fetch_diagnostics: dict = {}
        if settings.environment == "production":
            self.providers = self._production_providers()
            if self.providers:
                return
        if settings.default_job_source in {"auto", "adzuna"} and settings.has_adzuna_credentials:
            self.providers.append(AdzunaProvider())
        if settings.default_job_source in {"auto", "usajobs"} and settings.has_usajobs_credentials:
            self.providers.append(USAJobsProvider())
        if settings.default_job_source in {"auto", "themuse"}:
            self.providers.append(TheMuseProvider())
        if settings.default_job_source in {"auto", "remotive"}:
            self.providers.append(RemotiveProvider())
        if settings.default_job_source == "remoteok":
            self.providers.append(RemoteOKProvider())
        elif settings.default_job_source == "auto":
            self.providers.append(RemoteOKProvider())
        if settings.default_job_source == "arbeitnow":
            self.providers.append(ArbeitnowProvider())
        elif settings.default_job_source == "auto" and settings.environment != "production":
            # Arbeitnow frequently blocks cloud-hosted traffic with 403 responses.
            # Keep it available for local/dev exploration, but don't slow down or
            # clutter production analysis runs with a provider that is commonly blocked.
            self.providers.append(ArbeitnowProvider())
        if not self.providers:
            self.providers = [TheMuseProvider(), RemotiveProvider(), RemoteOKProvider(), ArbeitnowProvider()]

    def _production_providers(self) -> list[object]:
        source = (settings.default_job_source or "auto").strip().lower()
        providers: list[object] = [TheMuseProvider()]

        def add(provider: object) -> None:
            if any(existing.__class__ is provider.__class__ for existing in providers):
                return
            providers.append(provider)

        if source in {"auto", "adzuna"} and settings.has_adzuna_credentials:
            add(AdzunaProvider())
        if source in {"auto", "usajobs"} and settings.has_usajobs_credentials:
            add(USAJobsProvider())

        if source in {"auto", "themuse"}:
            add(RemotiveProvider())
            add(RemoteOKProvider())
            return providers

        if source == "remotive":
            add(RemotiveProvider())
            add(RemoteOKProvider())
            return providers

        if source == "remoteok":
            add(RemoteOKProvider())
            add(RemotiveProvider())
            return providers

        if source == "adzuna" and settings.has_adzuna_credentials:
            add(RemotiveProvider())
            add(RemoteOKProvider())
            return providers

        if source == "usajobs" and settings.has_usajobs_credentials:
            add(RemotiveProvider())
            add(RemoteOKProvider())
            return providers

        add(RemotiveProvider())
        add(RemoteOKProvider())
        return providers

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
        self.last_fetch_diagnostics = {
            "environment": settings.environment,
            "query": query,
            "location": location,
            "limit": limit,
            "providers": [],
        }
        if settings.environment == "production":
            live_jobs = await self._fetch_production_jobs(query=query, location=location, limit=limit)
            if live_jobs:
                return live_jobs

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
                            tags=item.get("tags") or [],
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
                        item.setdefault("preview", truncate(str(item.get("description", "")), 260))
                        item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                        item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, item)
                        collected.append(item)
                    if len(collected) >= limit * 2:
                        break
                if len(collected) >= limit * 2:
                    break
            if (
                settings.environment == "production"
                and getattr(provider, "source_name", "") == "themuse"
                and len(collected) >= max(limit, 8)
            ):
                break

        if settings.environment == "production":
            preferred_live = self._select_production_live_jobs(query=query, jobs=collected, limit=limit)
            if preferred_live:
                logger.info(
                    "Production live selection kept %s jobs from %s collected candidates for query=%s",
                    len(preferred_live),
                    len(collected),
                    query,
                )
                return preferred_live

        collected = self._filter_relevant_jobs(query, collected)

        if use_cache and collected:
            collected = JobCacheService(self.db).store_jobs(jobs=collected[:limit], query=query, location=location)
        return collected[:limit]

    async def _fetch_production_jobs(self, *, query: str, location: str, limit: int) -> list[dict]:
        async def safe_search(provider: object, search_query: str, search_location: str) -> list[dict]:
            source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
            provider_timeout = 4.5 if source_name == "themuse" else 3.5
            provider_diag = {
                "provider": provider.__class__.__name__,
                "source": source_name,
                "timeout_seconds": provider_timeout,
                "query": search_query,
                "location": search_location,
            }
            try:
                items = await asyncio.wait_for(
                    provider.search(query=search_query, location=search_location, limit=max(8, limit)),
                    timeout=provider_timeout,
                )
                provider_diag["result_count"] = len(items)
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return items
            except Exception as exc:
                logger.warning(
                    "Production job provider search failed for %s (query=%s, location=%s): %s",
                    provider.__class__.__name__,
                    search_query,
                    search_location,
                    exc,
                )
                provider_diag["error"] = str(exc)
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return []

        tasks = []
        for provider in self.providers:
            search_queries = self._search_queries(provider, query)
            search_locations = self._search_locations(provider, location)
            for search_query in search_queries:
                for search_location in search_locations[:1]:
                    tasks.append(safe_search(provider, search_query, search_location))

        provider_results = await asyncio.gather(*tasks)
        collected: list[dict] = []
        seen: set[str] = set()

        for items in provider_results:
            for item in items:
                key = dedupe_key(item)
                if key in seen:
                    continue
                seen.add(key)
                item.setdefault("normalized_data", {})
                item.setdefault("preview", truncate(str(item.get("description", "")), 260))
                item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, item)
                collected.append(item)

        preferred_live = self._select_production_live_jobs(query=query, jobs=collected, limit=limit)
        self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
        self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
        self.last_fetch_diagnostics["selected_live_sources"] = {
            source: len([item for item in preferred_live if item.get("source") == source])
            for source in sorted({item.get("source", "unknown") for item in preferred_live})
        }
        if preferred_live:
            logger.info(
                "Production live selection kept %s jobs from %s collected candidates for query=%s",
                len(preferred_live),
                len(collected),
                query,
            )
            return preferred_live
        return []

    def _select_production_live_jobs(self, *, query: str, jobs: list[dict], limit: int) -> list[dict]:
        live_jobs = [item for item in jobs if item.get("source") != "role-baseline"]
        if not live_jobs:
            return []

        ranked = sorted(
            live_jobs,
            key=lambda item: (
                self._title_hint_overlap(query, item),
                float(item.get("normalized_data", {}).get("role_fit_score", 0.0)),
                float(item.get("normalized_data", {}).get("market_quality_score", 0.0)),
                self._skill_overlap_score(query, item),
            ),
            reverse=True,
        )
        selected: list[dict] = []
        company_counts: dict[str, int] = {}

        def maybe_add(candidates: list[dict], cap_per_company: int) -> None:
            for item in candidates:
                if item in selected:
                    continue
                company = normalize_role(str(item.get("company", "")).lower()) or "unknown"
                if company_counts.get(company, 0) >= cap_per_company:
                    continue
                selected.append(item)
                company_counts[company] = company_counts.get(company, 0) + 1
                if len(selected) >= limit:
                    break

        strong_candidates = [item for item in ranked if self._is_production_live_candidate(query, item, strict=True)]
        maybe_add(strong_candidates, cap_per_company=2)
        if len(selected) < min(limit, 8):
            secondary_candidates = [item for item in ranked if self._is_production_live_candidate(query, item, strict=False)]
            maybe_add(secondary_candidates, cap_per_company=2)
        if len(selected) < limit:
            tertiary_candidates = [
                item
                for item in ranked
                if self._title_hint_overlap(query, item) >= 1
                or (
                    self._skill_overlap_score(query, item) >= 3.0
                    and float(item.get("normalized_data", {}).get("role_fit_score", 0.0)) >= 5.0
                )
            ]
            maybe_add(tertiary_candidates, cap_per_company=3)

        return selected[:limit]

    def _is_production_live_candidate(self, query: str, item: dict, *, strict: bool) -> bool:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        market_quality = float(normalized.get("market_quality_score", 0.0))
        title_overlap = self._title_hint_overlap(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        source = str(item.get("source", ""))

        if strict:
            if title_overlap >= 1 and role_fit >= 1.0:
                return True
            if title_overlap >= 1 and skill_overlap >= 1.0:
                return True
            if title_overlap >= 2 and (role_fit >= 1.6 or skill_overlap >= 1.0):
                return True
            if title_overlap == 0 and role_fit >= 6.0 and skill_overlap >= 2.0 and market_quality >= 40.0:
                return True
            if source in {"remotive", "remoteok", "arbeitnow"} and title_overlap >= 1 and market_quality >= 28.0:
                return True
            return False

        if title_overlap >= 1 and role_fit >= 0.5:
            return True
        if title_overlap >= 1 and role_fit >= 1.0:
            return True
        if title_overlap == 0 and skill_overlap >= 2.0 and role_fit >= 3.25:
            return True
        if market_quality >= 42.0 and title_overlap >= 1:
            return True
        return False

    def _title_hint_overlap(self, query: str, item: dict) -> int:
        title = str(item.get("title", "")).lower()
        hints = role_title_hints(query)
        overlap = 0
        for hint in hints:
            pattern = rf"\b{re.escape(hint)}\b" if " " not in hint else re.escape(hint)
            if re.search(pattern, title):
                overlap += 1
        return overlap

    def _skill_overlap_score(self, query: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        market_overlap = len(skills & role_market_hints(query))
        primary_overlap = len(skills & role_primary_hints(query))
        return (primary_overlap * 1.5) + market_overlap

    def _filter_relevant_jobs(self, query: str, jobs: list[dict]) -> list[dict]:
        ordered = sorted(
            jobs,
            key=lambda item: item.get("normalized_data", {}).get("market_quality_score", 0.0),
            reverse=True,
        )
        filtered = [item for item in ordered if self._passes_quality_gate(query, item)]
        if filtered:
            return filtered
        if settings.environment == "production":
            relaxed = [item for item in ordered if float(item.get("normalized_data", {}).get("role_fit_score", 0.0)) >= 6.0]
            return relaxed[: max(1, min(5, len(relaxed)))]
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
        if settings.environment == "production" and item.get("source") in {"remotive", "remoteok", "arbeitnow"} and role_fit >= 5.0:
            return True
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
        if settings.environment == "production":
            return production_query_variations(query)
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
