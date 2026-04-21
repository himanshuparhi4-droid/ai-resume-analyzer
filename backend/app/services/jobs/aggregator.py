from __future__ import annotations

import asyncio
import copy
import logging
import re
import time

from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.jobs.arbeitnow import ArbeitnowProvider
from app.services.jobs.adzuna import AdzunaProvider
from app.services.jobs.cache import JobCacheService
from app.services.jobs.greenhouse import GreenhouseProvider
from app.services.jobs.indianapi import IndianAPIProvider
from app.services.jobs.jobicy import JobicyProvider
from app.services.jobs.jooble import JoobleProvider
from app.services.jobs.lever import LeverProvider
from app.services.jobs.remoteok import RemoteOKProvider
from app.services.jobs.remotive import RemotiveProvider
from app.services.jobs.themuse import TheMuseProvider
from app.services.jobs.taxonomy import (
    ABSTRACT_CANONICAL_QUERY_FAMILIES,
    GENERIC_ROLE_MATCH_TOKENS,
    STOPWORDS,
    canonical_role_alignment,
    dedupe_key,
    is_sparse_live_market_role,
    normalize_role,
    provider_query_variations,
    production_query_variations,
    query_variations,
    role_domain,
    role_fit_score,
    role_profile,
    role_market_hints,
    role_negative_title_hints,
    role_primary_hints,
    role_query_tokens,
    role_title_alignment_score,
    role_title_hints,
)
from app.services.jobs.usajobs import USAJobsProvider
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION, extract_job_requirement_profile
from app.utils.text import truncate

logger = logging.getLogger(__name__)

GLOBAL_REMOTE_HINTS = {"remote", "worldwide", "global", "anywhere"}
INDIA_LOCATION_HINTS = {"india", "indian", "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "delhi", "gurgaon", "gurugram", "noida", "chennai", "kolkata", "ahmedabad"}
ASIA_LOCATION_HINTS = {"apac", "asia", "south asia", "southeast asia"}
NON_INDIA_REGION_HINTS = {
    "usa",
    "united states",
    "north america",
    "uk",
    "united kingdom",
    "emea",
    "europe",
    "canada",
    "australia",
    "new zealand",
    "latam",
    "latin america",
}
SOURCE_TRUST_WEIGHTS = {
    "greenhouse": 1.08,
    "lever": 1.07,
    "indianapi": 1.03,
    "jooble": 1.04,
    "jobicy": 1.0,
    "remotive": 0.94,
    "themuse": 0.92,
    "adzuna": 1.02,
    "usajobs": 0.88,
    "remoteok": 0.72,
    "arbeitnow": 0.62,
    "role-baseline": 0.58,
}
LOW_SIGNAL_DESCRIPTION_HINTS = (
    "equal opportunity",
    "equal employment opportunity",
    "all qualified applicants",
    "regard to race",
    "without regard to",
    "compensation and benefits",
    "employee benefits",
    "benefits package",
    "salary range",
    "country by country reporting",
    "transfer pricing",
)
GENERIC_TITLE_WEAKENERS = {
    "associate",
    "coordinator",
    "representative",
    "specialist",
    "support",
    "manager",
}
_PRODUCTION_JOB_CACHE: dict[str, dict] = {}
_PRODUCTION_INFLIGHT_FETCHES: dict[str, asyncio.Task] = {}
FREE_AUTO_SOURCES = {
    "jobicy",
    "remotive",
    "themuse",
    "greenhouse",
    "lever",
    "indianapi",
    "jooble",
    "adzuna",
    "remoteok",
    "arbeitnow",
    "usajobs",
}
KNOWN_PROVIDER_SOURCES = (
    "jobicy",
    "remotive",
    "themuse",
    "greenhouse",
    "lever",
    "indianapi",
    "jooble",
    "adzuna",
    "usajobs",
    "remoteok",
    "arbeitnow",
)


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
        if settings.default_job_source in {"auto", "greenhouse"} and (
            settings.has_greenhouse_boards or settings.environment == "production"
        ):
            self.providers.append(GreenhouseProvider())
        if settings.default_job_source in {"auto", "lever"} and settings.has_lever_companies:
            self.providers.append(LeverProvider())
        if settings.default_job_source == "indianapi" and settings.has_indianapi_credentials:
            self.providers.append(IndianAPIProvider())
        if settings.default_job_source in {"auto", "jooble"} and settings.has_jooble_credentials:
            self.providers.append(JoobleProvider())
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

    def _production_cache_key(self, *, query: str, location: str, limit: int) -> str:
        query_profile = role_profile(query)
        return "|".join(
            [
                query_profile.normalized_role,
                query_profile.cleaned_query or query_profile.normalized_role,
                normalize_role(location),
                str(limit),
                normalize_role(settings.default_job_source or "auto"),
            ]
        )

    def _get_production_cached_jobs(self, *, query: str, location: str, limit: int) -> list[dict] | None:
        if settings.environment != "production":
            return None
        cache_key = self._production_cache_key(query=query, location=location, limit=limit)
        cached = _PRODUCTION_JOB_CACHE.get(cache_key)
        if not cached:
            return None
        ttl_seconds = max(300, settings.production_live_cache_ttl_minutes * 60)
        if (time.time() - float(cached.get("stored_at", 0))) > ttl_seconds:
            _PRODUCTION_JOB_CACHE.pop(cache_key, None)
            return None
        self.last_fetch_diagnostics = copy.deepcopy(cached.get("diagnostics", {})) or {}
        self.last_fetch_diagnostics["cache_hit"] = True
        return copy.deepcopy(cached.get("jobs", []))

    def _store_production_cached_jobs(self, *, query: str, location: str, limit: int, jobs: list[dict]) -> None:
        if settings.environment != "production" or not jobs:
            return
        cache_key = self._production_cache_key(query=query, location=location, limit=limit)
        _PRODUCTION_JOB_CACHE[cache_key] = {
            "stored_at": time.time(),
            "jobs": copy.deepcopy(jobs),
            "diagnostics": copy.deepcopy(self.last_fetch_diagnostics),
        }

    def _persist_production_jobs(self, *, query: str, location: str, jobs: list[dict]) -> None:
        if self.db is None or not settings.enable_job_cache or not jobs:
            return
        try:
            JobCacheService(self.db).store_jobs(jobs=jobs, query=query, location=location)
        except Exception as exc:
            logger.warning("Production job cache persist failed for query=%s: %s", query, exc)

    def _annotate_item_scores(self, *, query: str, location: str, item: dict) -> None:
        normalized = item.setdefault("normalized_data", {})
        normalized["title_alignment_score"] = role_title_alignment_score(
            query,
            str(item.get("title", "")),
            description=str(item.get("description", "")),
            tags=item.get("tags") or [],
        )
        normalized["role_fit_score"] = role_fit_score(query, item)
        normalized["location_alignment_score"] = self._location_alignment_score(location, item)
        normalized["listing_quality_score"] = self._listing_quality_score(query, item)
        normalized["market_quality_score"] = self._market_quality_score(query, location, item)
        normalized["domain_alignment_score"] = self._role_domain_match_score(query, item)
        normalized["skill_overlap_score"] = self._skill_overlap_score(query, item)
        normalized["cache_query_bucket"] = self._cache_query_bucket(query, item)

    def _provider_is_selected_by_source(self, source_name: str) -> bool:
        source = (settings.default_job_source or "auto").strip().lower()
        if source == "auto":
            return source_name in FREE_AUTO_SOURCES
        if source_name == "jobicy":
            return source in {"jobicy", "themuse", "remotive"}
        if source_name == "remotive":
            return source in {"remotive", "remoteok", "adzuna", "usajobs"}
        if source_name == "themuse":
            return source == "themuse"
        if source_name == "indianapi":
            return source in {"auto", "indianapi"}
        return source == source_name

    def _provider_requirement_state(self, source_name: str) -> tuple[bool, str]:
        if source_name == "adzuna":
            return settings.has_adzuna_credentials, "missing_credentials"
        if source_name == "indianapi":
            return settings.has_indianapi_credentials, "missing_credentials"
        if source_name == "jooble":
            return settings.has_jooble_credentials, "missing_credentials"
        if source_name == "usajobs":
            return settings.has_usajobs_credentials, "missing_credentials"
        if source_name == "greenhouse":
            return (settings.has_greenhouse_boards or settings.environment == "production"), "missing_configuration"
        if source_name == "lever":
            return (settings.has_lever_companies or settings.environment == "production"), "missing_configuration"
        return True, "available"

    def _provider_availability_snapshot(self) -> list[dict]:
        active_sources = {
            str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
            for provider in self.providers
        }
        snapshot: list[dict] = []
        for source_name in KNOWN_PROVIDER_SOURCES:
            selected = self._provider_is_selected_by_source(source_name)
            available, unavailable_reason = self._provider_requirement_state(source_name)
            if source_name in active_sources:
                status = "active"
            elif not selected:
                status = "source_filtered"
            elif not available:
                status = unavailable_reason
            else:
                status = "inactive"
            snapshot.append(
                {
                    "source": source_name,
                    "status": status,
                    "selected_by_source": selected,
                    "available": available,
                }
            )
        return snapshot

    def _production_stage_soft_timeout(self, *, stage: str, query_domain: str | None, sparse_role: bool) -> float:
        if sparse_role:
            return 6.0 if stage == "primary" else 4.0
        if stage == "primary":
            if query_domain == "data":
                return 8.0
            if query_domain in {"software", "security"}:
                return 9.0
            return 7.5
        if stage == "supplemental":
            return 8.0
        return 6.5

    def _production_providers(self) -> list[object]:
        source = (settings.default_job_source or "auto").strip().lower()
        providers: list[object] = [JobicyProvider()]

        def add(provider: object) -> None:
            if any(existing.__class__ is provider.__class__ for existing in providers):
                return
            providers.append(provider)

        if source in {"auto", "adzuna"} and settings.has_adzuna_credentials:
            add(AdzunaProvider())
        if source in {"auto", "greenhouse"} and (
            settings.has_greenhouse_boards or settings.environment == "production"
        ):
            add(GreenhouseProvider())
        if source in {"auto", "lever"} and (
            settings.has_lever_companies or settings.environment == "production"
        ):
            add(LeverProvider())
        if source in {"auto", "indianapi"} and settings.has_indianapi_credentials:
            add(IndianAPIProvider())
        if source in {"auto", "jooble"} and settings.has_jooble_credentials:
            add(JoobleProvider())
        if source in {"auto", "usajobs"} and settings.has_usajobs_credentials:
            add(USAJobsProvider())

        if source in {"auto", "themuse", "jobicy"}:
            add(RemotiveProvider())
            if source in {"auto", "themuse"}:
                add(TheMuseProvider())
            return providers

        if source == "remotive":
            add(RemotiveProvider())
            add(JobicyProvider())
            return providers

        if source == "remoteok":
            add(RemoteOKProvider())
            add(RemotiveProvider())
            add(JobicyProvider())
            return providers

        if source == "adzuna" and settings.has_adzuna_credentials:
            add(RemotiveProvider())
            add(JobicyProvider())
            return providers

        if source == "usajobs" and settings.has_usajobs_credentials:
            add(RemotiveProvider())
            add(JobicyProvider())
            return providers

        add(RemotiveProvider())
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

    async def fetch_jobs(
        self,
        query: str,
        location: str,
        limit: int,
        force_refresh: bool = False,
    ) -> list[dict]:
        self.last_fetch_diagnostics = {
            "environment": settings.environment,
            "query": query,
            "location": location,
            "limit": limit,
            "providers": [],
            "provider_availability": self._provider_availability_snapshot(),
        }
        sparse_role = is_sparse_live_market_role(query)
        if settings.environment == "production":
            cache_key = self._production_cache_key(query=query, location=location, limit=limit)
            cached_live = self._get_production_cached_jobs(query=query, location=location, limit=limit)
            if cached_live:
                return cached_live
            production_display_floor = self._production_display_floor(query=query, limit=limit)
            cached_seed = []
            if not sparse_role:
                cached_seed = self._get_cached_production_fallback(query=query, location=location, limit=limit)
                if len(cached_seed) >= production_display_floor:
                    self.last_fetch_diagnostics["db_cache_short_circuit"] = True
                    self._store_production_cached_jobs(query=query, location=location, limit=limit, jobs=cached_seed)
                    return cached_seed
            inflight_task = _PRODUCTION_INFLIGHT_FETCHES.get(cache_key)
            if inflight_task and not inflight_task.done():
                self.last_fetch_diagnostics["shared_inflight_hit"] = True
                live_jobs = copy.deepcopy(await inflight_task)
            else:
                inflight_task = asyncio.create_task(
                    self._fetch_production_jobs(
                        query=query,
                        location=location,
                        limit=limit,
                    )
                )
                _PRODUCTION_INFLIGHT_FETCHES[cache_key] = inflight_task
                try:
                    live_jobs = copy.deepcopy(await inflight_task)
                finally:
                    if _PRODUCTION_INFLIGHT_FETCHES.get(cache_key) is inflight_task:
                        _PRODUCTION_INFLIGHT_FETCHES.pop(cache_key, None)
            if live_jobs:
                self._store_production_cached_jobs(query=query, location=location, limit=limit, jobs=live_jobs)
                self._persist_production_jobs(query=query, location=location, jobs=live_jobs)
                return live_jobs
            if sparse_role:
                return []
            if cached_seed:
                self.last_fetch_diagnostics["db_cache_rescue"] = True
                self._store_production_cached_jobs(query=query, location=location, limit=limit, jobs=cached_seed)
                return cached_seed
            if not sparse_role:
                cached_fallback = self._get_cached_production_fallback(query=query, location=location, limit=limit)
                if cached_fallback:
                    self._store_production_cached_jobs(query=query, location=location, limit=limit, jobs=cached_fallback)
                    return cached_fallback

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
                    self._annotate_item_scores(query=query, location=location, item=item)
                filtered_cached = self._filter_relevant_jobs(query, cached, location=location)
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
                        self._annotate_item_scores(query=query, location=location, item=item)
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
            preferred_live = self._select_production_live_jobs(query=query, location=location, jobs=collected, limit=limit)
            if preferred_live:
                logger.info(
                    "Production live selection kept %s jobs from %s collected candidates for query=%s",
                    len(preferred_live),
                    len(collected),
                    query,
                )
                return preferred_live

        collected = self._filter_relevant_jobs(query, collected, location=location)

        if use_cache and collected:
            collected = JobCacheService(self.db).store_jobs(jobs=collected[:limit], query=query, location=location)
        return collected[:limit]

    def _get_cached_production_fallback(self, *, query: str, location: str, limit: int) -> list[dict]:
        if self.db is None or not settings.enable_job_cache:
            return []
        try:
            cache = JobCacheService(self.db)
            cached = cache.get_cached_jobs_any_location(query=query, limit=max(limit * 6, settings.production_live_candidate_fetch))
        except Exception as exc:
            logger.warning("Production cache fallback lookup failed for query=%s: %s", query, exc)
            return []
        if not cached:
            return []
        for item in cached:
            item.setdefault("normalized_data", {})
            self._annotate_item_scores(query=query, location=location, item=item)
        exact_query_cached = [item for item in cached if self._cache_query_bucket(query, item) == "exact"]
        related_query_cached = [item for item in cached if self._cache_query_bucket(query, item) == "related"]
        canonical_query_cached = [
            item
            for item in cached
            if self._cache_query_bucket(query, item) == "canonical" and self._is_cache_role_safe_match(query, item)
        ]
        selected = []
        for bucket_name, bucket_jobs in (
            ("exact", exact_query_cached),
            ("related", related_query_cached),
            ("canonical", canonical_query_cached),
        ):
            if not bucket_jobs:
                continue
            selected = self._select_production_live_jobs(query=query, location=location, jobs=bucket_jobs, limit=limit)
            if selected:
                self.last_fetch_diagnostics["cache_fallback_bucket"] = bucket_name
                break
        if selected:
            self.last_fetch_diagnostics["cache_fallback_hit"] = True
            self.last_fetch_diagnostics["cache_fallback_count"] = len(selected)
            self.last_fetch_diagnostics["cache_fallback_bucket_counts"] = {
                "exact": len(exact_query_cached),
                "related": len(related_query_cached),
                "canonical": len(canonical_query_cached),
            }
        return selected

    def _query_signature(self, value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+ ]+", " ", str(value).lower())).strip()

    def _cache_query_bucket(self, query: str, item: dict) -> str:
        query_signature = self._query_signature(query)
        cached_query = self._query_signature(item.get("query_role", ""))
        if not cached_query:
            return "none"
        if cached_query == query_signature:
            return "exact"

        role_variants = {self._query_signature(variant) for variant in production_query_variations(query)}
        if cached_query in role_variants:
            return "related"

        cached_normalized = normalize_role(str(item.get("normalized_role") or item.get("query_role") or ""))
        if cached_normalized == normalize_role(query):
            return "canonical"
        return "none"

    def _is_cache_role_safe_match(self, query: str, item: dict) -> bool:
        if self._canonical_role_alignment(query, item) < 0:
            return False
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        return (
            title_precision >= 2
            or title_overlap >= 1
            or (family_overlap >= 2 and core_title_overlap >= 1)
            or (domain_score >= 2 and skill_overlap >= 1.5)
        )

    async def _fetch_production_jobs(
        self,
        *,
        query: str,
        location: str,
        limit: int,
    ) -> list[dict]:
        query_domain = role_domain(query)
        query_profile = role_profile(query)
        fetch_started_at = time.perf_counter()

        def _remaining_runtime_budget(*, reserve_seconds: float = 0.0) -> float:
            return max(
                settings.production_live_runtime_cap_seconds
                - (time.perf_counter() - fetch_started_at)
                - reserve_seconds,
                0.0,
            )

        def _silence_background_task(task: asyncio.Task) -> None:
            def _consume_result(completed: asyncio.Task) -> None:
                try:
                    completed.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            task.add_done_callback(_consume_result)

        async def safe_search(provider: object, search_query: str, search_location: str, stage: str) -> list[dict]:
            source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
            if source_name == "jobicy":
                provider_timeout = 8.5
            elif source_name == "greenhouse":
                if query_domain == "data":
                    provider_timeout = 8.0
                elif query_domain in {"software", "security"}:
                    provider_timeout = 10.0
                else:
                    provider_timeout = 8.5
            elif source_name == "lever":
                provider_timeout = 8.5
            elif source_name == "jooble":
                provider_timeout = 7.5
            elif source_name == "adzuna":
                provider_timeout = 7.5
            elif source_name == "remotive":
                provider_timeout = 8.0
            elif source_name == "themuse":
                provider_timeout = 7.5
            elif source_name == "findwork":
                provider_timeout = 7.0
            elif source_name == "remoteok":
                provider_timeout = 3.0
            else:
                provider_timeout = 5.0
            base_provider_timeout = provider_timeout
            remaining_global_budget = _remaining_runtime_budget(reserve_seconds=1.0)
            provider_diag = {
                "provider": provider.__class__.__name__,
                "source": source_name,
                "requested_timeout_seconds": base_provider_timeout,
                "query": search_query,
                "location": search_location,
                "stage": stage,
            }
            if remaining_global_budget <= 0.75:
                provider_diag["timeout_seconds"] = 0.0
                provider_diag["error"] = "skipped_insufficient_runtime_budget"
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return []
            provider_timeout = min(base_provider_timeout, max(0.75, remaining_global_budget))
            provider_diag["timeout_seconds"] = round(provider_timeout, 2)
            try:
                items = await asyncio.wait_for(
                    provider.search(query=search_query, location=search_location, limit=max(8, limit)),
                    timeout=provider_timeout,
                )
                provider_diag["result_count"] = len(items)
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return items
            except Exception as exc:
                error_message = str(exc) or exc.__class__.__name__
                logger.warning(
                    "Production job provider search failed for %s (query=%s, location=%s): %s",
                    provider.__class__.__name__,
                    search_query,
                    search_location,
                    error_message,
                )
                provider_diag["error"] = error_message
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return []

        collected: list[dict] = []
        seen: set[str] = set()
        near_seen: set[str] = set()
        sparse_role = is_sparse_live_market_role(query)
        live_floor = self._production_display_floor(query=query, limit=limit)
        partial_live_floor = self._production_partial_live_floor(query=query, limit=limit)

        def absorb_results(provider_results: list[list[dict]]) -> None:
            for items in provider_results:
                for item in items:
                    key = dedupe_key(item)
                    if key in seen:
                        continue
                    similarity_signature = self._job_similarity_signature(item)
                    if similarity_signature in near_seen:
                        continue
                    seen.add(key)
                    near_seen.add(similarity_signature)
                    item.setdefault("normalized_data", {})
                    item.setdefault("preview", truncate(str(item.get("description", "")), 260))
                    self._annotate_item_scores(query=query, location=location, item=item)
                    collected.append(item)

        async def _cancel_pending_tasks(pending: set[asyncio.Task], *, stage: str, soft_timeout: float, reason: str) -> None:
            if not pending:
                return
            for task in pending:
                task.cancel()
            done_after_cancel, still_pending = await asyncio.wait(pending, timeout=0.35)
            for task in still_pending:
                _silence_background_task(task)
            self.last_fetch_diagnostics.setdefault("stage_short_circuits", []).append(
                {
                    "stage": stage,
                    "cancelled_pending_tasks": len(pending),
                    "soft_timeout_seconds": soft_timeout,
                    "reason": reason,
                    "detached_pending_tasks": len(still_pending),
                    "cancelled_and_drained_tasks": len(done_after_cancel),
                }
            )

        async def run_stage(stage: str, providers: list[object]) -> list[dict]:
            tasks = []
            for provider in providers:
                search_queries = self._search_queries(provider, query)
                search_locations = self._search_locations(provider, location)
                for search_query in search_queries:
                    for search_location in search_locations[:1]:
                        task = asyncio.create_task(safe_search(provider, search_query, search_location, stage))
                        _silence_background_task(task)
                        tasks.append(task)
            if not tasks:
                return []
            base_soft_timeout = self._production_stage_soft_timeout(
                stage=stage,
                query_domain=query_domain,
                sparse_role=sparse_role,
            )
            if stage == "primary":
                reserve_seconds = 12.0 if not sparse_role else 5.0
            elif stage == "supplemental":
                reserve_seconds = 5.0
            else:
                reserve_seconds = 1.5
            remaining_global_budget = _remaining_runtime_budget(reserve_seconds=reserve_seconds)
            if remaining_global_budget <= 0.75:
                self.last_fetch_diagnostics.setdefault("stage_short_circuits", []).append(
                    {
                        "stage": stage,
                        "cancelled_pending_tasks": 0,
                        "soft_timeout_seconds": 0.0,
                        "reason": "insufficient_global_budget",
                        "detached_pending_tasks": 0,
                        "cancelled_and_drained_tasks": 0,
                    }
                )
                return self._select_production_live_jobs(query=query, location=location, jobs=collected, limit=limit)
            soft_timeout = min(base_soft_timeout, max(0.75, remaining_global_budget))
            started_at = time.perf_counter()
            pending = set(tasks)
            while pending:
                elapsed = time.perf_counter() - started_at
                remaining = soft_timeout - elapsed
                if remaining <= 0:
                    break
                done, pending = await asyncio.wait(
                    pending,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    break
                provider_results = [task.result() for task in done if not task.cancelled()]
                if provider_results:
                    absorb_results(provider_results)
                preferred_live = self._select_production_live_jobs(query=query, location=location, jobs=collected, limit=limit)
                if len(preferred_live) >= live_floor:
                    if pending:
                        await _cancel_pending_tasks(
                            pending,
                            stage=stage,
                            soft_timeout=soft_timeout,
                            reason="floor_reached",
                        )
                    return preferred_live
            if pending:
                await _cancel_pending_tasks(
                    pending,
                    stage=stage,
                    soft_timeout=soft_timeout,
                    reason="soft_timeout",
                )
            return self._select_production_live_jobs(query=query, location=location, jobs=collected, limit=limit)

        source_groups: dict[str, list[object]] = {}
        for provider in self.providers:
            source_groups.setdefault(str(getattr(provider, "source_name", provider.__class__.__name__)).lower(), []).append(provider)

        stage_results: list[dict] = []
        if sparse_role:
            primary_sources = ["remotive"]
            supplemental_sources: list[str] = []
        else:
            if query_domain == "data":
                # Render handles the faster API-driven providers well enough for
                # the first pass, but The Muse often turns an otherwise-valid
                # live fetch into a 20s+ stage on free-tier instances. Keep the
                # fast APIs first and use lighter ATS board feeds as the backup.
                primary_order = ["remotive", "jobicy", "jooble", "adzuna"]
                supplemental_order = ["lever", "indianapi"]
            elif query_domain == "security":
                primary_order = ["greenhouse", "lever", "remotive", "jooble", "adzuna"]
                supplemental_order = ["jobicy", "themuse", "indianapi"]
            elif query_domain == "software":
                primary_order = ["greenhouse", "lever", "remotive", "jobicy", "jooble", "adzuna"]
                supplemental_order = ["themuse", "indianapi"]
            elif query_domain in {"product", "design"}:
                primary_order = ["greenhouse", "jobicy", "themuse", "jooble", "adzuna"]
                supplemental_order = ["lever", "remotive", "indianapi"]
            else:
                primary_order = ["greenhouse", "lever", "jooble", "adzuna", "jobicy"]
                supplemental_order = ["remotive", "themuse"]

            primary_sources = [source for source in primary_order if source in source_groups]
            supplemental_sources = [source for source in supplemental_order if source in source_groups and source not in primary_sources]
        fallback_sources = [source for source in source_groups.keys() if source not in {*primary_sources, *supplemental_sources}]
        if query_domain not in {"software", "security"}:
            fallback_sources = [source for source in fallback_sources if source != "remoteok"]
        if query_domain == "data":
            # Greenhouse and Lever add a lot of ATS-board payload for data-role
            # searches on Render free tier, but they rarely improve the final
            # selected set enough to justify the extra memory and restart risk.
            fallback_sources = [source for source in fallback_sources if source not in {"greenhouse", "lever"}]
        elif (
            query_profile.normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES
            or any(head in {"admin", "administrator", "consultant", "manager", "designer", "writer"} for head in query_profile.head_terms)
        ):
            fallback_sources = [source for source in fallback_sources if source != "remoteok"]
        self.last_fetch_diagnostics["provider_plan"] = {
            "active_sources": sorted(source_groups.keys()),
            "primary_sources": primary_sources,
            "supplemental_sources": supplemental_sources,
            "fallback_sources": fallback_sources,
        }
        logger.info(
            "Production provider plan for query=%s: active=%s primary=%s supplemental=%s fallback=%s",
            query,
            sorted(source_groups.keys()),
            primary_sources,
            supplemental_sources,
            fallback_sources,
        )
        primary_providers = [provider for source in primary_sources for provider in source_groups.get(source, [])]
        preferred_live = await run_stage("primary", primary_providers)
        primary_selected_count = len(preferred_live)
        stage_results.append(
            {
                "stage": "primary",
                "sources": primary_sources,
                "collected_candidates": len(collected),
                "selected_live": len(preferred_live),
            }
        )
        logger.info(
            "Production stage result for query=%s: stage=primary candidates=%s selected=%s",
            query,
            len(collected),
            len(preferred_live),
        )
        if len(preferred_live) >= live_floor:
            self.last_fetch_diagnostics["stage_results"] = stage_results
            self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
            self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
            self.last_fetch_diagnostics["selected_live_sources"] = {
                source: len([item for item in preferred_live if item.get("source") == source])
                for source in sorted({item.get("source", "unknown") for item in preferred_live})
            }
            logger.info(
                "Production live selection reached floor with %s jobs from %s collected candidates for query=%s",
                len(preferred_live),
                len(collected),
                query,
            )
            return preferred_live
        if sparse_role:
            self.last_fetch_diagnostics["stage_results"] = stage_results
            self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
            self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
            self.last_fetch_diagnostics["selected_live_sources"] = {
                source: len([item for item in preferred_live if item.get("source") == source])
                for source in sorted({item.get("source", "unknown") for item in preferred_live})
            }
            if preferred_live:
                return preferred_live
            return []

        supplemental_providers = [provider for source in supplemental_sources for provider in source_groups.get(source, [])]
        if supplemental_providers:
            preferred_live = await run_stage("supplemental", supplemental_providers)
            stage_results.append(
                {
                    "stage": "supplemental",
                    "sources": supplemental_sources,
                    "collected_candidates": len(collected),
                    "selected_live": len(preferred_live),
                }
            )
            logger.info(
                "Production stage result for query=%s: stage=supplemental candidates=%s selected=%s",
                query,
                len(collected),
                len(preferred_live),
            )
            if len(preferred_live) >= live_floor:
                self.last_fetch_diagnostics["stage_results"] = stage_results
                self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
                self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
                self.last_fetch_diagnostics["selected_live_sources"] = {
                    source: len([item for item in preferred_live if item.get("source") == source])
                    for source in sorted({item.get("source", "unknown") for item in preferred_live})
                }
                logger.info(
                    "Production live selection reached floor after supplemental fetch with %s jobs from %s candidates for query=%s",
                    len(preferred_live),
                    len(collected),
                    query,
                )
                return preferred_live
            elapsed_after_supplemental = time.perf_counter() - fetch_started_at
            if len(preferred_live) >= partial_live_floor:
                self.last_fetch_diagnostics["stage_results"] = stage_results
                self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
                self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
                self.last_fetch_diagnostics["selected_live_sources"] = {
                    source: len([item for item in preferred_live if item.get("source") == source])
                    for source in sorted({item.get("source", "unknown") for item in preferred_live})
                }
                self.last_fetch_diagnostics["partial_live_return"] = {
                    "stage": "supplemental",
                    "selected_live": len(preferred_live),
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_after_supplemental, 2),
                    "reason": "acceptable_partial_live_set",
                }
                logger.info(
                    "Production live selection accepted partial result after supplemental fetch with %s jobs in %ss for query=%s",
                    len(preferred_live),
                    round(elapsed_after_supplemental, 2),
                    query,
                )
                return preferred_live
            if preferred_live and (
                (len(preferred_live) == primary_selected_count and query_domain != "data")
                or elapsed_after_supplemental >= 18.0
                or query_domain in {"product", "design"}
            ):
                self.last_fetch_diagnostics["stage_results"] = stage_results
                self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
                self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
                self.last_fetch_diagnostics["selected_live_sources"] = {
                    source: len([item for item in preferred_live if item.get("source") == source])
                    for source in sorted({item.get("source", "unknown") for item in preferred_live})
                }
                self.last_fetch_diagnostics["partial_live_return"] = {
                    "stage": "supplemental",
                    "selected_live": len(preferred_live),
                    "primary_selected_live": primary_selected_count,
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_after_supplemental, 2),
                    "reason": "preserve_response_after_supplemental",
                }
                logger.info(
                    "Production live selection returned early after supplemental fetch with %s jobs in %ss for query=%s",
                    len(preferred_live),
                    round(elapsed_after_supplemental, 2),
                    query,
                )
                return preferred_live

        fallback_providers = [provider for source in fallback_sources for provider in source_groups.get(source, [])]
        if fallback_providers:
            elapsed_before_fallback = time.perf_counter() - fetch_started_at
            remaining_budget = max(settings.production_live_runtime_cap_seconds - elapsed_before_fallback, 0.0)
            if preferred_live and (
                len(preferred_live) >= partial_live_floor
                or remaining_budget <= 6.0
            ):
                self.last_fetch_diagnostics["stage_results"] = stage_results
                self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
                self.last_fetch_diagnostics["selected_live_count"] = len(preferred_live)
                self.last_fetch_diagnostics["selected_live_sources"] = {
                    source: len([item for item in preferred_live if item.get("source") == source])
                    for source in sorted({item.get("source", "unknown") for item in preferred_live})
                }
                self.last_fetch_diagnostics["partial_live_return"] = {
                    "stage": "pre-fallback",
                    "selected_live": len(preferred_live),
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_before_fallback, 2),
                    "remaining_budget_seconds": round(remaining_budget, 2),
                    "reason": "preserve_response_budget",
                }
                logger.info(
                    "Skipping fallback stage and returning %s live jobs with %ss remaining budget for query=%s",
                    len(preferred_live),
                    round(remaining_budget, 2),
                    query,
                )
                return preferred_live
            preferred_live = await run_stage("fallback", fallback_providers)
            stage_results.append(
                {
                    "stage": "fallback",
                    "sources": fallback_sources,
                    "collected_candidates": len(collected),
                    "selected_live": len(preferred_live),
                }
            )
            logger.info(
                "Production stage result for query=%s: stage=fallback candidates=%s selected=%s",
                query,
                len(collected),
                len(preferred_live),
            )

        self.last_fetch_diagnostics["stage_results"] = stage_results
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

    def _select_production_live_jobs(self, *, query: str, location: str, jobs: list[dict], limit: int) -> list[dict]:
        live_jobs = [item for item in jobs if item.get("source") != "role-baseline"]
        if not live_jobs:
            return []

        target_live_count = self._production_live_target(query=query, limit=limit)
        display_floor = self._production_display_floor(query=query, limit=limit)

        ranked = sorted(
            live_jobs,
            key=lambda item: (
                self._canonical_role_alignment(query, item),
                float(item.get("normalized_data", {}).get("title_alignment_score", 0.0)),
                self._title_precision_score(query, item),
                self._role_domain_match_score(query, item),
                self._location_alignment_score(location, item),
                self._title_hint_overlap(query, item),
                self._core_token_overlap(query, item, include_description=False),
                self._skill_overlap_score(query, item),
                float(item.get("normalized_data", {}).get("role_fit_score", 0.0)),
                float(item.get("normalized_data", {}).get("market_quality_score", 0.0)),
            ),
            reverse=True,
        )
        precision_guarded = [item for item in ranked if self._passes_precise_query_guard(query, item)]
        if precision_guarded and len(precision_guarded) >= display_floor:
            ranked = precision_guarded
        elif precision_guarded:
            ranked = precision_guarded + [item for item in ranked if item not in precision_guarded]
        selected: list[dict] = []
        company_counts: dict[str, int] = {}
        company_title_counts: dict[str, int] = {}
        selected_signatures: set[str] = set()
        source_counts: dict[str, int] = {}
        selection_debug: dict[str, int | list[str]] = {
            "target_live_count": target_live_count,
            "ranked_candidates": len(ranked),
            "precision_guarded_candidates": len(precision_guarded),
        }

        def maybe_add(candidates: list[dict], cap_per_company: int) -> None:
            for item in candidates:
                if item in selected:
                    continue
                company = normalize_role(str(item.get("company", "")).lower()) or "unknown"
                title_key = normalize_role(str(item.get("title", "")).lower()) or "unknown"
                company_title_key = f"{company}::{title_key}"
                similarity_signature = self._job_similarity_signature(item)
                source = str(item.get("source", "unknown")).lower()
                max_source_count = 5 if source == "greenhouse" else 4 if source == "lever" else 3
                if company_counts.get(company, 0) >= cap_per_company:
                    continue
                if company_title_counts.get(company_title_key, 0) >= 1:
                    continue
                if similarity_signature in selected_signatures:
                    continue
                if len(selected) >= 4 and source_counts.get(source, 0) >= max_source_count:
                    continue
                selected.append(item)
                company_counts[company] = company_counts.get(company, 0) + 1
                company_title_counts[company_title_key] = company_title_counts.get(company_title_key, 0) + 1
                source_counts[source] = source_counts.get(source, 0) + 1
                selected_signatures.add(similarity_signature)
                if len(selected) >= limit:
                    break

        strong_candidates = [item for item in ranked if self._is_production_live_candidate(query, location, item, strict=True)]
        selection_debug["strong_candidates"] = len(strong_candidates)
        maybe_add(strong_candidates, cap_per_company=2)
        if len(selected) < target_live_count:
            secondary_candidates = [item for item in ranked if self._is_production_live_candidate(query, location, item, strict=False)]
            selection_debug["secondary_candidates"] = len(secondary_candidates)
            maybe_add(secondary_candidates, cap_per_company=3)
        if len(selected) < target_live_count:
            family_candidates = [item for item in ranked if self._is_family_live_candidate(query, location, item)]
            selection_debug["family_candidates"] = len(family_candidates)
            maybe_add(family_candidates, cap_per_company=4)
        if len(selected) < target_live_count:
            dense_jobicy_candidates = [
                item
                for item in ranked
                if item.get("source") == "jobicy"
                and (
                    self._title_hint_overlap(query, item) >= 1
                    or self._family_token_overlap(query, item) >= 1
                    or self._role_domain_match_score(query, item) >= 2
                )
            ]
            selection_debug["dense_jobicy_candidates"] = len(dense_jobicy_candidates)
            maybe_add(dense_jobicy_candidates, cap_per_company=4)
        if len(selected) < target_live_count:
            tertiary_candidates = [
                item
                for item in ranked
                if self._canonical_role_alignment(query, item) >= 0
                and (
                    self._title_hint_overlap(query, item) >= 1
                    or self._title_precision_score(query, item) >= 2
                    or self._family_token_overlap(query, item) >= 2
                    or (
                        self._role_domain_match_score(query, item) >= 2
                        and self._title_precision_score(query, item) >= 1
                    )
                )
            ]
            selection_debug["tertiary_candidates"] = len(tertiary_candidates)
            maybe_add(tertiary_candidates, cap_per_company=4)
        if len(selected) < target_live_count:
            themuse_dense_candidates = [
                item
                for item in ranked
                if item.get("source") == "themuse"
                and self._canonical_role_alignment(query, item) >= 0
                and (
                    self._title_precision_score(query, item) >= 2
                    or self._title_hint_overlap(query, item) >= 1
                    or self._family_token_overlap(query, item) >= 2
                )
            ]
            selection_debug["themuse_dense_candidates"] = len(themuse_dense_candidates)
            maybe_add(themuse_dense_candidates, cap_per_company=4)
        if len(selected) < target_live_count:
            fallback_candidates = [
                item
                for item in ranked
                if self._canonical_role_alignment(query, item) >= 0
                and (
                    self._title_hint_overlap(query, item) >= 1
                    or self._title_precision_score(query, item) >= 2
                    or self._family_token_overlap(query, item) >= 2
                    or (
                        self._role_domain_match_score(query, item) >= 2
                        and self._title_precision_score(query, item) >= 1
                    )
                )
            ]
            selection_debug["fallback_candidates"] = len(fallback_candidates)
            maybe_add(fallback_candidates, cap_per_company=5)
        if len(selected) < display_floor:
            last_resort_candidates = [
                item
                for item in ranked
                if item.get("source") == "jobicy"
                and self._canonical_role_alignment(query, item) >= 0
                and float(item.get("normalized_data", {}).get("market_quality_score", 0.0)) >= 18.0
            ]
            selection_debug["last_resort_jobicy_candidates"] = len(last_resort_candidates)
            maybe_add(last_resort_candidates, cap_per_company=5)
        if len(selected) < display_floor:
            last_resort_themuse = [
                item
                for item in ranked
                if item.get("source") == "themuse"
                and self._canonical_role_alignment(query, item) >= 0
                and self._title_precision_score(query, item) >= 1
                and float(item.get("normalized_data", {}).get("market_quality_score", 0.0)) >= 18.0
            ]
            selection_debug["last_resort_themuse_candidates"] = len(last_resort_themuse)
            maybe_add(last_resort_themuse, cap_per_company=5)

        selected = [item for item in selected if self._passes_final_live_guard(query, item)]

        filtered_source_counts: dict[str, int] = {}
        for item in selected:
            source = str(item.get("source", "unknown")).lower()
            filtered_source_counts[source] = filtered_source_counts.get(source, 0) + 1
        selection_debug["selected_count"] = len(selected)
        selection_debug["selected_titles"] = [str(item.get("title", "")) for item in selected[:8]]
        selection_debug["selected_sources"] = filtered_source_counts
        self.last_fetch_diagnostics["selection_debug"] = selection_debug
        if len(selected) < display_floor:
            logger.info(
                "Production selection debug for query=%s: %s",
                query,
                selection_debug,
            )
        return selected[:limit]

    def _passes_final_live_guard(self, query: str, item: dict) -> bool:
        query_domain = role_domain(query)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        specialty_overlap = self._specialty_token_overlap(query, item)
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))

        if canonical_alignment <= -3:
            return False
        if self._requires_specialty_guard(query) and specialty_overlap == 0:
            return False
        if query_domain in {"software", "security"} and title_precision <= 0 and title_overlap == 0 and family_overlap == 0:
            return False
        if query_domain == "data" and canonical_alignment < 0 and title_overlap == 0 and title_precision <= 0:
            return False
        if role_fit < 1.0 and title_precision <= 0 and core_title_overlap == 0:
            return False
        return True

    def _passes_precise_query_guard(self, query: str, item: dict) -> bool:
        raw_query = self._query_signature(query)
        normalized_query = self._query_signature(normalize_role(query))
        raw_tokens = [
            token
            for token in raw_query.split()
            if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS
        ]
        if not raw_tokens or raw_query == normalized_query or len(raw_tokens) > 2:
            return True
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False

        title_text = self._query_signature(item.get("title", ""))
        description_text = self._query_signature(item.get("description", ""))
        tags_text = " ".join(self._query_signature(tag) for tag in item.get("tags", []) if str(tag).strip())
        if self._is_mobile_web_mismatch(query, title_text):
            return False
        raw_phrase_hit = raw_query in title_text or raw_query in description_text or raw_query in tags_text
        raw_token_hits = sum(
            1
            for token in raw_tokens
            if re.search(rf"\b{re.escape(token)}\b", title_text)
            or re.search(rf"\b{re.escape(token)}\b", description_text)
            or re.search(rf"\b{re.escape(token)}\b", tags_text)
        )
        if raw_phrase_hit or raw_token_hits >= 1:
            return True
        if self._title_hint_overlap(query, item) >= 1:
            return True
        if self._title_precision_score(query, item) >= 2:
            return True
        if self._skill_overlap_score(query, item) >= 1.5 and self._role_domain_match_score(query, item) >= 2:
            return True
        return False

    def _is_mobile_web_mismatch(self, query: str, title_text: str) -> bool:
        normalized_query = normalize_role(query)
        if normalized_query not in {"frontend developer", "full stack developer"}:
            return False
        mobile_tokens = {"ios", "android", "mobile", "swift", "kotlin", "flutter"}
        web_tokens = {"react", "frontend", "web", "javascript", "typescript", "ui"}
        return any(token in title_text for token in mobile_tokens) and not any(token in title_text for token in web_tokens)

    def _contains_phrase(self, haystack: str, needle: str) -> bool:
        cleaned_haystack = re.sub(r"[^a-z0-9+ ]+", " ", str(haystack).lower()).strip()
        cleaned_haystack = re.sub(r"\s+", " ", cleaned_haystack)
        cleaned_needle = re.sub(r"[^a-z0-9+ ]+", " ", str(needle).lower()).strip()
        cleaned_needle = re.sub(r"\s+", " ", cleaned_needle)
        if not cleaned_haystack or not cleaned_needle:
            return False
        if " " in cleaned_needle:
            return cleaned_needle in cleaned_haystack
        return bool(re.search(rf"\b{re.escape(cleaned_needle)}\b", cleaned_haystack))

    def _job_similarity_signature(self, item: dict) -> str:
        company = normalize_role(str(item.get("company", "")).lower()) or "unknown"
        title = normalize_role(str(item.get("title", "")).lower()) or "unknown"
        description = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("description", "")).lower())
        desc_tokens = [
            token
            for token in description.split()
            if len(token) > 3 and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS
        ][:12]
        return f"{company}::{title}::{' '.join(desc_tokens[:10])}"

    def _requires_specialty_guard(self, query: str) -> bool:
        profile = role_profile(query)
        return bool(profile.specialty_tokens) and (
            profile.normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES
            or profile.cleaned_query != profile.normalized_role
        )

    def _specialty_token_overlap(self, query: str, item: dict) -> int:
        profile = role_profile(query)
        specialty_tokens = {
            token
            for token in profile.specialty_tokens
            if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS
        }
        if not specialty_tokens:
            return 0
        normalized = item.get("normalized_data", {}) or {}
        extracted_skills = " ".join(str(skill).lower() for skill in normalized.get("skills", []) or [])
        haystack = " ".join(
            [
                re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("title", "")).lower()),
                re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("description", "")).lower()),
                " ".join(re.sub(r"[^a-z0-9+ ]+", " ", str(tag).lower()) for tag in item.get("tags", []) if str(tag).strip()),
                extracted_skills,
            ]
        )
        return sum(1 for token in specialty_tokens if re.search(rf"\b{re.escape(token)}\b", haystack))

    def _has_explicit_role_alignment(self, query: str, item: dict) -> bool:
        title_text = str(item.get("title", "")).lower()
        negative_hints = role_negative_title_hints(query)
        if negative_hints and any(self._contains_phrase(title_text, hint) for hint in negative_hints):
            return False
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        role_fit = float(item.get("normalized_data", {}).get("role_fit_score", 0.0))
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        core_text_overlap = self._core_token_overlap(query, item, include_description=True)
        canonical_alignment = self._canonical_role_alignment(query, item)
        return (
            canonical_alignment >= 0
            and (
            title_overlap >= 1
            or (family_overlap >= 2 and core_title_overlap >= 1)
            or (domain_score >= 2 and self._title_precision_score(query, item) >= 1)
            or self._title_precision_score(query, item) >= 1
            or core_text_overlap >= 2
            or (domain_score >= 1 and skill_overlap >= 1.0 and self._title_precision_score(query, item) >= 1)
            or (title_overlap >= 1 and role_fit >= 1.0)
            )
        )

    def _core_token_overlap(self, query: str, item: dict, *, include_description: bool) -> int:
        query_tokens = role_query_tokens(query)
        if not query_tokens:
            return 0
        title_text = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("title", "")).lower())
        haystack = title_text
        if include_description:
            desc_text = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("description", "")).lower())
            haystack = f"{title_text} {desc_text}"
        return sum(1 for token in query_tokens if re.search(rf"\b{re.escape(token)}\b", haystack))

    def _canonical_role_alignment(self, query: str, item: dict) -> int:
        title_text = str(item.get("title", "")).lower()
        negative_hints = role_negative_title_hints(query)
        if negative_hints and any(self._contains_phrase(title_text, hint) for hint in negative_hints):
            return -3
        return canonical_role_alignment(query, str(item.get("title", "")))

    def _title_precision_score(self, query: str, item: dict) -> int:
        title_overlap = self._title_hint_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        family_overlap = self._family_token_overlap(query, item)
        if title_overlap >= 1:
            return title_overlap * 2
        if core_title_overlap >= 2:
            return core_title_overlap + family_overlap
        if family_overlap >= 2:
            return family_overlap
        return 0

    def _is_production_live_candidate(self, query: str, location: str, item: dict, *, strict: bool) -> bool:
        normalized = item.get("normalized_data", {}) or {}
        query_domain = role_domain(query)
        role_fit = float(normalized.get("role_fit_score", 0.0))
        market_quality = float(normalized.get("market_quality_score", 0.0))
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        domain_score = self._role_domain_match_score(query, item)
        location_score = self._location_alignment_score(location, item)
        source = str(item.get("source", ""))
        explicit_alignment = self._has_explicit_role_alignment(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_alignment = float(normalized.get("title_alignment_score", 0.0))
        specialty_overlap = self._specialty_token_overlap(query, item)

        if self._is_location_hard_mismatch(location, item):
            return False
        if self._requires_specialty_guard(query) and specialty_overlap == 0:
            return False
        if canonical_alignment < 0 and title_overlap == 0 and core_title_overlap == 0:
            return False
        if canonical_alignment < 0 and title_precision <= 0 and title_overlap == 0:
            return False
        if title_alignment <= -6.0:
            return False
        if (
            query_domain == "data"
            and not explicit_alignment
            and title_precision <= 0
            and title_overlap == 0
            and core_title_overlap == 0
            and family_overlap == 0
            and role_fit < 3.25
        ):
            return False
        if query_domain in {"software", "security"} and title_precision <= 0 and title_overlap == 0 and family_overlap == 0:
            return False
        if is_sparse_live_market_role(query):
            if title_precision <= 0 and title_overlap == 0 and family_overlap == 0:
                return False
            if strict:
                return (
                    (skill_overlap >= 1.0 and (title_precision >= 1 or family_overlap >= 1 or explicit_alignment))
                    or (title_overlap >= 1 and market_quality >= 36.0 and role_fit >= 1.0)
                )
            return (
                (skill_overlap >= 1.0 and domain_score >= 1 and (title_precision >= 1 or family_overlap >= 1))
                or (title_overlap >= 1 and market_quality >= 30.0 and role_fit >= 1.0)
            )

        if strict:
            if location_score < 0.2:
                return False
            if not explicit_alignment and market_quality < 48.0:
                return False
            if title_precision <= 0 and domain_score < 2 and not explicit_alignment:
                return False
            if source in {"themuse", "jobicy"} and domain_score >= 2 and (title_overlap >= 1 or skill_overlap >= 1.5 or family_overlap >= 1):
                return True
            if core_title_overlap >= 1 and skill_overlap >= 1.0:
                return True
            if source in {"themuse", "jobicy"} and (title_overlap >= 1 or family_overlap >= 2) and market_quality >= 18.0:
                return True
            if title_overlap >= 1 and role_fit >= 1.0:
                return True
            if title_overlap >= 1 and skill_overlap >= 1.0:
                return True
            if domain_score >= 2 and skill_overlap >= 1.5:
                return True
            if family_overlap >= 2 and role_fit >= 3.0 and skill_overlap >= 1.0:
                return True
            if title_overlap >= 2 and (role_fit >= 1.6 or skill_overlap >= 1.0):
                return True
            if title_overlap == 0 and role_fit >= 6.0 and skill_overlap >= 2.0 and market_quality >= 40.0:
                return True
            if source in {"remotive", "remoteok", "arbeitnow"} and title_overlap >= 1 and market_quality >= 28.0:
                return True
            return False

        if location_score <= 0.0:
            return False
        if not explicit_alignment and market_quality < 56.0:
            return False
        if title_precision <= 0 and domain_score < 2 and not explicit_alignment:
            return False
        if source in {"themuse", "jobicy"} and domain_score >= 2:
            return True
        if core_title_overlap >= 1 and (skill_overlap >= 1.0 or role_fit >= 2.0):
            return True
        if source in {"themuse", "jobicy"} and (title_overlap >= 1 or family_overlap >= 2):
            return True
        if title_overlap >= 1 and role_fit >= 0.5:
            return True
        if title_overlap >= 1 and role_fit >= 1.0:
            return True
        if domain_score >= 2 and (skill_overlap >= 1.0 or role_fit >= 1.5):
            return True
        if family_overlap >= 2 and role_fit >= 2.5:
            return True
        if title_overlap == 0 and skill_overlap >= 2.0 and role_fit >= 3.25:
            return True
        if market_quality >= 42.0 and title_overlap >= 1:
            return True
        return False

    def _is_family_live_candidate(self, query: str, location: str, item: dict) -> bool:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        market_quality = float(normalized.get("market_quality_score", 0.0))
        skill_overlap = self._skill_overlap_score(query, item)
        domain_score = self._role_domain_match_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        if self._is_location_hard_mismatch(location, item):
            return False
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False
        if self._canonical_role_alignment(query, item) < 0 and title_overlap == 0:
            return False
        if (
            role_domain(query) == "data"
            and title_overlap == 0
            and family_overlap == 0
            and self._title_precision_score(query, item) <= 0
            and role_fit < 3.25
        ):
            return False
        return (
            (domain_score >= 2 and skill_overlap >= 1.0)
            or (domain_score >= 1 and title_overlap >= 1)
            or (domain_score >= 1 and family_overlap >= 2 and role_fit >= 2.0)
            or (
                domain_score >= 1
                and market_quality >= 28.0
                and skill_overlap >= 1.0
                and self._title_precision_score(query, item) >= 1
            )
        )

    def _title_hint_overlap(self, query: str, item: dict) -> int:
        title = str(item.get("title", "")).lower()
        hints = role_title_hints(query)
        overlap = 0
        for hint in hints:
            pattern = rf"\b{re.escape(hint)}\b" if " " not in hint else re.escape(hint)
            if re.search(pattern, title):
                overlap += 1
        return overlap

    def _family_token_overlap(self, query: str, item: dict) -> int:
        title = normalize_role(str(item.get("title", "")).lower())
        title_tokens = {token for token in re.split(r"[^a-z0-9+]+", title) if token and len(token) > 2}
        query_tokens = {
            token
            for token in re.split(r"[^a-z0-9+]+", normalize_role(query))
            if token and len(token) > 2 and token not in {"engineer", "developer", "manager"}
        }
        return len(title_tokens & query_tokens)

    def _role_domain_match_score(self, query: str, item: dict) -> int:
        query_domain = role_domain(query)
        if not query_domain:
            return 0
        title_domain = role_domain(str(item.get("title", "")))
        score = 0
        if title_domain == query_domain:
            score += 2
        if self._title_hint_overlap(query, item) >= 1:
            score += 1
        if self._family_token_overlap(query, item) >= 1:
            score += 1
        return score

    def _production_live_target(self, *, query: str, limit: int) -> int:
        if is_sparse_live_market_role(query):
            return min(limit, 4)
        return min(limit, max(settings.production_live_display_minimum, settings.production_live_fetch_minimum))

    def _production_display_floor(self, *, query: str, limit: int) -> int:
        if is_sparse_live_market_role(query):
            return min(limit, 1)
        return min(limit, max(4, settings.production_live_display_minimum))

    def _production_partial_live_floor(self, *, query: str, limit: int) -> int:
        if is_sparse_live_market_role(query):
            return min(limit, 1)
        query_domain = role_domain(query)
        if query_domain in {"data", "security", "software"}:
            return min(limit, 3)
        return min(limit, 2)

    def _skill_overlap_score(self, query: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        market_overlap = len(skills & role_market_hints(query))
        primary_overlap = len(skills & role_primary_hints(query))
        return (primary_overlap * 1.5) + market_overlap

    def _filter_relevant_jobs(self, query: str, jobs: list[dict], *, location: str = "") -> list[dict]:
        ordered = sorted(
            jobs,
            key=lambda item: (
                self._location_alignment_score(location, item),
                item.get("normalized_data", {}).get("market_quality_score", 0.0),
            ),
            reverse=True,
        )
        filtered = [item for item in ordered if self._passes_quality_gate(query, item, location=location)]
        if filtered:
            return filtered
        if settings.environment == "production":
            relaxed = [
                item
                for item in ordered
                if float(item.get("normalized_data", {}).get("role_fit_score", 0.0)) >= 6.0
                and not self._is_location_hard_mismatch(location, item)
            ]
            return relaxed[: max(1, min(5, len(relaxed)))]
        return filtered if filtered else []

    def _listing_quality_score(self, query: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        title = normalize_role(str(item.get("title", "")))
        description = normalize_role(str(item.get("description", "")))
        skill_count = len({str(skill).lower() for skill in normalized.get("skills", []) or []})
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)

        score = 6.0
        word_count = len(description.split())
        if 90 <= word_count <= 950:
            score += 4.0
        elif 45 <= word_count <= 1200:
            score += 2.0
        if skill_count >= 6:
            score += 4.0
        elif skill_count >= 3:
            score += 2.0
        if requirement_quality >= 36.0:
            score += 3.0
        elif requirement_quality >= 22.0:
            score += 1.5
        if title_overlap >= 1:
            score += 2.0
        if family_overlap >= 1:
            score += 1.5
        if domain_score >= 2:
            score += 2.5
        if any(hint in description for hint in LOW_SIGNAL_DESCRIPTION_HINTS):
            score -= 4.0
        if title_overlap == 0 and family_overlap == 0:
            weakener_hits = sum(1 for token in GENERIC_TITLE_WEAKENERS if re.search(rf"\b{re.escape(token)}\b", title))
            score -= min(3.0, weakener_hits * 1.25)
        return round(max(0.0, min(score, 20.0)), 2)

    def _market_quality_score(self, query: str, location: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        listing_quality = float(normalized.get("listing_quality_score", self._listing_quality_score(query, item)))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        hint_overlap = len(skills & role_market_hints(query))
        primary_overlap = len(skills & role_primary_hints(query))
        location_score = self._location_alignment_score(location, item)
        listing_quality = float(normalized.get("listing_quality_score", self._listing_quality_score(query, item)))
        source_weight = SOURCE_TRUST_WEIGHTS.get(str(item.get("source", "")).lower(), 0.86)
        base_score = (role_fit * 10) + requirement_quality + min(10, skill_count * 1.5) + (hint_overlap * 8) + (primary_overlap * 12) + (location_score * 18) + (listing_quality * 1.7)
        return round(base_score * source_weight, 2)

    def _passes_quality_gate(self, query: str, item: dict, *, location: str = "") -> bool:
        normalized = item.get("normalized_data", {}) or {}
        title_text = str(item.get("title", "")).lower()
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        listing_quality = float(normalized.get("listing_quality_score", self._listing_quality_score(query, item)))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        explicit_alignment = self._has_explicit_role_alignment(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_alignment = float(normalized.get("title_alignment_score", 0.0))
        negative_hints = role_negative_title_hints(query)
        if self._is_location_hard_mismatch(location, item):
            return False
        if negative_hints and any(self._contains_phrase(title_text, hint) for hint in negative_hints):
            return False
        if canonical_alignment < 0 and title_overlap == 0 and core_title_overlap == 0:
            return False
        if title_alignment <= -6.0:
            return False
        if is_sparse_live_market_role(query):
            if skill_overlap < 1.0 and title_precision < 2:
                return False
            if requirement_quality < 18.0 and role_fit < 2.5:
                return False
        if settings.environment == "production" and item.get("source") in {"remotive", "remoteok", "arbeitnow"} and role_fit >= 5.0 and explicit_alignment:
            return True
        if not explicit_alignment and (title_overlap + family_overlap + domain_score) == 0:
            return False
        if title_precision <= 0 and domain_score < 2:
            return False
        if listing_quality < 4.5 and not explicit_alignment:
            return False
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
        if title_overlap == 0 and core_title_overlap == 0 and family_overlap == 0 and domain_score < 2 and skill_overlap < 1.5:
            return False

        return requirement_quality >= 22.0 or listing_quality >= 8.0 or skill_count >= 3 or role_fit >= 4.5

    def _location_alignment_score(self, requested_location: str, item: dict) -> float:
        requested = normalize_role(str(requested_location or "")).strip().lower()
        job_location = str(item.get("location", "") or "").strip().lower()
        remote = bool(item.get("remote"))

        if not requested or requested in GLOBAL_REMOTE_HINTS:
            return 1.0 if remote or any(hint in job_location for hint in GLOBAL_REMOTE_HINTS) else 0.82
        if requested in job_location:
            return 1.0

        if requested == "india":
            if any(token in job_location for token in INDIA_LOCATION_HINTS):
                return 1.0
            if any(token in job_location for token in ASIA_LOCATION_HINTS):
                return 0.88
            if remote and not job_location:
                return 0.84
            if any(token in job_location for token in GLOBAL_REMOTE_HINTS):
                return 0.82
            if remote and any(token in job_location for token in NON_INDIA_REGION_HINTS):
                return 0.72
            if any(token in job_location for token in NON_INDIA_REGION_HINTS):
                return 0.58
            return 0.66 if remote else 0.52

        if remote and not job_location:
            return 0.8
        if any(token in job_location for token in GLOBAL_REMOTE_HINTS):
            return 0.78
        if remote:
            return 0.68
        return 0.52

    def _is_location_hard_mismatch(self, requested_location: str, item: dict) -> bool:
        return False

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
            normalized = normalize_role(query)
            return [normalized or query]
        source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
        if settings.environment == "production":
            return provider_query_variations(query, source_name, production=True)
        return provider_query_variations(query, source_name, production=False)

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
