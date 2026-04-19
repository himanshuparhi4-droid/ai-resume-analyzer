from __future__ import annotations

import asyncio
import logging
import re

from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.jobs.arbeitnow import ArbeitnowProvider
from app.services.jobs.adzuna import AdzunaProvider
from app.services.jobs.cache import JobCacheService
from app.services.jobs.jobicy import JobicyProvider
from app.services.jobs.remoteok import RemoteOKProvider
from app.services.jobs.remotive import RemotiveProvider
from app.services.jobs.themuse import TheMuseProvider
from app.services.jobs.taxonomy import (
    dedupe_key,
    is_sparse_live_market_role,
    normalize_role,
    production_query_variations,
    query_variations,
    role_domain,
    role_fit_score,
    role_market_hints,
    role_primary_hints,
    role_query_tokens,
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
        providers: list[object] = [JobicyProvider()]

        def add(provider: object) -> None:
            if any(existing.__class__ is provider.__class__ for existing in providers):
                return
            providers.append(provider)

        if source in {"auto", "adzuna"} and settings.has_adzuna_credentials:
            add(AdzunaProvider())
        if source in {"auto", "usajobs"} and settings.has_usajobs_credentials:
            add(USAJobsProvider())

        if source in {"auto", "themuse", "jobicy"}:
            add(RemotiveProvider())
            if source == "themuse":
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
                    item["normalized_data"]["location_alignment_score"] = self._location_alignment_score(location, item)
                    item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, location, item)
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
                        item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                        item["normalized_data"]["location_alignment_score"] = self._location_alignment_score(location, item)
                        item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, location, item)
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

    async def _fetch_production_jobs(self, *, query: str, location: str, limit: int) -> list[dict]:
        async def safe_search(provider: object, search_query: str, search_location: str, stage: str) -> list[dict]:
            source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
            if source_name == "jobicy":
                provider_timeout = 6.5
            elif source_name == "remotive":
                provider_timeout = 3.75
            elif source_name == "themuse":
                provider_timeout = 8.0
            elif source_name == "remoteok":
                provider_timeout = 2.5
            else:
                provider_timeout = 4.0
            provider_diag = {
                "provider": provider.__class__.__name__,
                "source": source_name,
                "timeout_seconds": provider_timeout,
                "query": search_query,
                "location": search_location,
                "stage": stage,
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
        sparse_role = is_sparse_live_market_role(query)
        live_floor = self._production_display_floor(query=query, limit=limit)

        def absorb_results(provider_results: list[list[dict]]) -> None:
            for items in provider_results:
                for item in items:
                    key = dedupe_key(item)
                    if key in seen:
                        continue
                    seen.add(key)
                    item.setdefault("normalized_data", {})
                    item.setdefault("preview", truncate(str(item.get("description", "")), 260))
                    item["normalized_data"]["role_fit_score"] = role_fit_score(query, item)
                    item["normalized_data"]["location_alignment_score"] = self._location_alignment_score(location, item)
                    item["normalized_data"]["market_quality_score"] = self._market_quality_score(query, location, item)
                    collected.append(item)

        async def run_stage(stage: str, providers: list[object]) -> list[dict]:
            tasks = []
            for provider in providers:
                search_queries = self._search_queries(provider, query)
                search_locations = self._search_locations(provider, location)
                for search_query in search_queries:
                    for search_location in search_locations[:1]:
                        tasks.append(safe_search(provider, search_query, search_location, stage))
            if not tasks:
                return []
            provider_results = await asyncio.gather(*tasks)
            absorb_results(provider_results)
            return self._select_production_live_jobs(query=query, location=location, jobs=collected, limit=limit)

        source_groups: dict[str, list[object]] = {}
        for provider in self.providers:
            source_groups.setdefault(str(getattr(provider, "source_name", provider.__class__.__name__)).lower(), []).append(provider)

        stage_results: list[dict] = []
        primary_sources = ["remotive"] if sparse_role else ["jobicy"]
        primary_providers = [provider for source in primary_sources for provider in source_groups.get(source, [])]
        preferred_live = await run_stage("primary", primary_providers)
        stage_results.append(
            {
                "stage": "primary",
                "sources": primary_sources,
                "collected_candidates": len(collected),
                "selected_live": len(preferred_live),
            }
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

        supplemental_sources = [] if sparse_role else ["remotive"]
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

        fallback_sources = [source for source in source_groups.keys() if source not in {*primary_sources, *supplemental_sources}]
        fallback_providers = [provider for source in fallback_sources for provider in source_groups.get(source, [])]
        if fallback_providers:
            preferred_live = await run_stage("fallback", fallback_providers)
            stage_results.append(
                {
                    "stage": "fallback",
                    "sources": fallback_sources,
                    "collected_candidates": len(collected),
                    "selected_live": len(preferred_live),
                }
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
                self._role_domain_match_score(query, item),
                self._location_alignment_score(location, item),
                self._title_hint_overlap(query, item),
                self._skill_overlap_score(query, item),
                float(item.get("normalized_data", {}).get("role_fit_score", 0.0)),
                float(item.get("normalized_data", {}).get("market_quality_score", 0.0)),
            ),
            reverse=True,
        )
        selected: list[dict] = []
        company_counts: dict[str, int] = {}
        selection_debug: dict[str, int | list[str]] = {
            "target_live_count": target_live_count,
            "ranked_candidates": len(ranked),
        }

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
                if self._title_hint_overlap(query, item) >= 1
                or self._role_domain_match_score(query, item) >= 2
                or (
                    self._skill_overlap_score(query, item) >= 2.0
                    and (
                        float(item.get("normalized_data", {}).get("role_fit_score", 0.0)) >= 3.0
                        or self._role_domain_match_score(query, item) >= 1
                    )
                )
            ]
            selection_debug["tertiary_candidates"] = len(tertiary_candidates)
            maybe_add(tertiary_candidates, cap_per_company=4)
        if len(selected) < target_live_count:
            fallback_candidates = [
                item
                for item in ranked
                if self._role_domain_match_score(query, item) >= 1
                or self._skill_overlap_score(query, item) >= 1.5
            ]
            selection_debug["fallback_candidates"] = len(fallback_candidates)
            maybe_add(fallback_candidates, cap_per_company=5)
        if len(selected) < display_floor:
            last_resort_candidates = [
                item
                for item in ranked
                if item.get("source") == "jobicy"
                and float(item.get("normalized_data", {}).get("market_quality_score", 0.0)) >= 18.0
            ]
            selection_debug["last_resort_jobicy_candidates"] = len(last_resort_candidates)
            maybe_add(last_resort_candidates, cap_per_company=5)

        selection_debug["selected_count"] = len(selected)
        selection_debug["selected_titles"] = [str(item.get("title", "")) for item in selected[:8]]
        self.last_fetch_diagnostics["selection_debug"] = selection_debug
        return selected[:limit]

    def _has_explicit_role_alignment(self, query: str, item: dict) -> bool:
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        role_fit = float(item.get("normalized_data", {}).get("role_fit_score", 0.0))
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        core_text_overlap = self._core_token_overlap(query, item, include_description=True)
        return (
            title_overlap >= 1
            or family_overlap >= 1
            or domain_score >= 2
            or core_title_overlap >= 1
            or core_text_overlap >= 2
            or (domain_score >= 1 and skill_overlap >= 1.0)
            or (title_overlap >= 1 and role_fit >= 1.0)
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

    def _is_production_live_candidate(self, query: str, location: str, item: dict, *, strict: bool) -> bool:
        normalized = item.get("normalized_data", {}) or {}
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

        if self._is_location_hard_mismatch(location, item):
            return False

        if strict:
            if location_score < 0.2:
                return False
            if not explicit_alignment and market_quality < 48.0:
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
        return (
            (domain_score >= 2 and skill_overlap >= 1.0)
            or (domain_score >= 1 and title_overlap >= 1)
            or (domain_score >= 1 and family_overlap >= 1 and role_fit >= 2.0)
            or (domain_score >= 1 and market_quality >= 28.0 and skill_overlap >= 1.0)
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

    def _market_quality_score(self, query: str, location: str, item: dict) -> float:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        hint_overlap = len(skills & role_market_hints(query))
        primary_overlap = len(skills & role_primary_hints(query))
        location_score = self._location_alignment_score(location, item)
        return round((role_fit * 10) + requirement_quality + min(10, skill_count * 1.5) + (hint_overlap * 8) + (primary_overlap * 12) + (location_score * 18), 2)

    def _passes_quality_gate(self, query: str, item: dict, *, location: str = "") -> bool:
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        requirement_quality = float(normalized.get("requirement_quality", 0.0))
        skills = {str(skill).lower() for skill in normalized.get("skills", []) or []}
        skill_count = len(skills)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        explicit_alignment = self._has_explicit_role_alignment(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        if self._is_location_hard_mismatch(location, item):
            return False
        if settings.environment == "production" and item.get("source") in {"remotive", "remoteok", "arbeitnow"} and role_fit >= 5.0 and explicit_alignment:
            return True
        if not explicit_alignment and (title_overlap + family_overlap + domain_score) == 0:
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

        return requirement_quality >= 22.0 or skill_count >= 3 or role_fit >= 4.5

    def _location_alignment_score(self, requested_location: str, item: dict) -> float:
        requested = normalize_role(str(requested_location or "")).strip().lower()
        job_location = str(item.get("location", "") or "").strip().lower()
        remote = bool(item.get("remote"))

        if not requested or requested in GLOBAL_REMOTE_HINTS:
            return 1.0 if remote or any(hint in job_location for hint in GLOBAL_REMOTE_HINTS) else 0.75
        if requested in job_location:
            return 1.0

        if requested == "india":
            if any(token in job_location for token in INDIA_LOCATION_HINTS):
                return 1.0
            if any(token in job_location for token in ASIA_LOCATION_HINTS):
                return 0.72
            if remote and not job_location:
                return 0.78
            if any(token in job_location for token in GLOBAL_REMOTE_HINTS):
                return 0.74
            if any(token in job_location for token in NON_INDIA_REGION_HINTS):
                return 0.0
            return 0.18 if remote else 0.0

        if remote and not job_location:
            return 0.72
        if any(token in job_location for token in GLOBAL_REMOTE_HINTS):
            return 0.7
        return 0.0

    def _is_location_hard_mismatch(self, requested_location: str, item: dict) -> bool:
        requested = normalize_role(str(requested_location or "")).strip().lower()
        if not requested or requested in GLOBAL_REMOTE_HINTS:
            return False
        job_location = str(item.get("location", "") or "").strip().lower()
        if requested == "india":
            return any(token in job_location for token in NON_INDIA_REGION_HINTS)
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
            return [query]
        if settings.environment == "production":
            source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
            variations = production_query_variations(query)
            if source_name == "remotive":
                return variations[:2]
            return variations[:3]
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
