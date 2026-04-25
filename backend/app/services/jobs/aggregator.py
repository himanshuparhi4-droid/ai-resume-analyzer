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
    role_family,
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
from app.utils.text import strip_html, truncate

logger = logging.getLogger(__name__)

GLOBAL_REMOTE_HINTS = {"remote", "worldwide", "global", "anywhere"}
INDIA_LOCATION_HINTS = {"india", "indian", "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "delhi", "gurgaon", "gurugram", "noida", "chennai", "kolkata", "ahmedabad"}
ASIA_LOCATION_HINTS = {"apac", "asia", "south asia", "southeast asia"}
NON_INDIA_REGION_HINTS = {
    "usa",
    "u s",
    "u s a",
    "united states",
    "america",
    "north america",
    "uk",
    "united kingdom",
    "england",
    "scotland",
    "ireland",
    "emea",
    "europe",
    "european union",
    "canada",
    "australia",
    "new zealand",
    "netherlands",
    "germany",
    "france",
    "spain",
    "portugal",
    "italy",
    "belgium",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "poland",
    "switzerland",
    "austria",
    "singapore",
    "japan",
    "korea",
    "south korea",
    "china",
    "hong kong",
    "taiwan",
    "uae",
    "dubai",
    "middle east",
    "latam",
    "latin america",
}
SOURCE_TRUST_WEIGHTS = {
    "greenhouse": 1.08,
    "lever": 1.07,
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
    "jooble",
    "adzuna",
    "usajobs",
    "remoteok",
    "arbeitnow",
)
PRODUCTION_FAMILY_GROUPS = {
    "data": {"data analyst", "data scientist", "data engineer", "machine learning engineer", "database engineer"},
    "software": {"software engineer", "frontend developer", "full stack developer", "mobile developer", "embedded engineer", "qa engineer"},
    "infra": {"devops engineer", "support engineer", "solutions architect"},
    "security": {"cybersecurity engineer"},
    "product": {"product manager"},
    "design": {"ui/ux designer"},
    "enterprise": {"enterprise applications engineer"},
    "docs": {"technical writer"},
    "leadership": {"engineering leadership"},
}
DENSE_PRODUCTION_FAMILY_GROUPS = {"data", "software", "infra", "security"}
PRODUCTION_MIN_LIVE_TARGET = 10
BUSINESS_PRODUCTION_DOMAINS = {"marketing", "sales", "customer", "people", "finance", "operations"}


class JobAggregator:
    def __init__(self, db: Session | None = None) -> None:
        self.db = db
        self.providers = []
        self.last_fetch_diagnostics: dict = {}
        self.last_live_job_snapshot: list[dict] = []
        if (settings.default_job_source or "").strip().lower() == "indianapi":
            logger.info("IndianAPI source is disabled for this build; falling back to the auto provider mix.")
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
        normalized["requested_location"] = location
        normalized["location_match_tier"] = self._location_match_tier(location, item)

    def _prepare_listing_text(self, item: dict) -> None:
        description = strip_html(str(item.get("description", "") or ""))
        if description:
            item["description"] = truncate(description, 4000)
        preview_source = item.get("preview") or description
        preview = strip_html(str(preview_source or ""))
        if preview:
            item["preview"] = truncate(preview, 260)

    def _provider_is_selected_by_source(self, source_name: str) -> bool:
        source = (settings.default_job_source or "auto").strip().lower()
        if source == "indianapi":
            source = "auto"
        if source == "auto":
            return source_name in FREE_AUTO_SOURCES
        if source_name == "jobicy":
            return source in {"jobicy", "themuse", "remotive"}
        if source_name == "remotive":
            return source in {"remotive", "remoteok", "adzuna", "usajobs"}
        if source_name == "themuse":
            return source == "themuse"
        return source == source_name

    def _provider_requirement_state(self, source_name: str) -> tuple[bool, str]:
        if source_name == "adzuna":
            return settings.has_adzuna_credentials, "missing_credentials"
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

    def _production_family_group(self, query: str) -> str:
        profile = role_profile(query)
        normalized = profile.family_role or profile.normalized_role
        for group, canonicals in PRODUCTION_FAMILY_GROUPS.items():
            if normalized in canonicals:
                return group
        domain = role_domain(query) or role_domain(normalized)
        return domain or "general"

    def _is_dense_production_family(self, query: str) -> bool:
        return self._production_family_group(query) in DENSE_PRODUCTION_FAMILY_GROUPS

    def _production_stage_soft_timeout(self, *, stage: str, query: str, sparse_role: bool) -> float:
        if sparse_role:
            return 5.0 if stage == "primary" else 3.5
        if self._is_data_analyst_style_query(query):
            if stage == "primary":
                return 13.0
            if stage == "supplemental":
                return 11.5
        profile = role_profile(query)
        if (profile.family_role or profile.normalized_role) == "support engineer":
            if stage == "primary":
                return 24.0
            if stage == "supplemental":
                return 9.5
        family_group = self._production_family_group(query)
        dense_family = family_group in DENSE_PRODUCTION_FAMILY_GROUPS
        if stage == "primary":
            if dense_family:
                return 12.5
            if family_group in BUSINESS_PRODUCTION_DOMAINS:
                return 15.5
            if family_group in {"product", "design"}:
                return 15.0
            if family_group in {"enterprise", "docs", "leadership"}:
                return 15.0
            return 7.0
        if stage == "supplemental":
            if dense_family:
                return 10.0
            if family_group in BUSINESS_PRODUCTION_DOMAINS:
                return 10.5
            if family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                return 10.0
            return 7.5
        return 4.5

    def _production_underfill_grace_seconds(
        self,
        *,
        stage: str,
        query: str,
        current_live_count: int,
        pending_task_count: int,
        live_floor: int,
        partial_live_floor: int,
    ) -> float:
        if pending_task_count <= 0 or stage not in {"primary", "supplemental"}:
            return 0.0
        family_group = self._production_family_group(query)
        dense_role = self._is_dense_production_family(query)
        business_role = family_group in BUSINESS_PRODUCTION_DOMAINS
        if is_sparse_live_market_role(query) or not (dense_role or business_role):
            return 0.0
        target_floor = live_floor if stage == "primary" else partial_live_floor
        if current_live_count >= target_floor:
            return 0.0
        gap = max(target_floor - current_live_count, 0)
        if gap <= 0:
            return 0.0
        cap = 4.5 if business_role else 3.0
        return min(cap, 0.75 + (0.55 * min(gap, 5)) + (0.25 * min(pending_task_count, 4)))

    def _is_india_focused_location(self, location: str) -> bool:
        lowered = normalize_role(location)
        return bool(lowered and any(hint in lowered for hint in INDIA_LOCATION_HINTS))

    def _is_security_analyst_style_query(self, query: str) -> bool:
        profile = role_profile(query)
        normalized = profile.family_role or profile.normalized_role
        return normalized == "cybersecurity engineer" and any(
            head in {"analyst", "specialist"} for head in profile.head_terms
        )

    def _is_generic_security_search_query(self, query: str) -> bool:
        profile = role_profile(query)
        normalized = profile.family_role or profile.normalized_role
        cleaned_tokens = set((profile.cleaned_query or "").split())
        explicit_head_tokens = cleaned_tokens & {
            "admin",
            "administrator",
            "analyst",
            "architect",
            "consultant",
            "developer",
            "engineer",
            "manager",
            "officer",
            "specialist",
        }
        return (
            normalized == "cybersecurity engineer"
            and not explicit_head_tokens
            and bool(cleaned_tokens & {"cyber", "cybersecurity", "infosec", "security"})
        )

    def _is_data_analyst_style_query(self, query: str) -> bool:
        profile = role_profile(query)
        normalized = profile.family_role or profile.normalized_role
        return normalized == "data analyst"

    def _is_weak_software_live_family(self, query: str) -> bool:
        profile = role_profile(query)
        normalized = profile.family_role or profile.normalized_role
        return normalized in {"frontend developer", "mobile developer", "embedded engineer"}

    def _production_search_query_cap(self, *, source_name: str, query: str, location: str) -> int | None:
        family_group = self._production_family_group(query)
        india_focused_location = self._is_india_focused_location(location)
        security_analyst_style = self._is_security_analyst_style_query(query)
        generic_security_query = self._is_generic_security_search_query(query)
        data_analyst_style = self._is_data_analyst_style_query(query)
        weak_software_family = self._is_weak_software_live_family(query)
        profile = role_profile(query)
        canonical_query_role = profile.family_role or profile.normalized_role
        support_engineer_style = canonical_query_role == "support engineer"

        if source_name == "themuse" and family_group in {"data", "software", "security"}:
            return 1
        if source_name in {"adzuna", "jooble"}:
            if data_analyst_style:
                if india_focused_location and source_name == "adzuna":
                    return 1
                return 1 if not india_focused_location else 2
            if support_engineer_style:
                return 4 if india_focused_location else 2
            if family_group in BUSINESS_PRODUCTION_DOMAINS:
                if source_name == "adzuna" and india_focused_location:
                    return 1
                if source_name == "jooble" and india_focused_location:
                    return 4
                return 3 if india_focused_location else 2
            if source_name == "adzuna":
                if family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                    if family_group == "docs" and india_focused_location:
                        return 1
                    return 2 if india_focused_location else 1
                if canonical_query_role == "database engineer":
                    return 2 if india_focused_location else 1
            if family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                return 3 if india_focused_location else 2
            if canonical_query_role == "database engineer":
                return 4 if india_focused_location else 3
            if security_analyst_style or generic_security_query or weak_software_family:
                return 2 if india_focused_location else 1
            if family_group in {"software", "security", "infra"} and not india_focused_location:
                return 1
            if family_group == "data" and not india_focused_location:
                return 2
            return 2 if india_focused_location else 1
        if source_name == "remotive" and data_analyst_style:
            return 3 if india_focused_location else 2
        if source_name == "remotive" and family_group in {"product", "design", "enterprise", "docs", "leadership"}:
            return 5 if india_focused_location else 3
        if source_name == "remotive" and family_group in BUSINESS_PRODUCTION_DOMAINS:
            return 5 if india_focused_location else 4
        if source_name == "remotive" and canonical_query_role == "database engineer":
            return 6 if india_focused_location else 4
        if source_name == "remotive" and support_engineer_style:
            return 6 if india_focused_location else 4
        if source_name == "jobicy" and security_analyst_style:
            return 1
        if source_name == "jobicy" and data_analyst_style:
            return 1
        if source_name == "jobicy" and support_engineer_style:
            return 3 if india_focused_location else 2
        if source_name == "jobicy" and family_group in BUSINESS_PRODUCTION_DOMAINS:
            return 2
        if source_name == "jobicy" and family_group in {"enterprise", "docs", "leadership"}:
            return 3 if india_focused_location else 2
        if source_name == "remotive" and (security_analyst_style or generic_security_query):
            return 3
        return None

    def _build_production_provider_plan(
        self,
        *,
        query: str,
        location: str,
        source_groups: dict[str, list[object]],
    ) -> dict[str, object]:
        sparse_role = is_sparse_live_market_role(query)
        india_focused_location = self._is_india_focused_location(location)
        family_group = self._production_family_group(query)
        dense_family = family_group in DENSE_PRODUCTION_FAMILY_GROUPS
        security_analyst_style = self._is_security_analyst_style_query(query)
        generic_security_query = self._is_generic_security_search_query(query)
        data_analyst_style = self._is_data_analyst_style_query(query)
        weak_software_family = self._is_weak_software_live_family(query)
        profile = role_profile(query)
        support_engineer_style = (profile.family_role or profile.normalized_role) == "support engineer"

        fallback_order: list[str] = []
        if sparse_role:
            primary_order = ["remotive"]
            supplemental_order: list[str] = []
        elif dense_family:
            if india_focused_location:
                if security_analyst_style or generic_security_query:
                    primary_order = ["jooble", "adzuna", "remotive", "jobicy"]
                    supplemental_order = ["themuse", "greenhouse"]
                elif support_engineer_style:
                    primary_order = ["jooble", "adzuna", "remotive", "jobicy"]
                    supplemental_order = ["themuse", "greenhouse"]
                elif family_group == "data":
                    # India data-role coverage is strongest from location-aware
                    # sources. Keep the primary stage focused so Render free
                    # instances spend their first budget where India jobs exist.
                    primary_order = ["adzuna", "jooble"]
                    supplemental_order = ["jobicy", "remotive", "themuse"]
                elif weak_software_family:
                    primary_order = ["greenhouse", "remotive", "jobicy", "jooble"]
                    supplemental_order = ["themuse", "adzuna"]
                elif family_group in {"software", "infra"}:
                    primary_order = ["greenhouse", "remotive", "jobicy", "jooble"]
                    supplemental_order = ["themuse", "adzuna"]
                elif family_group == "security":
                    primary_order = ["jooble", "adzuna", "remotive", "jobicy"]
                    supplemental_order = ["themuse", "greenhouse"]
                else:
                    primary_order = ["greenhouse", "jooble", "remotive"]
                    supplemental_order = ["jobicy", "adzuna", "themuse"]
            elif family_group == "data":
                if data_analyst_style:
                    primary_order = ["remotive", "themuse", "jobicy", "adzuna", "greenhouse"]
                    supplemental_order = ["jooble"]
                else:
                    primary_order = ["greenhouse", "remotive", "jobicy"]
                    supplemental_order = ["themuse", "jooble", "adzuna"]
            elif security_analyst_style:
                primary_order = ["remotive", "greenhouse", "themuse"]
                supplemental_order = ["jobicy", "jooble", "adzuna"]
            elif weak_software_family:
                primary_order = ["remotive", "jobicy", "jooble"]
                supplemental_order = ["greenhouse", "themuse"]
                fallback_order = ["lever"]
            elif family_group in {"software", "infra"}:
                primary_order = ["remotive", "jobicy", "greenhouse"]
                supplemental_order = ["jooble", "themuse", "adzuna"]
            elif family_group == "security":
                primary_order = ["remotive", "greenhouse", "jobicy"]
                supplemental_order = ["themuse", "jooble", "adzuna"]
            else:
                primary_order = ["greenhouse", "remotive", "jobicy"]
                supplemental_order = ["themuse", "jooble", "adzuna"]
        elif family_group in BUSINESS_PRODUCTION_DOMAINS:
            if india_focused_location:
                # Adzuna is the strongest India-specific source for business
                # families; keep the primary stage narrow so Render does not
                # overload it with several simultaneous searches.
                primary_order = ["adzuna", "jooble"]
                supplemental_order = ["jobicy", "remotive", "themuse", "greenhouse"]
            else:
                primary_order = ["jobicy", "jooble", "adzuna", "remotive", "themuse"]
                supplemental_order = ["greenhouse"]
            fallback_order = ["lever"]
        elif family_group in {"product", "design"}:
            primary_order = ["jobicy", "jooble", "adzuna", "remotive", "themuse"]
            supplemental_order = ["greenhouse"]
            fallback_order = ["lever"]
        elif family_group in {"enterprise", "docs", "leadership"}:
            if india_focused_location and family_group == "docs":
                primary_order = ["adzuna", "jooble"]
                supplemental_order = ["jobicy", "remotive", "themuse", "greenhouse"]
            else:
                primary_order = (
                    ["jooble", "adzuna", "jobicy", "remotive", "themuse"]
                    if india_focused_location
                    else ["jobicy", "jooble", "adzuna", "remotive", "themuse"]
                )
                supplemental_order = ["greenhouse"]
            fallback_order = ["lever"]
        else:
            primary_order = (
                ["jooble", "adzuna", "jobicy", "remotive", "themuse"]
                if india_focused_location
                else ["jobicy", "jooble", "adzuna", "remotive", "themuse"]
            )
            supplemental_order = ["greenhouse"]
            fallback_order = ["lever"]

        primary_sources = [source for source in primary_order if source in source_groups]
        supplemental_sources = [source for source in supplemental_order if source in source_groups and source not in primary_sources]
        fallback_sources = [source for source in fallback_order if source in source_groups and source not in {*primary_sources, *supplemental_sources}]
        if not dense_family:
            fallback_sources.extend(
                source
                for source in source_groups.keys()
                if source not in {*primary_sources, *supplemental_sources, *fallback_sources}
            )
        if family_group not in {"software", "security", "infra"}:
            fallback_sources = [source for source in fallback_sources if source != "remoteok"]
        if dense_family:
            blocked_dense_sources = {"remoteok", "themuse"}
            if not weak_software_family:
                blocked_dense_sources.add("lever")
            fallback_sources = [source for source in fallback_sources if source not in blocked_dense_sources]
        elif (
            role_profile(query).normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES
            or any(head in {"admin", "administrator", "consultant", "manager", "designer", "writer"} for head in role_profile(query).head_terms)
        ):
            fallback_sources = [source for source in fallback_sources if source != "remoteok"]

        return {
            "family_group": family_group,
            "dense_family": dense_family,
            "india_focused_location": india_focused_location,
            "active_sources": sorted(source_groups.keys()),
            "primary_sources": primary_sources,
            "supplemental_sources": supplemental_sources,
            "fallback_sources": fallback_sources,
        }

    def _count_items_by_source(self, items: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            source = str(item.get("source", "unknown")).lower()
            counts[source] = counts.get(source, 0) + 1
        return counts

    def _aggregate_provider_request_counts(self) -> dict[str, dict[str, int]]:
        summary: dict[str, dict[str, int]] = {}
        for provider_diag in self.last_fetch_diagnostics.get("providers", []):
            source = str(provider_diag.get("source") or provider_diag.get("provider") or "unknown").lower()
            entry = summary.setdefault(
                source,
                {
                    "requests": 0,
                    "raw_returned": 0,
                    "timeouts": 0,
                    "errors": 0,
                },
            )
            entry["requests"] += 1
            entry["raw_returned"] += int(provider_diag.get("result_count", 0) or 0)
            error_text = str(provider_diag.get("error", "") or "")
            if error_text:
                entry["errors"] += 1
                if "timeout" in error_text.lower():
                    entry["timeouts"] += 1
        return summary

    def _aggregate_provider_attempt_rollup(self) -> dict[str, dict[str, object]]:
        summary: dict[str, dict[str, object]] = {}
        for provider_diag in self.last_fetch_diagnostics.get("providers", []):
            source = str(provider_diag.get("source") or provider_diag.get("provider") or "unknown").lower()
            entry = summary.setdefault(
                source,
                {
                    "requests": 0,
                    "successes": 0,
                    "skipped_budget": 0,
                    "timeouts": 0,
                    "errors": 0,
                    "raw_returned": 0,
                    "queries": [],
                    "stages": [],
                    "error_samples": [],
                    "_elapsed_total_ms": 0.0,
                    "_elapsed_count": 0,
                    "max_elapsed_ms": 0.0,
                },
            )
            entry["requests"] = int(entry["requests"]) + 1
            status = str(provider_diag.get("status") or "").strip().lower()
            if status == "success":
                entry["successes"] = int(entry["successes"]) + 1
            elif status == "skipped_budget":
                entry["skipped_budget"] = int(entry["skipped_budget"]) + 1

            result_count = int(provider_diag.get("result_count", 0) or 0)
            entry["raw_returned"] = int(entry["raw_returned"]) + result_count

            stage = str(provider_diag.get("stage") or "").strip()
            if stage and stage not in entry["stages"]:
                entry["stages"].append(stage)

            query = str(provider_diag.get("query") or "").strip()
            if query and query not in entry["queries"]:
                entry["queries"].append(query)

            elapsed_ms = float(provider_diag.get("elapsed_ms", 0.0) or 0.0)
            if elapsed_ms > 0:
                entry["_elapsed_total_ms"] = float(entry["_elapsed_total_ms"]) + elapsed_ms
                entry["_elapsed_count"] = int(entry["_elapsed_count"]) + 1
                entry["max_elapsed_ms"] = max(float(entry["max_elapsed_ms"]), elapsed_ms)

            error_text = str(provider_diag.get("error", "") or "").strip()
            if error_text:
                entry["errors"] = int(entry["errors"]) + 1
                if "timeout" in error_text.lower():
                    entry["timeouts"] = int(entry["timeouts"]) + 1
                if error_text not in entry["error_samples"]:
                    entry["error_samples"].append(error_text)

        cleaned: dict[str, dict[str, object]] = {}
        for source, entry in sorted(summary.items()):
            elapsed_count = int(entry.pop("_elapsed_count", 0) or 0)
            elapsed_total_ms = float(entry.pop("_elapsed_total_ms", 0.0) or 0.0)
            entry["avg_elapsed_ms"] = round(elapsed_total_ms / elapsed_count, 2) if elapsed_count else 0.0
            entry["max_elapsed_ms"] = round(float(entry.get("max_elapsed_ms", 0.0) or 0.0), 2)
            entry["queries"] = list(entry["queries"])[:8]
            entry["stages"] = list(entry["stages"])[:4]
            entry["error_samples"] = list(entry["error_samples"])[:3]
            cleaned[source] = entry
        return cleaned

    def _update_provider_attempt_rollup(self) -> dict[str, dict[str, object]]:
        rollup = self._aggregate_provider_attempt_rollup()
        self.last_fetch_diagnostics["provider_attempt_rollup"] = rollup
        return rollup

    def _log_provider_diagnostic_summary(self, *, query: str) -> None:
        rollup = self._update_provider_attempt_rollup()
        if not rollup:
            return
        underfill = self.last_fetch_diagnostics.get("underfill") or {}
        timeout_sources = underfill.get("timeout_sources") or []
        has_errors = any(
            int(payload.get("errors", 0) or 0) > 0
            or int(payload.get("timeouts", 0) or 0) > 0
            or int(payload.get("skipped_budget", 0) or 0) > 0
            for payload in rollup.values()
        )
        if underfill.get("reason") in {"", "sufficient_live_supply"} and not timeout_sources and not has_errors:
            return
        logger.info(
            "Production provider diagnostics for query=%s: %s",
            query,
            {
                "underfill": underfill,
                "stage_results": self.last_fetch_diagnostics.get("stage_results", []),
                "stage_short_circuits": self.last_fetch_diagnostics.get("stage_short_circuits", []),
                "provider_attempt_rollup": rollup,
            },
        )

    def _timeout_sources(self) -> list[str]:
        timeout_sources = {
            str(provider_diag.get("source") or provider_diag.get("provider") or "unknown").lower()
            for provider_diag in self.last_fetch_diagnostics.get("providers", [])
            if "timeout" in str(provider_diag.get("error", "") or "").lower()
        }
        return sorted(timeout_sources)

    def _production_providers(self) -> list[object]:
        source = (settings.default_job_source or "auto").strip().lower()
        if source == "indianapi":
            source = "auto"
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
            production_display_floor = self._production_display_floor(query=query, limit=limit)
            cached_live = self._get_production_cached_jobs(query=query, location=location, limit=limit)
            cached_seed = copy.deepcopy(cached_live or [])
            if cached_live and (sparse_role or len(cached_live) >= production_display_floor):
                return cached_live
            if cached_live:
                self.last_fetch_diagnostics["cache_hit_underfilled_retry"] = {
                    "cached_live_count": len(cached_live),
                    "required_live_floor": production_display_floor,
                }
            if not sparse_role:
                db_cached_seed = self._get_cached_production_fallback(query=query, location=location, limit=limit)
                if len(db_cached_seed) > len(cached_seed):
                    cached_seed = db_cached_seed
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
            self._log_provider_diagnostic_summary(query=query)
            if live_jobs:
                if not sparse_role and len(live_jobs) < production_display_floor and cached_seed:
                    live_jobs = self._blend_underfilled_production_live_jobs(
                        query=query,
                        location=location,
                        limit=limit,
                        live_jobs=live_jobs,
                        cached_jobs=cached_seed,
                    )
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
            return []

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
            search_queries = self._search_queries(provider, query, location)
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
                        self._prepare_listing_text(item)
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

    def _blend_underfilled_production_live_jobs(
        self,
        *,
        query: str,
        location: str,
        limit: int,
        live_jobs: list[dict],
        cached_jobs: list[dict],
    ) -> list[dict]:
        display_floor = self._production_display_floor(query=query, limit=limit)
        if len(live_jobs) >= display_floor or not cached_jobs:
            return live_jobs

        selection_debug_before = copy.deepcopy(self.last_fetch_diagnostics.get("selection_debug"))
        combined: list[dict] = []
        seen: set[str] = set()
        fresh_keys = {dedupe_key(item) for item in live_jobs}
        cached_candidate_count = 0
        for item in [*live_jobs, *cached_jobs]:
            key = dedupe_key(item)
            if not key or key in seen:
                continue
            seen.add(key)
            candidate = copy.deepcopy(item)
            self._prepare_listing_text(candidate)
            candidate.setdefault("normalized_data", {})
            self._annotate_item_scores(query=query, location=location, item=candidate)
            if key not in fresh_keys:
                cached_candidate_count += 1
            combined.append(candidate)

        selected = self._select_production_live_jobs(query=query, location=location, jobs=combined, limit=limit)
        if len(selected) > len(live_jobs):
            self.last_fetch_diagnostics["underfill_cache_blend"] = {
                "fresh_live_count": len(live_jobs),
                "cached_candidate_count": cached_candidate_count,
                "blended_live_count": len(selected),
                "required_live_floor": display_floor,
            }
            logger.info(
                "Production live selection blended %s fresh jobs with cached candidates to return %s jobs for query=%s",
                len(live_jobs),
                len(selected),
                query,
            )
            return selected

        if selection_debug_before is not None:
            self.last_fetch_diagnostics["selection_debug"] = selection_debug_before
        return live_jobs

    def _query_signature(self, value: str) -> str:
        raw_text = str(value or "").strip()
        raw_text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", raw_text)
        raw_text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", raw_text)
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+ ]+", " ", raw_text.lower())).strip()

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
        weak_software_family = self._is_weak_software_live_family(query)
        fetch_started_at = time.perf_counter()
        self.last_live_job_snapshot = []
        self.last_fetch_diagnostics = dict(self.last_fetch_diagnostics or {})
        self.last_fetch_diagnostics.setdefault("providers", [])
        self.last_fetch_diagnostics.setdefault("stage_short_circuits", [])

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
            india_focused_location = self._is_india_focused_location(location)
            security_analyst_style = self._is_security_analyst_style_query(query)
            data_analyst_style = self._is_data_analyst_style_query(query)
            weak_software_family = self._is_weak_software_live_family(query)
            family_group = self._production_family_group(query)
            canonical_query_role = query_profile.family_role or query_profile.normalized_role
            support_engineer_style = canonical_query_role == "support engineer"
            if source_name == "jobicy":
                if security_analyst_style:
                    provider_timeout = 7.0
                elif data_analyst_style:
                    provider_timeout = 9.5
                else:
                    provider_timeout = 8.0
            elif source_name == "greenhouse":
                if query_domain == "data":
                    provider_timeout = 12.0 if india_focused_location else 10.5
                elif query_domain in {"software", "security"}:
                    provider_timeout = 11.5 if india_focused_location else 10.0
                else:
                    provider_timeout = 7.5
            elif source_name == "lever":
                if query_domain == "data":
                    provider_timeout = 6.5
                elif query_domain in {"software", "security"}:
                    provider_timeout = 7.0
                else:
                    provider_timeout = 6.0
            elif source_name == "jooble":
                provider_timeout = 10.0 if india_focused_location else 6.0
                if india_focused_location and family_group in BUSINESS_PRODUCTION_DOMAINS:
                    provider_timeout = 12.0
                if india_focused_location and family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                    provider_timeout = 12.0
                if india_focused_location and support_engineer_style:
                    provider_timeout = 12.0
                if data_analyst_style and not india_focused_location:
                    provider_timeout = 8.5
                if security_analyst_style and not india_focused_location:
                    provider_timeout = 4.75
                elif weak_software_family and not india_focused_location:
                    provider_timeout = min(provider_timeout, 5.25)
            elif source_name == "adzuna":
                provider_timeout = 10.5 if india_focused_location else 5.5
                if india_focused_location and family_group in BUSINESS_PRODUCTION_DOMAINS:
                    provider_timeout = 14.0
                if india_focused_location and family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                    provider_timeout = 14.0
                if india_focused_location and support_engineer_style:
                    provider_timeout = 22.0
                if data_analyst_style:
                    provider_timeout = 12.0 if india_focused_location else 8.5
                if security_analyst_style and not india_focused_location:
                    provider_timeout = 4.75
                elif weak_software_family and not india_focused_location:
                    provider_timeout = min(provider_timeout, 5.0)
            elif source_name == "remotive":
                provider_timeout = 9.0
            elif source_name == "themuse":
                provider_timeout = 6.5 if query_domain in {"data", "software", "security"} else 5.5
                if india_focused_location and family_group in {"product", "design", "enterprise", "docs", "leadership"}:
                    provider_timeout = 8.0
            elif source_name == "findwork":
                provider_timeout = 6.0
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
                provider_diag["elapsed_ms"] = 0.0
                provider_diag["status"] = "skipped_budget"
                provider_diag["error"] = "skipped_insufficient_runtime_budget"
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return []
            provider_timeout = min(base_provider_timeout, max(0.75, remaining_global_budget))
            provider_diag["timeout_seconds"] = round(provider_timeout, 2)
            started_at = time.perf_counter()
            try:
                items = await asyncio.wait_for(
                    provider.search(query=search_query, location=search_location, limit=max(8, limit)),
                    timeout=provider_timeout,
                )
                provider_diag["elapsed_ms"] = round((time.perf_counter() - started_at) * 1000, 2)
                provider_diag["status"] = "success"
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
                provider_diag["elapsed_ms"] = round((time.perf_counter() - started_at) * 1000, 2)
                provider_diag["status"] = "timeout" if "timeout" in error_message.lower() else "error"
                provider_diag["error_type"] = exc.__class__.__name__
                provider_diag["error"] = error_message
                self.last_fetch_diagnostics["providers"].append(provider_diag)
                return []

        collected: list[dict] = []
        seen: set[str] = set()
        near_seen: set[str] = set()
        sparse_role = is_sparse_live_market_role(query)
        live_floor = self._production_display_floor(query=query, limit=limit)
        partial_live_floor = self._production_partial_live_floor(query=query, limit=limit)
        target_live_count = self._production_live_target(query=query, limit=limit)
        dense_role = self._is_dense_production_family(query)
        best_live: list[dict] = []
        best_live_stage = ""

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
                    self._prepare_listing_text(item)
                    item.setdefault("normalized_data", {})
                    item.setdefault("preview", truncate(str(item.get("description", "")), 260))
                    self._annotate_item_scores(query=query, location=location, item=item)
                    collected.append(item)

        def _selected_live_sources(selected_live: list[dict]) -> dict[str, int]:
            return {
                source: len([item for item in selected_live if item.get("source") == source])
                for source in sorted({item.get("source", "unknown") for item in selected_live})
            }

        def _live_result_strength(selected_live: list[dict]) -> tuple[int, int, float]:
            if not selected_live:
                return (0, 0, 0.0)
            source_count = len({item.get("source", "unknown") for item in selected_live})
            combined_signal = 0.0
            for item in selected_live:
                normalized = item.get("normalized_data") or {}
                combined_signal += float(normalized.get("role_fit_score") or 0.0)
                combined_signal += float(normalized.get("market_quality_score") or 0.0)
                combined_signal += float(normalized.get("title_alignment_score") or 0.0)
            return (len(selected_live), source_count, round(combined_signal, 4))

        def _remember_best_live(stage: str, selected_live: list[dict]) -> None:
            nonlocal best_live, best_live_stage
            if _live_result_strength(selected_live) > _live_result_strength(best_live):
                best_live = list(selected_live)
                best_live_stage = stage

        def _preserve_best_live(stage: str, selected_live: list[dict]) -> list[dict]:
            _remember_best_live(stage, selected_live)
            if _live_result_strength(best_live) > _live_result_strength(selected_live):
                logger.info(
                    "Production live selection preserved stronger %s result with %s jobs over %s result with %s jobs for query=%s",
                    best_live_stage,
                    len(best_live),
                    stage,
                    len(selected_live),
                    query,
                )
                self.last_fetch_diagnostics["best_live_preserved_after_stage"] = {
                    "stage": stage,
                    "stage_selected_live": len(selected_live),
                    "preserved_stage": best_live_stage,
                    "preserved_selected_live": len(best_live),
                }
                return list(best_live)
            return selected_live

        def _store_selected_live(
            selected_live: list[dict],
            *,
            partial_live_return: dict[str, object] | None = None,
        ) -> None:
            self.last_fetch_diagnostics["stage_results"] = stage_results
            self.last_fetch_diagnostics["collected_candidate_count"] = len(collected)
            self.last_fetch_diagnostics["selected_live_count"] = len(selected_live)
            self.last_fetch_diagnostics["selected_live_sources"] = _selected_live_sources(selected_live)
            if partial_live_return is None:
                self.last_fetch_diagnostics.pop("partial_live_return", None)
            else:
                self.last_fetch_diagnostics["partial_live_return"] = partial_live_return
            if best_live:
                self.last_fetch_diagnostics["best_live_result"] = {
                    "stage": best_live_stage,
                    "selected_live": len(best_live),
                    "selected_live_sources": _selected_live_sources(best_live),
                }

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
                search_queries = self._search_queries(provider, query, location)
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
                query=query,
                sparse_role=sparse_role,
            )
            if stage == "primary":
                reserve_seconds = 8.0 if not sparse_role else 4.0
            elif stage == "supplemental":
                reserve_seconds = 2.0
            else:
                reserve_seconds = 1.0
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
            grace_applied = False
            while pending:
                elapsed = time.perf_counter() - started_at
                remaining = soft_timeout - elapsed
                if remaining <= 0:
                    current_live = self._select_production_live_jobs(
                        query=query,
                        location=location,
                        jobs=collected,
                        limit=limit,
                    )
                    grace_seconds = 0.0
                    if not grace_applied:
                        grace_seconds = self._production_underfill_grace_seconds(
                            stage=stage,
                            query=query,
                            current_live_count=len(current_live),
                            pending_task_count=len(pending),
                            live_floor=live_floor,
                            partial_live_floor=partial_live_floor,
                        )
                        grace_seconds = min(
                            grace_seconds,
                            max(0.0, _remaining_runtime_budget(reserve_seconds=max(0.5, reserve_seconds - 0.5))),
                        )
                    if grace_seconds >= 0.75:
                        soft_timeout += grace_seconds
                        grace_applied = True
                        continue
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
                stage_completion_floor = target_live_count if dense_role else live_floor
                if len(preferred_live) >= stage_completion_floor:
                    if pending:
                        await _cancel_pending_tasks(
                            pending,
                            stage=stage,
                            soft_timeout=soft_timeout,
                            reason="target_reached" if stage_completion_floor == target_live_count else "floor_reached",
                        )
                    return preferred_live
                if (
                    stage == "supplemental"
                    and query_domain in {"data", "software", "security"}
                    and len(preferred_live) >= max(partial_live_floor + 2, 5)
                    and (
                        len(preferred_live) >= target_live_count
                        or _remaining_runtime_budget(reserve_seconds=1.0) <= 1.5
                    )
                ):
                    if pending:
                        await _cancel_pending_tasks(
                            pending,
                            stage=stage,
                            soft_timeout=soft_timeout,
                            reason="dense_partial_floor_reached",
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
        provider_plan = self._build_production_provider_plan(query=query, location=location, source_groups=source_groups)
        primary_sources = list(provider_plan["primary_sources"])
        supplemental_sources = list(provider_plan["supplemental_sources"])
        fallback_sources = list(provider_plan["fallback_sources"])
        self.last_fetch_diagnostics["provider_plan"] = provider_plan
        logger.info(
            "Production provider plan for query=%s: active=%s primary=%s supplemental=%s fallback=%s",
            query,
            provider_plan["active_sources"],
            primary_sources,
            supplemental_sources,
            fallback_sources,
        )
        primary_providers = [provider for source in primary_sources for provider in source_groups.get(source, [])]
        preferred_live = await run_stage("primary", primary_providers)
        primary_selected_count = len(preferred_live)
        primary_selection = self.last_fetch_diagnostics.get("selection_debug", {}) or {}
        stage_results.append(
            {
                "stage": "primary",
                "sources": primary_sources,
                "collected_candidates": len(collected),
                "selected_live": len(preferred_live),
                "precision_guarded_candidates": int(primary_selection.get("precision_guarded_candidates", 0) or 0),
                "upstream_family_safe_count": int(primary_selection.get("upstream_family_safe_count", 0) or 0),
                "underfill_reason": str((primary_selection.get("underfill") or {}).get("reason") or "none"),
            }
        )
        logger.info(
            "Production stage result for query=%s: stage=primary candidates=%s selected=%s",
            query,
            len(collected),
            len(preferred_live),
        )
        _remember_best_live("primary", preferred_live)
        should_continue_after_primary = (
            dense_role
            and not sparse_role
            and len(preferred_live) < target_live_count
            and bool(supplemental_sources or fallback_sources)
            and _remaining_runtime_budget(reserve_seconds=2.0) > 1.5
        )
        if len(preferred_live) >= live_floor and not should_continue_after_primary:
            _store_selected_live(preferred_live)
            logger.info(
                "Production live selection reached floor with %s jobs from %s collected candidates for query=%s",
                len(preferred_live),
                len(collected),
                query,
            )
            return preferred_live
        if sparse_role:
            _store_selected_live(preferred_live)
            if preferred_live:
                return preferred_live
            return []

        supplemental_providers = [provider for source in supplemental_sources for provider in source_groups.get(source, [])]
        if supplemental_providers:
            preferred_live = await run_stage("supplemental", supplemental_providers)
            supplemental_selection = self.last_fetch_diagnostics.get("selection_debug", {}) or {}
            stage_results.append(
                {
                    "stage": "supplemental",
                    "sources": supplemental_sources,
                    "collected_candidates": len(collected),
                    "selected_live": len(preferred_live),
                    "precision_guarded_candidates": int(supplemental_selection.get("precision_guarded_candidates", 0) or 0),
                    "upstream_family_safe_count": int(supplemental_selection.get("upstream_family_safe_count", 0) or 0),
                    "underfill_reason": str((supplemental_selection.get("underfill") or {}).get("reason") or "none"),
                }
            )
            logger.info(
                "Production stage result for query=%s: stage=supplemental candidates=%s selected=%s",
                query,
                len(collected),
                len(preferred_live),
            )
            preferred_live = _preserve_best_live("supplemental", preferred_live)
            supplemental_target_reached = len(preferred_live) >= target_live_count
            should_try_fallback_for_more = (
                bool(fallback_sources)
                and dense_role
                and not sparse_role
                and len(preferred_live) < target_live_count
                and _remaining_runtime_budget(reserve_seconds=1.0) > 2.5
            )
            if len(preferred_live) >= live_floor and (supplemental_target_reached or not should_try_fallback_for_more):
                _store_selected_live(preferred_live)
                logger.info(
                    "Production live selection reached floor after supplemental fetch with %s jobs from %s candidates for query=%s",
                    len(preferred_live),
                    len(collected),
                    query,
                )
                return preferred_live
            elapsed_after_supplemental = time.perf_counter() - fetch_started_at
            allow_weak_software_fallback = (
                weak_software_family
                and bool(fallback_sources)
                and len(preferred_live) < live_floor
                and _remaining_runtime_budget(reserve_seconds=1.0) > 2.5
            )
            if len(preferred_live) >= partial_live_floor:
                _store_selected_live(
                    preferred_live,
                    partial_live_return={
                    "stage": "supplemental",
                    "selected_live": len(preferred_live),
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_after_supplemental, 2),
                    "reason": "acceptable_partial_live_set",
                    },
                )
                logger.info(
                    "Production live selection accepted partial result after supplemental fetch with %s jobs in %ss for query=%s",
                    len(preferred_live),
                    round(elapsed_after_supplemental, 2),
                    query,
                )
                return preferred_live
            if (
                preferred_live
                and query_domain in {"data", "software", "security"}
                and len(preferred_live) >= 2
                and not allow_weak_software_fallback
            ):
                _store_selected_live(
                    preferred_live,
                    partial_live_return={
                    "stage": "supplemental",
                    "selected_live": len(preferred_live),
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_after_supplemental, 2),
                    "reason": "dense_domain_preserve_live_after_supplemental",
                    },
                )
                logger.info(
                    "Production live selection preserved %s dense-domain jobs after supplemental fetch in %ss for query=%s",
                    len(preferred_live),
                    round(elapsed_after_supplemental, 2),
                    query,
                )
                return preferred_live
            if preferred_live and (
                (len(preferred_live) == primary_selected_count and query_domain != "data")
                or (elapsed_after_supplemental >= 18.0 and query_domain not in {"data", "software", "security"})
                or query_domain in {"product", "design"}
            ) and not allow_weak_software_fallback:
                _store_selected_live(
                    preferred_live,
                    partial_live_return={
                    "stage": "supplemental",
                    "selected_live": len(preferred_live),
                    "primary_selected_live": primary_selected_count,
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_after_supplemental, 2),
                    "reason": "preserve_response_after_supplemental",
                    },
                )
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
            dense_role_live_guard = (
                query_domain in {"data", "software", "security"}
                and len(preferred_live) >= min(limit, 2)
                and (remaining_budget <= 9.0 or elapsed_before_fallback >= 24.0)
            )
            allow_weak_software_fallback = (
                weak_software_family
                and len(preferred_live) < live_floor
                and remaining_budget > 1.5
            )
            should_try_fallback_for_more = (
                dense_role
                and len(preferred_live) < target_live_count
                and remaining_budget > 2.5
            )
            if preferred_live and (
                len(preferred_live) >= partial_live_floor
                or dense_role_live_guard
                or (remaining_budget <= 6.0 and query_domain not in {"data", "software", "security"})
            ) and not allow_weak_software_fallback and not should_try_fallback_for_more:
                _store_selected_live(
                    preferred_live,
                    partial_live_return={
                    "stage": "pre-fallback",
                    "selected_live": len(preferred_live),
                    "partial_live_floor": partial_live_floor,
                    "elapsed_seconds": round(elapsed_before_fallback, 2),
                    "remaining_budget_seconds": round(remaining_budget, 2),
                    "reason": "dense_role_budget_guard" if dense_role_live_guard else "preserve_response_budget",
                    },
                )
                logger.info(
                    "Skipping fallback stage and returning %s live jobs with %ss remaining budget for query=%s",
                    len(preferred_live),
                    round(remaining_budget, 2),
                    query,
                )
                return preferred_live
            preferred_live = await run_stage("fallback", fallback_providers)
            fallback_selection = self.last_fetch_diagnostics.get("selection_debug", {}) or {}
            stage_results.append(
                {
                    "stage": "fallback",
                    "sources": fallback_sources,
                    "collected_candidates": len(collected),
                    "selected_live": len(preferred_live),
                    "precision_guarded_candidates": int(fallback_selection.get("precision_guarded_candidates", 0) or 0),
                    "upstream_family_safe_count": int(fallback_selection.get("upstream_family_safe_count", 0) or 0),
                    "underfill_reason": str((fallback_selection.get("underfill") or {}).get("reason") or "none"),
                }
            )
            logger.info(
                "Production stage result for query=%s: stage=fallback candidates=%s selected=%s",
                query,
                len(collected),
                len(preferred_live),
            )
            preferred_live = _preserve_best_live("fallback", preferred_live)

        preferred_live = _preserve_best_live("final", preferred_live)
        _store_selected_live(preferred_live)
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

        query_profile = role_profile(query)
        exact_precision_query = self._uses_strict_precision_guard(query)
        target_live_count = self._production_live_target(query=query, limit=limit)
        display_floor = self._production_display_floor(query=query, limit=limit)
        partial_live_floor = self._production_partial_live_floor(query=query, limit=limit)
        india_focused_location = self._is_india_focused_location(location)

        ranked = sorted(
            live_jobs,
            key=lambda item: (
                self._canonical_role_alignment(query, item),
                self._location_alignment_score(location, item) if india_focused_location else 0.0,
                float(item.get("normalized_data", {}).get("title_alignment_score", 0.0)),
                self._title_precision_score(query, item),
                -self._unrequested_title_penalty(query, item),
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
        exact_backup_candidates: list[dict] = []
        same_family_recovery_candidates: list[dict] = []
        if exact_precision_query:
            exact_backup_candidates = [
                item
                for item in ranked
                if item not in precision_guarded and self._passes_exact_query_backup_guard(query, location, item)
            ]
            if len(precision_guarded) >= max(2, display_floor):
                ranked = (
                    precision_guarded
                    if len(precision_guarded) >= target_live_count
                    else precision_guarded + exact_backup_candidates
                )
            else:
                excluded_candidates = [*precision_guarded, *exact_backup_candidates]
                same_family_recovery_candidates = [
                    item
                    for item in ranked
                    if item not in excluded_candidates
                    and self._passes_same_family_recovery_guard(query, location, item)
                ]
                ranked = precision_guarded + exact_backup_candidates + same_family_recovery_candidates
        elif precision_guarded and len(precision_guarded) >= display_floor:
            ranked = precision_guarded
        elif precision_guarded:
            ranked = precision_guarded + [item for item in ranked if item not in precision_guarded]
        selected: list[dict] = []
        company_counts: dict[str, int] = {}
        company_title_counts: dict[str, int] = {}
        selected_signatures: set[str] = set()
        source_counts: dict[str, int] = {}
        active_sources = {
            str(item.get("source", "unknown")).lower()
            for item in ranked
            if str(item.get("source", "")).strip()
        }
        active_source_count = max(1, len(active_sources))
        selection_debug: dict[str, int | list[str] | dict[str, int]] = {
            "target_live_count": target_live_count,
            "ranked_candidates": len(ranked),
            "precision_guarded_candidates": len(precision_guarded),
            "exact_backup_candidates": len(exact_backup_candidates),
            "same_family_recovery_candidates": len(same_family_recovery_candidates),
            "active_source_count": active_source_count,
            "exact_precision_query": int(exact_precision_query),
            "india_focused_location": int(india_focused_location),
            "partial_live_floor": partial_live_floor,
            "rejections": {
                "final_guard": 0,
                "company_cap": 0,
                "company_title_cap": 0,
                "similarity_cap": 0,
                "source_cap": 0,
                "post_selection_final_guard": 0,
            },
        }

        def source_selection_cap(source: str) -> int:
            trusted_dense_sources = {"remotive", "jobicy", "themuse", "greenhouse", "lever", "jooble", "adzuna"}
            sparse_sources = {"remoteok", "arbeitnow"}
            if source in sparse_sources:
                base_cap = 2 if source == "remoteok" else 1
            elif source in {"greenhouse", "lever"}:
                base_cap = 5
            elif source in trusted_dense_sources:
                base_cap = 4
            else:
                base_cap = 3

            dynamic_cap = max(
                base_cap,
                min(
                    limit,
                    (target_live_count + active_source_count - 1) // active_source_count + 1,
                ),
            )
            return dynamic_cap

        def maybe_add(candidates: list[dict], cap_per_company: int, *, allow_source_overflow: bool = False) -> None:
            rejection_counts = selection_debug["rejections"]
            india_focused_selection = self._is_india_focused_location(location)
            for item in candidates:
                if item in selected:
                    continue
                if self._is_location_hard_mismatch(location, item):
                    if isinstance(rejection_counts, dict):
                        rejection_counts["final_guard"] += 1
                    continue
                if not self._passes_final_live_guard(query, item):
                    if isinstance(rejection_counts, dict):
                        rejection_counts["final_guard"] += 1
                    continue
                source = str(item.get("source", "unknown")).lower()
                company = self._query_signature(str(item.get("company", ""))) or "unknown"
                title_key = self._query_signature(str(item.get("title", ""))) or "unknown"
                similarity_signature = self._job_similarity_signature(item)
                if company == "unknown":
                    fallback_company_id = self._query_signature(
                        item.get("external_id") or item.get("url") or item.get("location") or ""
                    )
                    company_identity = (
                        f"{source}::{fallback_company_id}"
                        if fallback_company_id
                        else f"{source}::{similarity_signature}"
                    )
                else:
                    company_identity = company
                company_title_key = f"{company_identity}::{title_key}"
                max_source_count = source_selection_cap(source)
                company_title_limit = 1
                has_distinct_listing_id = bool(
                    self._query_signature(item.get("external_id") or item.get("url") or "")
                )
                strong_distinct_india_listing = (
                    india_focused_selection
                    and has_distinct_listing_id
                    and len(selected) < target_live_count
                    and self._canonical_role_alignment(query, item) >= 1
                    and (
                        self._title_precision_score(query, item) >= 2
                        or self._title_hint_overlap(query, item) >= 1
                        or (
                            self._family_token_overlap(query, item) >= 1
                            and self._role_domain_match_score(query, item) >= 2
                        )
                    )
                )
                if (
                    not india_focused_selection
                    and has_distinct_listing_id
                    and source in {"jobicy", "themuse"}
                    and len(selected) < display_floor
                ):
                    company_title_limit = 3
                elif strong_distinct_india_listing and len(selected) < display_floor:
                    company_title_limit = 3
                elif (
                    india_focused_selection
                    and has_distinct_listing_id
                    and len(selected) >= display_floor
                    and len(selected) < target_live_count
                    and self._canonical_role_alignment(query, item) >= 2
                    and self._title_precision_score(query, item) >= 2
                ):
                    # Once India has a clean floor, allow one extra same-title
                    # listing from the same company if the posting has its own
                    # URL/id. This helps reach 15-20 without letting duplicates
                    # dominate the first page.
                    company_title_limit = 2
                elif (
                    not india_focused_selection
                    and has_distinct_listing_id
                    and len(selected) < display_floor
                    and self._canonical_role_alignment(query, item) >= 2
                    and self._title_precision_score(query, item) >= 2
                ):
                    company_title_limit = 2
                if company_counts.get(company_identity, 0) >= cap_per_company:
                    if isinstance(rejection_counts, dict):
                        rejection_counts["company_cap"] += 1
                    continue
                if company_title_counts.get(company_title_key, 0) >= company_title_limit:
                    if isinstance(rejection_counts, dict):
                        rejection_counts["company_title_cap"] += 1
                    continue
                if similarity_signature in selected_signatures:
                    allow_similarity_escape = (
                        strong_distinct_india_listing
                        or (
                            (
                                len(selected) < display_floor
                                or (has_distinct_listing_id and len(selected) < target_live_count)
                            )
                            and self._canonical_role_alignment(query, item) >= 2
                            and self._title_precision_score(query, item) >= 2
                        )
                    )
                    if not allow_similarity_escape:
                        if isinstance(rejection_counts, dict):
                            rejection_counts["similarity_cap"] += 1
                        continue
                if not allow_source_overflow and len(selected) >= display_floor and source_counts.get(source, 0) >= max_source_count:
                    if isinstance(rejection_counts, dict):
                        rejection_counts["source_cap"] += 1
                    continue
                selected.append(item)
                company_counts[company_identity] = company_counts.get(company_identity, 0) + 1
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
        if len(selected) < target_live_count and india_focused_location:
            india_target_fill_candidates = [
                item
                for item in ranked
                if not self._is_location_hard_mismatch(location, item)
                and self._passes_final_live_guard(query, item)
                and bool(self._query_signature(item.get("external_id") or item.get("url") or ""))
                and (
                    self._canonical_role_alignment(query, item) >= 1
                    or self._title_precision_score(query, item) >= 2
                    or self._title_hint_overlap(query, item) >= 1
                    or self._role_domain_match_score(query, item) >= 2
                )
            ]
            selection_debug["india_target_fill_candidates"] = len(india_target_fill_candidates)
            maybe_add(india_target_fill_candidates, cap_per_company=6, allow_source_overflow=True)

        post_filter_selected: list[dict] = []
        rejection_counts = selection_debug["rejections"]
        for item in selected:
            if not self._is_location_hard_mismatch(location, item) and self._passes_final_live_guard(query, item):
                post_filter_selected.append(item)
            elif isinstance(rejection_counts, dict):
                rejection_counts["post_selection_final_guard"] += 1
        selected = post_filter_selected
        self.last_live_job_snapshot = copy.deepcopy(selected[:limit])

        filtered_source_counts: dict[str, int] = {}
        for item in selected:
            source = str(item.get("source", "unknown")).lower()
            filtered_source_counts[source] = filtered_source_counts.get(source, 0) + 1
        family_safe_candidates = [
            item
            for item in live_jobs
            if not self._is_location_hard_mismatch(location, item) and self._passes_final_live_guard(query, item)
        ]
        raw_source_counts = self._count_items_by_source(live_jobs)
        precision_source_counts = self._count_items_by_source(precision_guarded)
        backup_source_counts = self._count_items_by_source(exact_backup_candidates)
        recovery_source_counts = self._count_items_by_source(same_family_recovery_candidates)
        family_safe_source_counts = self._count_items_by_source(family_safe_candidates)
        provider_request_summary = self._aggregate_provider_request_counts()
        provider_match_counts: dict[str, dict[str, int]] = {}
        for source in sorted(
            {
                *raw_source_counts.keys(),
                *precision_source_counts.keys(),
                *backup_source_counts.keys(),
                *recovery_source_counts.keys(),
                *family_safe_source_counts.keys(),
                *filtered_source_counts.keys(),
                *provider_request_summary.keys(),
            }
        ):
            request_summary = provider_request_summary.get(source, {})
            provider_match_counts[source] = {
                "requests": int(request_summary.get("requests", 0)),
                "raw_returned": int(request_summary.get("raw_returned", raw_source_counts.get(source, 0))),
                "precision_guarded": int(precision_source_counts.get(source, 0)),
                "exact_backup": int(backup_source_counts.get(source, 0)),
                "same_family_recovery": int(recovery_source_counts.get(source, 0)),
                "family_safe": int(family_safe_source_counts.get(source, 0)),
                "selected_live": int(filtered_source_counts.get(source, 0)),
                "timeouts": int(request_summary.get("timeouts", 0)),
                "errors": int(request_summary.get("errors", 0)),
            }
        timeout_sources = self._timeout_sources()
        required_live_floor = target_live_count if self._is_dense_production_family(query) else min(display_floor, target_live_count)
        if len(selected) >= required_live_floor:
            underfill_reason = "sufficient_live_supply"
        elif len(family_safe_candidates) >= required_live_floor:
            underfill_reason = "selector_over_pruning"
        elif timeout_sources:
            underfill_reason = "provider_timeout_or_upstream_scarcity"
        else:
            underfill_reason = "upstream_scarcity"
        selection_debug["upstream_family_safe_count"] = len(family_safe_candidates)
        selection_debug["provider_match_counts"] = provider_match_counts
        selection_debug["underfill"] = {
            "reason": underfill_reason,
            "required_live_floor": required_live_floor,
            "selected_live_count": len(selected),
            "upstream_family_safe_count": len(family_safe_candidates),
            "timeout_sources": timeout_sources,
        }
        selection_debug["selected_count"] = len(selected)
        selection_debug["selected_titles"] = [str(item.get("title", "")) for item in selected[:8]]
        selection_debug["selected_locations"] = [str(item.get("location", "")) for item in selected[:8]]
        selection_debug["selected_location_tiers"] = [
            str((item.get("normalized_data") or {}).get("location_match_tier") or self._location_match_tier(location, item))
            for item in selected[:8]
        ]
        selection_debug["selected_sources"] = filtered_source_counts
        self.last_fetch_diagnostics["selection_debug"] = selection_debug
        self.last_fetch_diagnostics["provider_request_summary"] = provider_request_summary
        self.last_fetch_diagnostics["provider_match_counts"] = provider_match_counts
        self.last_fetch_diagnostics["upstream_family_safe_count"] = len(family_safe_candidates)
        self.last_fetch_diagnostics["underfill"] = selection_debug["underfill"]
        if len(selected) < display_floor:
            logger.info(
                "Production selection debug for query=%s: %s",
                query,
                selection_debug,
            )
        return selected[:limit]

    def _passes_final_live_guard(self, query: str, item: dict) -> bool:
        query_domain = role_domain(query)
        normalized_query = normalize_role(query)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        specialty_overlap = self._specialty_token_overlap(query, item)
        normalized = item.get("normalized_data", {}) or {}
        role_fit = float(normalized.get("role_fit_score", 0.0))
        query_text = self._query_signature(query)
        title_text = self._expanded_title_query_text(item)

        if canonical_alignment <= -3:
            return False
        if self._requires_specialty_guard(query) and specialty_overlap == 0:
            return False
        if self._passes_family_bridge_guard(query, item):
            return True
        if normalized_query == "embedded engineer":
            has_embedded_signal = (
                self._contains_phrase(title_text, "embedded engineer")
                or self._contains_phrase(title_text, "embedded software")
                or self._contains_phrase(title_text, "embedded systems")
                or self._contains_phrase(title_text, "firmware")
                or self._contains_phrase(title_text, "microcontroller")
                or self._contains_phrase(title_text, "rtos")
                or self._contains_phrase(title_text, "iot")
                or (
                    self._contains_phrase(title_text, "embedded")
                    and re.search(r"\b(engineer|developer|software)\b", title_text)
                )
            )
            if not has_embedded_signal:
                return False
        if normalized_query == "enterprise applications engineer":
            has_platform_signal = any(
                self._contains_phrase(title_text, token)
                for token in {"salesforce", "sap", "erp", "crm", "oracle", "dynamics", "netsuite"}
            )
            has_role_signal = bool(
                re.search(r"\b(administrator|admin|developer|consultant|engineer|architect|analyst|specialist)\b", title_text)
            )
            if not (has_platform_signal and has_role_signal):
                return False
        if self._uses_strict_precision_guard(query):
            requested_leadership = {"manager", "director", "head", "chief", "vp", "vice", "president"}
            if not any(token in query_text.split() for token in requested_leadership) and re.search(
                r"\b(manager|director|head|chief|vp|vice president)\b",
                title_text,
            ):
                return False
            if query_domain == "data" and re.search(r"\bdata entry\b|\bentry operator\b", title_text):
                return False
            if (
                normalize_role(query) == "data analyst"
                and not self._contains_phrase(query_text, "business analyst")
                and self._contains_phrase(title_text, "business analyst")
            ):
                return False
        if query_domain in {"software", "security"} and title_precision <= 0 and title_overlap == 0 and family_overlap == 0:
            weak_software_recovery = (
                query_domain == "software"
                and self._is_weak_software_live_family(query)
                and canonical_alignment >= 1
                and specialty_overlap >= 1
                and role_fit >= 3.0
                and self._passes_contextual_family_recovery_guard(query, item)
            )
            if not weak_software_recovery:
                return False
        if (
            self._uses_strict_precision_guard(query)
            and title_precision <= 0
            and title_overlap == 0
            and core_title_overlap == 0
        ):
            if not self._passes_contextual_family_recovery_guard(query, item):
                return False
        if (
            self._uses_strict_precision_guard(query)
            and canonical_alignment == 1
            and title_precision <= 0
            and title_overlap == 0
        ):
            if not self._passes_contextual_family_recovery_guard(query, item):
                return False
        # Keep strict exact queries honest, but allow strong same-family recoveries
        # when the title is compact, aliased, or slightly reshaped by provider data.
        if query_domain == "data" and title_precision <= 0 and title_overlap == 0 and family_overlap == 0 and core_title_overlap == 0:
            if not self._passes_contextual_family_recovery_guard(query, item) and role_fit < 3.5:
                return False
        if query_domain == "data" and canonical_alignment < 0 and title_overlap == 0 and title_precision <= 0:
            return False
        if role_fit < 1.0 and title_precision <= 0 and core_title_overlap == 0:
            return False
        return True

    def _passes_family_bridge_guard(self, query: str, item: dict) -> bool:
        profile = role_profile(query)
        title_text = self._expanded_title_query_text(item)
        canonical_alignment = self._canonical_role_alignment(query, item)
        if canonical_alignment < 1:
            return False
        if self._is_mobile_web_mismatch(query, title_text):
            return False
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False
        role_fit = float((item.get("normalized_data") or {}).get("role_fit_score", 0.0))
        if role_fit < 1.5:
            return False
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        specialty_overlap = self._specialty_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        title_profile = role_profile(str(item.get("title", "")))
        head_overlap = bool(set(profile.head_terms) & set(title_profile.head_terms))

        if title_precision >= 1 or title_overlap >= 1:
            return True
        if profile.specialty_tokens and specialty_overlap >= 1 and not head_overlap and title_precision <= 0:
            return False
        if profile.domain == "data" and self._uses_strict_precision_guard(query):
            return canonical_alignment >= 0 and core_title_overlap >= 1 and (
                specialty_overlap >= 1
                or family_overlap >= 1
                or domain_score >= 2
                or role_fit >= 3.0
            )
        if canonical_alignment >= 3 and head_overlap and domain_score >= 2:
            return True
        if specialty_overlap >= 1 and (head_overlap or family_overlap >= 1) and domain_score >= 2:
            return True
        if canonical_alignment == 1 and head_overlap and (family_overlap >= 1 or domain_score >= 2):
            return core_title_overlap >= 1 or role_fit >= 2.0
        if profile.normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES and domain_score >= 2:
            return family_overlap >= 1 or role_fit >= 2.5
        return False

    def _contextual_signal_counts(self, query: str, item: dict) -> dict[str, int]:
        title_text = self._expanded_title_query_text(item)
        description_text = self._query_signature(item.get("description", ""))
        tags_text = " ".join(self._query_signature(tag) for tag in item.get("tags", []) if str(tag).strip())
        skills_text = " ".join(
            self._query_signature(skill)
            for skill in (item.get("normalized_data") or {}).get("skills", [])
            if str(skill).strip()
        )
        haystack = " ".join([title_text, description_text, tags_text, skills_text]).strip()
        if not haystack:
            return {"title_hint_hits": 0, "primary_hits": 0, "market_hits": 0}
        return {
            "title_hint_hits": sum(1 for hint in role_title_hints(query) if self._contains_phrase(title_text, hint)),
            "primary_hits": sum(1 for hint in role_primary_hints(query) if self._contains_phrase(haystack, hint)),
            "market_hits": sum(1 for hint in role_market_hints(query) if self._contains_phrase(haystack, hint)),
        }

    def _passes_contextual_family_recovery_guard(self, query: str, item: dict) -> bool:
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False

        query_domain = role_domain(query)
        if not query_domain:
            return False

        profile = role_profile(query)
        title_profile = role_profile(str(item.get("title", "")))
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        role_fit = float((item.get("normalized_data") or {}).get("role_fit_score", 0.0))
        head_overlap = bool(set(profile.head_terms) & set(title_profile.head_terms))
        signals = self._contextual_signal_counts(query, item)
        context_hits = (
            signals["title_hint_hits"]
            + (signals["primary_hits"] * 2)
            + min(signals["market_hits"], 4)
        )

        if title_precision >= 1 or title_overlap >= 1:
            return True
        if canonical_alignment < 0 and core_title_overlap < 2:
            return False
        if query_domain == "data":
            if title_overlap == 0 and core_title_overlap == 0:
                return (
                    (signals["market_hits"] >= 5 or skill_overlap >= 8.0)
                    and role_fit >= 2.0
                    and (
                        family_overlap >= 1
                        or domain_score >= 2
                        or head_overlap
                    )
                )
            return (
                (signals["primary_hits"] >= 3 or signals["market_hits"] >= 4 or skill_overlap >= 5.0)
                and role_fit >= 2.0
                and (
                    family_overlap >= 1
                    or core_title_overlap >= 1
                    or domain_score >= 2
                    or head_overlap
                )
            )
        if query_domain in {"software", "security", "infra"}:
            return (
                context_hits >= 3
                and role_fit >= 1.75
                and (
                    family_overlap >= 1
                    or core_title_overlap >= 1
                    or domain_score >= 2
                    or head_overlap
                )
            )
        return (
            context_hits >= 3
            and role_fit >= 2.0
            and (
                family_overlap >= 1
                or core_title_overlap >= 1
                or domain_score >= 2
                or head_overlap
            )
        )

    def _passes_exact_query_backup_guard(self, query: str, location: str, item: dict) -> bool:
        query_domain = role_domain(query)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        specialty_overlap = self._specialty_token_overlap(query, item)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        role_fit = float((item.get("normalized_data") or {}).get("role_fit_score", 0.0))

        if self._is_location_hard_mismatch(location, item):
            return False
        if self._passes_family_bridge_guard(query, item):
            return True
        if canonical_alignment < 0:
            return False
        if (
            self._uses_strict_precision_guard(query)
            and canonical_alignment == 1
            and title_precision <= 0
            and title_overlap == 0
        ):
            if not self._passes_contextual_family_recovery_guard(query, item):
                return False
        if self._requires_specialty_guard(query) and specialty_overlap == 0:
            return False
        if title_overlap >= 1 or title_precision >= 1:
            return True
        if query_domain == "data":
            if self._passes_contextual_family_recovery_guard(query, item):
                return True
            return canonical_alignment >= 0 and core_title_overlap >= 1 and (
                domain_score >= 2 or skill_overlap >= 1.0 or role_fit >= 3.0
            )
        if query_domain in {"software", "security"}:
            return core_title_overlap >= 1 and (
                domain_score >= 2 or role_fit >= 3.0 or family_overlap >= 1
            )
        return self._is_production_live_candidate(query, location, item, strict=False)

    def _passes_same_family_recovery_guard(self, query: str, location: str, item: dict) -> bool:
        if self._is_location_hard_mismatch(location, item):
            return False
        if not self._passes_final_live_guard(query, item):
            return False
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False
        if self._passes_family_bridge_guard(query, item):
            return True

        query_domain = role_domain(query)
        canonical_alignment = self._canonical_role_alignment(query, item)
        title_precision = self._title_precision_score(query, item)
        title_overlap = self._title_hint_overlap(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        domain_score = self._role_domain_match_score(query, item)
        skill_overlap = self._skill_overlap_score(query, item)
        role_fit = float((item.get("normalized_data") or {}).get("role_fit_score", 0.0))

        if query_domain == "data":
            if (
                self._uses_strict_precision_guard(query)
                and canonical_alignment == 1
                and title_precision <= 0
                and title_overlap == 0
            ):
                return self._passes_contextual_family_recovery_guard(query, item)
            if title_precision >= 1 or title_overlap >= 1:
                return True
            if self._passes_contextual_family_recovery_guard(query, item):
                return True
            return (
                canonical_alignment >= 0
                and core_title_overlap >= 1
                and (
                    family_overlap >= 1
                    or domain_score >= 2
                    or skill_overlap >= 1.0
                    or role_fit >= 2.5
                )
            )
        if canonical_alignment >= 1 and core_title_overlap >= 1:
            return True
        if title_precision >= 1 or title_overlap >= 1:
            return True
        if query_domain in {"software", "security"}:
            return core_title_overlap >= 1 and (
                family_overlap >= 1
                or skill_overlap >= 1.0
                or role_fit >= 2.5
            )
        return self._is_family_live_candidate(query, location, item)

    def _passes_precise_query_guard(self, query: str, item: dict) -> bool:
        profile = role_profile(query)
        raw_query = self._query_signature(profile.cleaned_query or query)
        raw_tokens = [
            token
            for token in raw_query.split()
            if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS
        ]
        if not raw_tokens:
            return True
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False

        title_text = self._expanded_title_query_text(item)
        description_text = self._query_signature(item.get("description", ""))
        tags_text = " ".join(self._query_signature(tag) for tag in item.get("tags", []) if str(tag).strip())
        if self._is_mobile_web_mismatch(query, title_text):
            return False
        matched_title_hints = self._matched_title_hints(query, item)
        raw_phrase_title_hit = self._contains_phrase(title_text, raw_query) or self._contains_phrase(tags_text, raw_query)
        raw_token_hits = sum(
            1
            for token in raw_tokens
            if self._contains_phrase(title_text, token)
            or self._contains_phrase(description_text, token)
            or self._contains_phrase(tags_text, token)
        )
        raw_title_token_hits = sum(
            1
            for token in raw_tokens
            if self._contains_phrase(title_text, token)
            or self._contains_phrase(tags_text, token)
        )
        title_hint_overlap = self._title_hint_overlap(query, item)
        title_precision = self._title_precision_score(query, item)
        family_overlap = self._family_token_overlap(query, item)
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        multi_word_title_hint_hit = any(" " in hint for hint in matched_title_hints)

        exact_precision_query = self._uses_strict_precision_guard(query)

        if exact_precision_query:
            if raw_phrase_title_hit:
                return True
            if multi_word_title_hint_hit:
                return True
            if len(raw_tokens) >= 2 and raw_title_token_hits >= 2:
                return True
            if title_precision >= 2 and (core_title_overlap >= 2 or family_overlap >= 2):
                return True
            return False

        if raw_phrase_title_hit or raw_token_hits >= 1:
            return True
        if title_hint_overlap >= 1:
            return True
        if title_precision >= 2:
            return True
        if self._skill_overlap_score(query, item) >= 1.5 and self._role_domain_match_score(query, item) >= 2:
            return True
        return False

    def _uses_strict_precision_guard(self, query: str) -> bool:
        profile = role_profile(query)
        if profile.cleaned_query == profile.normalized_role:
            return True
        if len(profile.cleaned_query.split()) < 2:
            return False
        if profile.normalized_role == "frontend developer":
            return False
        alias_target = profile.family_role or profile.normalized_role
        if not alias_target:
            return False
        known_aliases = {
            self._query_signature(alias)
            for alias in (
                provider_query_variations(alias_target, "jobicy", production=True)
                + production_query_variations(alias_target)
            )
            if alias
        }
        return profile.cleaned_query in known_aliases or profile.cleaned_query in {
            self._query_signature(hint) for hint in role_title_hints(query)
        }

    def _expanded_title_query_text(self, item: dict) -> str:
        raw_title = str(item.get("title", "") or "")
        cleaned_title = self._query_signature(raw_title)
        expanded_title = self._query_signature(role_profile(raw_title).cleaned_query)
        if expanded_title and expanded_title != cleaned_title:
            return f"{cleaned_title} {expanded_title}".strip()
        return cleaned_title

    def _matched_title_hints(self, query: str, item: dict) -> set[str]:
        title = self._expanded_title_query_text(item)
        matched: set[str] = set()
        for hint in role_title_hints(query):
            if self._contains_phrase(title, hint):
                matched.add(hint)
        return matched

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
            if cleaned_needle in cleaned_haystack:
                return True
            compact_haystack = re.sub(r"[^a-z0-9+]+", "", cleaned_haystack)
            compact_needle = re.sub(r"[^a-z0-9+]+", "", cleaned_needle)
            return bool(compact_needle and compact_needle in compact_haystack)
        if re.search(rf"\b{re.escape(cleaned_needle)}\b", cleaned_haystack):
            return True
        compact_haystack = re.sub(r"[^a-z0-9+]+", "", cleaned_haystack)
        compact_needle = re.sub(r"[^a-z0-9+]+", "", cleaned_needle)
        return bool(len(compact_needle) >= 8 and compact_needle in compact_haystack)

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
        canonical_role = profile.family_role or profile.normalized_role
        if profile.domain in BUSINESS_PRODUCTION_DOMAINS:
            return False
        if canonical_role in {"devops engineer", "support engineer", "technical writer", "ui/ux designer"}:
            return False
        if profile.normalized_role == "frontend developer":
            return False
        if self._is_generic_security_search_query(query):
            return False
        if self._is_security_analyst_style_query(query):
            return False
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
        expanded_title = self._expanded_title_query_text(item)
        haystack = " ".join(
            [
                expanded_title,
                re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("description", "")).lower()),
                " ".join(re.sub(r"[^a-z0-9+ ]+", " ", str(tag).lower()) for tag in item.get("tags", []) if str(tag).strip()),
                extracted_skills,
            ]
        )
        return sum(1 for token in specialty_tokens if self._contains_phrase(haystack, token))

    def _has_explicit_role_alignment(self, query: str, item: dict) -> bool:
        title_text = self._expanded_title_query_text(item)
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
        title_text = self._expanded_title_query_text(item)
        haystack = title_text
        if include_description:
            desc_text = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("description", "")).lower())
            haystack = f"{title_text} {desc_text}"
        return sum(1 for token in query_tokens if self._contains_phrase(haystack, token))

    def _canonical_role_alignment(self, query: str, item: dict) -> int:
        title_text = self._expanded_title_query_text(item)
        negative_hints = role_negative_title_hints(query)
        if negative_hints and any(self._contains_phrase(title_text, hint) for hint in negative_hints):
            return -3
        return canonical_role_alignment(query, title_text)

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

    def _unrequested_title_penalty(self, query: str, item: dict) -> int:
        query_text = self._query_signature(query)
        title_text = self._expanded_title_query_text(item)
        if not title_text:
            return 0
        requested_leadership = {"manager", "director", "head", "chief", "lead", "principal", "staff", "cto"}
        if any(token in query_text.split() for token in requested_leadership):
            return 0

        penalty = 0
        if re.search(r"\b(manager|director|head|chief|vp|vice president)\b", title_text):
            penalty += 2
        if "architect" in title_text and "architect" not in query_text:
            penalty += 1
        if "program manager" in title_text and "program manager" not in query_text:
            penalty += 1
        return penalty

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
        business_title_match = (
            query_domain in BUSINESS_PRODUCTION_DOMAINS
            and canonical_alignment >= 0
            and (
                title_overlap >= 1
                or title_precision >= 1
                or explicit_alignment
                or (domain_score >= 2 and (core_title_overlap >= 1 or family_overlap >= 1))
            )
        )
        if business_title_match and location_score > 0.0:
            if strict:
                return market_quality >= 10.0 or role_fit >= 0.4 or title_alignment >= 8.0
            return market_quality >= 8.0 or role_fit >= 0.2 or title_alignment >= 5.0
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
            if source in {"themuse", "jobicy"} and domain_score >= 2 and (
                title_overlap >= 1
                or family_overlap >= 1
                or (skill_overlap >= 1.5 and core_title_overlap >= 1)
            ):
                return True
            if core_title_overlap >= 1 and skill_overlap >= 1.0:
                return True
            if source in {"themuse", "jobicy"} and (title_overlap >= 1 or family_overlap >= 2) and market_quality >= 18.0:
                return True
            if title_overlap >= 1 and role_fit >= 1.0:
                return True
            if title_overlap >= 1 and skill_overlap >= 1.0:
                return True
            if domain_score >= 2 and skill_overlap >= 1.5 and (
                title_precision >= 1 or title_overlap >= 1 or family_overlap >= 1 or core_title_overlap >= 1
            ):
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
        # This path is intentionally more lenient than the strict=True path.
        # A higher threshold here caused valid secondary candidates to be discarded.
        if not explicit_alignment and market_quality < 40.0:
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
        core_title_overlap = self._core_token_overlap(query, item, include_description=False)
        title_precision = self._title_precision_score(query, item)
        if self._is_location_hard_mismatch(location, item):
            return False
        if self._requires_specialty_guard(query) and self._specialty_token_overlap(query, item) == 0:
            return False
        if self._passes_family_bridge_guard(query, item):
            return True
        if self._canonical_role_alignment(query, item) < 0 and title_overlap == 0:
            return False
        if (
            role_domain(query) in BUSINESS_PRODUCTION_DOMAINS
            and (
                title_overlap >= 1
                or title_precision >= 1
                or (domain_score >= 2 and (core_title_overlap >= 1 or family_overlap >= 1))
            )
        ):
            return True
        if (
            role_domain(query) == "data"
            and title_overlap == 0
            and family_overlap == 0
            and title_precision <= 0
            and core_title_overlap == 0
            and role_fit < 3.25
        ):
            return False
        return (
            (
                domain_score >= 2
                and skill_overlap >= 1.0
                and (title_precision >= 1 or title_overlap >= 1 or family_overlap >= 1 or core_title_overlap >= 1)
            )
            or (domain_score >= 1 and title_overlap >= 1)
            or (domain_score >= 1 and family_overlap >= 2 and role_fit >= 2.0)
            or (
                domain_score >= 1
                and market_quality >= 28.0
                and skill_overlap >= 1.0
                and title_precision >= 1
            )
        )

    def _title_hint_overlap(self, query: str, item: dict) -> int:
        return len(self._matched_title_hints(query, item))

    def _family_token_overlap(self, query: str, item: dict) -> int:
        raw_title = self._expanded_title_query_text(item)
        title_tokens = {token for token in raw_title.split() if token and len(token) > 2}
        query_tokens = {
            token
            for token in re.split(r"[^a-z0-9+]+", role_family(query))
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
        if self._is_dense_production_family(query):
            return min(limit, settings.production_live_fetch_maximum)
        return min(limit, max(PRODUCTION_MIN_LIVE_TARGET, settings.production_live_display_minimum, settings.production_live_fetch_minimum))

    def _production_display_floor(self, *, query: str, limit: int) -> int:
        if is_sparse_live_market_role(query):
            return min(limit, 1)
        if self._is_dense_production_family(query):
            return min(limit, max(6, settings.production_live_display_minimum))
        return min(limit, max(PRODUCTION_MIN_LIVE_TARGET, min(settings.production_live_display_minimum, settings.production_live_fetch_minimum)))

    def _production_partial_live_floor(self, *, query: str, limit: int) -> int:
        if is_sparse_live_market_role(query):
            return min(limit, 1)
        if self._is_dense_production_family(query):
            display_floor = self._production_display_floor(query=query, limit=limit)
            return min(limit, max(4, display_floor - 2))
        return min(limit, 4)

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
        title_text = self._expanded_title_query_text(item)
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
        if (
            role_domain(query) in BUSINESS_PRODUCTION_DOMAINS
            and (
                title_overlap >= 1
                or title_precision >= 1
                or explicit_alignment
                or (domain_score >= 2 and (core_title_overlap >= 1 or family_overlap >= 1))
            )
        ):
            return requirement_quality >= 8.0 or listing_quality >= 4.0 or role_fit >= 0.3
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

    def _location_match_tier(self, requested_location: str, item: dict) -> str:
        requested = normalize_role(str(requested_location or "")).strip().lower()
        job_location = normalize_role(str(item.get("location", "") or ""))
        raw_location = str(item.get("location", "") or "").strip().lower()
        remote = bool(item.get("remote")) or any(token in job_location for token in GLOBAL_REMOTE_HINTS)

        if not requested or requested in GLOBAL_REMOTE_HINTS:
            return "global_remote" if remote else "global_open"
        if requested == "india" or self._is_india_focused_location(requested_location):
            if any(token in job_location for token in INDIA_LOCATION_HINTS):
                return "india"
            if any(token in job_location for token in ASIA_LOCATION_HINTS):
                return "asia"
            if any(token in job_location for token in GLOBAL_REMOTE_HINTS):
                return "worldwide_remote"
            if remote and not raw_location:
                return "remote_unspecified"
            if remote and any(token in job_location for token in NON_INDIA_REGION_HINTS):
                return "remote_non_india_region"
            if any(token in job_location for token in NON_INDIA_REGION_HINTS):
                return "non_india_region"
            if remote:
                return "remote_other"
            if not raw_location or raw_location == "unknown":
                return "unknown"
            return "other_location"
        if requested and requested in job_location:
            return "exact"
        if remote:
            return "remote_fallback"
        return "other_location"

    def _is_location_hard_mismatch(self, requested_location: str, item: dict) -> bool:
        if not self._is_india_focused_location(requested_location):
            return False
        tier = self._location_match_tier(requested_location, item)
        # India searches should not quietly degrade into generic global remote
        # listings. Keep India, nearby Asia, and unknown ATS rows that lack a
        # location field, but reject explicit non-India/global-remote fallbacks
        # so the dropdown behaves as a real market filter rather than just sort.
        return tier not in {"india", "asia", "unknown"}

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

    def _search_queries(self, provider: object, query: str, location: str) -> list[str]:
        if not getattr(provider, "supports_query_variations", True):
            normalized = role_family(query) or normalize_role(query)
            return [normalized or query]
        source_name = str(getattr(provider, "source_name", provider.__class__.__name__)).lower()
        if settings.environment == "production":
            variations = provider_query_variations(query, source_name, production=True)
            if self._is_india_focused_location(location) and source_name in {"remotive"}:
                # Remotive has no location parameter. Add one India-biased query
                # without dropping the canonical query, so India runs get a real
                # market signal instead of only global remote leftovers.
                localized_variations: list[str] = []
                for variation in variations[:2]:
                    localized_variations.extend([variation, f"{variation} india"])
                variations = list(dict.fromkeys([*localized_variations, *variations]))
            cap = self._production_search_query_cap(source_name=source_name, query=query, location=location)
            if cap is not None:
                return variations[: max(1, cap)]
            return variations
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
