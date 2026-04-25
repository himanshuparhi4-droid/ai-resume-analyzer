from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import (
    normalize_role,
    role_baseline_skills,
    role_domain,
    role_family,
    role_fit_score,
    role_profile,
    role_title_alignment_score,
)
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION, extract_job_requirement_profile
from app.utils.text import strip_html, truncate

_GREENHOUSE_BOARD_INDEX_CACHE: dict[str, dict] = {}
_GREENHOUSE_JOB_DETAIL_CACHE: dict[str, dict] = {}
_MAX_GREENHOUSE_BOARD_CACHE_ENTRIES = 12
_MAX_GREENHOUSE_DETAIL_CACHE_ENTRIES = 96
logger = logging.getLogger(__name__)

# Public Greenhouse boards used as a lightweight ATS corpus when the app is
# running without custom board tokens. The set is role-family based so dense
# modern searches can widen live coverage without hard-coding per-role results.
_CURATED_GREENHOUSE_BOARDS = {
    "data": [
        "yipitdata",
        "affirm",
        "asana",
        "discord",
        "robinhood",
        "instacart",
    ],
    "software": [
        "stripe",
        "anthropic",
        "cloudflare",
        "datadog",
        "mongodb",
        "gitlab",
        "discord",
        "okta",
        "zscaler",
        "asana",
        "samsara",
        "robinhood",
        "coinbase",
        "instacart",
        "affirm",
        "rubrik",
        "figma",
    ],
    "security": [
        "stripe",
        "anthropic",
        "cloudflare",
        "okta",
        "zscaler",
        "datadog",
        "gitlab",
        "wizinc",
        "asana",
        "discord",
        "rubrik",
        "samsara",
        "robinhood",
        "affirm",
        "coinbase",
        "mongodb",
        "lyft",
    ],
    "product": [
        "airbnb",
        "asana",
        "affirm",
        "robinhood",
        "instacart",
        "figma",
        "discord",
    ],
    "design": [
        "figma",
        "airbnb",
        "asana",
        "discord",
    ],
}

_INDIA_LOCATION_HINTS = {
    "india",
    "indian",
    "bengaluru",
    "bangalore",
    "hyderabad",
    "pune",
    "mumbai",
    "delhi",
    "gurgaon",
    "gurugram",
    "noida",
    "chennai",
    "kolkata",
    "ahmedabad",
}
_INDIA_GREENHOUSE_BOARDS = {
    "data": ["yipitdata", "okta", "rubrik", "airbnb", "figma"],
    "software": ["okta", "rubrik", "yipitdata", "figma", "airbnb"],
    "security": ["okta", "rubrik", "yipitdata", "airbnb", "figma"],
    "product": ["airbnb", "figma", "okta", "rubrik"],
    "design": ["figma", "airbnb", "okta"],
}

_ROLE_SPECIFIC_GREENHOUSE_BOARDS = {
    "data analyst": ["yipitdata", "instacart", "affirm", "robinhood", "asana", "discord"],
    "data scientist": ["yipitdata", "asana", "affirm", "instacart", "robinhood", "discord"],
    "data engineer": ["yipitdata", "instacart", "asana", "affirm", "robinhood", "okta"],
    "software engineer": ["stripe", "anthropic", "cloudflare", "datadog", "mongodb", "gitlab", "okta", "discord", "asana", "figma", "robinhood", "affirm"],
    "full stack developer": ["okta", "discord", "asana", "figma", "robinhood"],
    "frontend developer": ["figma", "discord", "asana", "okta", "robinhood"],
    "devops engineer": ["okta", "affirm", "instacart", "rubrik", "asana"],
    "cybersecurity engineer": ["stripe", "anthropic", "cloudflare", "okta", "zscaler", "datadog", "gitlab", "wizinc", "asana", "discord", "rubrik", "samsara", "robinhood", "affirm", "coinbase", "mongodb", "lyft", "instacart"],
    "solutions architect": ["okta", "rubrik", "affirm", "asana", "robinhood"],
    "support engineer": ["okta", "asana", "robinhood", "affirm"],
}


class GreenhouseProvider:
    source_name = "greenhouse"
    supports_query_variations = False
    supports_location_variations = False

    async def search(self, query: str, location: str, limit: int) -> list[dict]:
        normalized_query = normalize_role(query)
        family_role = role_family(query)
        domain = role_domain(query)
        profile = role_profile(query)
        india_focused_location = self._is_india_focused_location(location)
        boards = self._boards_for_query(query, location=location)
        if not boards:
            return []

        timeout = httpx.Timeout(
            connect=min(4.0, settings.job_request_timeout_seconds),
            read=settings.job_request_timeout_seconds,
            write=10.0,
            pool=5.0,
        )
        async def fetch_board_batch(client: httpx.AsyncClient, board_batch: list[str]) -> list[object]:
            board_tasks = [asyncio.create_task(self._fetch_board_index(board, client=client)) for board in board_batch]
            try:
                if settings.environment == "production":
                    # The board request itself can legitimately take connect + read time
                    # on Render. A shorter batch timeout was racing successful HTTP 200s
                    # and turning useful board indexes into false Greenhouse timeouts.
                    if india_focused_location:
                        batch_timeout = min(9.0, max(6.75, settings.job_request_timeout_seconds - 4.0))
                    else:
                        batch_timeout = min(6.25, max(4.75, settings.job_request_timeout_seconds - 7.0))
                    done, pending = await asyncio.wait(board_tasks, timeout=batch_timeout)
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    batch_results: list[object] = []
                    for task in board_tasks:
                        if task not in done or task.cancelled():
                            batch_results.append(TimeoutError("greenhouse board index timeout"))
                            continue
                        try:
                            batch_results.append(task.result())
                        except Exception as exc:
                            batch_results.append(exc)
                    return batch_results
                return list(await asyncio.gather(*board_tasks, return_exceptions=True))
            except asyncio.CancelledError:
                for task in board_tasks:
                    task.cancel()
                await asyncio.gather(*board_tasks, return_exceptions=True)
                raise

        def absorb_board_results(board_batch: list[str], board_results: list[object]) -> None:
            for board, items in zip(board_batch, board_results):
                if isinstance(items, Exception):
                    logger.warning("Greenhouse board index fetch failed for %s: %s", board, items)
                    continue
                for item in items:
                    link = str(item.get("url") or item.get("external_id") or "").strip()
                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)
                    candidates.append(item)

        seen_links: set[str] = set()
        candidates: list[dict] = []
        async with httpx.AsyncClient(timeout=timeout) as client:
            board_results_raw = await fetch_board_batch(client, boards)
            absorb_board_results(boards, board_results_raw)
            if not candidates and settings.environment == "production" and not india_focused_location:
                alternate_boards = self._fallback_boards_for_query(query, tried=set(boards), limit=2)
                if alternate_boards:
                    alternate_results_raw = await fetch_board_batch(client, alternate_boards)
                    absorb_board_results(alternate_boards, alternate_results_raw)

            if not candidates:
                return []
            if india_focused_location:
                # India searches should not spend Render's small timeout budget
                # ranking global ATS rows. Keep exact India board matches first,
                # then fall back to nearby APAC/South Asia rows only if no India
                # locations were present in the fetched board indexes.
                india_location_candidates = [
                    item for item in candidates if self._location_alignment_score(location, item) >= 1.0
                ]
                if india_location_candidates:
                    candidates = india_location_candidates
                else:
                    asia_location_candidates = [
                        item for item in candidates if self._location_alignment_score(location, item) >= 0.86
                    ]
                    if asia_location_candidates:
                        candidates = asia_location_candidates

            target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
            security_analyst_style = family_role == "cybersecurity engineer" and "analyst" in profile.head_terms
            weak_software_family = family_role in {"frontend developer", "mobile developer", "embedded engineer"}
            if weak_software_family:
                # UI/mobile/embedded families benefit from board diversity, but
                # full hydration on Render free tier is expensive and tends to
                # time out before the selector sees the ATS-backed candidates.
                detail_fetch_budget = min(max(limit // 2, 2), 3)
                board_budget = 1
            elif security_analyst_style:
                # Analyst-shaped security searches usually get more value from
                # broad, index-only ATS coverage than from slow detail hydration.
                detail_fetch_budget = min(max(limit + 5, 18), 24)
                board_budget = 3
            elif domain == "security":
                detail_fetch_budget = min(max(limit + 5, 18), 24)
                board_budget = 3
            elif domain == "software" or family_role in {"devops engineer", "qa engineer", "solutions architect"}:
                detail_fetch_budget = min(max(limit + 2, 10), 14)
                board_budget = 2
            elif domain == "data" or family_role in {
                "product manager",
                "support engineer",
                "enterprise applications engineer",
                "technical writer",
                "ui/ux designer",
                "engineering leadership",
            }:
                # On Render free tier, index fetches are usually cheap while
                # detail hydration is the expensive step. Keep one strong role
                # candidate per curated board so the selector gets ATS-backed
                # diversity without forcing a long tail of detail requests.
                detail_fetch_budget = min(max(limit // 2 + 2, 4), 6)
                board_budget = 1
            else:
                detail_fetch_budget = min(max(limit * 2, 18), 24)
                board_budget = 4
            if settings.environment == "production":
                # Production now returns selected index-backed jobs without
                # detail hydration, so widening this budget improves coverage
                # without adding the old slow detail-fetch tail.
                detail_fetch_budget = min(max(detail_fetch_budget, min(limit, 10)), target_candidates)
                board_budget = max(board_budget, 2)
                if india_focused_location:
                    detail_fetch_budget = min(max(detail_fetch_budget, limit), target_candidates)
                    board_budget = max(board_budget, 3)
            positively_aligned = [
                item
                for item in candidates
                if role_title_alignment_score(
                    query,
                    str(item.get("title", "")),
                    description="",
                    tags=item.get("tags") or [],
                )
                > 0
            ]
            ranked_pool = (
                positively_aligned
                if india_focused_location and positively_aligned
                else positively_aligned if len(positively_aligned) >= max(limit * 2, 12) else candidates
            )
            ranked_candidates = sorted(
                ranked_pool,
                key=lambda item: (
                    self._location_alignment_score(location, item) if india_focused_location else 0.0,
                    role_title_alignment_score(
                        query,
                        str(item.get("title", "")),
                        description="",
                        tags=item.get("tags") or [],
                    ),
                    role_fit_score(query, item),
                    1 if item.get("remote") else 0,
                ),
                reverse=True,
            )

            selected_candidates: list[dict] = []
            board_counts: dict[str, int] = {}
            for item in ranked_candidates:
                board = str((item.get("normalized_data") or {}).get("board_token") or "")
                if not board:
                    continue
                if board_counts.get(board, 0) >= board_budget:
                    continue
                selected_candidates.append(item)
                board_counts[board] = board_counts.get(board, 0) + 1
                if len(selected_candidates) >= detail_fetch_budget:
                    break

            if settings.environment == "production":
                # The board index is already a live ATS source. Returning the
                # selected index candidates immediately prevents slow detail
                # hydration from causing the whole provider result to be
                # cancelled by the aggregator soft timeout.
                jobs = [
                    job
                    for item in selected_candidates
                    if (job := self._index_fallback_job(item, detail_hydration_skipped=True))
                ]
                positively_aligned_index_jobs = [
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
                ranked_pool = (
                    positively_aligned_index_jobs
                    if (india_focused_location and positively_aligned_index_jobs)
                    or len(positively_aligned_index_jobs) >= max(limit * 2, 12)
                    else jobs
                )
                ranked = sorted(
                    ranked_pool,
                    key=lambda item: (
                        self._location_alignment_score(location, item) if india_focused_location else 0.0,
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

            hydrated_raw = await asyncio.gather(
                *(
                    self._fetch_job_detail(
                        board=str((item.get("normalized_data") or {}).get("board_token") or ""),
                        job_id=str(item.get("external_id") or ""),
                        fallback=item,
                        client=client,
                    )
                    for item in selected_candidates
                ),
                return_exceptions=True,
            )

        jobs: list[dict] = []
        for candidate, item in zip(selected_candidates, hydrated_raw):
            if isinstance(item, Exception):
                logger.warning(
                    "Greenhouse job detail fetch failed for board=%s job=%s: %s",
                    str((candidate.get("normalized_data") or {}).get("board_token") or ""),
                    str(candidate.get("external_id") or ""),
                    item,
                )
                fallback_item = self._index_fallback_job(candidate)
                if fallback_item:
                    jobs.append(fallback_item)
                continue
            if item:
                jobs.append(item)
        positively_aligned_detailed = [
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
        ranked_pool = positively_aligned_detailed if len(positively_aligned_detailed) >= max(limit * 2, 12) else jobs
        ranked = sorted(
            ranked_pool,
            key=lambda item: (
                self._location_alignment_score(location, item) if self._is_india_focused_location(location) else 0.0,
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

    def _boards_for_query(self, query: str, *, location: str = "") -> list[str]:
        normalized = normalize_role(query)
        profile = role_profile(query)
        specific = _ROLE_SPECIFIC_GREENHOUSE_BOARDS.get(normalized)
        if specific:
            boards = list(specific)
        elif settings.has_greenhouse_boards:
            boards = settings.greenhouse_board_tokens
        else:
            boards = list(_CURATED_GREENHOUSE_BOARDS.get(role_domain(query) or "", []))
        india_focused_location = self._is_india_focused_location(location)
        if india_focused_location:
            family_domain = role_domain(query) or role_domain(normalized)
            boards = [
                *_INDIA_GREENHOUSE_BOARDS.get(family_domain or "", []),
                *boards,
            ]
        boards = list(dict.fromkeys(boards))
        if settings.environment == "production":
            family_domain = role_domain(query) or role_domain(normalized)
            board_limit = {
                "data": 5,
                "software": 8,
                "security": 8,
                "product": 4,
                "design": 3,
            }.get(family_domain, len(boards))
            if normalized in {"frontend developer", "mobile developer", "embedded engineer"}:
                board_limit = min(board_limit, 2)
            if normalized == "cybersecurity engineer" and "analyst" in profile.head_terms:
                board_limit = min(board_limit, 6)
            if india_focused_location:
                board_limit = max(board_limit, 6)
            boards = boards[:board_limit]
        return boards

    def _is_india_focused_location(self, location: str) -> bool:
        lowered = normalize_role(str(location or ""))
        return bool(lowered and any(hint in lowered for hint in _INDIA_LOCATION_HINTS))

    def _location_alignment_score(self, requested_location: str, item: dict) -> float:
        requested = normalize_role(str(requested_location or "")).strip().lower()
        job_location = str(item.get("location", "") or "").strip().lower()
        remote = bool(item.get("remote"))
        if not requested or requested in {"global", "remote", "worldwide", "anywhere"}:
            return 1.0 if remote or "remote" in job_location else 0.8
        if requested in job_location:
            return 1.0
        if self._is_india_focused_location(requested):
            if any(token in job_location for token in _INDIA_LOCATION_HINTS):
                return 1.0
            if any(token in job_location for token in {"apac", "asia", "south asia"}):
                return 0.86
            if remote and not job_location:
                return 0.82
            if "remote" in job_location or "worldwide" in job_location or "global" in job_location:
                return 0.8
            return 0.5
        if remote:
            return 0.72
        return 0.5

    def _fallback_boards_for_query(self, query: str, *, tried: set[str], limit: int) -> list[str]:
        normalized = normalize_role(query)
        domain = role_domain(query) or role_domain(normalized)
        candidates = [
            *_ROLE_SPECIFIC_GREENHOUSE_BOARDS.get(normalized, []),
            *_CURATED_GREENHOUSE_BOARDS.get(domain or "", []),
        ]
        if settings.has_greenhouse_boards:
            candidates.extend(settings.greenhouse_board_tokens)
        return [
            board
            for board in dict.fromkeys(candidates)
            if board and board not in tried
        ][:limit]

    async def _fetch_board_index(self, board: str, *, client: httpx.AsyncClient) -> list[dict]:
        self._prune_cache(_GREENHOUSE_BOARD_INDEX_CACHE, max_entries=_MAX_GREENHOUSE_BOARD_CACHE_ENTRIES)
        cached = _GREENHOUSE_BOARD_INDEX_CACHE.get(board)
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        if cached and now - cached["stored_at"] < ttl:
            return cached["jobs"]

        endpoint = f"{settings.greenhouse_base_url}/{board}/jobs"
        response = await client.get(
            endpoint,
            timeout=httpx.Timeout(
                connect=min(2.5, settings.job_request_timeout_seconds),
                read=min(3.5, settings.job_request_timeout_seconds),
                write=5.0,
                pool=3.0,
            ),
        )
        response.raise_for_status()
        payload = response.json()
        raw_jobs = payload.get("jobs") or []
        if settings.environment == "production":
            raw_jobs = raw_jobs[:45]

        jobs: list[dict] = []
        for item in raw_jobs:
            title = str(item.get("title") or "Unknown Role")
            location = str((item.get("location") or {}).get("name") or "Unknown")
            company = str(item.get("company_name") or self._company_from_board(board))
            tags = [tag for tag in [company, board, location] if tag]
            lightweight_description = (
                f"{title} role at {company}. Greenhouse ATS listing from the {board} board"
                + (f" in {location}." if location and location != "Unknown" else ".")
            )
            if settings.environment == "production":
                requirement_profile = {
                    "skills": [],
                    "skill_weights": {},
                    "skill_evidence": [],
                    "skill_extraction_mode": "greenhouse-index-title-only",
                    "requirement_quality": 0.0,
                    "normalization_version": JOB_REQUIREMENT_PROFILE_VERSION,
                }
            else:
                requirement_profile = extract_job_requirement_profile(
                    title=title,
                    description=lightweight_description,
                    tags=tags,
                    source="greenhouse-index",
                )
            jobs.append(
                {
                    "source": self.source_name,
                    "external_id": str(item.get("id") or item.get("absolute_url") or ""),
                    "title": title,
                    "company": company,
                    "location": location,
                    "remote": "remote" in location.lower(),
                    "url": str(item.get("absolute_url") or ""),
                    "description": lightweight_description,
                    "preview": truncate(lightweight_description, 260),
                    "tags": tags,
                    "normalized_data": {
                        "board_token": board,
                        "index_only": True,
                        **requirement_profile,
                    },
                    "posted_at": self._parse_datetime(item.get("updated_at") or item.get("updatedAt")),
                }
            )

        _GREENHOUSE_BOARD_INDEX_CACHE[board] = {"stored_at": now, "jobs": jobs}
        return jobs

    async def _fetch_job_detail(
        self,
        *,
        board: str,
        job_id: str,
        fallback: dict,
        client: httpx.AsyncClient,
    ) -> dict | None:
        if not board or not job_id:
            return None
        self._prune_cache(_GREENHOUSE_JOB_DETAIL_CACHE, max_entries=_MAX_GREENHOUSE_DETAIL_CACHE_ENTRIES)
        cache_key = f"{board}:{job_id}"
        cached = _GREENHOUSE_JOB_DETAIL_CACHE.get(cache_key)
        now = datetime.now(UTC)
        ttl = timedelta(minutes=settings.ats_board_cache_ttl_minutes)
        if cached and now - cached["stored_at"] < ttl:
            return cached["job"]

        endpoint = f"{settings.greenhouse_base_url}/{board}/jobs/{job_id}"
        response = await client.get(
            endpoint,
            params={"content": "true"},
            timeout=httpx.Timeout(
                connect=min(2.0, settings.job_request_timeout_seconds),
                read=min(2.75, settings.job_request_timeout_seconds),
                write=5.0,
                pool=3.0,
            ),
        )
        response.raise_for_status()
        item = response.json()

        raw_description = strip_html(str(item.get("content") or ""))
        title = str(item.get("title") or fallback.get("title") or "Unknown Role")
        location = str((item.get("location") or {}).get("name") or fallback.get("location") or "Unknown")
        company = str(item.get("company_name") or fallback.get("company") or self._company_from_board(board))
        metadata = item.get("metadata") or []
        department_tags = [str(entry.get("name") or "").strip() for entry in (item.get("departments") or []) if entry.get("name")]
        office_tags = [str(entry.get("name") or "").strip() for entry in (item.get("offices") or []) if entry.get("name")]
        metadata_tags = [str(entry.get("name") or "").strip() for entry in metadata if entry.get("name")]
        tags = [tag for tag in [*department_tags, *office_tags, *metadata_tags, company, board] if tag]
        extraction_description = truncate(raw_description, 900 if settings.environment == "production" else 4000)
        requirement_profile = extract_job_requirement_profile(title=title, description=extraction_description, tags=tags)
        description = truncate(raw_description, 4000)
        job = {
            "source": self.source_name,
            "external_id": str(item.get("id") or fallback.get("external_id") or ""),
            "title": title,
            "company": company,
            "location": location,
            "remote": "remote" in location.lower() or "remote" in description.lower(),
            "url": str(item.get("absolute_url") or fallback.get("url") or ""),
            "description": description,
            "preview": truncate(description, 260),
            "tags": tags,
            "normalized_data": {
                "board_token": board,
                **requirement_profile,
            },
            "posted_at": self._parse_datetime(item.get("updated_at") or item.get("updatedAt")) or fallback.get("posted_at"),
        }
        _GREENHOUSE_JOB_DETAIL_CACHE[cache_key] = {"stored_at": now, "job": job}
        return job

    def _index_fallback_job(self, fallback: dict, *, detail_hydration_skipped: bool = False) -> dict | None:
        if not fallback:
            return None
        normalized = {**(fallback.get("normalized_data") or {})}
        description = str(
            fallback.get("description")
            or f"{fallback.get('title', 'Unknown Role')} listing from Greenhouse ATS. Detailed requirements were temporarily unavailable."
        )
        if not normalized.get("skills"):
            requirement_profile = extract_job_requirement_profile(
                title=str(fallback.get("title") or "Unknown Role"),
                description=description,
                tags=fallback.get("tags") or [],
                source="greenhouse-index",
            )
            normalized.update(requirement_profile)
        if not normalized.get("skills"):
            title = str(fallback.get("title") or "Unknown Role")
            title_profile = role_profile(title)
            recognized_title_variant = title_profile.normalized_role != title_profile.cleaned_query or bool(title_profile.domain)
            if recognized_title_variant:
                inferred_skills = role_baseline_skills(title, limit=5)
                if inferred_skills:
                    normalized["skills"] = inferred_skills
                    normalized["skill_weights"] = {
                        **(normalized.get("skill_weights") or {}),
                        **{skill: 0.36 for skill in inferred_skills},
                    }
                    normalized["skill_evidence"] = [
                        *(normalized.get("skill_evidence") or []),
                        *[
                            {
                                "skill": skill,
                                "matched_text": title,
                                "snippet": title,
                                "source": "greenhouse-index",
                                "mode": "title-variant-inferred",
                            }
                            for skill in inferred_skills
                        ],
                    ]
                    normalized["requirement_quality"] = max(float(normalized.get("requirement_quality", 0.0)), 0.36)
        normalized["index_only"] = True
        normalized["detail_hydration_skipped"] = detail_hydration_skipped
        normalized["detail_fetch_failed"] = not detail_hydration_skipped
        fallback_job = {
            **fallback,
            "description": description,
            "preview": str(
                fallback.get("preview")
                or truncate(
                    description,
                    260,
                )
            ),
            "normalized_data": normalized,
        }
        return fallback_job

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

    def _company_from_board(self, board: str) -> str:
        cleaned = board.replace("-", " ").replace("_", " ").strip()
        return cleaned.title() if cleaned else "Greenhouse Company"

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
