from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role, role_domain, role_family, role_fit_score, role_title_alignment_score
from app.services.nlp.job_requirements import extract_job_requirement_profile
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
        "discord",
        "okta",
        "asana",
        "robinhood",
        "instacart",
        "affirm",
        "rubrik",
        "figma",
    ],
    "security": [
        "okta",
        "asana",
        "discord",
        "rubrik",
        "robinhood",
        "affirm",
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

_ROLE_SPECIFIC_GREENHOUSE_BOARDS = {
    "data analyst": ["yipitdata", "instacart", "affirm", "robinhood", "asana", "discord"],
    "data scientist": ["yipitdata", "asana", "affirm", "instacart", "robinhood", "discord"],
    "data engineer": ["yipitdata", "instacart", "asana", "affirm", "robinhood", "okta"],
    "software engineer": ["okta", "discord", "asana", "figma", "robinhood", "affirm"],
    "full stack developer": ["okta", "discord", "asana", "figma", "robinhood"],
    "frontend developer": ["figma", "discord", "asana", "okta", "robinhood"],
    "devops engineer": ["okta", "affirm", "instacart", "rubrik", "asana"],
    "cybersecurity engineer": ["okta", "asana", "discord", "rubrik", "robinhood", "affirm"],
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
        boards = self._boards_for_query(query)
        if not boards:
            return []

        timeout = httpx.Timeout(
            connect=min(4.0, settings.job_request_timeout_seconds),
            read=settings.job_request_timeout_seconds,
            write=10.0,
            pool=5.0,
        )
        async with httpx.AsyncClient(timeout=timeout) as client:
            board_results_raw = await asyncio.gather(
                *(self._fetch_board_index(board, client=client) for board in boards),
                return_exceptions=True,
            )

            seen_links: set[str] = set()
            candidates: list[dict] = []
            for board, items in zip(boards, board_results_raw):
                if isinstance(items, Exception):
                    logger.warning("Greenhouse board index fetch failed for %s: %s", board, items)
                    continue
                for item in items:
                    link = str(item.get("url") or item.get("external_id") or "").strip()
                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)
                    candidates.append(item)

            if not candidates:
                return []

            target_candidates = max(limit * 6, settings.production_live_candidate_fetch)
            if family_role in {"frontend developer", "mobile developer", "embedded engineer"}:
                # UI/mobile/embedded families benefit from board diversity, but
                # full hydration on Render free tier is expensive and tends to
                # time out before the selector sees the ATS-backed candidates.
                detail_fetch_budget = min(max(limit // 2 + 1, 3), 4)
                board_budget = 1
            elif domain in {"software", "security"} or family_role in {"devops engineer", "qa engineer", "solutions architect"}:
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
            ranked_pool = positively_aligned if len(positively_aligned) >= max(limit * 2, 12) else candidates
            ranked_candidates = sorted(
                ranked_pool,
                key=lambda item: (
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

    def _boards_for_query(self, query: str) -> list[str]:
        normalized = normalize_role(query)
        specific = _ROLE_SPECIFIC_GREENHOUSE_BOARDS.get(normalized)
        if specific:
            boards = list(specific)
        elif settings.has_greenhouse_boards:
            boards = settings.greenhouse_board_tokens
        else:
            boards = list(_CURATED_GREENHOUSE_BOARDS.get(role_domain(query) or "", []))
        if settings.environment == "production":
            family_domain = role_domain(query) or role_domain(normalized)
            board_limit = {
                "data": 4,
                "software": 5,
                "security": 4,
                "product": 4,
                "design": 3,
            }.get(family_domain, len(boards))
            if normalized == "frontend developer":
                board_limit = min(board_limit, 3)
            boards = boards[:board_limit]
        return boards

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
            raw_jobs = raw_jobs[:60]

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
        requirement_profile = extract_job_requirement_profile(title=title, description=raw_description, tags=tags)
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

    def _index_fallback_job(self, fallback: dict) -> dict | None:
        if not fallback:
            return None
        normalized = {**(fallback.get("normalized_data") or {})}
        normalized["index_only"] = True
        normalized["detail_fetch_failed"] = True
        fallback_job = {
            **fallback,
            "description": str(
                fallback.get("description")
                or f"{fallback.get('title', 'Unknown Role')} listing from Greenhouse ATS. Detailed requirements were temporarily unavailable."
            ),
            "preview": str(
                fallback.get("preview")
                or truncate(
                    str(
                        fallback.get("description")
                        or f"{fallback.get('title', 'Unknown Role')} listing from Greenhouse ATS. Detailed requirements were temporarily unavailable."
                    ),
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
