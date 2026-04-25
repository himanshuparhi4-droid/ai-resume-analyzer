from __future__ import annotations

from collections.abc import Iterable

from app.services.jobs.taxonomy import role_baseline_skills, role_title_alignment_score
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION


def build_fast_requirement_profile(
    *,
    query: str,
    title: str,
    description: str = "",
    tags: Iterable[str] = (),
    source: str,
    max_skills: int = 10,
) -> dict:
    """Build a cheap production profile so providers can return listings first.

    Full requirement extraction is useful, but on small hosted instances it can
    consume the provider timeout after the HTTP request already succeeded. This
    profile keeps ranking evidence explicit and conservative while avoiding the
    heavy per-listing extraction path.
    """

    ordered_skills: list[str] = []
    seen: set[str] = set()
    skill_sources = [
        ("title", role_baseline_skills(title, limit=max_skills)),
        ("query", role_baseline_skills(query, limit=max_skills)),
    ]
    skill_origin: dict[str, str] = {}
    for origin, skills in skill_sources:
        for skill in skills:
            key = str(skill).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            ordered_skills.append(skill)
            skill_origin[key] = origin
            if len(ordered_skills) >= max_skills:
                break
        if len(ordered_skills) >= max_skills:
            break

    tag_list = [str(tag).strip() for tag in tags if str(tag).strip()]
    title_alignment = role_title_alignment_score(query, title, description=description[:500], tags=tag_list)
    fast_role_fit = round(max(0.0, min(14.0, title_alignment / 2.8)), 2)
    requirement_quality = 18.0 if title_alignment > 0 else 10.0
    if not ordered_skills:
        requirement_quality = 6.0

    skill_weights = {}
    skill_evidence = []
    for skill in ordered_skills:
        key = str(skill).strip().lower()
        origin = skill_origin.get(key, "query")
        weight = 0.46 if origin == "title" else 0.34
        skill_weights[skill] = weight
        skill_evidence.append(
            {
                "skill": skill,
                "matched_text": title if origin == "title" else query,
                "snippet": title if origin == "title" else query,
                "source": source,
                "mode": f"production-fast-{origin}-prior",
            }
        )

    return {
        "skills": ordered_skills,
        "skill_weights": skill_weights,
        "skill_evidence": skill_evidence,
        "skill_extraction_mode": "production-fast-role-prior",
        "title_alignment_score": title_alignment,
        "role_fit_score": fast_role_fit,
        "requirement_quality": requirement_quality,
        "normalization_version": JOB_REQUIREMENT_PROFILE_VERSION,
    }
