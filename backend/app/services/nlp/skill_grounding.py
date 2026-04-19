from __future__ import annotations

import json
import re
from typing import Any

import httpx

from app.core.config import settings
from app.services.jobs.taxonomy import normalize_role
from app.services.nlp.job_requirements import JOB_REQUIREMENT_PROFILE_VERSION, extract_job_requirement_profile
from app.services.nlp.skill_extractor import KNOWN_SKILLS, extract_skill_evidence, extract_skill_matches, infer_skill_frequency
from app.utils.text import normalize_whitespace

try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


GENERIC_DYNAMIC_SKILLS = {
    "analysis",
    "analyst",
    "analytics",
    "business",
    "communication",
    "consultant",
    "data",
    "data analyst",
    "developer",
    "designer",
    "educator",
    "engineer",
    "experience",
    "instructor",
    "leadership",
    "manager",
    "management",
    "painter",
    "problem solving",
    "projects",
    "reporting",
    "research",
    "scientist",
    "specialist",
    "teacher",
    "teamwork",
}
RESUME_EVIDENCE_SECTION_PRIORITY = {
    "resume:experience": 5,
    "resume:projects": 5,
    "resume:skills": 4,
    "resume:education": 2,
    "resume:summary": 1,
    "resume": 1,
}
ACTION_CONTEXT_RE = re.compile(r"\b(built|created|developed|analyzed|cleaned|designed|improved|automated|delivered)\b", re.IGNORECASE)
REQUIREMENT_LANGUAGE_RE = re.compile(r"\b(required|requirements|must have|proficient in|experience with|hands[- ]on)\b", re.IGNORECASE)
ROLE_BASELINE_POOLS = {
    "data analyst": ["sql", "excel", "python", "pandas", "power bi", "tableau", "data analysis", "statistics"],
    "data scientist": ["python", "sql", "pandas", "numpy", "machine learning", "scikit-learn", "statistics"],
    "machine learning engineer": ["python", "machine learning", "scikit-learn", "tensorflow", "pytorch", "sql"],
    "data engineer": ["python", "sql", "spark", "hadoop", "etl", "aws", "postgresql"],
    "software engineer": ["python", "java", "javascript", "sql", "docker", "api"],
    "frontend developer": ["javascript", "typescript", "react", "next.js", "html", "css", "figma"],
    "full stack developer": ["react", "javascript", "typescript", "node.js", "sql", "api"],
    "devops engineer": ["aws", "docker", "kubernetes", "terraform", "linux", "ci/cd"],
    "cybersecurity engineer": ["network security", "cloud security", "siem", "splunk", "incident response", "iam", "vulnerability management", "firewall"],
    "qa engineer": ["testing", "pytest", "ci/cd", "api", "javascript"],
    "product manager": ["data analysis", "excel", "sql", "communication", "leadership"],
    "ui/ux designer": ["figma", "ui design", "ux design", "communication"],
    "graphic designer": ["figma", "ui design", "communication"],
    "digital marketer": ["seo", "data analysis", "excel", "communication"],
    "accountant": ["excel", "sql", "statistics"],
    "sales representative": ["communication", "excel", "leadership"],
    "customer support": ["communication", "problem solving"],
    "content writer": ["seo", "communication"],
    "carpenter": ["woodworking", "framing", "blueprint reading", "finish carpentry", "power tools", "measuring", "safety compliance"],
    "painter": ["painting", "surface preparation", "color matching", "spray painting", "safety compliance"],
    "teacher": ["lesson planning", "classroom management", "curriculum development", "student assessment", "differentiated instruction", "pedagogy"],
}
SPARSE_LIVE_MARKET_ROLES = {"teacher", "carpenter", "painter"}


class SkillGroundingService:
    async def ensure_market_coverage(
        self,
        *,
        role_query: str,
        location: str,
        resume_data: dict,
        jobs: list[dict],
    ) -> list[dict]:
        if not jobs:
            return jobs

        sample_metadata = self._inspect_market_sample(role_query=role_query, jobs=jobs)
        if not sample_metadata["needs_blend"]:
            return jobs

        fallback_jobs = await self.build_fallback_jobs(
            role_query=role_query,
            location=location,
            resume_data=resume_data,
            reason="Live job providers returned a narrow skill sample, so the market view was expanded with a role calibration baseline.",
        )
        for item in fallback_jobs:
            item.setdefault("normalized_data", {})
            item["normalized_data"]["market_blend_reason"] = sample_metadata["reason"]
            item["normalized_data"]["sampled_live_skills"] = sample_metadata["live_skills"]
        return jobs + fallback_jobs

    async def ground(self, *, role_query: str, resume_data: dict, jobs: list[dict]) -> tuple[dict, list[dict], dict[str, Any]]:
        resume_text = resume_data.get("raw_text", "")
        anchored_resume_evidence = self._extract_resume_skill_evidence(resume_data)
        resume_data["skill_evidence"] = anchored_resume_evidence
        resume_data["skills"] = sorted({item["skill"] for item in anchored_resume_evidence})
        baseline_only = bool(jobs) and all(job.get("source") == "role-baseline" for job in jobs)
        blended_market = any(job.get("source") == "role-baseline" for job in jobs) and any(job.get("source") != "role-baseline" for job in jobs)

        for job in jobs:
            normalized = job.setdefault("normalized_data", {})
            if not normalized.get("skills") or not normalized.get("skill_evidence"):
                normalized.update(
                    extract_job_requirement_profile(
                        title=job.get("title", ""),
                        description=job.get("description", ""),
                        source="job",
                    )
                )
            elif "skill_weights" not in normalized:
                normalized["skill_weights"] = {
                    skill: 0.82 if job.get("source") != "role-baseline" else 0.7
                    for skill in normalized.get("skills", [])
                }
            normalized.setdefault("skill_extraction_mode", "weighted-pattern")

        audit = None if baseline_only else await self._run_skill_audit(role_query=role_query, resume_data=resume_data, jobs=jobs)

        candidate_resume_skills = sorted(resume_data.get("skills", []))
        candidate_market_skills = sorted({skill for job in jobs for skill in job.get("normalized_data", {}).get("skills", [])})

        kept_resume_skills = (
            sorted(set(candidate_resume_skills) | set(self._normalize_existing_skills(audit.get("resume_keep", []), candidate_resume_skills)))
            if audit
            else candidate_resume_skills
        )
        kept_market_skills = self._normalize_existing_skills(audit.get("market_keep", []), candidate_market_skills) if audit else candidate_market_skills

        if audit and not kept_market_skills:
            kept_market_skills = candidate_market_skills
        if blended_market and audit:
            kept_market_skills = sorted(set(candidate_market_skills) | set(kept_market_skills))

        dynamic_resume_skills = self._normalize_dynamic_skills(audit.get("resume_add", []), resume_text) if audit else []
        dynamic_market_skills = self._normalize_dynamic_skills(
            audit.get("market_add", []),
            "\n".join(normalize_whitespace(f"{job.get('title', '')} {job.get('description', '')}") for job in jobs),
        ) if audit else []

        final_resume_skills = sorted(set(kept_resume_skills) | set(dynamic_resume_skills))
        final_market_skills = sorted(set(kept_market_skills) | set(dynamic_market_skills))

        resume_data["skills"] = final_resume_skills
        resume_data["skill_evidence"] = self._extract_resume_skill_evidence(resume_data, selected_skills=final_resume_skills)

        for job in jobs:
            normalized = job.setdefault("normalized_data", {})
            job_text = normalize_whitespace(f"{job.get('title', '')} {job.get('description', '')}")
            filtered_skills = [skill for skill in normalized.get("skills", []) if skill in final_market_skills]
            dynamic_for_job = [
                skill
                for skill in dynamic_market_skills
                if skill in final_market_skills and extract_skill_evidence(job_text, [skill], source="job")
            ]
            normalized["skills"] = sorted(set(filtered_skills) | set(dynamic_for_job))
            existing_weights = normalized.get("skill_weights", {}) or {}
            normalized["skill_weights"] = {
                skill: float(existing_weights.get(skill, 0.82 if job.get("source") != "role-baseline" else 0.7))
                for skill in normalized["skills"]
            }
            normalized["skill_evidence"] = extract_skill_evidence(job_text, normalized["skills"], source="job")
            if audit:
                normalized["skill_extraction_mode"] = "hybrid"

        return resume_data, jobs, {
            "mode": audit.get("mode", "pattern-only") if audit else "pattern-only",
            "resume_kept": final_resume_skills,
            "market_kept": final_market_skills,
            "resume_dropped": audit.get("resume_drop", []) if audit else [],
            "market_dropped": audit.get("market_drop", []) if audit else [],
            "role_focus": audit.get("role_focus", []) if audit else [],
        }

    async def build_fallback_jobs(self, *, role_query: str, location: str, resume_data: dict, reason: str | None = None) -> list[dict]:
        baseline = await self._generate_role_baseline(role_query=role_query, resume_data=resume_data)
        role_title = baseline.get("role_title") or role_query.title()
        skills = baseline.get("skills", [])
        if len(skills) < 2:
            return []
        experience_months = int(baseline.get("experience_months", 6) or 6)
        summary = reason or baseline.get("summary") or f"Fallback market baseline for {role_title} built when live job providers are unavailable."
        titles = baseline.get("titles") or [
            role_title,
            f"Junior {role_title}",
            f"Entry-Level {role_title}",
        ]
        skill_profiles = self._build_baseline_skill_profiles(skills)

        jobs: list[dict] = []
        for index, title in enumerate(titles[:3], start=1):
            profile_skills = skill_profiles[min(index - 1, len(skill_profiles) - 1)]
            description = (
                f"{summary} Typical market expectations for this variation center on {', '.join(profile_skills)}. "
                f"Representative entry-level requirement: about {experience_months} months of hands-on project or internship exposure."
            )
            normalized_data = {
                "skills": profile_skills,
                "skill_weights": self._build_baseline_skill_weights(skills, profile_skills),
                "skill_evidence": extract_skill_evidence(description, profile_skills, source="baseline"),
                "skill_extraction_mode": "llm-baseline",
                "baseline_reason": reason or "Live job providers were unavailable during analysis, so a role baseline was generated.",
                "baseline_confidence": baseline.get("confidence", "medium"),
                "requirement_quality": round(sum(self._build_baseline_skill_weights(skills, profile_skills).values()) / max(len(profile_skills), 1) * 100, 1),
                "normalization_version": JOB_REQUIREMENT_PROFILE_VERSION,
            }
            jobs.append(
                {
                "source": "role-baseline",
                "external_id": f"{self._slugify(role_query)}-{index}",
                "title": title,
                "company": "Model-generated market baseline",
                "location": location or "Remote",
                    "remote": True,
                    "url": "https://example.com/market-baseline",
                    "description": description,
                    "tags": [role_query, "baseline"],
                    "normalized_data": normalized_data,
                    "posted_at": None,
                }
            )
        return jobs

    def build_analysis_context(self, jobs: list[dict]) -> dict[str, Any]:
        live_job_count = sum(1 for job in jobs if job.get("source") != "role-baseline")
        baseline_job_count = sum(1 for job in jobs if job.get("source") == "role-baseline")
        live_company_count = len(
            {
                normalize_role(str(job.get("company", "")))
                for job in jobs
                if job.get("source") != "role-baseline" and normalize_role(str(job.get("company", "")))
            }
        )
        baseline_confidences = {
            str(job.get("normalized_data", {}).get("baseline_confidence", "medium"))
            for job in jobs
            if job.get("source") == "role-baseline"
        }
        live_source_counts: dict[str, int] = {}
        for job in jobs:
            source = str(job.get("source", "unknown"))
            if source == "role-baseline":
                continue
            live_source_counts[source] = live_source_counts.get(source, 0) + 1
        used_role_baseline = baseline_job_count > 0
        baseline_confidence = "low" if baseline_confidences == {"low"} else "high" if baseline_confidences == {"high"} else "medium"
        if live_job_count >= 6 and live_company_count >= 3 and not used_role_baseline:
            market_confidence = "high"
        elif live_job_count >= 3:
            market_confidence = "medium"
        elif used_role_baseline:
            market_confidence = "baseline-assisted"
        else:
            market_confidence = "low"
        if used_role_baseline and live_job_count == 0:
            return {
                "market_source": "role-baseline",
                "live_job_count": 0,
                "live_company_count": live_company_count,
                "used_role_baseline": True,
                "live_source_counts": live_source_counts,
                "baseline_confidence": baseline_confidence,
                "market_confidence": market_confidence,
                "build_tag": "2026-04-19-livefetch-debug-10",
                "message": (
                    "Live job providers did not return listings, and the system does not have a strong calibrated baseline for this role. Market-fit scoring is low confidence."
                    if baseline_confidence == "low"
                    else "Live job providers did not return listings for this run, so the score is an estimate built from a model-generated role baseline instead of real-time job data."
                ),
            }
        if used_role_baseline and live_job_count > 0:
            return {
                "market_source": "blended-market",
                "live_job_count": live_job_count,
                "live_company_count": live_company_count,
                "used_role_baseline": True,
                "live_source_counts": live_source_counts,
                "baseline_confidence": baseline_confidence,
                "market_confidence": market_confidence,
                "build_tag": "2026-04-19-livefetch-debug-10",
                "message": "Score grounded against live job descriptions, but the sampled market set was too narrow, so the model blended in a role baseline to surface likely missing tools and demand signals more realistically.",
            }
        return {
            "market_source": "live-jobs" if live_job_count else "none",
            "live_job_count": live_job_count,
            "live_company_count": live_company_count,
            "used_role_baseline": False,
            "live_source_counts": live_source_counts,
            "baseline_confidence": baseline_confidence,
            "market_confidence": market_confidence,
            "build_tag": "2026-04-19-livefetch-debug-10",
            "message": "Score grounded against live fetched job descriptions." if live_job_count else "No market listings were available for this run.",
        }

    def build_skill_report(
        self,
        *,
        role_query: str | None = None,
        resume_text: str,
        jobs: list[dict],
        matched_skills: list[str],
        missing_skills: list[dict],
        resume_skill_evidence: list[dict] | None = None,
    ) -> dict[str, Any]:
        market_skill_frequency = infer_skill_frequency(jobs, role_query=role_query)
        demand_map = {item["skill"]: item["share"] for item in market_skill_frequency}
        live_jobs_present = any(job.get("source") != "role-baseline" for job in jobs)
        resume_evidence_map: dict[str, list[str]] = {}
        resume_evidence_candidates = resume_skill_evidence or extract_skill_evidence(resume_text, matched_skills, source="resume")
        grouped_resume_evidence: dict[str, list[dict]] = {}
        for item in resume_evidence_candidates:
            grouped_resume_evidence.setdefault(item["skill"], []).append(item)
        for skill, items in grouped_resume_evidence.items():
            ranked_items = sorted(items, key=self._resume_evidence_rank, reverse=True)
            resume_evidence_map[skill] = [item["snippet"] for item in ranked_items[:2]]

        job_evidence_map: dict[str, list[dict[str, str]]] = {}
        for job in jobs:
            for item in job.get("normalized_data", {}).get("skill_evidence", []):
                bucket = job_evidence_map.setdefault(item["skill"], [])
                snippet = item.get("snippet")
                if not snippet:
                    continue
                if job.get("source") == "role-baseline":
                    snippet = f"Modeled market baseline for {job.get('title', 'this role')} emphasizes {item['skill']} in this role family."
                detail = {
                    "title": job.get("title", "Unknown Role"),
                    "company": job.get("company", "Unknown Company"),
                    "snippet": snippet,
                    "source": job.get("source", "unknown"),
                }
                if detail not in bucket:
                    bucket.append(detail)

        matched_skill_details = [
            {
                "skill": skill,
                "market_share": demand_map.get(skill, 0.0),
                "resume_evidence": resume_evidence_map.get(skill, [])[:2],
                "job_evidence": self._rank_job_evidence(job_evidence_map.get(skill, []), prefer_live=live_jobs_present)[:2],
                "primary_source": self._primary_evidence_source(job_evidence_map.get(skill, []), prefer_live=live_jobs_present),
            }
            for skill in matched_skills
        ]

        missing_skill_details = [
            {
                "skill": item["skill"],
                "market_share": item["share"],
                "job_evidence": self._rank_job_evidence(job_evidence_map.get(item["skill"], []), prefer_live=live_jobs_present)[:2],
                "primary_source": self._primary_evidence_source(job_evidence_map.get(item["skill"], []), prefer_live=live_jobs_present),
            }
            for item in missing_skills
        ]

        return {
            "market_skill_frequency": market_skill_frequency,
            "matched_skill_details": matched_skill_details,
            "missing_skill_details": missing_skill_details,
        }

    def _resume_evidence_rank(self, item: dict) -> tuple[int, int, int]:
        source = str(item.get("source", ""))
        snippet = str(item.get("snippet", ""))
        score = RESUME_EVIDENCE_SECTION_PRIORITY.get(source, 0)
        if ACTION_CONTEXT_RE.search(snippet):
            score += 2
        if re.search(r"\b\d[\d,]*\b|%", snippet):
            score += 1
        if "http" in snippet.lower() or "linkedin" in snippet.lower() or "github" in snippet.lower() or "@" in snippet:
            score -= 4
        return score, min(len(snippet), 220), -len(snippet)

    def _rank_job_evidence(self, items: list[dict[str, str]], *, prefer_live: bool) -> list[dict[str, str]]:
        def rank(item: dict[str, str]) -> tuple[int, int, int]:
            snippet = str(item.get("snippet", ""))
            source = str(item.get("source", ""))
            score = 0
            if prefer_live and source != "role-baseline":
                score += 4
            if REQUIREMENT_LANGUAGE_RE.search(snippet):
                score += 2
            if "typical market expectations" in snippet.lower():
                score -= 1
            return score, min(len(snippet), 220), -len(snippet)

        ranked = sorted(items, key=rank, reverse=True)
        if prefer_live:
            live = [item for item in ranked if item.get("source") != "role-baseline"]
            if live:
                return live
        return ranked

    def _primary_evidence_source(self, items: list[dict[str, str]], *, prefer_live: bool) -> str:
        ranked = self._rank_job_evidence(items, prefer_live=prefer_live)
        if not ranked:
            return "unknown"
        source = str(ranked[0].get("source", "unknown"))
        return source

    async def _run_skill_audit(self, *, role_query: str, resume_data: dict, jobs: list[dict]) -> dict[str, Any] | None:
        if settings.llm_provider == "openai":
            return await self._audit_with_openai(role_query=role_query, resume_data=resume_data, jobs=jobs)
        return None

    async def _generate_role_baseline(self, *, role_query: str, resume_data: dict) -> dict[str, Any]:
        if settings.llm_provider == "openai":
            generated = await self._baseline_with_openai(role_query=role_query, resume_data=resume_data)
            if generated:
                return generated
        fallback_skills = self._expand_baseline_skill_pool(role_query)
        confidence = "high" if len(fallback_skills) >= 4 else "low"
        return {
            "mode": "heuristic-baseline",
            "role_title": role_query.title(),
            "titles": [role_query.title(), f"Junior {role_query.title()}", f"Entry-Level {role_query.title()}"],
            "summary": (
                f"Fallback role baseline for {role_query} built when live jobs were unavailable."
                if confidence == "high"
                else f"No reliable live jobs were available, and the system only has a weak modeled baseline for {role_query}."
            ),
            "skills": fallback_skills,
            "experience_months": 6 if "senior" not in role_query.lower() else 24,
            "confidence": confidence,
        }

    async def _audit_with_ollama(self, *, role_query: str, resume_data: dict, jobs: list[dict]) -> dict[str, Any] | None:
        prompt = self._build_prompt(role_query=role_query, resume_data=resume_data, jobs=jobs)
        payload = {
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0},
        }
        try:
            async with httpx.AsyncClient(timeout=75.0) as client:
                response = await client.post(f"{settings.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            parsed = json.loads(data.get("response", "{}"))
            return self._normalize_audit(parsed)
        except Exception:
            return None

    async def _audit_with_openai(self, *, role_query: str, resume_data: dict, jobs: list[dict]) -> dict[str, Any] | None:
        if not settings.openai_api_key or OpenAI is None:
            return None
        prompt = self._build_prompt(role_query=role_query, resume_data=resume_data, jobs=jobs)
        try:
            client = OpenAI(api_key=settings.openai_api_key)
            response = client.responses.create(
                model=settings.openai_model,
                input=prompt,
                text={"format": {"type": "json_object"}},
            )
            output_text = "".join(item.text for item in response.output[0].content if getattr(item, "text", None))
            return self._normalize_audit(json.loads(output_text))
        except Exception:
            return None

    async def _baseline_with_ollama(self, *, role_query: str, resume_data: dict) -> dict[str, Any] | None:
        payload = {
            "model": settings.ollama_model,
            "prompt": self._build_baseline_prompt(role_query=role_query, resume_data=resume_data),
            "stream": False,
            "format": "json",
            "options": {"temperature": 0},
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(f"{settings.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            return self._normalize_baseline(json.loads(data.get("response", "{}")), role_query=role_query)
        except Exception:
            return None

    async def _baseline_with_openai(self, *, role_query: str, resume_data: dict) -> dict[str, Any] | None:
        if not settings.openai_api_key or OpenAI is None:
            return None
        try:
            client = OpenAI(api_key=settings.openai_api_key)
            response = client.responses.create(
                model=settings.openai_model,
                input=self._build_baseline_prompt(role_query=role_query, resume_data=resume_data),
                text={"format": {"type": "json_object"}},
            )
            output_text = "".join(item.text for item in response.output[0].content if getattr(item, "text", None))
            return self._normalize_baseline(json.loads(output_text), role_query=role_query)
        except Exception:
            return None

    def _build_prompt(self, *, role_query: str, resume_data: dict, jobs: list[dict]) -> str:
        job_summaries = []
        for index, job in enumerate(jobs[:6], start=1):
            summary = {
                "index": index,
                "title": job.get("title", "Unknown Role"),
                "company": job.get("company", "Unknown Company"),
                "skills": job.get("normalized_data", {}).get("skills", []),
                "description": normalize_whitespace(job.get("description", ""))[:550],
            }
            job_summaries.append(summary)

        payload = {
            "role_query": role_query,
            "resume_candidates": resume_data.get("skills", []),
            "resume_sections": {
                key: normalize_whitespace(value)[:600]
                for key, value in resume_data.get("sections", {}).items()
            },
            "market_candidates": sorted({skill for item in job_summaries for skill in item["skills"]}),
            "sampled_jobs": job_summaries,
        }
        return (
            "You are a strict skill-grounding auditor for a resume analyzer. "
            "Return JSON only with keys: mode, resume_keep, resume_drop, resume_add, market_keep, market_drop, market_add, role_focus. "
            "Rules: keep a resume skill only if the resume text explicitly supports it; "
            "keep a market skill only if it is clearly relevant to the target role across the sampled jobs; "
            "resume_add and market_add may include additional concrete hard skills or tools only if they are explicitly present in the text; "
            "never invent skills, never add soft skills, never add generic words. "
            f"Audit payload: {json.dumps(payload)}"
        )

    def _build_baseline_prompt(self, *, role_query: str, resume_data: dict) -> str:
        payload = {
            "role_query": role_query,
            "resume_sections": list(resume_data.get("sections", {}).keys()),
            "candidate_skill_pool": sorted(KNOWN_SKILLS),
        }
        return (
            "You are building a realistic market baseline for a resume analyzer when live job APIs are unavailable. "
            "Return JSON only with keys: mode, role_title, titles, summary, skills, experience_months. "
            "Choose 6 to 10 realistic hard skills or tools from candidate_skill_pool when possible, prefer entry-level or junior expectations when seniority is unclear, "
            "and do not include soft skills or invented technologies. Do not tailor the baseline to the candidate resume. "
            "The suggested titles should stay in the same role family as role_query. "
            f"Baseline payload: {json.dumps(payload)}"
        )

    def _normalize_audit(self, payload: dict[str, Any]) -> dict[str, Any]:
        def normalize_list(value: Any) -> list[str]:
            if isinstance(value, list):
                return [normalize_whitespace(str(item)).lower() for item in value if normalize_whitespace(str(item)).strip()]
            return []

        def normalize_drop_list(value: Any) -> list[dict]:
            if not isinstance(value, list):
                return []
            items: list[dict] = []
            for item in value:
                if isinstance(item, dict):
                    skill = normalize_whitespace(str(item.get("skill", ""))).lower()
                    reason = normalize_whitespace(str(item.get("reason", "")))
                    if skill:
                        items.append({"skill": skill, "reason": reason})
                elif item:
                    items.append({"skill": normalize_whitespace(str(item)).lower(), "reason": ""})
            return items

        return {
            "mode": str(payload.get("mode", "llm-grounded")),
            "resume_keep": normalize_list(payload.get("resume_keep")),
            "resume_drop": normalize_drop_list(payload.get("resume_drop")),
            "resume_add": normalize_list(payload.get("resume_add")),
            "market_keep": normalize_list(payload.get("market_keep")),
            "market_drop": normalize_drop_list(payload.get("market_drop")),
            "market_add": normalize_list(payload.get("market_add")),
            "role_focus": normalize_list(payload.get("role_focus")),
        }

    def _normalize_baseline(self, payload: dict[str, Any], *, role_query: str) -> dict[str, Any]:
        cleaned_skills = self._normalize_dynamic_skills(payload.get("skills", []), " ".join(payload.get("skills", [])))
        if not cleaned_skills:
            cleaned_skills = [self._normalize_label(item) for item in payload.get("skills", []) if self._is_valid_dynamic_skill(self._normalize_label(str(item)))]
        expanded = self._expand_baseline_skill_pool(role_query)
        cleaned_skills = self._filter_skills_for_role(role_query, cleaned_skills, expanded)
        normalized_experience = int(payload.get("experience_months", 6) or 6)
        if "senior" not in role_query.lower():
            normalized_experience = max(6, min(normalized_experience, 12))
        else:
            normalized_experience = max(24, normalized_experience)
        return {
            "mode": str(payload.get("mode", "llm-baseline")),
            "role_title": role_query.title(),
            "titles": [role_query.title(), f"Junior {role_query.title()}", f"Entry-Level {role_query.title()}"],
            "summary": normalize_whitespace(str(payload.get("summary", ""))),
            "skills": cleaned_skills[:10],
            "experience_months": normalized_experience,
            "confidence": "high" if len(cleaned_skills) >= 4 else "low",
        }

    def _normalize_existing_skills(self, selected: list[str], candidates: list[str]) -> list[str]:
        candidate_map = {self._normalize_label(skill): skill for skill in candidates}
        result = []
        for item in selected:
            key = self._normalize_label(item)
            if key in candidate_map:
                result.append(candidate_map[key])
        return sorted(set(result))

    def _normalize_dynamic_skills(self, selected: list[str], supporting_text: str) -> list[str]:
        result = []
        normalized_text = normalize_whitespace(supporting_text)
        for item in selected:
            cleaned = self._normalize_label(item)
            if not self._is_valid_dynamic_skill(cleaned):
                continue
            if not extract_skill_evidence(normalized_text, [cleaned], source="grounding"):
                continue
            result.append(cleaned)
        return sorted(set(result))

    def _normalize_label(self, value: str) -> str:
        cleaned = normalize_whitespace(value).lower().strip(" .,:;()[]{}")
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    def _is_valid_dynamic_skill(self, value: str) -> bool:
        if not value or len(value) < 2:
            return False
        if value in GENERIC_DYNAMIC_SKILLS:
            return False
        if len(value.split()) > 4:
            return False
        if not re.fullmatch(r"[a-z0-9+.#/& -]{2,40}", value):
            return False
        return True

    def _slugify(self, value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return cleaned or "role"

    def _expand_baseline_skill_pool(self, role_query: str) -> list[str]:
        normalized = normalize_role(role_query)
        if normalized in ROLE_BASELINE_POOLS:
            return list(ROLE_BASELINE_POOLS[normalized])

        lowered = normalize_whitespace(role_query).lower()
        if not lowered:
            return []

        query_tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if token and token not in {"and", "the", "for", "with", "role"}]
        ranked: list[str] = []
        seen: set[str] = set()

        for token in query_tokens:
            if token in KNOWN_SKILLS and token not in seen:
                ranked.append(token)
                seen.add(token)

        return ranked

    def _build_baseline_skill_profiles(self, skills: list[str]) -> list[list[str]]:
        ordered = list(dict.fromkeys(skills))
        if not ordered:
            return [[], [], []]
        if len(ordered) <= 3:
            return [ordered, ordered, ordered]

        profiles = [[], [], []]
        for index, skill in enumerate(ordered):
            if index == 0:
                target_profiles = (0, 1, 2)
            elif index in (1, 2):
                target_profiles = (0, 1)
            elif index in (3, 4):
                target_profiles = (0, 2)
            else:
                target_profiles = (2,)
            for profile_index in target_profiles:
                profiles[profile_index].append(skill)

        return [profile or ordered[: min(len(ordered), 4)] for profile in profiles]

    def _build_baseline_skill_weights(self, ranked_skills: list[str], profile_skills: list[str]) -> dict[str, float]:
        total = max(len(ranked_skills), 1)
        weights: dict[str, float] = {}
        for skill in profile_skills:
            rank = ranked_skills.index(skill) if skill in ranked_skills else total - 1
            relative_rank = rank / max(total - 1, 1)
            weights[skill] = round(max(0.58, 1.0 - (relative_rank * 0.38)), 2)
        return weights

    def _extract_resume_skill_evidence(self, resume_data: dict, selected_skills: list[str] | None = None) -> list[dict]:
        sections = resume_data.get("sections", {})
        raw_text = resume_data.get("raw_text", "")
        parse_signals = resume_data.get("parse_signals", {})
        targeted = set(selected_skills or [])
        section_priority = {
            "skills": 4,
            "experience": 3,
            "projects": 3,
            "education": 2,
            "summary": 1,
        }
        evidence_map: dict[str, list[dict]] = {}
        section_hits: dict[str, set[str]] = {}

        for section_name, section_text in sections.items():
            if not section_text:
                continue
            if section_name == "skills" and parse_signals.get("inferred_skills_section"):
                continue
            section_evidence = extract_skill_matches(section_text, source=f"resume:{section_name}")
            if targeted:
                section_evidence = [item for item in section_evidence if item["skill"] in targeted]
            for item in section_evidence:
                skill = item["skill"]
                section_hits.setdefault(skill, set()).add(section_name)
                evidence_map.setdefault(skill, [])
                if item["snippet"] not in {existing["snippet"] for existing in evidence_map[skill]}:
                    evidence_map[skill].append(item)

        if not evidence_map and raw_text:
            fallback = extract_skill_matches(raw_text, source="resume")
            if targeted:
                fallback = [item for item in fallback if item["skill"] in targeted]
            for item in fallback:
                evidence_map.setdefault(item["skill"], []).append(item)
                section_hits.setdefault(item["skill"], set()).add("raw_text")

        kept: list[dict] = []
        for skill, items in evidence_map.items():
            sections_for_skill = section_hits.get(skill, set())
            strongest_section = max((section_priority.get(name, 0) for name in sections_for_skill), default=0)
            if strongest_section >= 2 or len(items) >= 2:
                kept.extend(items[:2])

        if not kept and selected_skills:
            kept = extract_skill_evidence(raw_text, selected_skills, source="resume")

        return sorted(kept, key=lambda item: (item["skill"], item["snippet"]))

    def _inspect_market_sample(self, *, role_query: str, jobs: list[dict]) -> dict[str, Any]:
        normalized_role = normalize_role(role_query)
        live_jobs = [job for job in jobs if job.get("source") != "role-baseline"]
        live_skills = sorted(
            {
                skill
                for job in live_jobs
                for skill in job.get("normalized_data", {}).get("skills", [])
            }
        )
        expected_skills = self._expand_baseline_skill_pool(role_query)
        role_coverage = (len(set(live_skills) & set(expected_skills)) / len(expected_skills)) if expected_skills else 1.0
        company_count = len(
            {
                normalize_whitespace(str(job.get("company", "")).lower())
                for job in live_jobs
                if normalize_whitespace(str(job.get("company", "")).strip())
            }
        )
        title_count = len(
            {
                normalize_whitespace(str(job.get("title", "")).lower())
                for job in live_jobs
                if normalize_whitespace(str(job.get("title", "")).strip())
            }
        )
        sparse_role = normalized_role in SPARSE_LIVE_MARKET_ROLES
        target_live_floor = 1 if sparse_role else max(4, settings.production_live_display_minimum)
        sufficient_live_depth = len(live_jobs) >= target_live_floor
        live_target_reached = (
            not sparse_role
            and sufficient_live_depth
            and len(live_skills) >= 3
            and company_count >= 2
            and title_count >= 2
        )
        min_live_jobs = 1 if sparse_role else 4
        min_live_skills = 2 if sparse_role else max(3, min(5, len(expected_skills) or 4))
        min_role_coverage = 0.2 if sparse_role else 0.22
        min_company_count = 1 if sparse_role else 2
        min_title_count = 1 if sparse_role else 2
        needs_blend = False if live_target_reached else (
            len(live_jobs) < min_live_jobs
            or len(live_skills) < min_live_skills
            or role_coverage < min_role_coverage
            or company_count < min_company_count
            or title_count < min_title_count
        )
        if len(live_jobs) < min_live_jobs:
            reason = "Too few live job listings were available to form a stable market view."
        elif len(live_skills) < min_live_skills:
            reason = "The sampled live jobs exposed too few distinct hard skills for this role."
        elif role_coverage < min_role_coverage:
            reason = "The sampled live jobs missed several role-defining tools, so the market model needed calibration."
        elif company_count < min_company_count or title_count < min_title_count:
            reason = "The live sample was too concentrated in one company or role variation."
        else:
            reason = ""
        return {
            "needs_blend": needs_blend,
            "reason": reason,
            "live_skills": live_skills,
            "expected_skills": expected_skills,
            "role_coverage": round(role_coverage, 2),
            "sparse_role": sparse_role,
        }

    def _filter_skills_for_role(self, role_query: str, selected: list[str], expanded: list[str]) -> list[str]:
        lowered = normalize_role(role_query)
        selected_set = {self._normalize_label(item) for item in selected}
        expanded_set = set(expanded)

        if "data scientist" in lowered:
            allowed = expanded_set | {"statistics", "data visualization", "feature engineering"}
            return sorted((selected_set & allowed) | expanded_set)

        if "data analyst" in lowered:
            allowed = expanded_set | {"statistics", "reporting", "data visualization"}
            return sorted((selected_set & allowed) | expanded_set)

        if "machine learning" in lowered:
            allowed = expanded_set | {"statistics", "feature engineering"}
            return sorted((selected_set & allowed) | expanded_set)

        if expanded_set:
            combined = sorted(selected_set | expanded_set)
            if len(combined) < 5:
                return combined

        return sorted(selected_set or expanded_set)
