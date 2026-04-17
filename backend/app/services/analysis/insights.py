from __future__ import annotations

import json
from typing import Any

import httpx

from app.core.config import settings

try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


class InsightGenerator:
    async def summarize(self, resume_data: dict, score_payload: dict, role_query: str) -> dict[str, Any]:
        if settings.llm_provider == "ollama":
            summary = await self._summarize_with_ollama(resume_data, score_payload, role_query)
            if summary:
                return self._normalize_summary(summary, role_query)
        if settings.llm_provider == "openai":
            summary = await self._summarize_with_openai(resume_data, score_payload, role_query)
            if summary:
                return self._normalize_summary(summary, role_query)
        return self._normalize_summary(self._rule_based_summary(resume_data, score_payload, role_query), role_query)

    async def _summarize_with_ollama(self, resume_data: dict, score_payload: dict, role_query: str) -> dict[str, Any] | None:
        prompt = self._build_prompt(resume_data, score_payload, role_query)
        payload = {
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        }
        try:
            async with httpx.AsyncClient(timeout=settings.llm_summary_timeout_seconds) as client:
                response = await client.post(f"{settings.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            return json.loads(data.get("response", "{}"))
        except Exception:
            return None

    async def _summarize_with_openai(self, resume_data: dict, score_payload: dict, role_query: str) -> dict[str, Any] | None:
        if not settings.openai_api_key or OpenAI is None:
            return None
        prompt = self._build_prompt(resume_data, score_payload, role_query)
        try:
            client = OpenAI(api_key=settings.openai_api_key)
            response = client.responses.create(
                model=settings.openai_model,
                input=prompt,
                text={"format": {"type": "json_object"}},
            )
            output_text = "".join(item.text for item in response.output[0].content if getattr(item, "text", None))
            return json.loads(output_text)
        except Exception:
            return None

    def _rule_based_summary(self, resume_data: dict, score_payload: dict, role_query: str) -> dict[str, Any]:
        strengths = []
        if len(score_payload.get("matched_skills", [])) >= 5:
            strengths.append("Strong alignment with current market skills for the chosen role.")
        experience_years = float(score_payload.get("experience_years", resume_data.get("experience_years", 0)) or 0)
        if experience_years >= 2:
            strengths.append("Experience level is credible for many mid-level openings.")
        elif experience_years >= 0.25:
            strengths.append("Internship and project work provide early-career evidence for entry-level roles.")
        if resume_data.get("sections", {}).get("projects"):
            strengths.append("Projects section gives practical evidence beyond keyword matching.")
        if not strengths:
            strengths.append("Resume already contains a usable base for targeted optimization.")

        missing = score_payload.get("missing_skills", [])[:3]
        weaknesses = [
            f"{item['skill']} represents {item['share']}% of the current market-demand signal but is missing from the resume."
            for item in missing
        ] or ["Add more role-specific evidence and measurable outcomes."]
        analysis_context = score_payload.get("analysis_context", {})
        if analysis_context.get("used_role_baseline"):
            weaknesses.insert(0, "This run used a fallback role baseline because live jobs were unavailable, so market-fit signals are lower confidence.")

        next_steps = [
            "Tailor your summary and experience bullets to the target role title.",
            "Add measurable impact using numbers, percentages, or business outcomes.",
            "Include missing high-demand tools only if you truly know them.",
        ]
        return {
            "mode": "rule-based",
            "target_role": role_query,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "next_steps": next_steps,
        }

    def _normalize_summary(self, summary: dict[str, Any], role_query: str) -> dict[str, Any]:
        def normalize_list(value: Any) -> list[str]:
            if isinstance(value, list):
                return [str(item) for item in value if str(item).strip()]
            if isinstance(value, dict):
                return [str(item) for item in value.values() if str(item).strip()]
            if isinstance(value, str):
                return [value] if value.strip() else []
            return []

        strengths = normalize_list(summary.get("strengths"))
        return {
            "mode": str(summary.get("mode", "rule-based")),
            "target_role": str(summary.get("target_role", role_query)),
            "strengths": strengths,
            "weaknesses": normalize_list(summary.get("weaknesses")),
            "next_steps": normalize_list(summary.get("next_steps")),
        }

    def _build_prompt(self, resume_data: dict, score_payload: dict, role_query: str) -> str:
        sections = resume_data.get("sections", {}) or {}
        section_preview = {
            name: str(content).strip()[:220]
            for name, content in sections.items()
            if str(content).strip()
        }
        matched_skills = score_payload.get("matched_skills", [])[:8]
        missing_skills = [
            {
                "skill": item.get("skill"),
                "demand": item.get("share"),
            }
            for item in score_payload.get("missing_skills", [])[:5]
        ]
        compact_payload = {
            "overall_score": score_payload.get("overall_score"),
            "breakdown": score_payload.get("breakdown", {}),
            "experience_years": score_payload.get("experience_years", resume_data.get("experience_years", 0)),
            "analysis_context": score_payload.get("analysis_context", {}),
            "matched_skills": matched_skills,
            "missing_skills": missing_skills,
            "resume_archetype": resume_data.get("resume_archetype", {}),
            "parse_signals": resume_data.get("parse_signals", {}),
        }
        return (
            "You are an expert resume analyst. Return strict JSON with keys: mode, target_role, strengths, weaknesses, next_steps. "
            "Keep every list short and practical for a real candidate. "
            f"Role: {role_query}. "
            f"Resume sections preview: {section_preview}. "
            f"Skills: {resume_data.get('skills', [])[:12]}. "
            f"Compact score payload: {compact_payload}."
        )
