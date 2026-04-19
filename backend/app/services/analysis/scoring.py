from __future__ import annotations

import re

from app.schemas.common import ScoreBreakdown
from app.services.nlp.embeddings import EmbeddingService
from app.services.nlp.skill_extractor import infer_skill_frequency
from app.utils.text import truncate

JOB_YEARS_RE = re.compile(r"(\d{1,2})\+?\s+years")
ACTION_VERBS = {"built", "led", "designed", "implemented", "optimized", "delivered", "improved", "launched"}


class ScoringEngine:
    def __init__(self) -> None:
        self.embedding_service = EmbeddingService()

    def score(self, resume_data: dict, jobs: list[dict], *, role_query: str | None = None) -> dict:
        baseline_job_count = sum(1 for job in jobs if job.get("source") == "role-baseline")
        baseline_only = bool(jobs) and baseline_job_count == len(jobs)
        blended_market = baseline_job_count > 0 and not baseline_only
        market_confidence = 0.82 if baseline_only else 0.9 if blended_market else 1.0
        resume_skills = set(resume_data.get("skills", []))
        skill_frequency = infer_skill_frequency(jobs, role_query=role_query)
        demand_map = {item["skill"]: item["share"] for item in skill_frequency}
        market_skills = set(demand_map.keys())

        matched_skills = sorted(resume_skills & market_skills)
        missing_skills = [
            {"skill": skill, "share": demand_map[skill]}
            for skill in sorted(market_skills - resume_skills, key=lambda item: demand_map[item], reverse=True)
        ]

        skill_match = self._skill_match_score(resume_skills, jobs) * market_confidence
        semantic_match = self._semantic_score(resume_data.get("raw_text", ""), jobs)
        experience_match = self._experience_score(resume_data.get("experience_years", 0), jobs)
        market_demand = self._market_demand_score(matched_skills, demand_map) * market_confidence
        resume_quality = self._resume_quality_score(resume_data)
        ats_compliance = self._ats_score(resume_data)

        breakdown = ScoreBreakdown(
            skill_match=skill_match,
            semantic_match=semantic_match,
            experience_match=experience_match,
            market_demand=market_demand,
            resume_quality=resume_quality,
            ats_compliance=ats_compliance,
        )
        overall_score = round(
            (skill_match * 0.25)
            + (semantic_match * 0.20)
            + (experience_match * 0.15)
            + (market_demand * 0.15)
            + (resume_quality * 0.15)
            + (ats_compliance * 0.10),
            2,
        )
        overall_score = self._apply_role_alignment_penalty(
            overall_score=overall_score,
            matched_skills=matched_skills,
            skill_match=skill_match,
            semantic_match=semantic_match,
            market_demand=market_demand,
        )

        ranked_jobs = []
        relevance_scores = self.embedding_service.similarities_to_many(
            resume_data.get("raw_text", ""),
            [f"{job['title']} {job['description']}" for job in jobs],
        )
        for job, relevance in zip(jobs, relevance_scores):
            ranked_jobs.append({**job, "relevance_score": relevance, "preview": truncate(job["description"], 180)})
        ranked_jobs.sort(
            key=lambda item: (
                1 if item.get("source") != "role-baseline" else 0,
                item["relevance_score"],
            ),
            reverse=True,
        )
        top_matches = ranked_jobs[:5]
        if blended_market and not any(item.get("source") == "role-baseline" for item in top_matches):
            baseline_candidate = next((item for item in ranked_jobs if item.get("source") == "role-baseline"), None)
            if baseline_candidate:
                top_matches = top_matches[:-1] + [baseline_candidate]

        return {
            "overall_score": overall_score,
            "breakdown": breakdown.model_dump(),
            "matched_skills": matched_skills,
            "missing_skills": missing_skills[:10],
            "market_skill_frequency": skill_frequency,
            "top_job_matches": top_matches,
        }

    def _skill_match_score(self, resume_skills: set[str], jobs: list[dict]) -> float:
        if not jobs:
            return 0.0
        weighted_scores = []
        weighted_total = 0.0
        for job in jobs:
            normalized = job.get("normalized_data", {}) or {}
            skill_weights = normalized.get("skill_weights", {}) or {}
            if not skill_weights:
                skills = normalized.get("skills", []) or []
                skill_weights = {
                    skill: 0.82 if job.get("source") != "role-baseline" else 0.7
                    for skill in skills
                }
            total_weight = sum(float(weight) for weight in skill_weights.values())
            if total_weight <= 0:
                continue
            covered_weight = sum(
                float(weight)
                for skill, weight in skill_weights.items()
                if skill in resume_skills
            )
            job_weight = 1.0 if job.get("source") != "role-baseline" else 0.7
            weighted_scores.append((covered_weight / total_weight) * job_weight)
            weighted_total += job_weight
        if weighted_total <= 0:
            return 0.0
        return round((sum(weighted_scores) / weighted_total) * 100, 2)

    def _semantic_score(self, resume_text: str, jobs: list[dict]) -> float:
        if not jobs:
            return 0.0
        scores = self.embedding_service.similarities_to_many(
            resume_text,
            [f"{item['title']} {item['description']}" for item in jobs[:5]],
        )
        return round(sum(scores) / len(scores), 2)

    def _experience_score(self, candidate_years: float, jobs: list[dict]) -> float:
        if not jobs:
            return 0.0
        target_years = []
        for job in jobs:
            matches = JOB_YEARS_RE.findall(job.get("description", "").lower())
            if matches:
                target_years.append(int(matches[0]))
        baseline_credit = self._baseline_experience_credit(candidate_years)
        if not target_years:
            return baseline_credit
        average_target = sum(target_years) / len(target_years)
        if average_target <= 0:
            return baseline_credit

        if candidate_years >= average_target:
            upside = min(14.0, (candidate_years - average_target) * 5.0)
            return round(min(92.0, baseline_credit + 10.0 + upside), 2)

        shortage_ratio = max(0.0, min(candidate_years / max(average_target, 0.25), 1.0))
        floor = 10.0 if candidate_years <= 0 else max(16.0, baseline_credit * 0.5)
        score = floor + (baseline_credit - floor) * (0.45 + (0.55 * shortage_ratio))
        return round(max(floor, min(score, 88.0)), 2)

    def _baseline_experience_credit(self, candidate_years: float) -> float:
        if candidate_years >= 8:
            return 86.0
        if candidate_years >= 5:
            return 76.0
        if candidate_years >= 3:
            return 64.0
        if candidate_years >= 2:
            return 58.0
        if candidate_years >= 1:
            return 50.0
        if candidate_years >= 0.5:
            return 32.0
        if candidate_years >= 0.17:
            return 24.0
        if candidate_years > 0:
            return 18.0
        return 10.0

    def _market_demand_score(self, matched_skills: list[str], demand_map: dict[str, float]) -> float:
        if not demand_map:
            return 0.0
        total_possible = sum(demand_map.values())
        covered = sum(demand_map.get(skill, 0) for skill in matched_skills)
        return round((covered / total_possible) * 100, 2) if total_possible else 0.0

    def _apply_role_alignment_penalty(
        self,
        *,
        overall_score: float,
        matched_skills: list[str],
        skill_match: float,
        semantic_match: float,
        market_demand: float,
    ) -> float:
        if matched_skills:
            return overall_score
        if skill_match <= 5 and market_demand <= 5 and semantic_match < 35:
            return round(overall_score * 0.45, 2)
        if skill_match < 18 and market_demand < 18 and semantic_match < 45:
            return round(overall_score * 0.62, 2)
        if skill_match < 30 and market_demand < 30 and semantic_match < 52:
            return round(overall_score * 0.78, 2)
        return overall_score

    def _resume_quality_score(self, resume_data: dict) -> float:
        text = resume_data.get("raw_text", "")
        sections = resume_data.get("sections", {})
        parse_signals = resume_data.get("parse_signals", {})
        archetype = resume_data.get("resume_archetype", {}).get("type", "general_resume")
        words = text.split()
        base = 18
        preferred_upper = 1400 if archetype == "academic_cv" else 900
        soft_upper = 1800 if archetype == "academic_cv" else 1100
        if 250 <= len(words) <= preferred_upper:
            base += 12
        elif 180 <= len(words) <= soft_upper:
            base += 7
        elif len(words) < 160:
            base -= 4
        else:
            base -= 3
        if len(sections) >= 4:
            base += 10
        verb_hits = sum(1 for word in ACTION_VERBS if word in text.lower())
        base += min(8, verb_hits * 1.5)
        quantified_hits = len(re.findall(r"\b\d+%|\b\d+[kKmM]?\b", text))
        base += min(10, quantified_hits * 1.4)
        summary_word_count = len(sections.get("summary", "").split())
        if 12 <= summary_word_count <= 70:
            base += 5
        elif sections.get("summary"):
            base += 2
        if parse_signals.get("summary_line_count", 0) >= 4:
            base -= 2
        if parse_signals.get("skills_count", 0) >= 6:
            base += 4
        if parse_signals.get("experience_action_line_count", 0) >= 4:
            base += 10
        elif parse_signals.get("experience_action_line_count", 0) >= 2:
            base += 6
        if parse_signals.get("experience_quantified_line_count", 0) >= 3:
            base += 10
        elif parse_signals.get("experience_quantified_line_count", 0) >= 1:
            base += 5
        if parse_signals.get("projects_action_line_count", 0) >= 2:
            base += 5
        if parse_signals.get("projects_quantified_line_count", 0) >= 1:
            base += 4
        if sections.get("experience") and sections.get("projects"):
            base += 4
        if sections.get("experience") and sections.get("skills"):
            base += 4
        if parse_signals.get("action_verb_variety_count", 0) >= 5:
            base += 5
        elif parse_signals.get("action_verb_variety_count", 0) >= 3:
            base += 3
        avg_bullet_words = float(parse_signals.get("avg_bullet_word_count", 0.0) or 0.0)
        if 9 <= avg_bullet_words <= 24:
            base += 5
        elif avg_bullet_words > 30:
            base -= 3
        if parse_signals.get("evidence_bullet_count", 0) >= 4:
            base += 4
        if parse_signals.get("section_balance_score", 0) >= 75:
            base += 8
        elif parse_signals.get("section_balance_score", 0) >= 60:
            base += 4
        if parse_signals.get("dominant_section_share", 0) > 0.62:
            base -= 4
        if parse_signals.get("skills_focus_share", 0) > 0.42 and parse_signals.get("experience_section_word_count", 0) < 60:
            base -= 8
        if archetype in {"project_first_entry_level", "modern_two_column_project_first"} and sections.get("projects"):
            base += 6
        if archetype == "academic_cv" and len(sections) >= 5:
            base += 6
        if archetype == "europass_cv":
            base += 2
        if archetype == "technical_portfolio_resume" and parse_signals.get("portfolio_link_count", 0) >= 1:
            base += 5
        if archetype == "creative_portfolio_resume" and parse_signals.get("portfolio_link_count", 0) >= 1:
            base += 6
        if parse_signals.get("contact_link_count", 0) >= 2:
            base += 2
        if not parse_signals.get("suspicious_url_count", 0) and not parse_signals.get("suspicious_token_count", 0):
            base += 3
        if parse_signals.get("chronology_signal_count", 0) >= 2:
            base += 2
        if parse_signals.get("objective_marker_count", 0):
            base -= 4
        if parse_signals.get("first_person_pronoun_count", 0) >= 2:
            base -= 3

        penalties = (
            min(4, parse_signals.get("merged_header_count", 0) * 2)
            + min(2, parse_signals.get("inline_header_count", 0) * 0.75)
            + min(8, parse_signals.get("section_leakage_count", 0) * 4)
            + min(4, parse_signals.get("suspicious_token_count", 0) * 1)
            + min(4, parse_signals.get("suspicious_url_count", 0) * 2)
            + (2 if parse_signals.get("inferred_skills_section") else 0)
            + (5 if sections.get("experience") and parse_signals.get("experience_action_line_count", 0) == 0 else 0)
            + (5 if sections.get("experience") and parse_signals.get("experience_quantified_line_count", 0) == 0 else 0)
            + (3 if sections.get("experience") and parse_signals.get("date_range_count", 0) == 0 else 0)
            + min(8, parse_signals.get("long_evidence_line_count", 0) * 1.5)
            + min(6, parse_signals.get("dense_paragraph_line_count", 0) * 1.5)
            + min(6, parse_signals.get("weak_bullet_count", 0) * 1.2)
            + min(4, parse_signals.get("short_bullet_count", 0) * 1.0)
        )
        if parse_signals.get("multi_column_detected") and parse_signals.get("explicit_header_count", 0) >= 4:
            penalties = max(0, penalties - 2)
        return float(max(18, min(base - penalties, 96)))

    def _ats_score(self, resume_data: dict) -> float:
        sections = resume_data.get("sections", {})
        text = resume_data.get("raw_text", "")
        parse_signals = resume_data.get("parse_signals", {})
        archetype = resume_data.get("resume_archetype", {}).get("type", "general_resume")
        score = 16
        required_sections = {"experience", "education", "skills"}
        score += len(required_sections & set(sections.keys())) * 10
        if resume_data.get("contact", {}).get("email"):
            score += 8
        if resume_data.get("contact", {}).get("phone"):
            score += 6
        content_type = str(resume_data.get("content_type", "")).lower()
        if "wordprocessingml" in content_type:
            score += 4
        elif "pdf" in content_type:
            score += 2
        if 200 <= len(text.split()) <= 1100:
            score += 5
        if parse_signals.get("explicit_header_count", 0) >= 5:
            score += 16
        elif parse_signals.get("explicit_header_count", 0) >= 3:
            score += 12
        elif parse_signals.get("explicit_header_count", 0) >= 1:
            score += 5
        if "table" not in text.lower() and "textbox" not in text.lower():
            score += 5
        if sections.get("skills") and not parse_signals.get("inferred_skills_section"):
            score += 6
        if parse_signals.get("date_range_count", 0) >= 1:
            score += 10
        if parse_signals.get("date_range_count", 0) >= 3:
            score += 3
        if parse_signals.get("contact_link_count", 0) >= 1:
            score += 3
        if parse_signals.get("bullet_like_line_count", 0) >= 2:
            score += 4
        if parse_signals.get("chronology_signal_count", 0) >= 2:
            score += 4
        if parse_signals.get("section_balance_score", 0) >= 70:
            score += 2
        if parse_signals.get("multi_column_detected"):
            score -= 8
            if parse_signals.get("explicit_header_count", 0) >= 4:
                score += 2
        if archetype == "europass_cv":
            score += 4
        if archetype in {"reverse_chronological", "one_page_concise"}:
            score += 3
        if not parse_signals.get("suspicious_url_count", 0) and not parse_signals.get("suspicious_token_count", 0):
            score += 4

        penalties = (
            min(12, parse_signals.get("merged_header_count", 0) * 4)
            + min(10, parse_signals.get("inline_header_count", 0) * 2)
            + min(10, parse_signals.get("section_leakage_count", 0) * 4)
            + min(8, parse_signals.get("suspicious_token_count", 0) * 1)
            + min(8, parse_signals.get("suspicious_url_count", 0) * 3)
            + (10 if parse_signals.get("inferred_skills_section") else 0)
            + (6 if sections.get("experience") and parse_signals.get("date_range_count", 0) == 0 else 0)
            + (3 if sections.get("experience") and parse_signals.get("explicit_header_count", 0) < 2 else 0)
            + (4 if parse_signals.get("portfolio_link_count", 0) == 0 and archetype in {"creative_portfolio_resume", "technical_portfolio_resume"} else 0)
            + min(5, parse_signals.get("dense_paragraph_line_count", 0) * 1.0)
            + min(4, parse_signals.get("objective_marker_count", 0) * 2)
        )
        return float(max(16, min(score - penalties, 98)))
