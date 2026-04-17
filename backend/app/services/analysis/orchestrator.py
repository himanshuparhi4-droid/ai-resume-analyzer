from __future__ import annotations

import secrets
from datetime import date, datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.models.analysis import AnalysisRun
from app.models.resume import ResumeDocument
from app.models.user import User
from app.schemas.analysis import AnalysisResponse, RecommendationItem
from app.services.analysis.insights import InsightGenerator
from app.services.analysis.scoring import ScoringEngine
from app.services.jobs.aggregator import JobAggregator
from app.services.nlp.skill_grounding import SkillGroundingService
from app.services.parsers.resume_parser import ResumeParser
from app.utils.text import truncate


class AnalysisOrchestrator:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.resume_parser = ResumeParser()
        self.job_aggregator = JobAggregator(db)
        self.scoring_engine = ScoringEngine()
        self.insight_generator = InsightGenerator()
        self.skill_grounding = SkillGroundingService()

    async def analyze_resume(self, *, filename: str, content_type: str, file_bytes: bytes, role_query: str, location: str, limit: int, user: User | None = None) -> AnalysisResponse:
        resume_data = self.resume_parser.parse(filename, content_type, file_bytes)
        jobs = await self.job_aggregator.fetch_jobs(query=role_query, location=location, limit=limit)
        if not jobs:
            jobs = await self.skill_grounding.build_fallback_jobs(role_query=role_query, location=location, resume_data=resume_data)
        else:
            jobs = await self.skill_grounding.ensure_market_coverage(
                role_query=role_query,
                location=location,
                resume_data=resume_data,
                jobs=jobs,
            )
        resume_data, jobs, grounding_metadata = await self.skill_grounding.ground(role_query=role_query, resume_data=resume_data, jobs=jobs)
        score_payload = self.scoring_engine.score(resume_data, jobs)
        score_payload["skill_grounding"] = grounding_metadata
        score_payload["analysis_context"] = self.skill_grounding.build_analysis_context(jobs)
        score_payload["experience_years"] = resume_data.get("experience_years", 0)
        ai_summary = await self.insight_generator.summarize(resume_data, score_payload, role_query)
        recommendations = self._build_recommendations(score_payload, resume_data)
        resume_record = ResumeDocument(user_id=user.id if user else None, filename=filename, mime_type=content_type, raw_text=resume_data["raw_text"], structured_data=resume_data)
        self.db.add(resume_record)
        self.db.flush()
        analysis_record = AnalysisRun(
            resume_id=resume_record.id,
            user_id=user.id if user else None,
            role_query=role_query,
            overall_score=float(self._json_safe(score_payload["overall_score"])),
            component_scores=self._json_safe(score_payload["breakdown"]),
            top_job_matches=self._json_safe(score_payload["top_job_matches"]),
            matched_skills=self._json_safe(score_payload["matched_skills"]),
            missing_skills=self._json_safe(score_payload["missing_skills"]),
            recommendations=self._json_safe([item.model_dump() for item in recommendations]),
            ai_summary=self._json_safe(ai_summary),
            share_token=secrets.token_urlsafe(12),
        )
        self.db.add(analysis_record)
        self.db.commit()
        self.db.refresh(analysis_record)
        return self._to_response(analysis_record, resume_data)

    def get_analysis(self, analysis_id: str) -> AnalysisResponse | None:
        analysis = self.db.get(AnalysisRun, analysis_id)
        if not analysis:
            return None
        resume = self.db.get(ResumeDocument, analysis.resume_id)
        return self._to_response(analysis, resume.structured_data if resume else {})

    def get_public_analysis(self, share_token: str) -> AnalysisResponse | None:
        analysis = self.db.scalar(select(AnalysisRun).where(AnalysisRun.share_token == share_token))
        return self.get_analysis(analysis.id) if analysis else None

    def list_user_history(self, user: User) -> list[AnalysisRun]:
        return list(self.db.scalars(select(AnalysisRun).where(AnalysisRun.user_id == user.id).order_by(desc(AnalysisRun.created_at))).all())

    def compare(self, *, user: User, current_id: str, previous_id: str | None = None) -> dict:
        current = self.db.get(AnalysisRun, current_id)
        if not current or current.user_id != user.id:
            raise ValueError("Current analysis not found")
        previous = self.db.get(AnalysisRun, previous_id) if previous_id else None
        if previous_id and (not previous or previous.user_id != user.id):
            raise ValueError("Previous analysis not found")
        if previous is None:
            previous = next((item for item in self.list_user_history(user) if item.id != current.id and item.role_query == current.role_query), None)
        previous_scores = previous.component_scores if previous else {}
        component_deltas = {key: round(float(current.component_scores.get(key, 0)) - float(previous_scores.get(key, 0)), 2) for key in current.component_scores.keys()}
        score_delta = round(current.overall_score - float(previous.overall_score if previous else 0), 2)
        summary = f"Your score changed by {score_delta} points." if previous else "No earlier analysis found for this role, so this is your baseline."
        return {
            "current_analysis_id": current.id,
            "previous_analysis_id": previous.id if previous else None,
            "score_delta": score_delta,
            "component_deltas": component_deltas,
            "summary": summary,
        }

    def delete_analysis(self, *, user: User, analysis_id: str) -> bool:
        analysis = self.db.get(AnalysisRun, analysis_id)
        if not analysis or analysis.user_id != user.id:
            return False
        resume = self.db.get(ResumeDocument, analysis.resume_id)
        self.db.delete(analysis)
        if resume:
            self.db.delete(resume)
        self.db.commit()
        return True

    def delete_user_data(self, user: User) -> None:
        for item in self.list_user_history(user):
            resume = self.db.get(ResumeDocument, item.resume_id)
            self.db.delete(item)
            if resume:
                self.db.delete(resume)
        db_user = self.db.get(User, user.id)
        if db_user:
            self.db.delete(db_user)
        self.db.commit()

    def _to_response(self, analysis: AnalysisRun, resume_data: dict | None) -> AnalysisResponse:
        resume_data = resume_data or {}
        sections = resume_data.get("sections", {})
        raw_text = resume_data.get("raw_text", "")
        skill_report = self.skill_grounding.build_skill_report(
            resume_text=raw_text,
            jobs=analysis.top_job_matches,
            matched_skills=analysis.matched_skills,
            missing_skills=analysis.missing_skills,
            resume_skill_evidence=resume_data.get("skill_evidence"),
        )
        analysis_context = self.skill_grounding.build_analysis_context(analysis.top_job_matches)
        component_feedback = self._build_component_feedback(
            breakdown=analysis.component_scores,
            analysis=analysis,
            resume_data=resume_data,
            analysis_context=analysis_context,
        )
        return AnalysisResponse(
            analysis_id=analysis.id,
            role_query=analysis.role_query,
            overall_score=analysis.overall_score,
            breakdown=analysis.component_scores,
            matched_skills=analysis.matched_skills,
            missing_skills=analysis.missing_skills,
            matched_skill_details=skill_report["matched_skill_details"],
            missing_skill_details=skill_report["missing_skill_details"],
            market_skill_frequency=skill_report["market_skill_frequency"],
            top_job_matches=analysis.top_job_matches,
            analysis_context=analysis_context,
            resume_archetype=resume_data.get("resume_archetype", {}),
            component_feedback=component_feedback,
            recommendations=[RecommendationItem(**item) for item in analysis.recommendations],
            ai_summary=analysis.ai_summary,
            resume_sections=sections,
            resume_preview=truncate(raw_text, 400),
            share_token=analysis.share_token,
            created_at=analysis.created_at,
        )

    def _build_recommendations(self, score_payload: dict, resume_data: dict) -> list[RecommendationItem]:
        items: list[RecommendationItem] = []
        parse_signals = resume_data.get("parse_signals", {})
        breakdown = score_payload.get("breakdown", {})
        archetype = resume_data.get("resume_archetype", {}).get("type", "general_resume")

        for item in score_payload.get("missing_skills", [])[:2]:
            items.append(
                RecommendationItem(
                    title=f"Add or learn {item['skill']}",
                    detail=f"This skill accounts for {item['share']}% of the current market-demand signal for the chosen role.",
                    impact="High",
                )
            )
        if archetype in {"modern_two_column", "modern_two_column_project_first"}:
            items.append(
                RecommendationItem(
                    title="Keep a clean ATS export alongside the modern design",
                    detail="This resume uses a modern multi-column layout. Keep this version for portfolio sharing, but also export a simpler ATS-friendly version for job portals.",
                    impact="Medium",
                )
            )
        if archetype in {"project_first_entry_level", "modern_two_column_project_first"}:
            items.append(
                RecommendationItem(
                    title="Turn projects into recruiter-style evidence",
                    detail="Project-first resumes work best when each project states the dataset, tools, outcome, and business relevance in one or two sharp bullets.",
                    impact="High",
                )
            )
        if archetype == "skills_first":
            items.append(
                RecommendationItem(
                    title="Add a clearer chronology under your skills summary",
                    detail="Skills-first resumes need a visible timeline. Make sure each internship, project, or role has dates, title, and concrete outcomes.",
                    impact="Medium",
                )
            )
        if archetype == "academic_cv":
            items.append(
                RecommendationItem(
                    title="Trim academic detail for industry applications",
                    detail="Academic CVs often need a shorter industry version. Move the strongest role-relevant research, tools, and outcomes closer to the top.",
                    impact="Medium",
                )
            )
        if archetype == "europass_cv":
            items.append(
                RecommendationItem(
                    title="Tailor the Europass structure to the target role",
                    detail="Europass resumes are comprehensive, but recruiter-facing versions work better when the target-role tools and strongest outcomes are surfaced earlier.",
                    impact="Medium",
                )
            )

        if (
            parse_signals.get("merged_header_count", 0)
            or parse_signals.get("inline_header_count", 0) >= 2
            or parse_signals.get("suspicious_token_count", 0) >= 3
        ):
            items.append(
                RecommendationItem(
                    title="Use a cleaner one-column resume export",
                    detail="This file shows OCR or merged-section noise. A clean DOCX or simpler PDF layout will improve ATS parsing and scoring accuracy.",
                    impact="High",
                )
            )
        if float(breakdown.get("ats_compliance", 0) or 0) < 70:
            items.append(
                RecommendationItem(
                    title="Make ATS parsing easier",
                    detail="Use a simpler one-column layout, keep headings on separate lines, and avoid text that gets merged during PDF export.",
                    impact="High",
                )
            )
        if parse_signals.get("inferred_skills_section") or parse_signals.get("section_leakage_count", 0):
            items.append(
                RecommendationItem(
                    title="Separate Skills, Experience, and Projects into clean blocks",
                    detail="The parser had to infer or recover some sections. Put each major heading on its own line and keep the skills list separate from project descriptions.",
                    impact="High",
                )
            )
        if parse_signals.get("suspicious_url_count", 0):
            items.append(
                RecommendationItem(
                    title="Fix contact links and exported URL text",
                    detail="At least one link looks corrupted in the PDF extract. Replace broken LinkedIn or portfolio text with clean clickable URLs before exporting again.",
                    impact="Medium",
                )
            )
        if parse_signals.get("date_range_count", 0) == 0 and resume_data.get("sections", {}).get("experience"):
            items.append(
                RecommendationItem(
                    title="Add clear month-year dates for each experience block",
                    detail="Use a recruiter-friendly format like `Jun 2025 - Jul 2025` for internships, projects, and roles so ATS systems can reliably read chronology.",
                    impact="High",
                )
            )
        if parse_signals.get("quantified_line_count", 0) < 2:
            items.append(
                RecommendationItem(
                    title="Add numbers to at least two experience or project bullets",
                    detail="Include dataset size, accuracy, dashboards built, time saved, KPI lift, or reports automated. Exact metrics raise both resume quality and recruiter trust.",
                    impact="High",
                )
            )
        if parse_signals.get("contact_link_count", 0) == 0:
            items.append(
                RecommendationItem(
                    title="Add clean portfolio or profile links in the header",
                    detail="Include LinkedIn, GitHub, portfolio, or case-study links as full clean URLs so ATS systems and recruiters can verify your work quickly.",
                    impact="Medium",
                )
            )
        if float(breakdown.get("resume_quality", 0) or 0) < 70:
            items.append(
                RecommendationItem(
                    title="Quantify project outcomes with metrics",
                    detail="Resume quality is being held back by low-evidence bullets. Add numbers such as accuracy gains, dashboards delivered, time saved, datasets analyzed, or model performance.",
                    impact="High",
                )
            )
        if float(breakdown.get("experience_match", 0) or 0) < 60:
            items.append(
                RecommendationItem(
                    title="Strengthen experience proof with analyst-style outcomes",
                    detail="Show what you built, what dataset or business problem you handled, and what changed because of your work. That lifts both experience and semantic fit.",
                    impact="Medium",
                )
            )
        items.append(RecommendationItem(title="Rewrite bullets with business outcomes", detail="Replace responsibility-style bullets with action + result + metric statements.", impact="High"))
        items.append(RecommendationItem(title="Match the role headline", detail="Use the exact role language from target jobs in your summary, projects, and skills sections.", impact="Medium"))
        deduped: list[RecommendationItem] = []
        seen_titles: set[str] = set()
        for item in items:
            if item.title in seen_titles:
                continue
            seen_titles.add(item.title)
            deduped.append(item)
        return deduped[:6]

    def _build_component_feedback(self, *, breakdown: dict, analysis: AnalysisRun, resume_data: dict, analysis_context: dict) -> dict[str, list[str]]:
        parse_signals = resume_data.get("parse_signals", {})
        missing_skills = analysis.missing_skills or []
        matched_skills = analysis.matched_skills or []
        role_query = analysis.role_query.lower()
        archetype_type = resume_data.get("resume_archetype", {}).get("type", "general_resume")
        archetype_label = resume_data.get("resume_archetype", {}).get("label", "Resume")
        market_skill_names = [item["skill"] for item in missing_skills[:3]]
        if not market_skill_names and role_query == "data analyst":
            market_skill_names = ["excel", "power bi", "tableau"]

        feedback = {
            "skill_match": [
                (
                    "Strong overlap with the sampled market set."
                    if not market_skill_names
                    else f"Biggest remaining tool gaps: {', '.join(market_skill_names)}."
                )
            ],
            "semantic_match": [
                "Semantic fit is driven by how closely your summary, project titles, and bullet language mirror the target role."
            ],
            "experience_match": [
                "Early-career experience is real here, but the score will rise when each project or internship bullet shows scope, tools, and measurable outcomes."
            ],
            "market_demand": [
                (
                    "Demand is estimated from a blended market sample because the live jobs were too narrow."
                    if analysis_context.get("market_source") == "blended-market"
                    else "Demand is estimated from a role baseline because live jobs were unavailable."
                    if analysis_context.get("market_source") == "role-baseline"
                    else "Demand reflects the repeated tools and concepts found across the sampled jobs."
                )
            ],
            "resume_quality": [
                f"{archetype_label} quality is judged on clean sections, quantified outcomes, and readable evidence instead of keyword stuffing."
            ],
            "ats_compliance": [
                "ATS checks focus on parseability: clear headings, stable links, dedicated skills blocks, and a stable export layout."
            ],
        }

        feedback["resume_quality"].append("This score is mostly document-specific, so it should only move when the resume itself gets clearer or stronger.")
        feedback["ats_compliance"].append("This score is layout-specific, so it will usually stay similar for the same file until the export or section structure changes.")

        if parse_signals.get("merged_header_count", 0):
            feedback["ats_compliance"].append("Some headings or sections appear merged in the extracted text. Put each heading on its own line.")
        if parse_signals.get("suspicious_url_count", 0):
            feedback["ats_compliance"].append("One or more exported links look corrupted. Clean up LinkedIn or portfolio URLs before exporting again.")
        if parse_signals.get("multi_column_detected"):
            feedback["ats_compliance"].append("This looks like a multi-column resume. Keep an ATS-friendly export for portals that still struggle with columns.")
        if parse_signals.get("date_range_count", 0) == 0 and resume_data.get("sections", {}).get("experience"):
            feedback["ats_compliance"].append("Chronology is weak in the parsed text. Add month-year date ranges to each role, internship, and major project.")
        if parse_signals.get("contact_link_count", 0) == 0:
            feedback["ats_compliance"].append("Add at least one clean LinkedIn, GitHub, or portfolio URL in the header so the contact block is easier to verify.")
        if parse_signals.get("section_leakage_count", 0) or parse_signals.get("inferred_skills_section"):
            feedback["resume_quality"].append("Separate Skills, Projects, and Experience so each block is easy to parse and scan.")
        if parse_signals.get("quantified_line_count", 0) < 2:
            feedback["resume_quality"].append("There are too few quantified results in the extracted text. Add metrics to at least two bullets so impact is concrete.")
        if parse_signals.get("bullet_like_line_count", 0) < 2 and resume_data.get("sections", {}).get("experience"):
            feedback["resume_quality"].append("Experience reads more like a paragraph than recruiter-friendly evidence. Break it into short bullets with one achievement each.")
        if float(breakdown.get("resume_quality", 0) or 0) < 70:
            feedback["resume_quality"].append("Add outcome-driven bullets with numbers, dataset size, model accuracy, dashboards, or business impact.")
        if archetype_type in {"project_first_entry_level", "modern_two_column_project_first"}:
            feedback["experience_match"].append("Project-first resumes should make each project read like job evidence: tools used, problem solved, and measurable result.")
        if archetype_type == "skills_first":
            feedback["experience_match"].append("Skills-first resumes need clearer dates and chronology so recruiters can trust the experience story.")
        if matched_skills and not missing_skills:
            feedback["skill_match"].append("The current sample did not expose a strong missing-skill cluster, so use the recommendations below to widen your tool coverage.")
        return feedback

    def _json_safe(self, value):
        if isinstance(value, dict):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [self._json_safe(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if hasattr(value, "item") and callable(getattr(value, "item")):
            try:
                return self._json_safe(value.item())
            except Exception:
                return value
        return value
