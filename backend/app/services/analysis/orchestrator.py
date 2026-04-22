from __future__ import annotations

import asyncio
import logging
import secrets
import time
from datetime import date, datetime
from statistics import mean

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.analysis import AnalysisRun
from app.models.resume import ResumeDocument
from app.models.user import User
from app.schemas.analysis import AnalysisResponse, RecommendationItem
from app.services.analysis.insights import InsightGenerator
from app.services.analysis.scoring import ScoringEngine
from app.services.jobs.aggregator import JobAggregator
from app.services.jobs.taxonomy import role_market_hints, role_primary_hints
from app.services.nlp.skill_grounding import SkillGroundingService
from app.services.nlp.skill_extractor import infer_skill_frequency
from app.services.parsers.resume_parser import ResumeParser
from app.utils.text import truncate

logger = logging.getLogger(__name__)


class AnalysisOrchestrator:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.resume_parser = ResumeParser()
        self.job_aggregator = JobAggregator(db)
        self.scoring_engine = ScoringEngine()
        self.insight_generator = InsightGenerator()
        self.skill_grounding = SkillGroundingService()

    async def analyze_resume(
        self,
        *,
        filename: str,
        content_type: str,
        file_bytes: bytes,
        role_query: str,
        location: str,
        limit: int,
        user: User | None = None,
    ) -> AnalysisResponse:
        started = time.perf_counter()
        resume_data = self.resume_parser.parse(filename, content_type, file_bytes)
        logger.info("Analysis step: parsed resume in %sms", round((time.perf_counter() - started) * 1000, 2))
        jobs: list[dict]
        if settings.environment == "production":
            logger.info("Analysis step: using lightweight production review path with live market fetch")
            production_limit = min(
                max(limit, settings.production_live_fetch_minimum),
                settings.production_live_fetch_maximum,
            )
            try:
                jobs = await asyncio.wait_for(
                    self.job_aggregator.fetch_jobs(
                        query=role_query,
                        location=location,
                        limit=production_limit,
                    ),
                    # Hosted providers on Render can spend several seconds on
                    # connection/setup before the actual response phase starts.
                    # Keep a firm production cap, but leave enough room for the
                    # primary + supplemental live-fetch stages to finish.
                    timeout=max(settings.job_fetch_timeout_seconds, settings.production_live_runtime_cap_seconds),
                )
            except asyncio.TimeoutError:
                partial_live = list(getattr(self.job_aggregator, "last_live_job_snapshot", []) or [])
                if partial_live:
                    logger.warning(
                        "Analysis step: production live fetch timed out, preserving %s partial live jobs instead of full baseline fallback",
                        len(partial_live),
                    )
                    jobs = partial_live
                else:
                    logger.warning("Analysis step: production live fetch timed out, falling back to role baseline")
                    jobs = []
            if jobs:
                original_live_count = len(jobs)
                jobs = await self.skill_grounding.ensure_market_coverage(
                    role_query=role_query,
                    location=location,
                    resume_data=resume_data,
                    jobs=jobs,
                )
                logger.info(
                    "Analysis step: production live fetch produced %s jobs, expanded to %s jobs in %sms",
                    original_live_count,
                    len(jobs),
                    round((time.perf_counter() - started) * 1000, 2),
                )
            else:
                jobs = await self.skill_grounding.build_fallback_jobs(role_query=role_query, location=location, resume_data=resume_data)
                logger.info(
                    "Analysis step: fallback baseline built with %s jobs in %sms",
                    len(jobs),
                    round((time.perf_counter() - started) * 1000, 2),
                )
            score_payload = self._build_lightweight_score_payload(resume_data=resume_data, jobs=jobs, role_query=role_query)
            logger.info("Analysis step: lightweight scoring finished in %sms", round((time.perf_counter() - started) * 1000, 2))
            score_payload["skill_grounding"] = {
                "mode": "production-lightweight-live" if any(job.get("source") != "role-baseline" for job in jobs) else "production-lightweight-baseline",
                "resume_kept": resume_data.get("skills", []),
                "market_kept": sorted({skill for item in jobs for skill in item.get("normalized_data", {}).get("skills", [])}),
                "resume_dropped": [],
                "market_dropped": [],
                "role_focus": [],
            }
        else:
            try:
                if settings.enable_live_market_fetch:
                    jobs = await asyncio.wait_for(
                        self.job_aggregator.fetch_jobs(
                            query=role_query,
                            location=location,
                            limit=limit,
                        ),
                        timeout=settings.job_fetch_timeout_seconds,
                    )
                else:
                    logger.info("Analysis step: live market fetch disabled, using fallback baseline")
                    jobs = []
            except asyncio.TimeoutError:
                logger.warning("Analysis step: live job fetch timed out after %ss, using fallback baseline", settings.job_fetch_timeout_seconds)
                jobs = []
            logger.info(
                "Analysis step: market fetch produced %s jobs in %sms",
                len(jobs),
                round((time.perf_counter() - started) * 1000, 2),
            )
            if not jobs:
                jobs = await self.skill_grounding.build_fallback_jobs(role_query=role_query, location=location, resume_data=resume_data)
            else:
                jobs = await self.skill_grounding.ensure_market_coverage(
                    role_query=role_query,
                    location=location,
                    resume_data=resume_data,
                    jobs=jobs,
                )
            logger.info(
                "Analysis step: market normalization finished with %s jobs in %sms",
                len(jobs),
                round((time.perf_counter() - started) * 1000, 2),
            )
            resume_data, jobs, grounding_metadata = await self.skill_grounding.ground(role_query=role_query, resume_data=resume_data, jobs=jobs)
            logger.info("Analysis step: grounding finished in %sms", round((time.perf_counter() - started) * 1000, 2))
            score_payload = self.scoring_engine.score(resume_data, jobs, role_query=role_query)
            logger.info("Analysis step: scoring finished in %sms", round((time.perf_counter() - started) * 1000, 2))
            score_payload["skill_grounding"] = grounding_metadata
        score_payload["analysis_context"] = self.skill_grounding.build_analysis_context(jobs)
        logger.info("Analysis step: analysis context built in %sms", round((time.perf_counter() - started) * 1000, 2))
        fetch_diagnostics = getattr(self.job_aggregator, "last_fetch_diagnostics", None)
        if fetch_diagnostics:
            score_payload["analysis_context"]["fetch_diagnostics"] = fetch_diagnostics
            logger.info("Analysis step: attached fetch diagnostics in %sms", round((time.perf_counter() - started) * 1000, 2))
        score_payload["experience_years"] = resume_data.get("experience_years", 0)
        if settings.environment == "production":
            ai_summary = self.insight_generator._normalize_summary(
                self.insight_generator._rule_based_summary(resume_data, score_payload, role_query),
                role_query,
            )
        else:
            ai_summary = await self.insight_generator.summarize(resume_data, score_payload, role_query)
        logger.info("Analysis step: summary finished in %sms", round((time.perf_counter() - started) * 1000, 2))
        recommendations = self._build_recommendations(score_payload, resume_data)
        logger.info("Analysis step: recommendations built in %sms", round((time.perf_counter() - started) * 1000, 2))
        if user is None:
            logger.info("Analysis step: returning ephemeral response in %sms", round((time.perf_counter() - started) * 1000, 2))
            return self._build_ephemeral_response(
                role_query=role_query,
                score_payload=score_payload,
                recommendations=recommendations,
                ai_summary=ai_summary,
                resume_data=resume_data,
            )
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
        logger.info("Analysis step: persisting analysis to database")
        self.db.commit()
        self.db.refresh(analysis_record)
        logger.info("Analysis step: persisted analysis in %sms", round((time.perf_counter() - started) * 1000, 2))
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

    def _build_ephemeral_response(
        self,
        *,
        role_query: str,
        score_payload: dict,
        recommendations: list[RecommendationItem],
        ai_summary: dict,
        resume_data: dict,
    ) -> AnalysisResponse:
        try:
            skill_report = self.skill_grounding.build_skill_report(
                role_query=role_query,
                resume_text=resume_data.get("raw_text", ""),
                jobs=score_payload.get("top_job_matches", []),
                matched_skills=score_payload.get("matched_skills", []),
                missing_skills=score_payload.get("missing_skills", []),
                resume_skill_evidence=resume_data.get("skill_evidence"),
            )
        except Exception:
            logger.exception("Analysis step: skill report build failed, returning reduced detail payload")
            skill_report = {
                "matched_skill_details": [],
                "missing_skill_details": [],
                "market_skill_frequency": [],
            }
        analysis_context = score_payload.get("analysis_context", self.skill_grounding.build_analysis_context(score_payload.get("top_job_matches", [])))
        try:
            component_feedback = self._build_component_feedback(
                breakdown=score_payload.get("breakdown", {}),
                analysis=type(
                    "EphemeralAnalysis",
                    (),
                    {
                        "top_job_matches": score_payload.get("top_job_matches", []),
                        "matched_skills": score_payload.get("matched_skills", []),
                        "missing_skills": score_payload.get("missing_skills", []),
                        "component_scores": score_payload.get("breakdown", {}),
                        "role_query": role_query,
                    },
                )(),
                resume_data=resume_data,
                analysis_context=analysis_context,
            )
        except Exception:
            logger.exception("Analysis step: component feedback build failed, returning reduced feedback payload")
            component_feedback = {}
        return AnalysisResponse(
            analysis_id=f"preview-{secrets.token_hex(8)}",
            role_query=role_query,
            overall_score=score_payload["overall_score"],
            breakdown=score_payload["breakdown"],
            matched_skills=score_payload["matched_skills"],
            missing_skills=score_payload["missing_skills"],
            matched_skill_details=skill_report["matched_skill_details"],
            missing_skill_details=skill_report["missing_skill_details"],
            market_skill_frequency=skill_report["market_skill_frequency"],
            top_job_matches=score_payload["top_job_matches"],
            analysis_context=analysis_context,
            resume_archetype=resume_data.get("resume_archetype", {}),
            component_feedback=component_feedback,
            recommendations=recommendations,
            ai_summary=ai_summary,
            resume_sections=resume_data.get("sections", {}),
            resume_preview=truncate(resume_data.get("raw_text", ""), 400),
            share_token=None,
            created_at=datetime.utcnow(),
        )

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
        try:
            skill_report = self.skill_grounding.build_skill_report(
                role_query=analysis.role_query,
                resume_text=raw_text,
                jobs=analysis.top_job_matches,
                matched_skills=analysis.matched_skills,
                missing_skills=analysis.missing_skills,
                resume_skill_evidence=resume_data.get("skill_evidence"),
            )
        except Exception:
            logger.exception("Analysis step: persisted skill report build failed, returning reduced detail payload")
            skill_report = {
                "matched_skill_details": [],
                "missing_skill_details": [],
                "market_skill_frequency": [],
            }
        analysis_context = self.skill_grounding.build_analysis_context(analysis.top_job_matches)
        try:
            component_feedback = self._build_component_feedback(
                breakdown=analysis.component_scores,
                analysis=analysis,
                resume_data=resume_data,
                analysis_context=analysis_context,
            )
        except Exception:
            logger.exception("Analysis step: persisted component feedback build failed, returning reduced feedback payload")
            component_feedback = {}
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

    def _build_lightweight_score_payload(self, *, resume_data: dict, jobs: list[dict], role_query: str) -> dict:
        resume_text = resume_data.get("raw_text", "")
        resume_skills = set(resume_data.get("skills", []))
        baseline_only = bool(jobs) and all(job.get("source") == "role-baseline" for job in jobs)
        baseline_confidences = {
            str(job.get("normalized_data", {}).get("baseline_confidence", "medium"))
            for job in jobs
            if job.get("source") == "role-baseline"
        }
        baseline_confidence = "low" if baseline_confidences == {"low"} else "high" if baseline_confidences == {"high"} else "medium"
        low_confidence_baseline_only = baseline_only and not any(
            str(job.get("normalized_data", {}).get("baseline_confidence", "medium")) == "high"
            for job in jobs
        ) and baseline_confidence == "low"
        scoring_jobs = [] if low_confidence_baseline_only else jobs
        skill_frequency = infer_skill_frequency(scoring_jobs, role_query=role_query)
        demand_map = {item["skill"]: item["share"] for item in skill_frequency}
        market_skills = set(demand_map.keys())
        role_skill_pool = market_skills | role_market_hints(role_query) | role_primary_hints(role_query)

        matched_skills = sorted(resume_skills & market_skills)
        missing_skills = [
            {"skill": skill, "share": demand_map[skill]}
            for skill in sorted(market_skills - resume_skills, key=lambda item: demand_map[item], reverse=True)
        ][:10]

        live_jobs = [job for job in scoring_jobs if job.get("source") != "role-baseline"]
        live_company_count = len(
            {
                str(job.get("company", "")).strip().lower()
                for job in live_jobs
                if str(job.get("company", "")).strip()
            }
        )
        market_confidence_factor = 1.0
        if live_jobs:
            if len(live_jobs) >= 6 and live_company_count >= 3:
                market_confidence_factor = 1.0
            elif len(live_jobs) >= 4:
                market_confidence_factor = 0.92
            elif len(live_jobs) >= 2:
                market_confidence_factor = 0.84
            else:
                market_confidence_factor = 0.76
            if any(job.get("source") == "role-baseline" for job in jobs):
                market_confidence_factor = min(market_confidence_factor, 0.9)
        elif jobs:
            market_confidence_factor = 0.78

        if baseline_only:
            if baseline_confidence == "high":
                market_confidence_factor = min(market_confidence_factor, 0.72)
            elif baseline_confidence == "medium":
                market_confidence_factor = min(market_confidence_factor, 0.55)
            else:
                market_confidence_factor = 0.0

        skill_match = round((len(matched_skills) / max(len(market_skills), 1)) * 100, 2) if market_skills else 0.0
        scoring_relevance_scores = self.scoring_engine.semantic_relevance_scores(
            resume_data,
            scoring_jobs,
            role_query=role_query,
        )
        display_relevance_scores = self.scoring_engine.semantic_relevance_scores(
            resume_data,
            jobs,
            role_query=role_query,
        )
        semantic_match = round(mean(scoring_relevance_scores), 2) if scoring_relevance_scores else 0.0
        if scoring_jobs:
            experience_match = self.scoring_engine._experience_score(resume_data.get("experience_years", 0), scoring_jobs)
        elif baseline_only:
            experience_match = round(self.scoring_engine._baseline_experience_credit(resume_data.get("experience_years", 0)) * 0.55, 2)
        else:
            experience_match = 0.0
        total_demand = sum(demand_map.values())
        covered_demand = sum(demand_map.get(skill, 0) for skill in matched_skills)
        market_demand = round((covered_demand / total_demand) * 100, 2) if total_demand else 0.0
        skill_match = round(skill_match * market_confidence_factor, 2)
        market_demand = round(market_demand * market_confidence_factor, 2)
        resume_quality = self.scoring_engine._resume_quality_score(resume_data)
        ats_compliance = self.scoring_engine._ats_score(resume_data)

        if baseline_only:
            if baseline_confidence == "high":
                semantic_match = min(semantic_match, 42.0)
                experience_match = min(experience_match, 56.0)
            elif baseline_confidence == "medium":
                semantic_match = min(semantic_match, 34.0)
                experience_match = min(experience_match, 44.0)
            else:
                semantic_match = min(semantic_match, 22.0)
                experience_match = min(experience_match, 30.0)

        overall_score = round(
            (skill_match * 0.25)
            + (semantic_match * 0.20)
            + (experience_match * 0.15)
            + (market_demand * 0.15)
            + (resume_quality * 0.15)
            + (ats_compliance * 0.10),
            2,
        )
        overall_score = self.scoring_engine._apply_role_alignment_penalty(
            overall_score=overall_score,
            matched_skills=matched_skills,
            skill_match=skill_match,
            semantic_match=semantic_match,
            market_demand=market_demand,
        )
        if baseline_only:
            if baseline_confidence == "high":
                overall_score = min(overall_score, 52.0)
            elif baseline_confidence == "medium":
                overall_score = min(overall_score, 44.0)
            else:
                overall_score = min(overall_score, 34.0)

        ranked_jobs = []
        for job, relevance in zip(jobs, display_relevance_scores):
            normalized = {**(job.get("normalized_data", {}) or {})}
            filtered_skills = [
                skill
                for skill in normalized.get("skills", [])
                if skill in role_skill_pool
            ]
            if filtered_skills:
                filtered_skills = sorted(
                    filtered_skills,
                    key=lambda skill: (
                        demand_map.get(skill, 0.0),
                        float((normalized.get("skill_weights", {}) or {}).get(skill, 0.0)),
                    ),
                    reverse=True,
                )
                normalized["skills"] = filtered_skills
                normalized["skill_weights"] = {
                    skill: float((normalized.get("skill_weights", {}) or {}).get(skill, 0.0))
                    for skill in filtered_skills
                }
                normalized["skill_evidence"] = [
                    item
                    for item in normalized.get("skill_evidence", []) or []
                    if item.get("skill") in role_skill_pool
                ][:4]
            elif normalized.get("skills"):
                normalized["skills"] = list(normalized.get("skills", [])[:4])
            normalized["match_strength_label"] = self._job_match_strength_label(job={**job, "normalized_data": normalized})
            normalized["selection_reasons"] = self._build_job_match_reasons(
                role_query=role_query,
                job={**job, "normalized_data": normalized},
                demand_map=demand_map,
                role_skill_pool=role_skill_pool,
            )
            normalized["fit_metrics"] = {
                "title_alignment": round(float(normalized.get("title_alignment_score", 0.0) or 0.0), 1),
                "role_fit": round(float(normalized.get("role_fit_score", 0.0) or 0.0), 1),
                "market_quality": round(float(normalized.get("market_quality_score", 0.0) or 0.0), 1),
                "skill_overlap": round(float(normalized.get("skill_overlap_score", 0.0) or 0.0), 1),
            }
            ranked_jobs.append(
                {
                    **job,
                    "normalized_data": normalized,
                    "relevance_score": relevance,
                    "preview": truncate(job["description"], 180),
                    "role_fit_score": float(job.get("normalized_data", {}).get("role_fit_score", 0.0)),
                    "market_quality_score": float(job.get("normalized_data", {}).get("market_quality_score", 0.0)),
                }
            )
        ranked_jobs.sort(
            key=lambda item: (
                1 if item.get("source") != "role-baseline" else 0,
                item.get("role_fit_score", 0.0),
                item.get("market_quality_score", 0.0),
                item.get("relevance_score", 0.0),
            ),
            reverse=True,
        )
        live_top_matches = [item for item in ranked_jobs if item.get("source") != "role-baseline"]
        display_limit = 10 if live_top_matches else 5
        stored_matches = ranked_jobs[: max(display_limit, min(len(ranked_jobs), 24))]
        if (
            len(live_top_matches) < display_limit
            and any(item.get("source") == "role-baseline" for item in scoring_jobs)
            and not any(item.get("source") == "role-baseline" for item in stored_matches)
        ):
            baseline_candidate = next((item for item in ranked_jobs if item.get("source") == "role-baseline"), None)
            if baseline_candidate:
                stored_matches = [*stored_matches, baseline_candidate]

        return {
            "overall_score": overall_score,
            "breakdown": {
                "skill_match": skill_match,
                "semantic_match": semantic_match,
                "experience_match": experience_match,
                "market_demand": market_demand,
                "resume_quality": resume_quality,
                "ats_compliance": ats_compliance,
            },
            "matched_skills": matched_skills,
            "missing_skills": missing_skills,
            "market_skill_frequency": skill_frequency,
            "top_job_matches": stored_matches,
        }

    def _token_overlap(self, left: str, right: str) -> float:
        left_tokens = {token.lower() for token in left.split() if len(token) > 2}
        right_tokens = {token.lower() for token in right.split() if len(token) > 2}
        if not left_tokens or not right_tokens:
            return 0.0
        return round((len(left_tokens & right_tokens) / len(right_tokens)) * 100, 2)

    def _job_match_strength_label(self, *, job: dict) -> str:
        normalized = job.get("normalized_data", {}) or {}
        source = str(job.get("source", "unknown"))
        if source == "role-baseline":
            confidence = str(normalized.get("baseline_confidence", "medium"))
            if confidence == "low":
                return "Conservative fallback"
            if confidence == "medium":
                return "Guarded baseline"
            return "Calibration baseline"

        title_alignment = float(normalized.get("title_alignment_score", 0.0) or 0.0)
        role_fit = float(normalized.get("role_fit_score", 0.0) or 0.0)
        skill_overlap = float(normalized.get("skill_overlap_score", 0.0) or 0.0)
        if title_alignment >= 24 or role_fit >= 12:
            return "Strong role match"
        if title_alignment >= 12 or role_fit >= 7 or skill_overlap >= 3:
            return "Related role match"
        return "Broad live match"

    def _build_job_match_reasons(self, *, role_query: str, job: dict, demand_map: dict[str, float], role_skill_pool: set[str]) -> list[str]:
        normalized = job.get("normalized_data", {}) or {}
        source = str(job.get("source", "unknown"))
        if source == "role-baseline":
            reasons = [str(normalized.get("baseline_reason") or "Used as a calibration baseline because live coverage was too weak.")]
            if str(normalized.get("baseline_confidence", "medium")) == "low":
                reasons.append("This fallback is intentionally conservative and should not be treated like real-time market demand.")
            elif str(normalized.get("baseline_confidence", "medium")) == "medium":
                reasons.append("Used only to widen sparse market coverage after the live sample came back thin.")
            else:
                reasons.append("Used as a role-family benchmark to widen market skill coverage, not as a direct hiring signal.")
            return reasons[:2]

        reasons: list[str] = []
        title_alignment = float(normalized.get("title_alignment_score", 0.0) or 0.0)
        market_quality = float(normalized.get("market_quality_score", 0.0) or 0.0)
        if title_alignment >= 24:
            reasons.append("The job title closely matches the requested role.")
        elif title_alignment >= 12:
            reasons.append("The title sits in the same role family as the requested role.")

        role_skills = [
            skill
            for skill in normalized.get("skills", []) or []
            if skill in role_skill_pool
        ]
        if role_skills:
            ranked_skills = sorted(role_skills, key=lambda skill: (demand_map.get(skill, 0.0), skill), reverse=True)
            reasons.append(f"Extracted role signals: {', '.join(ranked_skills[:3])}.")

        if market_quality >= 90:
            reasons.append("This listing carried strong requirement detail and survived the strict live-ranking filters.")
        elif market_quality >= 60:
            reasons.append("This listing had enough usable requirement detail to influence the market sample.")

        if not reasons:
            reasons.append(f"Kept as a filtered live listing for {role_query} after title, domain, and skill checks.")
        return reasons[:3]

    def _describe_experience_signal(self, experience_years: float) -> str:
        if experience_years <= 0:
            return "No clear dated experience was detected yet, so projects and internships need to carry more of the proof."
        if experience_years < 0.5:
            months = max(1, round(experience_years * 12))
            return f"Detected roughly {months} months of experience signal. Clear project scope and dated work samples can still push this score upward."
        if experience_years < 1.25:
            months = round(experience_years * 12)
            return f"Detected roughly {months} months of early-career experience. The score rises fastest when each bullet shows tools, scope, and measurable outcomes."
        if experience_years < 3:
            return f"Detected about {experience_years:.1f} years of experience signal. Stronger quantified outcomes should move this into a more competitive early-career range."
        return f"Detected about {experience_years:.1f} years of experience signal. The next gain comes from making the strongest role-relevant wins easier to verify."

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
        if archetype == "functional_resume":
            items.append(
                RecommendationItem(
                    title="Add role-by-role evidence below the functional headings",
                    detail="Functional resumes need credibility anchors. Add short dated role entries, internships, or projects under each capability area.",
                    impact="High",
                )
            )
        if archetype == "executive_cv":
            items.append(
                RecommendationItem(
                    title="Lead with scope, scale, and leadership outcomes",
                    detail="Executive CVs work best when each section shows team size, revenue, budget, transformation scope, or organizational impact.",
                    impact="High",
                )
            )
        if archetype == "creative_portfolio_resume":
            items.append(
                RecommendationItem(
                    title="Pair portfolio links with ATS-friendly text evidence",
                    detail="Creative resumes should still include role-specific tools, outcomes, and project context in plain text so ATS systems can read them reliably.",
                    impact="Medium",
                )
            )
        if archetype == "technical_portfolio_resume":
            items.append(
                RecommendationItem(
                    title="Translate portfolio work into recruiter-ready bullets",
                    detail="Keep GitHub or portfolio links, but also explain stack, dataset, decisions, and measurable outcomes directly in the resume.",
                    impact="High",
                )
            )
        if archetype == "one_page_concise":
            items.append(
                RecommendationItem(
                    title="Use the one-page format for signal, not compression",
                    detail="A concise resume is strongest when every line earns its place with a tool, action, and result rather than short generic bullets.",
                    impact="Medium",
                )
            )
        if archetype == "certification_first_resume":
            items.append(
                RecommendationItem(
                    title="Balance certifications with project or internship proof",
                    detail="Certifications help, but recruiter confidence rises when at least two bullets show how you used those tools in practice.",
                    impact="High",
                )
            )
        if archetype == "research_transition_resume":
            items.append(
                RecommendationItem(
                    title="Translate research into industry outcomes",
                    detail="Convert research-heavy language into business or product impact by naming datasets, tools, methodology, and the decision value of the work.",
                    impact="High",
                )
            )
        if archetype == "teaching_cv":
            items.append(
                RecommendationItem(
                    title="Lead with classroom outcomes and teaching methods",
                    detail="Teaching CVs land better when they foreground curriculum design, classroom results, student outcomes, and age-group or subject specialization.",
                    impact="High",
                )
            )
        if archetype == "government_resume":
            items.append(
                RecommendationItem(
                    title="Keep a shorter recruiter-facing version alongside the long government format",
                    detail="Government resumes can stay detailed, but private-sector applications usually perform better with a tighter version that highlights role fit and quantified outcomes earlier.",
                    impact="Medium",
                )
            )
        if archetype == "long_form_cv":
            items.append(
                RecommendationItem(
                    title="Trim low-signal detail from the long-form CV",
                    detail="A long CV is fine when every section adds role evidence. Cut repetition and move the strongest tools, outcomes, and recent work closer to the top.",
                    impact="Medium",
                )
            )
        if archetype == "career_change_resume":
            items.append(
                RecommendationItem(
                    title="Translate past experience into the target-role vocabulary",
                    detail="Career-change resumes score better when old responsibilities are rewritten in the language of the new role, backed by projects or certifications.",
                    impact="High",
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
        experience_years = float(resume_data.get("experience_years", 0) or 0)
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
                self._describe_experience_signal(experience_years)
            ],
            "market_demand": [
                (
                    "Demand is estimated from a blended market sample because the live jobs were too narrow."
                    if analysis_context.get("market_source") == "blended-market"
                    else "Demand is estimated from a weak fallback baseline because live jobs were unavailable, so role-fit confidence is limited."
                    if analysis_context.get("market_source") == "role-baseline" and analysis_context.get("baseline_confidence") == "low"
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
        if archetype_type == "functional_resume":
            feedback["experience_match"].append("Functional resumes need concrete dated evidence under each capability block so the skill claims feel credible.")
        if archetype_type == "executive_cv":
            feedback["resume_quality"].append("Executive resumes score best when each bullet communicates organizational scope, leadership, and measurable business impact.")
        if archetype_type in {"creative_portfolio_resume", "technical_portfolio_resume"}:
            feedback["ats_compliance"].append("Portfolio-heavy resumes still need plain-text tool, role, and outcome evidence so ATS systems can capture the signal.")
        if archetype_type == "research_transition_resume":
            feedback["experience_match"].append("Research-to-industry resumes score better when experiments and studies are translated into product, business, or analytics outcomes.")
        if analysis_context.get("market_source") == "role-baseline" and analysis_context.get("baseline_confidence") != "high":
            feedback["experience_match"].append("Because the live market sample was weak, this experience score is intentionally conservative instead of assuming full market fit.")
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
