import type { AnalysisResponse } from "./types";

function listSkills(values: string[], fallback: string) {
  return values.length ? values.join(", ") : fallback;
}

function listMissingSkills(values: AnalysisResponse["missing_skills"], fallback: string) {
  const skills = values
    .filter((item) => item.signal_source !== "weak-resume-proof")
    .map((item) => item.skill)
    .filter(Boolean);
  return skills.length ? skills.join(", ") : fallback;
}

function listWeakProof(values: NonNullable<AnalysisResponse["weak_skill_proofs"]>, fallback: string) {
  const skills = values.map((item) => item.skill).filter(Boolean);
  return skills.length ? skills.join(", ") : fallback;
}

function formatBulletLines(items: string[]) {
  return items.length ? items.map((item) => `- ${item}`).join("\n") : "- No item available.";
}

export function buildWrittenAnalysisReport(result: AnalysisResponse) {
  const strengths = Array.isArray(result.ai_summary.strengths) ? result.ai_summary.strengths : [];
  const weaknesses = Array.isArray(result.ai_summary.weaknesses) ? result.ai_summary.weaknesses : [];
  const topJobs = result.top_job_matches.slice(0, 5).map((job, index) => `${index + 1}. ${job.title} at ${job.company} (${job.location})`);

  return [
    "Resume Analysis Report",
    "======================",
    "",
    `Target role: ${result.role_query}`,
    `Overall score: ${Math.round(result.overall_score)}/100`,
    `Market location: ${result.analysis_context?.target_location ?? "Not specified"}`,
    `Live jobs used: ${result.analysis_context?.live_job_count ?? result.top_job_matches.length}`,
    "",
    "Score Breakdown",
    "---------------",
    `ATS compatibility: ${Math.round(result.breakdown.ats_compliance)}/100`,
    `Resume quality: ${Math.round(result.breakdown.resume_quality)}/100`,
    `Skill match: ${Math.round(result.breakdown.skill_match)}/100`,
    `Role/semantic match: ${Math.round(result.breakdown.semantic_match)}/100`,
    `Experience match: ${Math.round(result.breakdown.experience_match)}/100`,
    `Market demand: ${Math.round(result.breakdown.market_demand)}/100`,
    "",
    "Matched Skills",
    "--------------",
    listSkills(result.matched_skills, "No strong matched skills were found in this run."),
    "",
    "Missing Skills",
    "--------------",
    listMissingSkills(result.missing_skills, "No major missing skill cluster was found in this run."),
    "",
    "Skills Needing Stronger Proof",
    "-----------------------------",
    listWeakProof(result.weak_skill_proofs ?? [], "No major weak-proof skill cluster was found in this run."),
    "",
    "Strengths",
    "---------",
    formatBulletLines(strengths),
    "",
    "Weaknesses",
    "----------",
    formatBulletLines(weaknesses),
    "",
    "Recommendations",
    "---------------",
    formatBulletLines(result.recommendations.map((item, index) => `${index + 1}. ${item.title} (${item.impact}): ${item.detail}`)),
    "",
    "Top Job Matches",
    "---------------",
    formatBulletLines(topJobs),
    "",
    "Note",
    "----",
    "Use this report as a focused editing checklist. Add evidence, metrics, and role-specific keywords naturally rather than stuffing terms.",
    "",
  ].join("\n");
}

export function downloadWrittenAnalysisReport(result: AnalysisResponse) {
  const report = buildWrittenAnalysisReport(result);
  const blob = new Blob([report], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  const safeRole = result.role_query.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "resume";
  anchor.href = url;
  anchor.download = `${safeRole}-resume-analysis-report.txt`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}
