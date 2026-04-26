import type { AnalysisResponse, ScoreBreakdown } from "../lib/types";

type ResultCommandCenterProps = {
  result: AnalysisResponse;
};

const SCORE_LABELS: Record<keyof ScoreBreakdown, string> = {
  skill_match: "Skill Match",
  semantic_match: "Semantic Match",
  experience_match: "Experience",
  market_demand: "Market Demand",
  resume_quality: "Resume Quality",
  ats_compliance: "ATS",
};

function scoreEntries(breakdown: ScoreBreakdown) {
  return (Object.entries(breakdown) as [keyof ScoreBreakdown, number][]).sort((left, right) => left[1] - right[1]);
}

function marketConfidenceLabel(result: AnalysisResponse) {
  const confidence = result.analysis_context?.market_confidence;
  if (typeof confidence === "object" && confidence) {
    if (confidence.baseline_only) return "Baseline only";
    if (confidence.blended_market) return "Blended market";
    if (typeof confidence.factor === "number") {
      if (confidence.factor >= 0.96) return "High";
      if (confidence.factor >= 0.86) return "Medium";
      return "Limited";
    }
  }
  if (typeof confidence === "string") return confidence;
  return "Measured";
}

function buildTopFixes(result: AnalysisResponse) {
  const weakest = scoreEntries(result.breakdown).slice(0, 3);
  const fixes = result.recommendations.slice(0, 3).map((item) => ({
    title: item.title,
    detail: item.detail,
    impact: item.impact,
  }));

  for (const [key] of weakest) {
    if (fixes.length >= 3) break;
    const feedback = result.component_feedback?.[key]?.[1] ?? result.component_feedback?.[key]?.[0];
    if (feedback) {
      fixes.push({
        title: `Improve ${SCORE_LABELS[key]}`,
        detail: feedback,
        impact: "targeted",
      });
    }
  }

  return fixes.slice(0, 3);
}

function normalizeNextSteps(nextSteps: AnalysisResponse["ai_summary"]["next_steps"]) {
  if (Array.isArray(nextSteps)) return nextSteps;
  if (typeof nextSteps === "string") return [nextSteps];
  if (nextSteps && typeof nextSteps === "object") return Object.values(nextSteps);
  return [];
}

export function ResultCommandCenter({ result }: ResultCommandCenterProps) {
  const entries = scoreEntries(result.breakdown);
  const weakest = entries[0];
  const strongest = entries[entries.length - 1];
  const topFixes = buildTopFixes(result);
  const strengths = result.ai_summary.strengths ?? [];
  const nextSteps = normalizeNextSteps(result.ai_summary.next_steps);
  const liveJobs = result.analysis_context?.live_job_count ?? result.top_job_matches.filter((job) => job.source !== "role-baseline").length;
  const companies = result.analysis_context?.live_company_count;

  return (
    <section className="grid gap-5 xl:grid-cols-[1fr_0.9fr]">
      <article className="ink-panel rounded-[2.35rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <p className="font-mono text-xs font-bold uppercase tracking-[0.34em] text-white/60">Executive Summary</p>
            <h2 className="mt-3 font-display text-4xl font-extrabold leading-none tracking-[-0.055em] text-white sm:text-5xl">
              {result.role_query} readiness report
            </h2>
          </div>
          <div className="rounded-[1.4rem] border border-white/10 bg-white/[0.08] px-5 py-4 text-right">
            <p className="font-display text-5xl font-extrabold tracking-[-0.07em] text-white">{Math.round(result.overall_score)}</p>
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.2em] text-white/55">overall score</p>
          </div>
        </div>

        <div className="mt-7 grid gap-3 md:grid-cols-3">
          <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.18em] text-white/50">Confidence</p>
            <p className="mt-2 font-display text-2xl font-extrabold text-white">{marketConfidenceLabel(result)}</p>
            <p className="mt-2 text-sm leading-6 text-white/70">
              {companies ? `${liveJobs} live jobs from ${companies} companies` : `${liveJobs} live jobs sampled`}
            </p>
          </div>
          <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.18em] text-white/50">Strongest Area</p>
            <p className="mt-2 font-display text-2xl font-extrabold text-white">{SCORE_LABELS[strongest[0]]}</p>
            <p className="mt-2 text-sm leading-6 text-white/70">{Math.round(strongest[1])}% score</p>
          </div>
          <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.18em] text-white/50">Weakest Area</p>
            <p className="mt-2 font-display text-2xl font-extrabold text-white">{SCORE_LABELS[weakest[0]]}</p>
            <p className="mt-2 text-sm leading-6 text-white/70">{Math.round(weakest[1])}% score</p>
          </div>
        </div>

        <div className="mt-7 rounded-[1.5rem] border border-white/10 bg-white/[0.07] p-5">
          <p className="font-display text-2xl font-extrabold tracking-[-0.04em] text-white">Report Preview</p>
          <p className="mt-3 text-sm font-semibold leading-7 text-white/75">
            This report summarizes score evidence, top fixes, skill gaps, recommendations, and job matches. It is structured so a recruiter-style review can be understood in under two minutes.
          </p>
        </div>
      </article>

      <article className="glass-panel rounded-[2.35rem] p-5 sm:p-7">
        <p className="eyebrow">Top 3 Fixes</p>
        <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
          Do these first
        </h3>
        <div className="mt-6 grid gap-3">
          {topFixes.map((fix, index) => (
            <div key={`${fix.title}-${index}`} className="signal-panel rounded-[1.45rem] p-4">
              <div className="flex items-start gap-4">
                <span className="grid h-10 w-10 shrink-0 place-items-center rounded-2xl bg-ink font-display text-lg font-extrabold text-white dark:bg-sea dark:text-ink">
                  {index + 1}
                </span>
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <h4 className="font-display text-xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{fix.title}</h4>
                    <span className="rounded-full bg-sea/15 px-3 py-1 text-[10px] font-black uppercase tracking-[0.16em] text-ink dark:text-slate-100">{fix.impact}</span>
                  </div>
                  <p className="mt-2 text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">{fix.detail}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
        <div className="mt-5 grid gap-2 rounded-[1.35rem] border border-ink/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
          <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">Quick Strengths</p>
          {(strengths.length ? strengths : nextSteps).slice(0, 2).map((item) => (
            <p key={item} className="text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">{item}</p>
          ))}
        </div>
      </article>
    </section>
  );
}
