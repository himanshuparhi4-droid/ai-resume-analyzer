import type { ScoreBreakdown } from "../lib/types";

const LABELS: Record<keyof ScoreBreakdown, string> = {
  skill_match: "Skill Match",
  semantic_match: "Semantic Match",
  experience_match: "Experience Match",
  market_demand: "Market Demand",
  resume_quality: "Resume Quality",
  ats_compliance: "ATS Compliance"
};

type ScoreGridProps = {
  overallScore: number;
  breakdown: ScoreBreakdown;
  roleQuery?: string;
  resumeArchetype?: {
    type?: string;
    label?: string;
    confidence?: number;
    reasons?: string[];
  };
  componentFeedback?: Partial<Record<keyof ScoreBreakdown, string[]>>;
};

function buildVerdict(overallScore: number, breakdown: ScoreBreakdown, roleQuery?: string) {
  const targetRole = roleQuery ? roleQuery : "target role";

  if (overallScore >= 80) {
    return {
      label: "Strong match",
      detail: `This resume is already competitive for ${targetRole} roles and mainly needs polishing, stronger metrics, and fresher market alignment.`,
    };
  }

  if (overallScore >= 65) {
    return {
      label: "Promising entry-level fit",
      detail: `This profile has a credible foundation for ${targetRole} roles. The biggest gains now come from filling a few missing tools and making experience evidence more concrete.`,
    };
  }

  if (overallScore >= 50) {
    const weakestArea =
      breakdown.experience_match <= breakdown.skill_match && breakdown.experience_match <= breakdown.semantic_match
        ? "hands-on experience"
        : breakdown.skill_match <= breakdown.semantic_match
          ? "role-specific tools"
          : "how closely the resume language matches the target role";
    return {
      label: "Emerging fit",
      detail: `This looks like an early-career ${targetRole} profile with real potential. The current gap is mostly in ${weakestArea}, not in overall ability.`,
    };
  }

  return {
    label: "Needs stronger targeting",
    detail: `This resume is not a strong ${targetRole} match yet, but the biggest issue is targeting. Improving role-specific evidence and tool coverage should move the score quickly.`,
  };
}

export function ScoreGrid({ overallScore, breakdown, roleQuery, resumeArchetype, componentFeedback = {} }: ScoreGridProps) {
  const verdict = buildVerdict(overallScore, breakdown, roleQuery);

  return (
    <section className="grid gap-4 lg:grid-cols-[0.85fr_1.15fr]">
      <div className="rounded-[2rem] bg-ink p-8 text-white shadow-soft transition-colors duration-300 dark:bg-[#071923]">
        <p className="font-mono text-xs uppercase tracking-[0.35em] text-white/60">Overall Score</p>
        <div className="mt-8 flex items-end gap-3">
          <span className="font-display text-7xl leading-none">{Math.round(overallScore)}</span>
          <span className="mb-2 text-lg text-white/70">/100</span>
        </div>
        <p className="mt-4 max-w-sm text-sm leading-6 text-white/75">
          This is the weighted score using skills, semantic fit, experience, market demand, resume quality, and ATS compliance.
        </p>
        {resumeArchetype?.label ? (
          <div className="mt-5 rounded-[1rem] border border-white/10 bg-white/5 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-white/55">Detected Resume Type</p>
            <p className="mt-2 font-semibold text-white">{resumeArchetype.label}</p>
            {resumeArchetype.reasons?.[0] ? (
              <p className="mt-2 text-sm leading-6 text-white/70">{resumeArchetype.reasons[0]}</p>
            ) : null}
          </div>
        ) : null}
        <div className="mt-6 rounded-[1.4rem] border border-white/10 bg-white/5 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-white/55">Verdict</p>
          <h3 className="mt-2 font-display text-3xl text-white">{verdict.label}</h3>
          <p className="mt-2 text-sm leading-6 text-white/75">{verdict.detail}</p>
        </div>
      </div>
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {Object.entries(breakdown).map(([key, value]) => (
          <div key={key} className="rounded-[1.5rem] border border-ink/10 bg-white p-5 shadow-soft transition-colors duration-300 dark:border-white/10 dark:bg-white/[0.04]">
            <p className="text-sm font-semibold text-slate-700 dark:text-slate-300">{LABELS[key as keyof ScoreBreakdown]}</p>
            <div className="mt-4 h-3 rounded-full bg-mist transition-colors duration-300 dark:bg-white/[0.06]">
              <div
                className="h-3 rounded-full bg-gradient-to-r from-sea to-ember"
                style={{ width: `${Math.max(8, Math.min(value, 100))}%` }}
              />
            </div>
            <p className="mt-4 font-display text-3xl text-ink dark:text-slate-50">{Math.round(value)}%</p>
            {componentFeedback[key as keyof ScoreBreakdown]?.length ? (
              <div className="mt-3 space-y-2">
                {componentFeedback[key as keyof ScoreBreakdown]?.slice(0, 2).map((item) => (
                  <p key={item} className="text-sm leading-6 text-slate-700 dark:text-slate-300">
                    {item}
                  </p>
                ))}
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </section>
  );
}
