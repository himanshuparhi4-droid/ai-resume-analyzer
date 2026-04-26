import type { ParserConfidence, ScoreBreakdown } from "../lib/types";

const LABELS: Record<keyof ScoreBreakdown, string> = {
  skill_match: "Skill Match",
  semantic_match: "Semantic Match",
  experience_match: "Experience Match",
  market_demand: "Market Demand",
  resume_quality: "Resume Quality",
  ats_compliance: "ATS Compliance",
};

const DESCRIPTIONS: Record<keyof ScoreBreakdown, string> = {
  skill_match: "Detected tools against role demand",
  semantic_match: "Language closeness to the target role",
  experience_match: "Depth, dates, and proof of practice",
  market_demand: "Coverage of high-frequency market skills",
  resume_quality: "Clarity, specificity, and measurable evidence",
  ats_compliance: "Parser readability and section structure",
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
  parserConfidence?: ParserConfidence;
  componentFeedback?: Partial<Record<keyof ScoreBreakdown, string[]>>;
};

function buildVerdict(overallScore: number, breakdown: ScoreBreakdown, roleQuery?: string) {
  const targetRole = roleQuery ? roleQuery : "target role";

  if (overallScore >= 80) {
    return {
      label: "Strong match",
      detail: `This resume is already competitive for ${targetRole}. The next gains are sharper metrics, fresher market alignment, and cleaner proof placement.`,
    };
  }

  if (overallScore >= 65) {
    return {
      label: "Promising fit",
      detail: `This profile has a credible foundation for ${targetRole}. Focus on the highest-demand gaps and make each key skill visible in projects or experience.`,
    };
  }

  if (overallScore >= 50) {
    const weakestArea =
      breakdown.experience_match <= breakdown.skill_match && breakdown.experience_match <= breakdown.semantic_match
        ? "hands-on evidence"
        : breakdown.skill_match <= breakdown.semantic_match
          ? "role-specific tools"
          : "role-targeted language";
    return {
      label: "Emerging fit",
      detail: `The resume has usable signal, but the biggest bottleneck is ${weakestArea}. This is fixable with targeted evidence rather than a full rewrite.`,
    };
  }

  return {
    label: "Needs sharper targeting",
    detail: `This is not yet reading as a strong ${targetRole} match. The path forward is to add role-specific tools, clearer project proof, and cleaner parsed sections.`,
  };
}

function parserConfidenceLabel(parserConfidence?: ParserConfidence) {
  if (!parserConfidence) {
    return null;
  }
  const label = parserConfidence.label ? `${parserConfidence.label[0].toUpperCase()}${parserConfidence.label.slice(1)}` : "Measured";
  const score = typeof parserConfidence.score === "number" ? ` ${Math.round(parserConfidence.score)}%` : "";
  return `${label}${score}`;
}

function bandLabel(value: number) {
  if (value >= 80) return "strong";
  if (value >= 65) return "usable";
  if (value >= 50) return "thin";
  return "risk";
}

export function ScoreGrid({
  overallScore,
  breakdown,
  roleQuery,
  resumeArchetype,
  parserConfidence,
  componentFeedback = {},
}: ScoreGridProps) {
  const verdict = buildVerdict(overallScore, breakdown, roleQuery);
  const parserLabel = parserConfidenceLabel(parserConfidence);
  const roundedScore = Math.round(overallScore);

  return (
    <section className="grid gap-5 xl:grid-cols-[0.78fr_1.22fr]">
      <div className="ink-panel overflow-hidden rounded-[2.25rem] p-6 sm:p-8">
        <p className="font-mono text-xs font-bold uppercase tracking-[0.34em] text-white/65">Overall Signal</p>
        <div className="mt-8 grid place-items-center">
          <div
            className="grid h-56 w-56 place-items-center rounded-full p-3"
            style={{ background: `conic-gradient(var(--accent) ${Math.max(0, Math.min(roundedScore, 100))}%, rgba(255,255,255,0.12) 0)` }}
          >
            <div className="grid h-full w-full place-items-center rounded-full bg-[#07131c]/92 text-center dark:bg-[#061016]/95">
              <div>
                <p className="font-display text-7xl font-extrabold leading-none tracking-[-0.08em] text-white">{roundedScore}</p>
                <p className="mt-1 font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/60">out of 100</p>
              </div>
            </div>
          </div>
        </div>
        <div className="mt-8 rounded-[1.5rem] border border-white/10 bg-white/[0.07] p-5">
          <p className="font-display text-3xl font-extrabold tracking-[-0.045em] text-white">{verdict.label}</p>
          <p className="mt-3 text-sm leading-7 text-white/78">{verdict.detail}</p>
        </div>
        <div className="mt-4 grid gap-3">
          {resumeArchetype?.label ? (
            <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.06] p-4">
              <p className="font-mono text-[11px] font-bold uppercase tracking-[0.22em] text-white/55">Detected Resume Type</p>
              <p className="mt-2 font-semibold text-white">{resumeArchetype.label}</p>
              {resumeArchetype.reasons?.[0] ? <p className="mt-2 text-sm leading-6 text-white/72">{resumeArchetype.reasons[0]}</p> : null}
            </div>
          ) : null}
          {parserLabel ? (
            <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.06] p-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <p className="font-mono text-[11px] font-bold uppercase tracking-[0.22em] text-white/55">Parser Confidence</p>
                <span className="rounded-full bg-sea px-3 py-1 text-xs font-black uppercase tracking-[0.16em] text-ink">{parserLabel}</span>
              </div>
              {parserConfidence?.strong_recovered_structure ? (
                <p className="mt-2 text-sm leading-6 text-white/72">Layout risk was softened because dates, bullets, and sections were recovered.</p>
              ) : parserConfidence?.risk_reasons?.length ? (
                <p className="mt-2 text-sm leading-6 text-white/72">Review risk: {parserConfidence.risk_reasons.slice(0, 2).join(", ")}</p>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {Object.entries(breakdown).map(([key, value], index) => {
          const typedKey = key as keyof ScoreBreakdown;
          const feedback = componentFeedback[typedKey]?.slice(0, 2) ?? [];
          return (
            <article
              key={key}
              className={`signal-panel rounded-[1.65rem] p-5 ${index === 0 ? "md:col-span-2 xl:col-span-1" : ""}`}
            >
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="text-sm font-extrabold text-ink dark:text-slate-100">{LABELS[typedKey]}</p>
                  <p className="mt-1 text-xs font-semibold leading-5 text-slate-600 dark:text-slate-400">{DESCRIPTIONS[typedKey]}</p>
                </div>
                <span className="pill">{bandLabel(Number(value))}</span>
              </div>
              <div className="mt-5 h-2.5 overflow-hidden rounded-full bg-ink/10 dark:bg-white/10">
                <div className="h-full rounded-full bg-gradient-to-r from-sea via-gold to-ember" style={{ width: `${Math.max(6, Math.min(Number(value), 100))}%` }} />
              </div>
              <p className="mt-4 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">{Math.round(Number(value))}%</p>
              {feedback.length ? (
                <div className="mt-4 grid gap-2">
                  {feedback.map((item) => (
                    <p key={item} className="rounded-2xl bg-white/45 px-3 py-2 text-xs font-semibold leading-5 text-slate-700 dark:bg-white/[0.05] dark:text-slate-300">
                      {item}
                    </p>
                  ))}
                </div>
              ) : null}
            </article>
          );
        })}
      </div>
    </section>
  );
}
