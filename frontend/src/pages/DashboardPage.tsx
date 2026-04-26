import { Link } from "react-router-dom";
import { ScoreGrid } from "../components/ScoreGrid";
import type { AnalysisResponse, ScoreBreakdown } from "../lib/types";

type DashboardPageProps = {
  result: AnalysisResponse | null;
};

const SCORE_CARDS: { label: string; key?: keyof ScoreBreakdown; get?: (result: AnalysisResponse) => number; detail: string }[] = [
  { label: "Overall Resume Score", get: (result) => result.overall_score, detail: "Your combined resume signal across role fit, clarity, ATS, and market alignment." },
  { label: "ATS Compatibility", key: "ats_compliance", detail: "How clearly screening systems can read structure, sections, and formatting." },
  { label: "Keyword Match", key: "semantic_match", detail: "How closely your resume language aligns with the target role." },
  { label: "Formatting Quality", key: "resume_quality", detail: "How polished, specific, and impact-oriented the resume appears." },
  { label: "Skills Relevance", key: "skill_match", detail: "How well your detected skills match role and market expectations." },
  { label: "Work Experience Clarity", key: "experience_match", detail: "How clearly your experience proves relevant practical ability." },
];

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">No analysis yet</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Upload a resume to see your dashboard.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        The dashboard appears after analysis and includes ATS compatibility, role alignment, keyword fit, formatting quality, and improvement priorities.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

export function DashboardPage({ result }: DashboardPageProps) {
  if (!result) {
    return <EmptyResultState />;
  }

  const weakness = Array.isArray(result.ai_summary.weaknesses) ? result.ai_summary.weaknesses[0] : null;
  const strength = Array.isArray(result.ai_summary.strengths) ? result.ai_summary.strengths[0] : null;

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Analysis dashboard</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              {result.role_query} resume analysis
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Your resume shows where it is already working and where targeted changes can improve ATS readability, recruiter clarity, and role alignment.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Link className="primary-button" to="/feedback">
              View Detailed Feedback
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/suggestions">
              Get Improvement Suggestions
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/skills">
              View Skill Evidence
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/jobs">
              View Live Job Matches
            </Link>
          </div>
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {SCORE_CARDS.map((card) => {
          const value = card.get ? card.get(result) : card.key ? result.breakdown[card.key] : 0;
          return (
            <article key={card.label} className="signal-panel rounded-[1.65rem] p-5">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h2 className="font-display text-2xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{card.label}</h2>
                  <p className="mt-2 text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">{card.detail}</p>
                </div>
                <span className="pill">{Math.round(value)}%</span>
              </div>
              <div className="mt-5 h-2.5 overflow-hidden rounded-full bg-ink/10 dark:bg-white/10">
                <div className="h-full rounded-full bg-gradient-to-r from-sea via-cyan-300 to-blue-400" style={{ width: `${Math.max(6, Math.min(value, 100))}%` }} />
              </div>
            </article>
          );
        })}
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="soft-card rounded-[1.75rem] p-5">
          <p className="eyebrow">What works</p>
          <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            {strength ?? "The resume has usable signal. Review detailed feedback to understand which sections are carrying the strongest evidence."}
          </p>
        </article>
        <article className="soft-card rounded-[1.75rem] p-5">
          <p className="eyebrow">Priority focus</p>
          <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            {weakness ?? "Focus on clearer impact statements, stronger action verbs, and improved keyword alignment for your target role."}
          </p>
        </article>
      </section>

      <ScoreGrid
        overallScore={result.overall_score}
        breakdown={result.breakdown}
        roleQuery={result.role_query}
        resumeArchetype={result.resume_archetype}
        parserConfidence={result.analysis_context?.parser_confidence}
        componentFeedback={result.component_feedback ?? {}}
      />
    </div>
  );
}
