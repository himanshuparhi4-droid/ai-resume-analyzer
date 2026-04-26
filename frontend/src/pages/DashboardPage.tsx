import { Link } from "react-router-dom";
import type { AnalysisResponse, ScoreBreakdown } from "../lib/types";

type DashboardPageProps = {
  result: AnalysisResponse | null;
};

const METRICS: { label: string; key: keyof ScoreBreakdown; detail: string }[] = [
  { label: "ATS Compatibility", key: "ats_compliance", detail: "Structure, headings, dates, and parse readability." },
  { label: "Keyword Match", key: "semantic_match", detail: "How closely the resume language matches the target role." },
  { label: "Formatting Score", key: "resume_quality", detail: "Layout consistency, clarity, and overall polish." },
  { label: "Experience Quality", key: "experience_match", detail: "How well experience proves relevant outcomes and responsibility." },
];

function limitTextList(value: unknown, fallback: string[], max = 3) {
  if (Array.isArray(value) && value.length) {
    return value.map(String).filter(Boolean).slice(0, max);
  }
  return fallback.slice(0, max);
}

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Dashboard</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Upload a resume to see your score.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        After analysis, this page shows your resume score, top strengths, top issues, and the key score areas to improve.
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

  const issueFallback = result.recommendations.length
    ? result.recommendations.map((item) => item.title)
    : ["Add measurable achievements", "Improve keyword alignment", "Make formatting more consistent"];
  const strengthFallback = result.matched_skills.length
    ? result.matched_skills.slice(0, 3).map((skill) => `Relevant ${skill} signal is present in the resume.`)
    : ["The resume has usable role signal.", "Core sections were detected.", "The analysis found a clear target role path."];

  const strengths = limitTextList(result.ai_summary.strengths, strengthFallback);
  const issues = limitTextList(result.ai_summary.weaknesses, issueFallback);
  const primaryIssue = issues[0] ?? "improving measurable achievements, keyword alignment, and formatting consistency";

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="grid gap-6 lg:grid-cols-[0.7fr_1.3fr] lg:items-center">
          <div className="rounded-[1.5rem] border border-slate-900/10 bg-white/55 p-6 text-center dark:border-white/10 dark:bg-white/[0.04]">
            <p className="eyebrow">Overall resume score</p>
            <p className="mt-4 font-display text-7xl font-extrabold tracking-[-0.08em] text-ink dark:text-slate-50">{Math.round(result.overall_score)}</p>
            <p className="mt-1 text-sm font-black uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">out of 100</p>
          </div>
          <div>
            <p className="eyebrow">Analysis dashboard</p>
            <h1 className="mt-3 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
              {result.role_query} resume results
            </h1>
            <p className="mt-4 max-w-3xl text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
              Your resume score is {Math.round(result.overall_score)}/100. Focus first on {primaryIssue.toLowerCase()}.
            </p>
            <div className="mt-6 flex flex-wrap gap-3">
              <Link className="primary-button" to="/feedback">
                View Detailed Feedback
              </Link>
              <Link className="ghost-button min-h-[3.35rem] px-6" to="/suggestions">
                Get Suggestions
              </Link>
              <Link className="ghost-button min-h-[3.35rem] px-6" to="/jobs">
                Live Job Matches
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Top 3 strengths</p>
          <div className="mt-4 grid gap-3">
            {strengths.map((item, index) => (
              <div key={`${item}-${index}`} className="rounded-[1.1rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                <p className="text-sm font-bold leading-7 text-slate-700 dark:text-slate-300">{index + 1}. {item}</p>
              </div>
            ))}
          </div>
        </article>

        <article className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Top 3 issues</p>
          <div className="mt-4 grid gap-3">
            {issues.map((item, index) => (
              <div key={`${item}-${index}`} className="rounded-[1.1rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                <p className="text-sm font-bold leading-7 text-slate-700 dark:text-slate-300">{index + 1}. {item}</p>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {METRICS.map((metric) => {
          const value = Math.round(result.breakdown[metric.key]);
          return (
            <article key={metric.key} className="signal-panel rounded-[1.5rem] p-5">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h2 className="font-display text-2xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{metric.label}</h2>
                  <p className="mt-2 text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">{metric.detail}</p>
                </div>
                <span className="pill">{value}%</span>
              </div>
              <div className="mt-5 h-2.5 overflow-hidden rounded-full bg-slate-900/10 dark:bg-white/10">
                <div className="h-full rounded-full bg-cyan-400" style={{ width: `${Math.max(6, Math.min(value, 100))}%` }} />
              </div>
            </article>
          );
        })}
      </section>
    </div>
  );
}
