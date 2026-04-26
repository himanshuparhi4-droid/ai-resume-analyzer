import { Link } from "react-router-dom";
import { downloadWrittenAnalysisReport } from "../lib/reportExport";
import type { AnalysisResponse, RecommendationItem, User } from "../lib/types";

type SuggestionsPageProps = {
  result: AnalysisResponse | null;
  user: User | null;
  onAnalyzeAnother: () => void;
};

const ACTION_VERBS = ["Developed", "Improved", "Automated", "Analyzed", "Optimized", "Built", "Delivered", "Reduced"];

const FALLBACK_RECOMMENDATIONS: RecommendationItem[] = [
  {
    title: "Add measurable outcomes",
    detail: "Use numbers, scope, or clear results in the strongest experience and project bullets.",
    impact: "High",
  },
  {
    title: "Match the target role language",
    detail: "Use the exact role keywords naturally in the summary, skills, and strongest proof points.",
    impact: "Medium",
  },
  {
    title: "Improve scan readability",
    detail: "Keep headings simple, bullets concise, and formatting consistent across sections.",
    impact: "Medium",
  },
];

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Suggestions</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Analyze a resume to get improvement suggestions.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Suggested rewrites, missing keywords, and priority improvements appear here after analysis.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

export function SuggestionsPage({ result, user, onAnalyzeAnother }: SuggestionsPageProps) {
  if (!result) {
    return <EmptyResultState />;
  }

  const recommendations = result.recommendations.length ? result.recommendations : FALLBACK_RECOMMENDATIONS;
  const missingKeywords = result.missing_skills.filter((item) => item.signal_source !== "weak-resume-proof").slice(0, 8);
  const weakProof = [...(result.weak_skill_proofs ?? []), ...result.missing_skills.filter((item) => item.signal_source === "weak-resume-proof")].slice(0, 5);

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Suggestions</p>
            <h1 className="mt-3 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
              Improve the resume with specific edits.
            </h1>
            <p className="mt-4 max-w-3xl text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
              Start with the highest-impact fixes, then update weak bullets, keywords, and formatting one section at a time.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <button className="ghost-button min-h-[3.35rem] px-6" onClick={() => downloadWrittenAnalysisReport(result)} type="button">
              Download Written Report
            </button>
            <button className="primary-button" onClick={onAnalyzeAnother} type="button">
              Analyze Another Resume
            </button>
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[1.05fr_0.95fr]">
        <article className="signal-panel rounded-[2rem] p-6 sm:p-7">
          <p className="eyebrow">Rewrite example</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Make weak bullets more specific.</h2>
          <div className="mt-5 grid gap-4">
            <div className="rounded-[1.25rem] border border-slate-900/10 bg-white/55 p-4 dark:border-white/10 dark:bg-white/[0.04]">
              <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">Before</p>
              <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">Worked on backend APIs.</p>
            </div>
            <div className="rounded-[1.25rem] border border-cyan-300/20 bg-cyan-300/10 p-4">
              <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">After</p>
              <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-200">
                Developed and optimized backend APIs to improve application performance and system reliability.
              </p>
            </div>
            <div className="rounded-[1.25rem] border border-slate-900/10 bg-white/45 p-4 dark:border-white/10 dark:bg-white/[0.04]">
              <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">Why this is better</p>
              <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
                This version uses stronger action language and explains the impact of the work instead of only naming the task.
              </p>
            </div>
          </div>
        </article>

        <article className="signal-panel rounded-[2rem] p-6 sm:p-7">
          <p className="eyebrow">Priority improvements</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Fix these first.</h2>
          <div className="mt-5 grid gap-3">
            {recommendations.slice(0, 5).map((item, index) => (
              <div key={`${item.title}-${index}`} className="rounded-[1.2rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                <div className="flex flex-wrap items-center gap-3">
                  <span className="grid h-8 w-8 place-items-center rounded-xl bg-slate-950 text-sm font-extrabold text-white dark:bg-cyan-300/15">{index + 1}</span>
                  <p className="font-display text-xl font-extrabold tracking-[-0.035em] text-ink dark:text-slate-50">{item.title}</p>
                  <span className="pill">{item.impact}</span>
                </div>
                <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{item.detail}</p>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <article className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Missing keywords</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Add role language carefully.</h2>
          <div className="mt-4 flex flex-wrap gap-2">
            {missingKeywords.length ? (
              missingKeywords.map((item) => (
                <span key={item.skill} className="rounded-full border border-slate-900/10 bg-white/60 px-3 py-1 text-sm font-bold text-slate-700 dark:border-white/10 dark:bg-white/[0.06] dark:text-slate-200">
                  {item.skill}
                </span>
              ))
            ) : (
              <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">No major missing keyword cluster was found in this run.</p>
            )}
          </div>
        </article>

        <article className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Action verbs</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Start bullets strongly.</h2>
          <div className="mt-4 flex flex-wrap gap-2">
            {ACTION_VERBS.map((verb) => (
              <span key={verb} className="rounded-full border border-slate-900/10 bg-white/60 px-3 py-1 text-sm font-bold text-slate-700 dark:border-white/10 dark:bg-white/[0.06] dark:text-slate-200">
                {verb}
              </span>
            ))}
          </div>
        </article>

        <article className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Formatting</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Keep it easy to scan.</h2>
          <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            Use consistent headings, concise bullets, simple dates, and a layout that stays readable in ATS systems.
          </p>
        </article>
      </section>

      {weakProof.length ? (
        <section className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
          <p className="eyebrow">Skills needing stronger proof</p>
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {weakProof.map((item) => (
              <div key={`${item.skill}-${item.share}`} className="rounded-[1.2rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                <p className="font-bold text-ink dark:text-slate-100">{item.skill}</p>
                <p className="mt-1 text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">
                  Add a project, outcome, or concrete usage example so this skill feels proven.
                </p>
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <section className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="eyebrow">Next step</p>
            <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              {user ? "Open saved reports to compare this resume with earlier versions." : "Create a free account only if you want to save this report and compare future versions."}
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/jobs">
              View Live Jobs
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to={user ? "/reports" : "/login"}>
              {user ? "Open Saved Reports" : "Save Report"}
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
