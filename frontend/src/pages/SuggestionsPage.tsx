import { Link } from "react-router-dom";
import type { AnalysisResponse, User } from "../lib/types";

type SuggestionsPageProps = {
  result: AnalysisResponse | null;
  user: User | null;
  onAnalyzeAnother: () => void;
};

const ACTION_VERBS = ["Developed", "Improved", "Automated", "Analyzed", "Optimized", "Built", "Delivered", "Reduced"];

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

  const missingKeywords = result.missing_skills.filter((item) => item.signal_source !== "weak-resume-proof").slice(0, 8);
  const weakProof = [...(result.weak_skill_proofs ?? []), ...result.missing_skills.filter((item) => item.signal_source === "weak-resume-proof")].slice(0, 6);

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Suggested improvements</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              Practical changes for a stronger resume.
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Replace passive descriptions with achievement-focused bullet points that clearly show your contribution, tools used, and measurable impact.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <button className="ghost-button min-h-[3.35rem] px-6" onClick={() => window.print()} type="button">
              Download Report
            </button>
            <button className="primary-button" onClick={onAnalyzeAnother} type="button">
              Analyze Another Resume
            </button>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/skills">
              View Skill Evidence
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/jobs">
              View Live Jobs
            </Link>
          </div>
        </div>
      </section>

      <section className="grid items-start gap-4 lg:grid-cols-[1.15fr_0.85fr]">
        <div className="glass-panel rounded-[2rem] p-5 sm:p-7">
          <p className="eyebrow">Priority list</p>
          <h2 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Highest-impact fixes</h2>
          <div className="mt-6 grid gap-4">
            {result.recommendations.map((item, index) => (
              <article key={`${item.title}-${index}`} className="signal-panel rounded-[1.5rem] p-5">
                <div className="flex items-start gap-4">
                  <span className="grid h-10 w-10 shrink-0 place-items-center rounded-2xl bg-slate-950 font-display text-lg font-extrabold text-white dark:bg-cyan-300/15">
                    {index + 1}
                  </span>
                  <div>
                    <div className="flex flex-wrap items-center gap-3">
                      <h3 className="font-display text-2xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{item.title}</h3>
                      <span className="pill">{item.impact}</span>
                    </div>
                    <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{item.detail}</p>
                  </div>
                </div>
              </article>
            ))}
          </div>
        </div>

        <div className="grid content-start gap-4 self-start">
          <article className="signal-panel rounded-[2rem] p-5 sm:p-6">
            <p className="eyebrow">Missing keywords</p>
            <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Add role-specific language.</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Use these terms naturally in your summary, skills, and strongest project or experience bullets. Do not stuff keywords without proof.
            </p>
            <div className="mt-4 flex flex-wrap gap-2">
              {missingKeywords.length ? (
                missingKeywords.map((item) => (
                  <span key={item.skill} className="rounded-full bg-cyan-300/15 px-3 py-1 text-sm font-bold text-ink dark:text-slate-100">
                    {item.skill}
                  </span>
                ))
              ) : (
                <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">No major missing keyword cluster was found in this run.</p>
              )}
            </div>
          </article>

          <article className="signal-panel rounded-[2rem] p-5 sm:p-6">
            <p className="eyebrow">Better action verbs</p>
            <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Make bullets sound outcome-focused.</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Start important bullets with strong verbs, then add the tool, scope, and result.
            </p>
            <div className="mt-4 flex flex-wrap gap-2">
              {ACTION_VERBS.map((verb) => (
                <span key={verb} className="rounded-full bg-white/50 px-3 py-1 text-sm font-bold text-slate-700 dark:bg-white/[0.06] dark:text-slate-200">
                  {verb}
                </span>
              ))}
            </div>
          </article>

          <article className="signal-panel rounded-[2rem] p-5 sm:p-6">
            <p className="eyebrow">Formatting recommendation</p>
            <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Keep improvements easy to scan.</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Update one section at a time: headline, skills, top projects, then experience bullets. This keeps the resume clearer and avoids over-editing.
            </p>
          </article>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="ink-panel rounded-[2rem] p-6 sm:p-7">
          <p className="font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/65">Rewrite example</p>
          <div className="mt-5 grid gap-4">
            <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.06] p-4">
              <p className="text-xs font-black uppercase tracking-[0.2em] text-white/50">Before</p>
              <p className="mt-2 text-sm font-semibold leading-7 text-white/82">Worked on backend APIs for the project.</p>
            </div>
            <div className="rounded-[1.25rem] border border-cyan-300/20 bg-cyan-300/10 p-4">
              <p className="text-xs font-black uppercase tracking-[0.2em] text-cyan-100/70">After</p>
              <p className="mt-2 text-sm font-semibold leading-7 text-white">
                Developed and optimized backend APIs to improve application performance, data flow, and system reliability.
              </p>
            </div>
          </div>
        </article>

        <article className="signal-panel rounded-[2rem] p-6 sm:p-7">
          <p className="eyebrow">Skills needing stronger proof</p>
          <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">Show evidence, not only keywords.</h2>
          <div className="mt-4 grid gap-3">
            {weakProof.length ? (
              weakProof.map((item) => (
                <div key={`${item.skill}-${item.share}`} className="soft-card rounded-[1.2rem] p-4">
                  <p className="font-bold text-ink dark:text-slate-100">{item.skill}</p>
                  <p className="mt-1 text-sm font-semibold leading-6 text-slate-700 dark:text-slate-300">
                    Add a project, outcome, or concrete usage example so this skill feels proven.
                  </p>
                </div>
              ))
            ) : (
              <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
                The main recommendations above are more important than weak-proof cleanup for this run.
              </p>
            )}
          </div>
        </article>
      </section>

      <section className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="eyebrow">Save report</p>
            <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              {user ? "Your signed-in account can keep reports and comparisons available later." : "Create a free account to save this report and track future improvements."}
            </p>
          </div>
          <Link className="ghost-button min-h-[3.35rem] px-6" to={user ? "/reports" : "/login"}>
            {user ? "Open Saved Reports" : "Save Report"}
          </Link>
        </div>
      </section>
    </div>
  );
}
