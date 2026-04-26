import { Link } from "react-router-dom";

const POINTS = [
  ["What the tool does", "It reviews a resume for clarity, role alignment, ATS readability, keyword relevance, and recruiter readability."],
  ["How analysis works", "The report combines resume parsing, scoring signals, job-market context, and practical recommendations."],
  ["Privacy note", "Your resume is used to generate feedback. Accounts are optional and only needed for saving reports."],
  ["Career-focused feedback", "Suggestions are written to help users improve proof, wording, formatting, and role-specific positioning."],
];

export function AboutPage() {
  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <p className="eyebrow">About</p>
        <h1 className="mt-3 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
          Resume analysis built for practical improvement.
        </h1>
        <p className="mt-4 max-w-3xl text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
          Resume Intelligence helps candidates understand what is working, what is weak, and what to improve before applying.
        </p>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        {POINTS.map(([title, detail]) => (
          <article key={title} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
            <h2 className="font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">{title}</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{detail}</p>
          </article>
        ))}
      </section>

      <section className="signal-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="eyebrow">Start</p>
            <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Upload a resume without signing in. Create an account only when you want saved reports or version comparison.
            </p>
          </div>
          <Link className="primary-button" to="/upload">
            Analyze My Resume
          </Link>
        </div>
      </section>
    </div>
  );
}
