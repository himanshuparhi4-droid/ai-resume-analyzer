import { Link } from "react-router-dom";

const TRUST_POINTS = [
  ["AI-powered resume analysis", "Review resume structure, keyword strength, role relevance, and clarity in one focused workflow."],
  ["ATS-focused recommendations", "Identify formatting and section issues that may reduce automated readability."],
  ["Privacy-first processing", "Your resume contains personal career information. We treat it with care and use it to generate your analysis."],
  ["Practical improvement suggestions", "Translate generic resume advice into clear, prioritized actions you can apply immediately."],
];

export function AboutPage() {
  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <p className="eyebrow">About and trust</p>
        <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
          Resume feedback built around modern hiring standards.
        </h1>
        <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
          Our platform analyzes resumes using modern hiring standards, ATS best practices, and recruiter-focused evaluation methods to provide practical, easy-to-understand feedback.
        </p>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        {TRUST_POINTS.map(([title, detail]) => (
          <article key={title} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
            <h2 className="font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">{title}</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{detail}</p>
          </article>
        ))}
      </section>

      <section className="ink-panel rounded-[2rem] p-6 sm:p-8">
        <p className="font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/65">Privacy promise</p>
        <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-white">Career data deserves careful handling.</h2>
        <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-white/76">
          Your resume contains personal career information. We treat it with care and only use it to generate your analysis and recommendations.
        </p>
        <Link className="primary-button mt-6" to="/upload">
          Analyze My Resume
        </Link>
      </section>
    </div>
  );
}
