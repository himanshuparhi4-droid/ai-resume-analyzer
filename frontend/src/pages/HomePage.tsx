import { Link } from "react-router-dom";

const STEPS = [
  ["Upload", "Add your resume and choose the role you want to target."],
  ["Analyze", "Review ATS readability, keywords, formatting, skills, and role fit."],
  ["Improve", "Use focused recommendations to fix the highest-impact issues first."],
];

const FEEDBACK = [
  "Resume structure and formatting clarity",
  "ATS compatibility and readable section signals",
  "Keyword relevance for the target role",
  "Recruiter readability and impact-focused language",
];

export function HomePage() {
  return (
    <div className="grid gap-6 md:gap-8">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8 lg:p-10">
        <div className="grid gap-8 lg:grid-cols-[1.05fr_0.95fr] lg:items-center">
          <div>
            <p className="eyebrow">AI resume analysis</p>
            <h1 className="mt-4 max-w-4xl font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-6xl dark:text-slate-50">
              Improve your resume with clear, AI-powered feedback.
            </h1>
            <p className="mt-5 max-w-2xl text-base font-semibold leading-8 text-slate-700 md:text-lg dark:text-slate-300">
              Upload your resume and get practical insights on structure, clarity, ATS compatibility, keyword relevance, and recruiter readability.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link className="primary-button" to="/upload">
                Analyze My Resume
              </Link>
              <a className="ghost-button min-h-[3.35rem] px-6" href="#how-it-works">
                View How It Works
              </a>
            </div>
          </div>

          <div className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
            <p className="eyebrow">What feedback you get</p>
            <div className="mt-5 grid gap-3">
              {FEEDBACK.map((item) => (
                <div key={item} className="flex items-start gap-3 rounded-[1.1rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                  <span className="mt-1 h-2.5 w-2.5 rounded-full bg-cyan-400" />
                  <p className="text-sm font-bold leading-6 text-slate-700 dark:text-slate-300">{item}</p>
                </div>
              ))}
            </div>
            <div className="mt-5 rounded-[1.2rem] border border-cyan-300/20 bg-cyan-300/10 p-4">
              <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-200">
                Login is optional. You only need an account if you want to save reports, compare versions, or view history later.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section id="how-it-works" className="grid gap-4 lg:grid-cols-3">
        {STEPS.map(([title, detail], index) => (
          <article key={title} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
            <p className="eyebrow">0{index + 1}</p>
            <h2 className="mt-3 font-display text-3xl font-extrabold tracking-[-0.045em] text-ink dark:text-slate-50">{title}</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{detail}</p>
          </article>
        ))}
      </section>

      <section className="grid gap-4 lg:grid-cols-[1fr_0.9fr]">
        <article className="signal-panel rounded-[2rem] p-6 sm:p-8">
          <p className="eyebrow">Product focus</p>
          <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
            See what to fix before you apply.
          </h2>
          <p className="mt-4 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            The analysis highlights the resume sections that need clearer evidence, stronger keywords, cleaner formatting, or better role alignment.
          </p>
        </article>
        <article className="signal-panel rounded-[2rem] p-6 sm:p-8">
          <p className="eyebrow">Privacy note</p>
          <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
            Your resume stays part of the analysis workflow.
          </h2>
          <p className="mt-4 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            Your resume is processed to generate personalized feedback. Saving reports is optional and only happens when you create an account.
          </p>
        </article>
      </section>
    </div>
  );
}
