import { Link } from "react-router-dom";

const HOW_IT_WORKS = [
  ["Upload", "Add a PDF or DOCX resume securely. No account is required to start."],
  ["Analyze", "Review ATS compatibility, keyword strength, clarity, skills, and role fit."],
  ["Improve", "Use prioritized recommendations to make your resume sharper before applying."],
];

const BENEFITS = [
  "Identify weak sections, missing keywords, and formatting issues.",
  "Understand how your resume compares with target-role expectations.",
  "Strengthen bullet points with measurable achievements and clearer impact.",
  "Keep login optional unless you want saved reports and version history.",
];

export function HomePage() {
  return (
    <div className="grid gap-6 md:gap-8">
      <section className="glass-panel relative overflow-hidden rounded-[2rem] p-6 sm:p-8 lg:p-12">
        <div className="absolute -left-24 top-8 h-72 w-72 rounded-full bg-blue-500/10 blur-3xl" />
        <div className="absolute -right-24 bottom-0 h-80 w-80 rounded-full bg-cyan-300/10 blur-3xl" />
        <div className="relative grid gap-8 xl:grid-cols-[1.05fr_0.95fr] xl:items-center">
          <div>
            <p className="eyebrow">AI-powered career feedback</p>
            <h1 className="mt-5 max-w-5xl font-display text-[clamp(2.7rem,6vw,6rem)] font-extrabold leading-[0.92] tracking-[-0.065em] text-ink dark:text-slate-50">
              Improve Your Resume with AI-Powered Career Feedback
            </h1>
            <p className="mt-6 max-w-3xl text-base leading-8 text-slate-700 sm:text-lg dark:text-slate-200">
              Upload your resume and receive instant insights on ATS compatibility, formatting, keyword strength, clarity, and overall impact.
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

          <div className="ink-panel rounded-[1.8rem] p-5 sm:p-6">
            <div className="flex items-center justify-between gap-4">
              <p className="font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/65">Resume quality scan</p>
              <span className="rounded-full border border-emerald-300/15 bg-emerald-300/10 px-3 py-1 text-[11px] font-black uppercase tracking-[0.18em] text-emerald-100">
                No login needed
              </span>
            </div>
            <div className="mt-6 grid gap-3 sm:grid-cols-2">
              {["ATS Optimization", "Keyword Strength", "Formatting Quality", "Recruiter Readability"].map((item) => (
                <div key={item} className="rounded-[1.2rem] border border-white/10 bg-slate-950/35 p-4">
                  <div className="flex items-center justify-between">
                    <p className="font-semibold text-white">{item}</p>
                    <span className="h-2 w-2 rounded-full bg-cyan-300 shadow-[0_0_18px_rgba(103,232,249,0.45)]" />
                  </div>
                  <p className="mt-2 text-sm leading-6 text-white/70">Clear signals for practical resume improvement.</p>
                </div>
              ))}
            </div>
            <div className="mt-5 rounded-[1.25rem] border border-cyan-300/15 bg-slate-950/45 p-4">
              <p className="font-display text-2xl font-extrabold tracking-[-0.04em] text-white">Feedback designed for modern hiring systems.</p>
              <p className="mt-2 text-sm font-semibold leading-6 text-slate-300">
                Get practical, easy-to-understand resume feedback designed around ATS best practices, recruiter expectations, and role-specific market signals.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section id="how-it-works" className="grid gap-4 lg:grid-cols-3">
        {HOW_IT_WORKS.map(([title, detail], index) => (
          <article key={title} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
            <p className="font-mono text-xs font-bold uppercase tracking-[0.22em] text-slate-500 dark:text-slate-400">0{index + 1}</p>
            <h2 className="mt-4 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">{title}</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{detail}</p>
          </article>
        ))}
      </section>

      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="grid gap-8 lg:grid-cols-[0.85fr_1.15fr] lg:items-start">
          <div>
            <p className="eyebrow">Why it helps</p>
            <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
              Clear feedback before you apply.
            </h2>
            <p className="mt-4 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Identify weak sections, missing keywords, formatting issues, and opportunities to make your resume more impactful.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {BENEFITS.map((benefit) => (
              <div key={benefit} className="soft-card rounded-[1.35rem] p-4">
                <p className="text-sm font-bold leading-7 text-slate-700 dark:text-slate-300">{benefit}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="signal-panel rounded-[2rem] p-6 sm:p-8">
          <p className="eyebrow">ATS optimization</p>
          <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Built for readability and screening systems.</h2>
          <p className="mt-4 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            Find formatting and structure issues that may affect automated screening, then improve section clarity and keyword alignment for your target role.
          </p>
        </article>
        <article className="signal-panel rounded-[2rem] p-6 sm:p-8">
          <p className="eyebrow">Privacy first</p>
          <h2 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Your resume is treated with care.</h2>
          <p className="mt-4 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
            Your resume is processed to generate personalized feedback. Creating an account is optional and only needed when you want to save reports.
          </p>
        </article>
      </section>
    </div>
  );
}
