type HeaderProps = {
  theme: "light" | "dark";
  onToggleTheme: () => void;
};

function ThemeGlyph({ theme }: { theme: "light" | "dark" }) {
  if (theme === "dark") {
    return (
      <svg aria-hidden="true" className="h-4 w-4" fill="none" viewBox="0 0 24 24">
        <path
          d="M20 15.2A8.8 8.8 0 0 1 8.8 4a8.8 8.8 0 1 0 11.2 11.2Z"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="1.8"
        />
      </svg>
    );
  }

  return (
    <svg aria-hidden="true" className="h-4 w-4" fill="none" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="4" stroke="currentColor" strokeWidth="1.8" />
      <path
        d="M12 2.5v2.2M12 19.3v2.2M21.5 12h-2.2M4.7 12H2.5M18.7 5.3l-1.6 1.6M6.9 17.1l-1.6 1.6M18.7 18.7l-1.6-1.6M6.9 6.9 5.3 5.3"
        stroke="currentColor"
        strokeLinecap="round"
        strokeWidth="1.8"
      />
    </svg>
  );
}

const REVIEW_STAGES = [
  { label: "Upload", value: "Read structure and content" },
  { label: "Compare", value: "Map against role expectations" },
  { label: "Improve", value: "Prioritize evidence-based fixes" },
];

export function Header({ theme, onToggleTheme }: HeaderProps) {
  return (
    <header className="glass-panel relative overflow-hidden rounded-[2rem] p-5 sm:p-7 lg:p-8">
      <div className="absolute inset-0 opacity-75">
        <div className="absolute -left-24 top-0 h-64 w-64 rounded-full bg-blue-500/10 blur-3xl" />
        <div className="absolute -right-24 bottom-0 h-72 w-72 rounded-full bg-cyan-300/10 blur-3xl" />
      </div>

      <div className="relative z-10">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="grid h-11 w-11 place-items-center rounded-2xl border border-white/10 bg-slate-950 text-sm font-black text-cyan-100 shadow-soft dark:bg-cyan-300/10 dark:text-cyan-100">
              AI
            </div>
            <div>
              <p className="eyebrow">Resume Signal Studio</p>
              <p className="mt-1 text-sm font-semibold text-slate-700 dark:text-slate-200">AI-powered resume analysis for stronger job applications.</p>
            </div>
          </div>
          <button className="ghost-button" onClick={onToggleTheme} type="button">
            <ThemeGlyph theme={theme} />
            <span className="ml-2">{theme === "dark" ? "Light" : "Dark"} Mode</span>
          </button>
        </div>

        <div className="mt-8 grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(360px,0.95fr)] xl:items-stretch">
          <div>
            <div className="inline-flex rounded-full border border-slate-900/10 bg-white/60 p-1 text-xs font-extrabold uppercase tracking-[0.16em] text-slate-800 backdrop-blur dark:border-white/10 dark:bg-slate-950/45 dark:text-slate-100">
              <span className="rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-cyan-950 dark:text-cyan-100">Resume review</span>
              <span className="px-3 py-1 text-slate-600 dark:text-slate-300">Market-informed feedback</span>
            </div>
            <h1 className="mt-5 max-w-4xl font-display text-[clamp(2.35rem,5vw,4.8rem)] font-extrabold leading-[0.93] tracking-[-0.06em] text-ink dark:text-slate-50">
              Resume feedback that stays tied to real hiring signals.
            </h1>
            <p className="mt-5 max-w-3xl text-base leading-8 text-slate-700 dark:text-slate-200">
              Review ATS readability, role alignment, skill evidence, and market fit in one focused workflow before you apply.
            </p>
            <div className="mt-6 grid gap-3 sm:grid-cols-3">
              {REVIEW_STAGES.map((stage, index) => (
                <div key={stage.label} className="soft-card rounded-[1.2rem] p-4 transition duration-200 hover:-translate-y-0.5 hover:border-cyan-300/30">
                  <p className="font-mono text-[11px] font-bold uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">0{index + 1}</p>
                  <p className="mt-2 font-display text-lg font-bold text-ink dark:text-slate-50">{stage.label}</p>
                  <p className="mt-1 text-sm leading-5 text-slate-700 dark:text-slate-300">{stage.value}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="ink-panel relative overflow-hidden rounded-[1.75rem] border border-cyan-300/10 p-5 sm:p-6">
            <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan-300/45 to-transparent" />
            <div className="absolute right-6 top-6 h-24 w-24 rounded-full border border-white/5" />
            <div className="absolute right-12 top-12 h-12 w-12 rounded-full border border-cyan-200/10" />
            <div className="relative">
              <div className="flex items-center justify-between gap-4">
                <p className="font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/65">Review dashboard</p>
                <span className="rounded-full border border-emerald-300/15 bg-emerald-300/10 px-3 py-1 text-[11px] font-black uppercase tracking-[0.18em] text-emerald-100">Live analysis</span>
              </div>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                {[
                  ["Resume Parsing", "Checks whether your resume is structured clearly and readable by screening systems."],
                  ["Skill Evidence", "Identifies which skills are clearly proven, weakly supported, or missing."],
                  ["Market Alignment", "Compares your profile with role requirements and current job listing patterns."],
                  ["ATS Readability", "Highlights formatting and structure issues that may affect automated screening."],
                ].map(([title, detail]) => (
                  <div key={title} className="rounded-[1.1rem] border border-white/10 bg-slate-950/30 p-4 backdrop-blur transition duration-200 hover:border-cyan-300/20 hover:bg-slate-900/40">
                    <div className="flex items-center justify-between gap-3">
                      <p className="font-semibold text-white">{title}</p>
                      <span className="h-2 w-2 rounded-full bg-cyan-300 shadow-[0_0_18px_rgba(103,232,249,0.45)]" />
                    </div>
                    <p className="mt-1 text-xs leading-5 text-white/70">{detail}</p>
                  </div>
                ))}
              </div>
              <div className="mt-4 rounded-[1.15rem] border border-cyan-300/15 bg-slate-950/45 p-4 text-slate-100 shadow-[0_18px_46px_rgba(2,6,23,0.22)]">
                <p className="font-display text-xl font-extrabold tracking-[-0.03em]">Evidence-based resume feedback</p>
                <p className="mt-1 text-sm font-semibold leading-6 text-slate-300">Recommendations stay grounded in resume content, target-role expectations, and market signals.</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
