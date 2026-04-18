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

export function Header({ theme, onToggleTheme }: HeaderProps) {
  return (
    <header className="relative overflow-hidden rounded-[2rem] border border-white/70 bg-white/80 p-6 shadow-soft backdrop-blur md:p-10 dark:border-[#223543] dark:bg-[#10202b]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,_rgba(94,194,183,0.24),_transparent_36%),radial-gradient(circle_at_bottom_right,_rgba(255,138,91,0.18),_transparent_30%)] dark:bg-[radial-gradient(circle_at_top_left,_rgba(94,194,183,0.14),_transparent_28%),radial-gradient(circle_at_bottom_right,_rgba(255,138,91,0.1),_transparent_24%)]" />
      <div className="relative">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Resume Intelligence Studio</p>
          <button
            className="inline-flex items-center gap-2 rounded-full border border-ink/10 bg-white/75 px-4 py-2 text-sm font-semibold text-ink shadow-sm transition hover:border-sea hover:text-ink dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
            onClick={onToggleTheme}
            type="button"
          >
            <ThemeGlyph theme={theme} />
            <span>{theme === "dark" ? "Light mode" : "Dark mode"}</span>
          </button>
        </div>
        <div className="mt-8 grid gap-8 lg:grid-cols-[minmax(0,1.15fr)_minmax(300px,0.85fr)] lg:items-end">
          <div className="space-y-5">
            <div className="space-y-3">
              <h1 className="max-w-4xl font-display text-4xl leading-tight text-ink md:text-6xl dark:text-slate-50">
                Evaluate resume fit against live hiring signals, not just static keyword checks.
              </h1>
              <p className="max-w-2xl text-base leading-7 text-slate-700 md:text-lg dark:text-slate-200">
                Upload a resume, compare it against live job requirements for a target role, and get evidence-backed feedback on missing tools,
                ATS readability, and experience strength.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <span className="rounded-full border border-ink/10 bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-200">
                Live market sample first
              </span>
              <span className="rounded-full border border-ink/10 bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-200">
                Location-aware role targeting
              </span>
              <span className="rounded-full border border-ink/10 bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-200">
                Evidence-backed recommendations
              </span>
            </div>
          </div>
          <div className="grid gap-4 rounded-[1.75rem] bg-ink p-5 text-white dark:border dark:border-[#2a4150] dark:bg-[#0b1821]">
            <div className="flex items-center justify-between gap-4">
              <p className="font-mono text-xs uppercase tracking-[0.3em] text-white/75">What this review covers</p>
              <span className="rounded-full bg-white/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-white/90">
                Review stack
              </span>
            </div>
            <div className="grid gap-3 text-sm leading-6 text-white/90">
              <span>Live job-market sampling and requirement extraction</span>
              <span>Role fit across skills, semantics, experience, and demand</span>
              <span>ATS parseability and document-quality feedback</span>
              <span>Evidence snippets showing why a skill was matched or flagged</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
