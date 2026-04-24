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
  { label: "Parse", value: "layout + sections" },
  { label: "Fetch", value: "live role market" },
  { label: "Judge", value: "evidence + confidence" },
];

export function Header({ theme, onToggleTheme }: HeaderProps) {
  return (
    <header className="glass-panel relative overflow-hidden rounded-[2.25rem] p-5 sm:p-7 lg:p-10">
      <div className="absolute inset-0 opacity-80">
        <div className="absolute -left-24 top-10 h-64 w-64 rounded-full bg-sea/25 blur-3xl" />
        <div className="absolute -right-16 bottom-0 h-72 w-72 rounded-full bg-ember/20 blur-3xl" />
      </div>

      <div className="relative z-10">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="grid h-11 w-11 place-items-center rounded-2xl bg-ink text-sm font-black text-white shadow-soft dark:bg-sea dark:text-ink">
              AI
            </div>
            <div>
              <p className="eyebrow">Resume Signal Studio</p>
              <p className="mt-1 text-sm font-semibold text-slate-700 dark:text-slate-200">Live market fit, parser confidence, recruiter clarity</p>
            </div>
          </div>
          <button className="ghost-button" onClick={onToggleTheme} type="button">
            <ThemeGlyph theme={theme} />
            <span className="ml-2">{theme === "dark" ? "Light" : "Dark"} Mode</span>
          </button>
        </div>

        <div className="mt-10 grid gap-8 xl:grid-cols-[minmax(0,1.08fr)_minmax(360px,0.92fr)] xl:items-end">
          <div>
            <div className="inline-flex rounded-full border border-ink/10 bg-white/55 p-1 text-xs font-extrabold uppercase tracking-[0.16em] text-ink backdrop-blur dark:border-white/10 dark:bg-white/10 dark:text-slate-100">
              <span className="rounded-full bg-gradient-to-r from-gold via-ember to-sea px-3 py-1 text-ink shadow-[0_10px_28px_rgba(255,88,63,0.22)]">Evidence first</span>
              <span className="px-3 py-1">15-20 listing target</span>
            </div>
            <h1 className="mt-6 max-w-5xl font-display text-[clamp(2.7rem,7vw,6.6rem)] font-extrabold leading-[0.88] tracking-[-0.065em] text-ink dark:text-slate-50">
              Turn a resume into a hiring signal map.
            </h1>
            <p className="mt-6 max-w-3xl text-base leading-8 text-slate-700 sm:text-lg dark:text-slate-200">
              Upload once and see the role fit story the way a serious reviewer would: what parsed cleanly, what the market asked for, what is truly missing, and what simply needs stronger proof.
            </p>
            <div className="mt-7 grid gap-3 sm:grid-cols-3">
              {REVIEW_STAGES.map((stage, index) => (
                <div key={stage.label} className="soft-card rounded-[1.35rem] p-4">
                  <p className="font-mono text-xs font-bold uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">0{index + 1}</p>
                  <p className="mt-3 font-display text-xl font-bold text-ink dark:text-slate-50">{stage.label}</p>
                  <p className="mt-1 text-sm text-slate-700 dark:text-slate-300">{stage.value}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="ink-panel relative overflow-hidden rounded-[2rem] p-5 sm:p-6">
            <div className="absolute right-6 top-6 h-24 w-24 rounded-full border border-white/10" />
            <div className="absolute right-12 top-12 h-12 w-12 rounded-full border border-white/10" />
            <div className="relative">
              <div className="flex items-center justify-between gap-4">
                <p className="font-mono text-xs font-bold uppercase tracking-[0.28em] text-white/65">Review Engine</p>
                <span className="rounded-full bg-white/10 px-3 py-1 text-[11px] font-black uppercase tracking-[0.18em] text-white/85">Production path</span>
              </div>
              <div className="mt-7 grid gap-3">
                {[
                  ["Skill grounding", "present vs weak proof vs absent"],
                  ["Provider telemetry", "live count, sources, timeout risk"],
                  ["ATS readability", "parse risk separate from score"],
                  ["Recruiter lens", "prioritized actions, not generic advice"],
                ].map(([title, detail]) => (
                  <div key={title} className="rounded-[1.2rem] border border-white/10 bg-white/[0.07] p-4 backdrop-blur">
                    <div className="flex items-center justify-between gap-3">
                      <p className="font-semibold text-white">{title}</p>
                      <span className="h-2.5 w-2.5 rounded-full bg-sea shadow-[0_0_24px_rgba(102,216,205,0.9)]" />
                    </div>
                    <p className="mt-1 text-sm leading-6 text-white/72">{detail}</p>
                  </div>
                ))}
              </div>
              <div className="mt-5 rounded-[1.2rem] bg-gradient-to-br from-gold via-ember to-sea p-4 text-ink shadow-[0_18px_46px_rgba(255,88,63,0.26)]">
                <p className="font-display text-2xl font-extrabold tracking-[-0.03em]">Confidence-aware by design</p>
                <p className="mt-1 text-sm font-semibold leading-6 opacity-85">Dense searches now aim for a richer 15-20 listing market view, while still labeling limited provider runs honestly.</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
