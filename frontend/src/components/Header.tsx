export function Header() {
  return (
    <header className="relative overflow-hidden rounded-[2rem] border border-white/70 bg-white/80 p-8 shadow-soft backdrop-blur md:p-12">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,_rgba(94,194,183,0.28),_transparent_40%),radial-gradient(circle_at_bottom_right,_rgba(255,138,91,0.2),_transparent_32%)]" />
      <div className="relative grid gap-8 md:grid-cols-[1.35fr_0.65fr] md:items-end">
        <div className="space-y-5">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">Resume Intelligence Studio</p>
          <div className="space-y-3">
            <h1 className="max-w-3xl font-display text-4xl leading-tight text-ink md:text-6xl">
              Evaluate resume fit against live hiring signals, not just static keyword checks.
            </h1>
            <p className="max-w-2xl text-base leading-7 text-slate-700 md:text-lg">
              Upload a resume, compare it against live job requirements for a target role, and get evidence-backed feedback on missing tools,
              ATS readability, and experience strength.
            </p>
          </div>
        </div>
        <div className="grid gap-3 rounded-[1.5rem] bg-ink p-5 text-white">
          <p className="font-mono text-xs uppercase tracking-[0.3em] text-white/60">What this review covers</p>
          <div className="grid gap-2 text-sm leading-6 text-white/85">
            <span>Live job-market sampling and requirement extraction</span>
            <span>Role fit across skills, semantics, experience, and demand</span>
            <span>ATS parseability and document-quality feedback</span>
            <span>Evidence snippets showing why a skill was matched or flagged</span>
          </div>
        </div>
      </div>
    </header>
  );
}
