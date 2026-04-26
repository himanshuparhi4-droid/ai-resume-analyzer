import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Page not found</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">This page does not exist.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Return home or start a new resume analysis.
      </p>
      <div className="mt-6 flex flex-wrap justify-center gap-3">
        <Link className="primary-button" to="/upload">
          Analyze My Resume
        </Link>
        <Link className="ghost-button min-h-[3.35rem] px-6" to="/">
          Go Home
        </Link>
      </div>
    </section>
  );
}
