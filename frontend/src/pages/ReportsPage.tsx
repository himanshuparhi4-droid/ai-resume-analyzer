import { Link } from "react-router-dom";
import { HistoryPanel } from "../components/HistoryPanel";
import type { ComparisonResponse, HistoryItem, User } from "../lib/types";

type ReportsPageProps = {
  user: User | null;
  history: HistoryItem[];
  comparison: ComparisonResponse | null;
  comparisonError?: string | null;
  onCompare: (currentId: string) => Promise<void>;
};

export function ReportsPage({ user, history, comparison, comparisonError, onCompare }: ReportsPageProps) {
  if (!user) {
    return (
      <section className="glass-panel rounded-[2rem] p-8 text-center">
        <p className="eyebrow">Saved reports</p>
        <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Sign in to view saved reports.</h1>
        <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
          Sign in to view saved reports, or continue analyzing resumes without an account.
        </p>
        <div className="mt-6 flex flex-wrap justify-center gap-3">
          <Link className="primary-button" to="/login">
            Sign In
          </Link>
          <Link className="ghost-button min-h-[3.35rem] px-6" to="/upload">
            Continue Without Account
          </Link>
        </div>
      </section>
    );
  }

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <p className="eyebrow">Saved reports</p>
        <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
          Resume history and comparisons
        </h1>
        <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
          Review your past resume analyses, track improvements, and compare how your resume has changed over time.
        </p>
      </section>
      <HistoryPanel history={history} comparison={comparison} comparisonError={comparisonError} onCompare={onCompare} />
    </div>
  );
}
