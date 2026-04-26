import { Link } from "react-router-dom";
import { AuthPanel } from "../components/AuthPanel";
import type { User } from "../lib/types";

type LoginPageProps = {
  user: User | null;
  busy: boolean;
  error: string | null;
  onRegister: (payload: { email: string; fullName: string; password: string }) => Promise<void>;
  onLogin: (payload: { email: string; password: string }) => Promise<void>;
  onResetPassword: (payload: { email: string; fullName: string; newPassword: string }) => Promise<void>;
  onLogout: () => void;
  onClearError: () => void;
};

export function LoginPage({ user, busy, error, onRegister, onLogin, onResetPassword, onLogout, onClearError }: LoginPageProps) {
  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Optional account</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              Save and Track Your Resume Progress
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Create an account to save your analysis reports, compare resume versions, and track improvements over time.
            </p>
          </div>
          <Link className="primary-button" to="/upload">
            Continue Without Account
          </Link>
        </div>
      </section>

      <AuthPanel
        user={user}
        onRegister={onRegister}
        onLogin={onLogin}
        onResetPassword={onResetPassword}
        onLogout={onLogout}
        busy={busy}
        error={error}
        onClearError={onClearError}
      />

      <section className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
        <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
          You can analyze your resume without signing up. An account is only required if you want to save your reports or access them later.
        </p>
      </section>
    </div>
  );
}
