import { FormEvent, useState } from "react";
import type { User } from "../lib/types";

type AuthPanelProps = {
  user: User | null;
  onRegister: (payload: { email: string; fullName: string; password: string }) => Promise<void>;
  onLogin: (payload: { email: string; password: string }) => Promise<void>;
  onResetPassword: (payload: { email: string; fullName: string; newPassword: string }) => Promise<void>;
  onLogout: () => void;
  busy?: boolean;
  error?: string | null;
  onClearError?: () => void;
};

export function AuthPanel({ user, onRegister, onLogin, onResetPassword, onLogout, busy = false, error, onClearError }: AuthPanelProps) {
  const [mode, setMode] = useState<"login" | "register" | "reset">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [fullName, setFullName] = useState("");
  const [localError, setLocalError] = useState<string | null>(null);
  const visibleError = localError ?? error ?? null;

  function switchMode(nextMode: "login" | "register" | "reset") {
    setMode(nextMode);
    setPassword("");
    setLocalError(null);
    onClearError?.();
  }

  function validateForm() {
    if (!email.trim()) {
      return "Enter your email before continuing.";
    }
    if ((mode === "register" || mode === "reset") && fullName.trim().length < 2) {
      return mode === "reset" ? "Enter the full name used when you registered." : "Enter your full name.";
    }
    if (password.length < 8) {
      return mode === "reset" ? "Choose a new password with at least 8 characters." : "Password must be at least 8 characters.";
    }
    return null;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const validationError = validateForm();
    if (validationError) {
      setLocalError(validationError);
      onClearError?.();
      return;
    }
    setLocalError(null);
    if (mode === "register") {
      await onRegister({ email, fullName, password });
    } else if (mode === "reset") {
      await onResetPassword({ email, fullName, newPassword: password });
    } else {
      await onLogin({ email, password });
    }
    setPassword("");
    if (mode !== "login") {
      setFullName("");
    }
  }

  if (user) {
    return (
      <section className="signal-panel rounded-[2rem] p-5 sm:p-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="eyebrow">Account saved</p>
            <h3 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{user.full_name}</h3>
            <p className="mt-1 text-sm font-semibold text-slate-700 dark:text-slate-300">{user.email}</p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <span className="pill">Review history on</span>
            <button className="ghost-button" onClick={onLogout} type="button">
              Log out
            </button>
          </div>
        </div>
      </section>
    );
  }

  const actionLabel = busy
    ? mode === "register"
      ? "Creating account..."
      : mode === "reset"
        ? "Resetting password..."
        : "Signing in..."
    : mode === "register"
      ? "Create account"
      : mode === "reset"
        ? "Reset password"
        : "Log in";

  return (
    <section className="signal-panel rounded-[2rem] p-5 sm:p-6">
      <div className="grid gap-6 lg:grid-cols-[0.85fr_1.15fr] lg:items-start">
        <div>
          <p className="eyebrow">Account</p>
          <h3 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">Save reviews and track improvements.</h3>
          <p className="mt-3 text-sm leading-6 text-slate-700 dark:text-slate-300">
            Create an account to save resume reviews, compare versions, and track progress over time.
          </p>
        </div>

        <div>
          {mode === "reset" ? (
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <span className="pill">Password recovery</span>
              <button className="ghost-button" onClick={() => switchMode("login")} type="button">
                Back to log in
              </button>
            </div>
          ) : (
            <div className="mb-4 inline-flex rounded-full border border-slate-900/10 bg-white/55 p-1 text-sm font-extrabold shadow-sm dark:border-white/10 dark:bg-slate-950/45">
              <button
                className={`rounded-full px-5 py-2 transition ${mode === "login" ? "bg-slate-950 text-white shadow-sm dark:border dark:border-cyan-300/20 dark:bg-cyan-300/10 dark:text-cyan-50" : "text-slate-700 hover:text-ink dark:text-slate-300 dark:hover:text-slate-50"}`}
                onClick={() => switchMode("login")}
                type="button"
              >
                Log in
              </button>
              <button
                className={`rounded-full px-5 py-2 transition ${mode === "register" ? "bg-slate-950 text-white shadow-sm dark:border dark:border-cyan-300/20 dark:bg-cyan-300/10 dark:text-cyan-50" : "text-slate-700 hover:text-ink dark:text-slate-300 dark:hover:text-slate-50"}`}
                onClick={() => switchMode("register")}
                type="button"
              >
                Create account
              </button>
            </div>
          )}

          {visibleError ? (
            <div
              className="mb-4 rounded-[1.1rem] border border-red-300/70 bg-red-50 px-4 py-3 text-sm font-semibold leading-6 text-red-800 shadow-sm dark:border-red-400/30 dark:bg-red-500/10 dark:text-red-100"
              role="alert"
              aria-live="assertive"
            >
              {visibleError}
            </div>
          ) : null}

          {busy ? (
            <div
              className="mb-4 rounded-[1.1rem] border border-cyan-300/40 bg-cyan-50 px-4 py-3 text-sm font-semibold leading-6 text-cyan-900 shadow-sm dark:border-cyan-300/20 dark:bg-cyan-300/10 dark:text-cyan-50"
              role="status"
              aria-live="polite"
            >
              Secure account service is responding. The first request can take a little longer if the backend is waking up.
            </div>
          ) : null}

          <form className="grid gap-3 md:grid-cols-3" onSubmit={handleSubmit}>
            {mode === "register" || mode === "reset" ? (
              <input
                className="field-control"
                placeholder={mode === "reset" ? "Registered full name" : "Full name"}
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                required
                minLength={2}
              />
            ) : null}
            <input
              className="field-control"
              placeholder="Email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
            <input
              className="field-control"
              placeholder={mode === "reset" ? "New password" : "Password"}
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={8}
            />
            <button className="primary-button" type="submit" disabled={busy}>
              {actionLabel}
            </button>
          </form>

          {mode === "reset" ? (
            <p className="mt-4 text-sm leading-6 text-slate-700 dark:text-slate-300">
              Enter your registered details to set a new password for this account.
            </p>
          ) : (
            <div className="mt-4 flex justify-end">
              <button className="text-sm font-extrabold text-cyan-700 transition hover:text-ink dark:text-cyan-200 dark:hover:text-slate-50" onClick={() => switchMode("reset")} type="button">
                Forgot password?
              </button>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
