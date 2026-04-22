import { FormEvent, useState } from "react";
import type { User } from "../lib/types";

type AuthPanelProps = {
  user: User | null;
  onRegister: (payload: { email: string; fullName: string; password: string }) => Promise<void>;
  onLogin: (payload: { email: string; password: string }) => Promise<void>;
  onResetPassword: (payload: { email: string; fullName: string; newPassword: string }) => Promise<void>;
  onLogout: () => void;
  busy?: boolean;
};

export function AuthPanel({ user, onRegister, onLogin, onResetPassword, onLogout, busy = false }: AuthPanelProps) {
  const [mode, setMode] = useState<"login" | "register" | "reset">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [fullName, setFullName] = useState("");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
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
      <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft transition-colors duration-300 md:p-8 dark:border-[#223543] dark:bg-[#10202b]">
        <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Account</p>
        <div className="mt-3 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h3 className="font-display text-2xl text-ink dark:text-slate-50">{user.full_name}</h3>
            <p className="text-sm text-slate-700 dark:text-slate-200">{user.email}</p>
          </div>
          <button
            className="rounded-full border border-ink/15 px-4 py-2 text-sm font-semibold text-ink transition hover:border-sea hover:bg-sea/10 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
            onClick={onLogout}
            type="button"
          >
            Logout
          </button>
        </div>
      </section>
    );
  }

  return (
    <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft transition-colors duration-300 md:p-8 dark:border-[#223543] dark:bg-[#10202b]">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Account</p>
          <h3 className="mt-2 font-display text-2xl text-ink dark:text-slate-50">Save analyses and compare revisions</h3>
        </div>
        {mode === "reset" ? (
          <button
            className="rounded-full border border-ink/10 px-4 py-2 text-sm font-semibold text-ink transition hover:border-sea hover:bg-sea/10 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
            onClick={() => setMode("login")}
            type="button"
          >
            Back to login
          </button>
        ) : (
          <div className="flex gap-2 rounded-full bg-mist p-1 text-sm font-semibold transition-colors duration-300 dark:bg-[#132531]">
            <button
              className={`rounded-full px-4 py-2 transition ${
                mode === "login" ? "bg-ink text-white dark:bg-sea dark:text-ink" : "text-ink dark:text-slate-100"
              }`}
              onClick={() => setMode("login")}
              type="button"
            >
              Login
            </button>
            <button
              className={`rounded-full px-4 py-2 transition ${
                mode === "register" ? "bg-ink text-white dark:bg-sea dark:text-ink" : "text-ink dark:text-slate-100"
              }`}
              onClick={() => setMode("register")}
              type="button"
            >
              Register
            </button>
          </div>
        )}
      </div>
      <form className="mt-5 grid gap-4 md:grid-cols-3" onSubmit={handleSubmit}>
        {mode === "register" || mode === "reset" ? (
          <input
            className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 text-ink transition-colors duration-300 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-50 dark:placeholder:text-slate-400"
            placeholder={mode === "reset" ? "Full name used at registration" : "Full name"}
            value={fullName}
            onChange={(e) => setFullName(e.target.value)}
          />
        ) : null}
        <input
          className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 text-ink transition-colors duration-300 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-50 dark:placeholder:text-slate-400"
          placeholder="Email"
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
        <input
          className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 text-ink transition-colors duration-300 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-50 dark:placeholder:text-slate-400"
          placeholder={mode === "reset" ? "New password" : "Password"}
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <button
          className="rounded-full bg-ink px-5 py-3 font-semibold text-white transition disabled:opacity-60 dark:bg-sea dark:text-ink dark:hover:bg-[#81ddd3]"
          type="submit"
          disabled={busy}
        >
          {busy ? "Please wait..." : mode === "register" ? "Create account" : mode === "reset" ? "Reset password" : "Login"}
        </button>
      </form>
      {mode === "reset" ? (
        <p className="mt-4 text-sm leading-6 text-slate-700 dark:text-slate-300">
          This deployment does not send email reset links yet. Recovery checks your email and full name on this deployment,
          then signs you back in with the new password.
        </p>
      ) : (
        <div className="mt-4 flex justify-end">
          <button
            className="text-sm font-semibold text-sea transition hover:text-ink dark:hover:text-slate-50"
            onClick={() => setMode("reset")}
            type="button"
          >
            Forgot password?
          </button>
        </div>
      )}
    </section>
  );
}
