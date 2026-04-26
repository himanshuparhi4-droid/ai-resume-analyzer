import type { ReactNode } from "react";
import { Link, NavLink } from "react-router-dom";
import type { User } from "../lib/types";
import { Seo } from "./Seo";

type SiteShellProps = {
  children: ReactNode;
  theme: "light" | "dark";
  onToggleTheme: () => void;
  user: User | null;
  onLogout: () => void;
};

function ThemeGlyph({ theme }: { theme: "light" | "dark" }) {
  if (theme === "dark") {
    return (
      <svg aria-hidden="true" className="h-4 w-4" fill="none" viewBox="0 0 24 24">
        <path d="M20 15.2A8.8 8.8 0 0 1 8.8 4a8.8 8.8 0 1 0 11.2 11.2Z" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
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

const NAV_ITEMS = [
  { label: "Home", to: "/" },
  { label: "Upload Resume", to: "/upload" },
  { label: "Dashboard", to: "/dashboard" },
  { label: "Feedback", to: "/feedback" },
  { label: "Suggestions", to: "/suggestions" },
  { label: "Pricing", to: "/pricing" },
  { label: "About", to: "/about" },
];

export function SiteShell({ children, theme, onToggleTheme, user, onLogout }: SiteShellProps) {
  return (
    <main className="min-h-screen bg-transparent text-ink transition-colors duration-300 dark:text-slate-100">
      <Seo />
      <div className="studio-shell">
        <nav className="glass-panel sticky top-3 z-30 rounded-2xl px-5 py-4 backdrop-blur-xl">
          <div className="flex flex-wrap items-center justify-between gap-4 xl:flex-nowrap">
            <Link className="flex shrink-0 items-center gap-3" to="/">
              <span className="grid h-12 w-12 place-items-center rounded-2xl border border-white/10 bg-slate-950 text-base font-black text-cyan-100 dark:bg-cyan-300/10">
                AI
              </span>
              <span>
                <span className="block font-display text-xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">Resume Intelligence</span>
                <span className="hidden text-xs font-bold uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400 sm:block">Career feedback studio</span>
              </span>
            </Link>

            <div className="flex min-w-0 flex-wrap items-center justify-end gap-2 xl:flex-nowrap">
              <div className="hidden flex-wrap items-center gap-1 lg:flex">
                {NAV_ITEMS.map((item) => (
                  <NavLink
                    key={item.to}
                    className={({ isActive }) =>
                      `rounded-xl px-3 py-2.5 text-sm font-extrabold transition ${
                        isActive ? "bg-slate-950 text-white dark:bg-cyan-300/15 dark:text-cyan-50" : "text-slate-700 hover:bg-white/60 dark:text-slate-300 dark:hover:bg-white/[0.06]"
                      }`
                    }
                    to={item.to}
                  >
                    {item.label}
                  </NavLink>
                ))}
              </div>

              <button className="ghost-button" onClick={onToggleTheme} type="button">
                <ThemeGlyph theme={theme} />
                <span className="ml-2 hidden sm:inline">{theme === "dark" ? "Light" : "Dark"} Mode</span>
              </button>

              {user ? (
                <>
                  <Link className="ghost-button" to="/reports">
                    Reports
                  </Link>
                  <button className="ghost-button" onClick={onLogout} type="button">
                    Log out
                  </button>
                </>
              ) : (
                <Link className="ghost-button" to="/login">
                  Sign In
                </Link>
              )}

              <Link className="h-12 shrink-0 rounded-xl bg-cyan-300 px-5 py-3 text-sm font-extrabold uppercase tracking-[0.12em] text-slate-950 shadow-xl shadow-cyan-950/20 transition hover:-translate-y-0.5 hover:bg-cyan-200" to="/upload">
                Analyze My Resume
              </Link>
            </div>
          </div>

          <div className="mt-3 flex gap-2 overflow-x-auto pb-1 lg:hidden">
            {NAV_ITEMS.map((item) => (
              <NavLink
                key={item.to}
                className={({ isActive }) =>
                  `shrink-0 rounded-full px-3 py-2 text-xs font-extrabold transition ${
                    isActive ? "bg-slate-950 text-white dark:bg-cyan-300/15 dark:text-cyan-50" : "bg-white/40 text-slate-700 dark:bg-white/[0.04] dark:text-slate-300"
                  }`
                }
                to={item.to}
              >
                {item.label}
              </NavLink>
            ))}
          </div>
        </nav>

        {children}

        <footer className="rounded-[1.5rem] border border-slate-900/10 bg-white/45 p-5 text-sm font-semibold text-slate-600 dark:border-white/10 dark:bg-white/[0.03] dark:text-slate-400">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p>Resume Intelligence Studio helps candidates improve resume clarity, ATS readability, and role alignment.</p>
            <Link className="font-extrabold text-cyan-700 dark:text-cyan-200" to="/upload">
              Start a resume analysis
            </Link>
          </div>
        </footer>
      </div>
    </main>
  );
}
