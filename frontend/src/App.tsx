import { useEffect, useRef, useState } from "react";
import { analyzeResume, compareAnalyses, getHistory, login, register, resetPassword, setAuthToken } from "./api/client";
import { AuthPanel } from "./components/AuthPanel";
import { Header } from "./components/Header";
import { HistoryPanel } from "./components/HistoryPanel";
import { JobMatchesTable } from "./components/JobMatchesTable";
import { ScoreGrid } from "./components/ScoreGrid";
import { SkillGapChart } from "./components/SkillGapChart";
import { SuggestionsPanel } from "./components/SuggestionsPanel";
import { UploadPanel, type UploadInput } from "./components/UploadPanel";
import { formatApiErrorDetail } from "./lib/text";
import type { AnalysisResponse, ComparisonResponse, HistoryItem, User } from "./lib/types";

const TOKEN_KEY = "resume-analyzer-token";
const USER_KEY = "resume-analyzer-user";
const THEME_KEY = "resume-analyzer-theme";

type ThemeMode = "light" | "dark";

function getBackendErrorMessage(err: any, fallback: string): string {
  const backendDetail = formatApiErrorDetail(err?.response?.data?.detail);
  if (backendDetail) {
    return backendDetail;
  }
  if (err?.message === "BACKEND_WAKE_TIMEOUT") {
    return "The backend is still waking up. Wait about 30 to 60 seconds, then try again.";
  }
  if (err?.code === "ECONNABORTED") {
    return "The backend took too long to respond. If Render is waking the service up, wait about 30 to 60 seconds and try again.";
  }
  if (err?.message === "Network Error" || err?.code === "ERR_NETWORK" || err?.request) {
    return "The browser did not get a response from the backend. If you opened a Vercel preview URL, switch to the main site or wait for the backend to wake up, then retry.";
  }
  return fallback;
}

function App() {
  const [result, setResult] = useState<AnalysisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [authLoading, setAuthLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);
  const [theme, setTheme] = useState<ThemeMode>("dark");
  const analyzeRequestId = useRef(0);

  useEffect(() => {
    const savedToken = localStorage.getItem(TOKEN_KEY);
    const savedUser = localStorage.getItem(USER_KEY);
    const savedTheme = localStorage.getItem(THEME_KEY) as ThemeMode | null;
    if (savedToken) {
      setAuthToken(savedToken);
    }
    if (savedUser) {
      setUser(JSON.parse(savedUser));
    }
    if (savedTheme === "light" || savedTheme === "dark") {
      setTheme(savedTheme);
      return;
    }
    if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
      setTheme("dark");
    }
  }, []);

  useEffect(() => {
    if (user) {
      void refreshHistory();
    }
  }, [user]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    document.documentElement.style.colorScheme = theme;
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  async function refreshHistory() {
    try {
      setHistory(await getHistory());
    } catch {
      setHistory([]);
    }
  }

  async function handleAnalyze(payload: UploadInput) {
    const requestId = ++analyzeRequestId.current;
    try {
      setLoading(true);
      setAnalysisError(null);
      setComparison(null);
      setResult(null);
      const data = await analyzeResume(payload);
      if (requestId !== analyzeRequestId.current) {
        return;
      }
      setResult(data);
      if (user) {
        await refreshHistory();
      }
    } catch (err: any) {
      if (requestId !== analyzeRequestId.current) {
        return;
      }
      setAnalysisError(getBackendErrorMessage(err, "Analysis failed before the app received a usable response. Retry once after the backend is fully up."));
    } finally {
      if (requestId === analyzeRequestId.current) {
        setLoading(false);
      }
    }
  }

  async function handleRegister(payload: { email: string; fullName: string; password: string }) {
    try {
      setAuthLoading(true);
      setAuthError(null);
      const data = await register(payload);
      setAuthToken(data.access_token);
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      setAuthError(null);
      setUser(data.user);
    } catch (err: any) {
      setAuthError(getBackendErrorMessage(err, "Registration failed. Try again with a fresh email."));
    } finally {
      setAuthLoading(false);
    }
  }

  async function handleLogin(payload: { email: string; password: string }) {
    try {
      setAuthLoading(true);
      setAuthError(null);
      const data = await login(payload);
      setAuthToken(data.access_token);
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      setAuthError(null);
      setUser(data.user);
    } catch (err: any) {
      setAuthError(
        getBackendErrorMessage(
          err,
          "Login failed. Check your email and password. If this deployment says the email already exists, use Forgot password to reset it here."
        )
      );
    } finally {
      setAuthLoading(false);
    }
  }

  async function handleResetPassword(payload: { email: string; fullName: string; newPassword: string }) {
    try {
      setAuthLoading(true);
      setAuthError(null);
      const data = await resetPassword(payload);
      setAuthToken(data.access_token);
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      setAuthError(null);
      setUser(data.user);
    } catch (err: any) {
      setAuthError(
        getBackendErrorMessage(
          err,
          "Password reset failed. Double-check the email and full name used when you registered on this deployment."
        )
      );
    } finally {
      setAuthLoading(false);
    }
  }

  function handleLogout() {
    setAuthToken(null);
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
    setUser(null);
    setHistory([]);
    setComparison(null);
    setAuthError(null);
  }

  async function handleCompare(currentId: string) {
    try {
      setComparison(await compareAnalyses(currentId));
    } catch {
      setAnalysisError("Could not compare analyses yet. Run at least two saved analyses for the same role.");
    }
  }

  return (
    <main className="min-h-screen bg-transparent text-ink transition-colors duration-300 dark:text-slate-100">
      <div className="studio-shell">
        <Header theme={theme} onToggleTheme={() => setTheme((current) => (current === "light" ? "dark" : "light"))} />
        <AuthPanel
          user={user}
          onRegister={handleRegister}
          onLogin={handleLogin}
          onResetPassword={handleResetPassword}
          onLogout={handleLogout}
          busy={authLoading}
          error={authError}
          onClearError={() => setAuthError(null)}
        />
        <UploadPanel loading={loading} onSubmit={handleAnalyze} />
        {analysisError ? (
          <div className="rounded-[1.4rem] border border-red-300/70 bg-red-50 p-4 text-sm font-semibold leading-6 text-red-800 shadow-soft transition-colors duration-300 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-100">
            {analysisError}
          </div>
        ) : null}
        {result ? (
          <div className="grid gap-5 pb-8 md:gap-7">
            {result.analysis_context?.message ? (
              <div className="rounded-[1.5rem] border border-amber-200 bg-amber-50/90 p-4 text-sm font-semibold leading-6 text-amber-950 shadow-soft transition-colors duration-300 dark:border-amber-400/30 dark:bg-amber-400/10 dark:text-amber-100">
                <span className="font-semibold">Review context:</span> {result.analysis_context.message}
              </div>
            ) : null}
            <section className="glass-panel rounded-[2rem] p-5 sm:p-6">
              <div className="flex flex-wrap items-center justify-between gap-5">
                <div>
                  <p className="eyebrow">Review complete</p>
                  <h2 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
                    {result.role_query} resume review
                  </h2>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div className="soft-card rounded-[1rem] px-4 py-3">
                    <p className="font-display text-2xl font-extrabold text-ink dark:text-slate-50">{Math.round(result.overall_score)}</p>
                    <p className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400">Score</p>
                  </div>
                  <div className="soft-card rounded-[1rem] px-4 py-3">
                    <p className="font-display text-2xl font-extrabold text-ink dark:text-slate-50">{result.analysis_context?.live_job_count ?? result.top_job_matches.filter((job) => job.source !== "role-baseline").length}</p>
                    <p className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400">Jobs</p>
                  </div>
                  <div className="soft-card rounded-[1rem] px-4 py-3">
                    <p className="font-display text-2xl font-extrabold text-ink dark:text-slate-50">{result.missing_skills.length}</p>
                    <p className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400">Gaps</p>
                  </div>
                </div>
              </div>
            </section>
            <ScoreGrid
              overallScore={result.overall_score}
              breakdown={result.breakdown}
              roleQuery={result.role_query}
              resumeArchetype={result.resume_archetype}
              parserConfidence={result.analysis_context?.parser_confidence}
              componentFeedback={result.component_feedback ?? {}}
            />
            <SkillGapChart
              missingSkills={result.missing_skills}
              matchedSkills={result.matched_skills}
              matchedSkillDetails={result.matched_skill_details ?? []}
              missingSkillDetails={result.missing_skill_details ?? []}
              weakSkillProofs={result.weak_skill_proofs ?? []}
              weakSkillProofDetails={result.weak_skill_proof_details ?? []}
              detectedSkills={result.analysis_context?.parsed_resume_skills ?? []}
            />
            <SuggestionsPanel recommendations={result.recommendations} aiSummary={result.ai_summary} resumePreview={result.resume_preview} />
            <JobMatchesTable jobs={result.top_job_matches} analysisContext={result.analysis_context} />
          </div>
        ) : (
          <section className="glass-panel rounded-[2rem] p-8 text-center">
            <p className="eyebrow">Ready when you are</p>
            <p className="mt-3 font-display text-4xl font-extrabold tracking-[-0.05em] text-ink transition-colors duration-300 dark:text-slate-50">Your resume review will appear here.</p>
            <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 transition-colors duration-300 dark:text-slate-300">
              Upload a resume and choose a target role to see ATS readability, skill gaps, role fit, job evidence, and recommended improvements.
            </p>
          </section>
        )}
        <HistoryPanel history={history} comparison={comparison} onCompare={handleCompare} />
      </div>
    </main>
  );
}

export default App;
