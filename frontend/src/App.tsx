import { useEffect, useRef, useState } from "react";
import { analyzeResume, compareAnalyses, getHistory, login, register, setAuthToken } from "./api/client";
import { AuthPanel } from "./components/AuthPanel";
import { Header } from "./components/Header";
import { HistoryPanel } from "./components/HistoryPanel";
import { JobMatchesTable } from "./components/JobMatchesTable";
import { ScoreGrid } from "./components/ScoreGrid";
import { SkillGapChart } from "./components/SkillGapChart";
import { SuggestionsPanel } from "./components/SuggestionsPanel";
import { UploadPanel, type UploadInput } from "./components/UploadPanel";
import type { AnalysisResponse, ComparisonResponse, HistoryItem, User } from "./lib/types";

const TOKEN_KEY = "resume-analyzer-token";
const USER_KEY = "resume-analyzer-user";

function getBackendErrorMessage(err: any, fallback: string): string {
  const backendDetail = err?.response?.data?.detail;
  if (backendDetail) {
    return backendDetail;
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
  const [error, setError] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);
  const analyzeRequestId = useRef(0);

  useEffect(() => {
    const savedToken = localStorage.getItem(TOKEN_KEY);
    const savedUser = localStorage.getItem(USER_KEY);
    if (savedToken) {
      setAuthToken(savedToken);
    }
    if (savedUser) {
      setUser(JSON.parse(savedUser));
    }
  }, []);

  useEffect(() => {
    if (user) {
      void refreshHistory();
    }
  }, [user]);

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
      setError(null);
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
      setError(getBackendErrorMessage(err, "Analysis failed before the app received a usable response. Retry once after the backend is fully up."));
    } finally {
      if (requestId === analyzeRequestId.current) {
        setLoading(false);
      }
    }
  }

  async function handleRegister(payload: { email: string; fullName: string; password: string }) {
    try {
      setAuthLoading(true);
      setError(null);
      const data = await register(payload);
      setAuthToken(data.access_token);
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      setUser(data.user);
    } catch (err: any) {
      setError(getBackendErrorMessage(err, "Registration failed. Try again with a fresh email."));
    } finally {
      setAuthLoading(false);
    }
  }

  async function handleLogin(payload: { email: string; password: string }) {
    try {
      setAuthLoading(true);
      setError(null);
      const data = await login(payload);
      setAuthToken(data.access_token);
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      setUser(data.user);
    } catch (err: any) {
      if (err?.response?.status === 401) {
        setError("Login failed on the hosted app. This deployment uses its own database, so if you have not created an account here yet, use Register first.");
      } else {
        setError(getBackendErrorMessage(err, "Login failed. Check your email and password."));
      }
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
  }

  async function handleCompare(currentId: string) {
    try {
      setComparison(await compareAnalyses(currentId));
    } catch {
      setError("Could not compare analyses yet. Run at least two saved analyses for the same role.");
    }
  }

  return (
    <main className="min-h-screen bg-mist text-ink">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-6 md:px-8 md:py-8">
        <Header />
        <AuthPanel user={user} onRegister={handleRegister} onLogin={handleLogin} onLogout={handleLogout} busy={authLoading} />
        <UploadPanel loading={loading} onSubmit={handleAnalyze} />
        {error ? <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div> : null}
        {result ? (
          <div className="grid gap-6 pb-8">
            {result.analysis_context?.message ? (
              <div className="rounded-[1.5rem] border border-amber-200 bg-amber-50 p-4 text-sm leading-6 text-amber-900">
                <span className="font-semibold">Scoring context:</span> {result.analysis_context.message}
              </div>
            ) : null}
            <ScoreGrid
              overallScore={result.overall_score}
              breakdown={result.breakdown}
              roleQuery={result.role_query}
              resumeArchetype={result.resume_archetype}
              componentFeedback={result.component_feedback ?? {}}
            />
            <SkillGapChart
              missingSkills={result.missing_skills}
              matchedSkills={result.matched_skills}
              matchedSkillDetails={result.matched_skill_details ?? []}
              missingSkillDetails={result.missing_skill_details ?? []}
            />
            <SuggestionsPanel recommendations={result.recommendations} aiSummary={result.ai_summary} resumePreview={result.resume_preview} />
            <JobMatchesTable jobs={result.top_job_matches} />
          </div>
        ) : (
          <section className="rounded-[2rem] border border-ink/10 bg-white p-8 text-center shadow-soft">
            <p className="font-display text-3xl text-ink">Your first review will populate this workspace.</p>
            <p className="mx-auto mt-3 max-w-2xl text-sm leading-7 text-slate-700">Upload a resume, choose a target role, and the app will compare the document against live market requirements, ATS signals, and role-fit evidence.</p>
          </section>
        )}
        <HistoryPanel history={history} comparison={comparison} onCompare={handleCompare} />
      </div>
    </main>
  );
}

export default App;
