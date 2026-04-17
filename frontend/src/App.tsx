import { useEffect, useState } from "react";
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

function App() {
  const [result, setResult] = useState<AnalysisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [authLoading, setAuthLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);

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
    try {
      setLoading(true);
      setError(null);
      const data = await analyzeResume(payload);
      setResult(data);
      if (user) {
        await refreshHistory();
      }
    } catch (err: any) {
      const backendDetail = err?.response?.data?.detail;
      if (backendDetail) {
        setError(backendDetail);
      } else if (err?.message === "Network Error" || err?.code === "ERR_NETWORK" || err?.request) {
        setError(
          "The browser did not get a response from the backend. Make sure the FastAPI server is still running, wait for any auto-reload to finish, then try again."
        );
      } else {
        setError("Analysis failed before the app received a usable response. Retry once after the backend is fully up.");
      }
    } finally {
      setLoading(false);
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
      setError(err?.response?.data?.detail ?? "Registration failed. Try again with a fresh email.");
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
      setError(err?.response?.data?.detail ?? "Login failed. Check your email and password.");
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
