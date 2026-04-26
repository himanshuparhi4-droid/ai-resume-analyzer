import { useEffect, useRef, useState } from "react";
import { BrowserRouter, Route, Routes, useNavigate } from "react-router-dom";
import { analyzeResume, compareAnalyses, getHistory, login, register, resetPassword, setAuthToken } from "./api/client";
import { SiteShell } from "./components/SiteShell";
import type { UploadInput } from "./components/UploadPanel";
import { clearAnalysisSession, loadAnalysisFromSession, saveAnalysisToSession } from "./lib/analysisSession";
import { formatApiErrorDetail } from "./lib/text";
import type { AnalysisResponse, ComparisonResponse, HistoryItem, User } from "./lib/types";
import { AboutPage } from "./pages/AboutPage";
import { DashboardPage } from "./pages/DashboardPage";
import { FeedbackPage } from "./pages/FeedbackPage";
import { HomePage } from "./pages/HomePage";
import { JobMatchesPage } from "./pages/JobMatchesPage";
import { LoginPage } from "./pages/LoginPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { PricingPage } from "./pages/PricingPage";
import { ReportsPage } from "./pages/ReportsPage";
import { SkillInsightsPage } from "./pages/SkillInsightsPage";
import { SuggestionsPage } from "./pages/SuggestionsPage";
import { UploadPage } from "./pages/UploadPage";

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

function AppRoutes() {
  const navigate = useNavigate();
  const [result, setResult] = useState<AnalysisResponse | null>(() => loadAnalysisFromSession());
  const [loading, setLoading] = useState(false);
  const [authLoading, setAuthLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);
  const [theme, setTheme] = useState<ThemeMode>("light");
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
      const data = await analyzeResume(payload);
      if (requestId !== analyzeRequestId.current) {
        return;
      }
      saveAnalysisToSession(data);
      setResult(data);
      if (user) {
        await refreshHistory();
      }
      navigate("/dashboard");
    } catch (err: any) {
      if (requestId !== analyzeRequestId.current) {
        return;
      }
      setAnalysisError(getBackendErrorMessage(err, "Analysis failed before the app received a usable response. Retry once after the backend is fully up."));
      navigate("/upload");
    } finally {
      if (requestId === analyzeRequestId.current) {
        setLoading(false);
      }
    }
  }

  function handleAnalyzeAnother() {
    clearAnalysisSession();
    setResult(null);
    setAnalysisError(null);
    setComparison(null);
    navigate("/upload");
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
      navigate("/upload");
    }
  }

  return (
    <SiteShell
      theme={theme}
      onToggleTheme={() => setTheme((current) => (current === "light" ? "dark" : "light"))}
      user={user}
      onLogout={handleLogout}
    >
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/upload" element={<UploadPage loading={loading} error={analysisError} onAnalyze={handleAnalyze} />} />
        <Route path="/dashboard" element={<DashboardPage result={result} />} />
        <Route path="/feedback" element={<FeedbackPage result={result} />} />
        <Route path="/suggestions" element={<SuggestionsPage result={result} user={user} onAnalyzeAnother={handleAnalyzeAnother} />} />
        <Route path="/skills" element={<SkillInsightsPage result={result} />} />
        <Route path="/jobs" element={<JobMatchesPage result={result} />} />
        <Route
          path="/login"
          element={
            <LoginPage
              user={user}
              busy={authLoading}
              error={authError}
              onRegister={handleRegister}
              onLogin={handleLogin}
              onResetPassword={handleResetPassword}
              onLogout={handleLogout}
              onClearError={() => setAuthError(null)}
            />
          }
        />
        <Route path="/reports" element={<ReportsPage user={user} history={history} comparison={comparison} onCompare={handleCompare} />} />
        <Route path="/pricing" element={<PricingPage />} />
        <Route path="/about" element={<AboutPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </SiteShell>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  );
}

export default App;
