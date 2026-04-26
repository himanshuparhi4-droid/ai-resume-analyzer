import type { AnalysisResponse } from "./types";

export const ANALYSIS_SESSION_KEY = "resume-analyzer-latest-analysis";

export function loadAnalysisFromSession(): AnalysisResponse | null {
  try {
    const stored = sessionStorage.getItem(ANALYSIS_SESSION_KEY);
    return stored ? (JSON.parse(stored) as AnalysisResponse) : null;
  } catch {
    return null;
  }
}

export function saveAnalysisToSession(result: AnalysisResponse) {
  sessionStorage.setItem(ANALYSIS_SESSION_KEY, JSON.stringify(result));
}

export function clearAnalysisSession() {
  sessionStorage.removeItem(ANALYSIS_SESSION_KEY);
}
