import axios from "axios";
import type { AnalysisResponse, AuthResponse, ComparisonResponse, HistoryItem } from "../lib/types";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";
const PUBLIC_ROOT = API_BASE.replace(/\/api\/v1$/, "");
const AUTH_TIMEOUT_MS = 30_000;
const DEFAULT_TIMEOUT_MS = 20_000;
const ANALYSIS_TIMEOUT_MS = 180_000;
const HEALTH_TIMEOUT_MS = 12_000;
const HEALTH_RETRIES = 6;
const HEALTH_DELAY_MS = 5_000;
const HEALTH_FRESHNESS_MS = 120_000;

export const api = axios.create({ baseURL: API_BASE, timeout: DEFAULT_TIMEOUT_MS });
let lastHealthyAt = 0;

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function shouldWakeRetry(error: unknown) {
  const err = error as any;
  return err?.code === "ERR_NETWORK" || err?.code === "ECONNABORTED" || (!!err?.request && !err?.response);
}

async function waitForBackendAwake() {
  for (let attempt = 0; attempt < HEALTH_RETRIES; attempt += 1) {
    try {
      await axios.get(`${PUBLIC_ROOT}/healthz`, { timeout: HEALTH_TIMEOUT_MS });
      lastHealthyAt = Date.now();
      return;
    } catch {
      if (attempt === HEALTH_RETRIES - 1) {
        throw new Error("BACKEND_WAKE_TIMEOUT");
      }
      await delay(HEALTH_DELAY_MS);
    }
  }
}

async function ensureBackendReady() {
  if ((Date.now() - lastHealthyAt) < HEALTH_FRESHNESS_MS) {
    return;
  }
  await waitForBackendAwake();
}

async function withWakeRetry<T>(request: () => Promise<T>) {
  try {
    return await request();
  } catch (error) {
    if (!shouldWakeRetry(error)) {
      throw error;
    }
    await waitForBackendAwake();
    return request();
  }
}

export function setAuthToken(token: string | null) {
  if (token) {
    api.defaults.headers.common.Authorization = `Bearer ${token}`;
  } else {
    delete api.defaults.headers.common.Authorization;
  }
}

export function getBackendRoot() {
  return PUBLIC_ROOT;
}

async function fileToBase64(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;
  let binary = "";
  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, index + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
}

export async function register(input: { email: string; fullName: string; password: string }): Promise<AuthResponse> {
  const { data } = await withWakeRetry(() =>
    api.post<AuthResponse>("/auth/register", {
      email: input.email,
      full_name: input.fullName,
      password: input.password
    }, {
      timeout: AUTH_TIMEOUT_MS
    })
  );
  return data;
}

export async function login(input: { email: string; password: string }): Promise<AuthResponse> {
  const { data } = await withWakeRetry(() =>
    api.post<AuthResponse>("/auth/login", input, {
      timeout: AUTH_TIMEOUT_MS
    })
  );
  return data;
}

export async function getHistory(): Promise<HistoryItem[]> {
  const { data } = await withWakeRetry(() =>
    api.get<HistoryItem[]>("/users/me/analyses", {
      timeout: DEFAULT_TIMEOUT_MS
    })
  );
  return data;
}

export async function compareAnalyses(currentId: string, previousId?: string): Promise<ComparisonResponse> {
  const { data } = await withWakeRetry(() =>
    api.get<ComparisonResponse>("/users/me/analyses/compare", {
      params: { current_id: currentId, previous_id: previousId },
      timeout: DEFAULT_TIMEOUT_MS
    })
  );
  return data;
}

export async function analyzeResume(input: { file: File; roleQuery: string; location: string; limit: number }): Promise<AnalysisResponse> {
  await ensureBackendReady();
  const fileBase64 = await fileToBase64(input.file);
  const { data } = await withWakeRetry(() =>
    api.post<AnalysisResponse>("/analyses/resume-json", {
      filename: input.file.name,
      content_type: input.file.type || "application/octet-stream",
      file_base64: fileBase64,
      role_query: input.roleQuery,
      location: input.location,
      limit: input.limit
    }, {
      timeout: ANALYSIS_TIMEOUT_MS
    })
  );
  return data;
}
