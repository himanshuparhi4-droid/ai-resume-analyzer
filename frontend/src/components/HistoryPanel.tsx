import type { ComparisonResponse, HistoryItem } from "../lib/types";
import { getBackendRoot } from "../api/client";

type HistoryPanelProps = {
  history: HistoryItem[];
  comparison: ComparisonResponse | null;
  comparisonError?: string | null;
  onCompare: (currentId: string) => Promise<void>;
};

function formatAnalysisTime(value: string) {
  if (!value) {
    return "Time unavailable";
  }
  const normalized = /z$|[+-]\d{2}:\d{2}$/i.test(value) ? value : `${value}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(parsed);
}

function normalizeRole(value: string) {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function hasComparableVersion(history: HistoryItem[], item: HistoryItem) {
  const role = normalizeRole(item.role_query);
  return history.some((other) => other.analysis_id !== item.analysis_id && normalizeRole(other.role_query) === role);
}

export function HistoryPanel({ history, comparison, comparisonError, onCompare }: HistoryPanelProps) {
  const root = getBackendRoot();
  return (
    <section className="glass-panel rounded-[2.25rem] p-5 sm:p-7">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">History</p>
          <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Saved analyses and version comparisons</h3>
        </div>
      </div>
      <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
        <div className="grid gap-3">
          {history.length ? (
            history.map((item) => {
              const canCompare = hasComparableVersion(history, item);
              return (
                <article key={item.analysis_id} className="soft-card rounded-[1.5rem] p-5">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <h4 className="font-semibold text-ink dark:text-slate-100">{item.role_query}</h4>
                      <p className="text-sm text-slate-700 dark:text-slate-200">Score {Math.round(item.overall_score)} - {formatAnalysisTime(item.created_at)}</p>
                      {!canCompare ? (
                        <p className="mt-1 text-xs font-semibold text-slate-500 dark:text-slate-400">Run another saved analysis for this role to enable comparison.</p>
                      ) : null}
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <a
                        className="rounded-full border border-ink/15 px-3 py-2 text-xs font-semibold text-ink transition hover:border-sea hover:bg-sea/10 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
                        href={`${root}/api/v1/reports/analyses/${item.analysis_id}.pdf`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        PDF
                      </a>
                      {item.share_token ? (
                        <a
                          className="rounded-full border border-ink/15 px-3 py-2 text-xs font-semibold text-ink transition hover:border-sea hover:bg-sea/10 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
                          href={`${root}/api/v1/public/analyses/${item.share_token}`}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Share
                        </a>
                      ) : null}
                      <button
                        className="rounded-full bg-ink px-3 py-2 text-xs font-semibold text-white transition disabled:cursor-not-allowed disabled:opacity-45 dark:bg-sea dark:text-ink"
                        disabled={!canCompare}
                        onClick={() => onCompare(item.analysis_id)}
                        title="Compare needs at least two saved reports for the same target role."
                        type="button"
                      >
                        Compare
                      </button>
                    </div>
                  </div>
                </article>
              );
            })
          ) : (
            <p className="text-sm leading-6 text-slate-700 dark:text-slate-200">Login and run analyses to build your score history.</p>
          )}
        </div>
        <div className="ink-panel rounded-[1.75rem] p-5 text-white sm:p-6">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-white/75">Comparison</p>
          {comparisonError ? (
            <div className="mt-4 rounded-[1.1rem] border border-red-300/30 bg-red-400/10 p-4 text-sm leading-6 text-red-50">
              {comparisonError}
            </div>
          ) : comparison ? (
            <div className="mt-4 grid gap-4 text-sm leading-6 text-white/90">
              <p>{comparison.summary}</p>
              <p>Score delta: {comparison.score_delta > 0 ? "+" : ""}{comparison.score_delta}</p>
              <div className="grid gap-2">
                {Object.entries(comparison.component_deltas).map(([key, value]) => (
                  <div key={key} className="flex items-center justify-between border-b border-white/10 pb-2">
                    <span>{key}</span>
                    <span>{value > 0 ? "+" : ""}{value}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="mt-4 text-sm leading-6 text-white/85">Choose a saved analysis that has another saved version for the same target role.</p>
          )}
        </div>
      </div>
    </section>
  );
}
