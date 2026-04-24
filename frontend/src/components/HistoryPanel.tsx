import type { ComparisonResponse, HistoryItem } from "../lib/types";
import { getBackendRoot } from "../api/client";

type HistoryPanelProps = {
  history: HistoryItem[];
  comparison: ComparisonResponse | null;
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

export function HistoryPanel({ history, comparison, onCompare }: HistoryPanelProps) {
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
          {history.length ? history.map((item) => (
            <article key={item.analysis_id} className="soft-card rounded-[1.5rem] p-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h4 className="font-semibold text-ink dark:text-slate-100">{item.role_query}</h4>
                  <p className="text-sm text-slate-700 dark:text-slate-200">Score {Math.round(item.overall_score)} • {formatAnalysisTime(item.created_at)}</p>
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
                  <button className="rounded-full bg-ink px-3 py-2 text-xs font-semibold text-white transition dark:bg-sea dark:text-ink" onClick={() => onCompare(item.analysis_id)} type="button">Compare</button>
                </div>
              </div>
            </article>
          )) : <p className="text-sm leading-6 text-slate-700 dark:text-slate-200">Login and run analyses to build your score history.</p>}
        </div>
        <div className="ink-panel rounded-[1.75rem] p-5 text-white sm:p-6">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-white/75">Comparison</p>
          {comparison ? (
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
          ) : <p className="mt-4 text-sm leading-6 text-white/85">Choose any saved analysis to compare it against your previous version for the same role.</p>}
        </div>
      </div>
    </section>
  );
}
