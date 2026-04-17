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
    <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft md:p-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">History</p>
          <h3 className="mt-2 font-display text-3xl text-ink">Saved analyses and version comparisons</h3>
        </div>
      </div>
      <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
        <div className="grid gap-3">
          {history.length ? history.map((item) => (
            <article key={item.analysis_id} className="rounded-[1.5rem] bg-mist p-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h4 className="font-semibold text-ink">{item.role_query}</h4>
                  <p className="text-sm text-slate-700">Score {Math.round(item.overall_score)} • {formatAnalysisTime(item.created_at)}</p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <a className="rounded-full border border-ink/15 px-3 py-2 text-xs font-semibold text-ink" href={`${root}/api/v1/reports/analyses/${item.analysis_id}.pdf`} target="_blank" rel="noreferrer">PDF</a>
                  {item.share_token ? <a className="rounded-full border border-ink/15 px-3 py-2 text-xs font-semibold text-ink" href={`${root}/api/v1/public/analyses/${item.share_token}`} target="_blank" rel="noreferrer">Share</a> : null}
                  <button className="rounded-full bg-ink px-3 py-2 text-xs font-semibold text-white" onClick={() => onCompare(item.analysis_id)} type="button">Compare</button>
                </div>
              </div>
            </article>
          )) : <p className="text-sm leading-6 text-slate-700">Login and run analyses to build your score history.</p>}
        </div>
        <div className="rounded-[1.5rem] bg-ink p-6 text-white">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-white/60">Comparison</p>
          {comparison ? (
            <div className="mt-4 grid gap-4 text-sm leading-6 text-white/85">
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
          ) : <p className="mt-4 text-sm leading-6 text-white/75">Choose any saved analysis to compare it against your previous version for the same role.</p>}
        </div>
      </div>
    </section>
  );
}
