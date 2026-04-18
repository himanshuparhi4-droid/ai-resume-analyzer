import type { RecommendationItem } from "../lib/types";

type SuggestionsPanelProps = {
  recommendations: RecommendationItem[];
  aiSummary: {
    mode?: string;
    strengths?: string[];
    weaknesses?: string[];
    next_steps?: string[] | Record<string, string> | string;
  };
  resumePreview: string;
};

export function SuggestionsPanel({ recommendations, aiSummary, resumePreview }: SuggestionsPanelProps) {
  const strengths = Array.isArray(aiSummary.strengths) ? aiSummary.strengths : [];
  const weaknesses = Array.isArray(aiSummary.weaknesses) ? aiSummary.weaknesses : [];
  const nextSteps = Array.isArray(aiSummary.next_steps)
    ? aiSummary.next_steps
    : typeof aiSummary.next_steps === "string"
      ? [aiSummary.next_steps]
      : aiSummary.next_steps && typeof aiSummary.next_steps === "object"
        ? Object.values(aiSummary.next_steps)
        : [];

  return (
    <section className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
      <div className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft transition-colors duration-300 md:p-8 dark:border-white/10 dark:bg-white/[0.04]">
        <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-400">Recommendations</p>
        <h3 className="mt-2 font-display text-3xl text-ink dark:text-slate-50">Priority actions to improve this resume</h3>
        <div className="mt-6 grid gap-4">
          {recommendations.map((item) => (
            <article key={item.title} className="rounded-[1.5rem] bg-mist p-5 transition-colors duration-300 dark:bg-white/[0.03]">
              <div className="flex items-center justify-between gap-3">
                <h4 className="font-semibold text-ink dark:text-slate-100">{item.title}</h4>
                <span className="rounded-full bg-ink px-3 py-1 text-xs uppercase tracking-[0.2em] text-white dark:bg-sea dark:text-ink">{item.impact}</span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-700 dark:text-slate-300">{item.detail}</p>
            </article>
          ))}
        </div>
      </div>
      <div className="grid gap-4">
        <div className="rounded-[2rem] bg-gradient-to-br from-ink to-[#143446] p-6 text-white shadow-soft md:p-8 dark:from-[#071923] dark:to-[#133242]">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-white/55">Analysis Summary</p>
          <div className="mt-5 grid gap-4 text-sm leading-6 text-white/85">
            <div>
              <p className="font-semibold text-white">Strengths</p>
              <ul className="mt-2 list-disc pl-5">
                {strengths.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
            <div>
              <p className="font-semibold text-white">Weaknesses</p>
              <ul className="mt-2 list-disc pl-5">
                {weaknesses.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
            <div>
              <p className="font-semibold text-white">Next Steps</p>
              <ul className="mt-2 list-disc pl-5">
                {nextSteps.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          </div>
        </div>
        <div className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft transition-colors duration-300 md:p-8 dark:border-white/10 dark:bg-white/[0.04]">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-400">Parsed Resume Snapshot</p>
          <p className="mt-4 text-sm leading-7 text-slate-700 dark:text-slate-300">{resumePreview}</p>
        </div>
      </div>
    </section>
  );
}
