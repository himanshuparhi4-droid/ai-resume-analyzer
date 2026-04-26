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
  resumeSections?: Record<string, string>;
};

function impactClass(impact: string) {
  const normalized = impact.toLowerCase();
  if (normalized.includes("high")) {
    return "bg-ember text-white";
  }
  if (normalized.includes("medium")) {
    return "bg-gold text-ink";
  }
  return "bg-sea text-ink";
}

const SECTION_LABELS: Record<string, string> = {
  summary: "Summary",
  experience: "Experience",
  projects: "Projects",
  education: "Education",
  skills: "Skills",
  certifications: "Certifications",
  research: "Research",
  publications: "Publications",
  teaching: "Teaching",
  awards: "Awards",
  languages: "Languages",
  interests: "Interests",
};

const SECTION_ORDER = [
  "summary",
  "experience",
  "projects",
  "education",
  "skills",
  "certifications",
  "research",
  "publications",
  "teaching",
  "awards",
  "languages",
  "interests",
];

export function SuggestionsPanel({ recommendations, aiSummary, resumePreview, resumeSections = {} }: SuggestionsPanelProps) {
  const strengths = Array.isArray(aiSummary.strengths) ? aiSummary.strengths : [];
  const weaknesses = Array.isArray(aiSummary.weaknesses) ? aiSummary.weaknesses : [];
  const nextSteps = Array.isArray(aiSummary.next_steps)
    ? aiSummary.next_steps
    : typeof aiSummary.next_steps === "string"
      ? [aiSummary.next_steps]
      : aiSummary.next_steps && typeof aiSummary.next_steps === "object"
        ? Object.values(aiSummary.next_steps)
        : [];
  const parsedSections = [
    ...SECTION_ORDER.filter((key) => resumeSections[key]),
    ...Object.keys(resumeSections).filter((key) => !SECTION_ORDER.includes(key) && resumeSections[key]),
  ];

  return (
    <section className="grid gap-5 xl:grid-cols-[1.05fr_0.95fr]">
      <div className="glass-panel rounded-[2.25rem] p-5 sm:p-7">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="eyebrow">Action Queue</p>
            <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Highest-leverage fixes</h3>
          </div>
          <span className="pill">Ranked by impact</span>
        </div>
        <div className="mt-6 grid gap-4">
          {recommendations.map((item, index) => (
            <article key={`${item.title}-${index}`} className="signal-panel rounded-[1.65rem] p-5">
              <div className="flex items-start gap-4">
                <div className="grid h-11 w-11 shrink-0 place-items-center rounded-2xl bg-ink font-display text-xl font-extrabold text-white dark:bg-sea dark:text-ink">
                  {index + 1}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <h4 className="font-display text-2xl font-extrabold tracking-[-0.04em] text-ink dark:text-slate-50">{item.title}</h4>
                    <span className={`rounded-full px-3 py-1 text-xs font-black uppercase tracking-[0.18em] ${impactClass(item.impact)}`}>{item.impact}</span>
                  </div>
                  <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{item.detail}</p>
                </div>
              </div>
            </article>
          ))}
        </div>
      </div>

      <div className="grid gap-5">
        <div className="ink-panel rounded-[2.25rem] p-5 sm:p-7">
          <p className="font-mono text-xs font-bold uppercase tracking-[0.34em] text-white/65">Recruiter-Style Summary</p>
          <div className="mt-6 grid gap-5 text-sm leading-7 text-white/84">
            <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
              <p className="font-display text-2xl font-extrabold tracking-[-0.04em] text-white">What works</p>
              <div className="mt-3 grid gap-2">
                {(strengths.length ? strengths : ["No strong strengths were returned for this run yet."]).map((item) => (
                  <p key={item} className="border-l-2 border-sea pl-3">{item}</p>
                ))}
              </div>
            </div>
            <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
              <p className="font-display text-2xl font-extrabold tracking-[-0.04em] text-white">What blocks it</p>
              <div className="mt-3 grid gap-2">
                {(weaknesses.length ? weaknesses : ["No major weaknesses were returned for this run yet."]).map((item) => (
                  <p key={item} className="border-l-2 border-ember pl-3">{item}</p>
                ))}
              </div>
            </div>
            <div className="rounded-[1.35rem] border border-white/10 bg-white/[0.07] p-4">
              <p className="font-display text-2xl font-extrabold tracking-[-0.04em] text-white">Next moves</p>
              <div className="mt-3 grid gap-2">
                {(nextSteps.length ? nextSteps : ["Run another analysis after updating the resume."]).map((item) => (
                  <p key={item} className="border-l-2 border-gold pl-3">{item}</p>
                ))}
              </div>
            </div>
          </div>
        </div>
        <div className="signal-panel rounded-[2rem] p-5 sm:p-6">
          <p className="eyebrow">Parsed Snapshot</p>
          <div className="mt-4 max-h-72 overflow-auto rounded-[1.25rem] bg-white/45 p-4 text-sm font-semibold leading-7 text-slate-700 dark:bg-white/[0.05] dark:text-slate-300">
            {parsedSections.length ? (
              <div className="grid gap-4">
                {parsedSections.map((key) => (
                  <div key={key} className="rounded-[1rem] border border-slate-200/70 bg-white/45 p-3 dark:border-white/10 dark:bg-white/[0.04]">
                    <p className="text-[10px] font-black uppercase tracking-[0.24em] text-slate-500 dark:text-slate-400">
                      {SECTION_LABELS[key] ?? key.replace(/_/g, " ")}
                    </p>
                    <p className="mt-2">{resumeSections[key]}</p>
                  </div>
                ))}
              </div>
            ) : (
              resumePreview
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
