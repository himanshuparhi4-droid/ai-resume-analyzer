import { Link } from "react-router-dom";
import type { AnalysisResponse } from "../lib/types";

type FeedbackPageProps = {
  result: AnalysisResponse | null;
};

const FEEDBACK_SECTIONS = [
  {
    key: "summary",
    title: "Professional Summary",
    scoreKey: "semantic_match",
    fallback: "A focused summary helps recruiters understand your target role, strongest tools, and career direction quickly.",
  },
  {
    key: "experience",
    title: "Work Experience",
    scoreKey: "experience_match",
    fallback: "Your work experience should show responsibilities, tools used, scope, and measurable outcomes.",
  },
  {
    key: "skills",
    title: "Skills",
    scoreKey: "skill_match",
    fallback: "Your skills section should prioritize role-specific tools and avoid burying high-value keywords.",
  },
  {
    key: "education",
    title: "Education",
    scoreKey: "ats_compliance",
    fallback: "Education should be easy to scan and include degree, institution, dates, and relevant coursework when useful.",
  },
  {
    key: "formatting",
    title: "Formatting",
    scoreKey: "resume_quality",
    fallback: "Consistent spacing, simple headings, and clear bullets help recruiters and screening systems read the resume faster.",
  },
  {
    key: "ats",
    title: "ATS Optimization",
    scoreKey: "ats_compliance",
    fallback: "ATS-friendly resumes use plain headings, readable text, clear dates, and role-aligned keywords.",
  },
  {
    key: "readability",
    title: "Recruiter Readability",
    scoreKey: "resume_quality",
    fallback: "Recruiters should quickly see what you did, how you did it, and why it mattered.",
  },
] as const;

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Detailed feedback</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Run an analysis to unlock section feedback.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Upload your resume first, then return here for section-by-section guidance.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

function firstFeedback(result: AnalysisResponse, scoreKey: keyof AnalysisResponse["breakdown"]) {
  return result.component_feedback?.[scoreKey]?.[0] ?? null;
}

function secondFeedback(result: AnalysisResponse, scoreKey: keyof AnalysisResponse["breakdown"]) {
  return result.component_feedback?.[scoreKey]?.[1] ?? null;
}

export function FeedbackPage({ result }: FeedbackPageProps) {
  if (!result) {
    return <EmptyResultState />;
  }

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Detailed feedback</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              Section-by-section resume review
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Review what works, what needs improvement, and the recommended changes for each major resume section.
            </p>
          </div>
          <Link className="primary-button" to="/suggestions">
            View Suggested Improvements
          </Link>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        {FEEDBACK_SECTIONS.map((section) => {
          const scoreKey = section.scoreKey as keyof AnalysisResponse["breakdown"];
          const resumeSection = result.resume_sections?.[section.key] ?? "";
          const works = firstFeedback(result, scoreKey) ?? section.fallback;
          const needs = secondFeedback(result, scoreKey) ?? "This section can be improved by making the evidence more specific, measurable, and aligned with the target role.";
          const recommended =
            section.key === "experience"
              ? "Add measurable outcomes and stronger action-oriented language to the most important bullets."
              : section.key === "skills"
                ? "Add role-specific keywords that match the jobs you want, then prove those skills in projects or experience."
                : section.key === "ats"
                  ? "Keep headings simple, dates readable, and content in selectable text so screening systems can parse it accurately."
                  : "Make the section easier to scan and connect it more clearly to the role you are targeting.";

          return (
            <article key={section.key} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="eyebrow">{section.title}</p>
                  <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">
                    {Math.round(result.breakdown[scoreKey])}% signal
                  </h2>
                </div>
                <span className="pill">{resumeSection ? "Detected" : "Review"}</span>
              </div>

              {resumeSection ? (
                <div className="mt-4 rounded-[1.15rem] border border-slate-900/10 bg-white/45 p-4 text-sm font-semibold leading-7 text-slate-700 dark:border-white/10 dark:bg-white/[0.04] dark:text-slate-300">
                  {resumeSection}
                </div>
              ) : null}

              <div className="mt-4 grid gap-3">
                <div>
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">What works well</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{works}</p>
                </div>
                <div>
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">What needs improvement</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{needs}</p>
                </div>
                <div className="rounded-[1.15rem] border border-cyan-300/20 bg-cyan-300/10 p-4">
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">Recommended change</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-200">{recommended}</p>
                </div>
              </div>
            </article>
          );
        })}
      </section>
    </div>
  );
}
