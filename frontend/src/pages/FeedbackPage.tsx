import { Link } from "react-router-dom";
import type { AnalysisResponse, ScoreBreakdown } from "../lib/types";

type FeedbackPageProps = {
  result: AnalysisResponse | null;
};

const FEEDBACK_SECTIONS: {
  key: string;
  title: string;
  scoreKey: keyof ScoreBreakdown;
  works: string;
  needs: string;
  fix: string;
}[] = [
  {
    key: "summary",
    title: "Summary",
    scoreKey: "semantic_match",
    works: "The summary gives recruiters a quick first signal for the target role.",
    needs: "It should clearly name the target role, strongest tools, and the type of problems you solve.",
    fix: "Rewrite the summary in two or three lines using role-specific keywords and one concrete career strength.",
  },
  {
    key: "experience",
    title: "Work Experience",
    scoreKey: "experience_match",
    works: "Experience entries give the resume practical evidence beyond a skills list.",
    needs: "Several bullets may need clearer outcomes, scope, tools used, or measurable results.",
    fix: "Convert responsibility-style bullets into action + tool + result statements wherever possible.",
  },
  {
    key: "skills",
    title: "Skills",
    scoreKey: "skill_match",
    works: "The skills section gives the analysis a base set of role signals to compare against the market sample.",
    needs: "Some high-value role keywords may be missing or present without enough supporting evidence.",
    fix: "Add the most relevant missing keywords naturally, then prove them in projects or experience bullets.",
  },
  {
    key: "formatting",
    title: "Formatting",
    scoreKey: "resume_quality",
    works: "Readable formatting helps recruiters scan the resume quickly.",
    needs: "Formatting should stay consistent across headings, bullets, dates, spacing, and section order.",
    fix: "Use plain section headings, consistent bullet style, and a simple one-column structure for safer parsing.",
  },
  {
    key: "ats",
    title: "ATS Optimization",
    scoreKey: "ats_compliance",
    works: "ATS-friendly structure makes the resume easier for screening systems to parse.",
    needs: "Complex formatting, unclear headings, or weak keyword alignment can reduce automated readability.",
    fix: "Keep text selectable, avoid heavy graphics, use standard headings, and match key role language accurately.",
  },
  {
    key: "readability",
    title: "Recruiter Readability",
    scoreKey: "resume_quality",
    works: "Clear language helps recruiters understand your fit quickly.",
    needs: "Recruiters should see impact, tools, and relevance without reading every line twice.",
    fix: "Lead with stronger verbs, remove vague phrasing, and make the strongest achievements easy to find.",
  },
];

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Detailed feedback</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Run an analysis to unlock section feedback.</h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Upload your resume first, then return here for practical section-by-section guidance.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

function feedbackAt(result: AnalysisResponse, scoreKey: keyof ScoreBreakdown, index: number) {
  return result.component_feedback?.[scoreKey]?.[index] ?? null;
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
            <h1 className="mt-3 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
              Review each resume section.
            </h1>
            <p className="mt-4 max-w-3xl text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
              Each section shows what is working, what needs improvement, and the next practical fix.
            </p>
          </div>
          <Link className="primary-button" to="/suggestions">
            View Suggested Improvements
          </Link>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        {FEEDBACK_SECTIONS.map((section) => {
          const works = feedbackAt(result, section.scoreKey, 0) ?? section.works;
          const needs = feedbackAt(result, section.scoreKey, 1) ?? section.needs;
          const score = Math.round(result.breakdown[section.scoreKey]);

          return (
            <article key={section.key} className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="eyebrow">{section.title}</p>
                  <h2 className="mt-2 font-display text-3xl font-extrabold tracking-[-0.05em] text-ink dark:text-slate-50">{score}% signal</h2>
                </div>
                <span className="pill">Review</span>
              </div>

              <div className="mt-5 grid gap-4">
                <div>
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">What works well</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{works}</p>
                </div>
                <div>
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">What needs improvement</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{needs}</p>
                </div>
                <div className="rounded-[1.15rem] border border-cyan-300/20 bg-cyan-300/10 p-4">
                  <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500 dark:text-slate-400">Suggested fix</p>
                  <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-200">{section.fix}</p>
                </div>
              </div>
            </article>
          );
        })}
      </section>
    </div>
  );
}
