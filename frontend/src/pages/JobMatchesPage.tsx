import { Link } from "react-router-dom";
import { JobMatchesTable } from "../components/JobMatchesTable";
import type { AnalysisResponse } from "../lib/types";

type JobMatchesPageProps = {
  result: AnalysisResponse | null;
};

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Live job matches</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
        Analyze a resume to see live job listings.
      </h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Live job matches and apply links appear after the resume is analyzed for a target role and market location.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

export function JobMatchesPage({ result }: JobMatchesPageProps) {
  if (!result) {
    return <EmptyResultState />;
  }

  const liveJobs = result.top_job_matches.filter((job) => job.source !== "role-baseline");

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Live job matches</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              Apply to matching job listings.
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              These listings come from the market sample used in your score. Open a live listing to review the job and apply on the source website.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <span className="pill">{liveJobs.length} live listings</span>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/dashboard">
              Back to Dashboard
            </Link>
          </div>
        </div>
      </section>

      <JobMatchesTable jobs={result.top_job_matches} analysisContext={result.analysis_context} />
    </div>
  );
}
