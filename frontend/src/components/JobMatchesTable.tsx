import type { JobMatch } from "../lib/types";

type JobMatchesTableProps = {
  jobs: JobMatch[];
};

function describeExtractionMode(mode?: string) {
  if (mode === "weighted-pattern" || mode === "hybrid") {
    return "Requirement extraction";
  }
  if (mode === "llm-baseline" || mode === "heuristic-baseline") {
    return "Role baseline";
  }
  return mode ?? "Market sample";
}

export function JobMatchesTable({ jobs }: JobMatchesTableProps) {
  return (
    <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft md:p-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">Job Matches</p>
          <h3 className="mt-2 font-display text-3xl text-ink">Market sample behind this score</h3>
        </div>
        <p className="text-sm text-slate-700">Listings are ranked by role fit and requirement quality before they influence scoring.</p>
      </div>
      <div className="grid gap-4">
        {jobs.map((job) => (
          <article key={`${job.source}-${job.url}`} className="rounded-[1.5rem] border border-ink/10 p-5">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h4 className="text-lg font-semibold text-ink">{job.title}</h4>
                <p className="text-sm text-slate-700">{job.company} - {job.location}</p>
              </div>
              <div className="flex flex-wrap gap-2">
                {job.remote ? <span className="rounded-full bg-sea/15 px-3 py-1 text-xs font-semibold text-ink">Remote</span> : null}
                {typeof job.relevance_score === "number" ? (
                  <span className="rounded-full bg-ember/15 px-3 py-1 text-xs font-semibold text-ink">
                    Relevance {Math.round(job.relevance_score)}%
                  </span>
                ) : null}
                {typeof job.normalized_data.skill_extraction_mode === "string" ? (
                  <span className="rounded-full bg-mist px-3 py-1 text-xs font-semibold text-ink">
                    {describeExtractionMode(job.normalized_data.skill_extraction_mode)}
                  </span>
                ) : null}
              </div>
            </div>
            <p className="mt-4 text-sm leading-6 text-slate-700">{job.preview ?? job.description}</p>
            <div className="mt-4 flex flex-wrap gap-2">
              {(job.normalized_data.skills ?? []).slice(0, 8).map((skill) => (
                <span key={skill} className="rounded-full bg-mist px-3 py-1 text-xs font-semibold text-ink">
                  {skill}
                </span>
              ))}
            </div>
            {Array.isArray(job.normalized_data.skill_evidence) && job.normalized_data.skill_evidence.length ? (
              <div className="mt-4 grid gap-2">
                {job.normalized_data.skill_evidence.slice(0, 2).map((item) => (
                  <p key={`${job.url}-${item.skill}-${item.snippet}`} className="text-xs leading-5 text-slate-600">
                    <span className="font-semibold text-ink">{item.skill}:</span> {item.snippet}
                  </p>
                ))}
              </div>
            ) : null}
            {job.source === "role-baseline" ? (
              <p className="mt-4 text-xs leading-5 text-slate-600">
                This card is a modeled role benchmark used to widen the market sample when live listings were too sparse.
              </p>
            ) : (
              <a
                className="mt-4 inline-flex rounded-full border border-ink/15 px-4 py-2 text-sm font-semibold text-ink transition hover:border-sea hover:bg-sea/10"
                href={job.url}
                target="_blank"
                rel="noreferrer"
              >
                Open listing
              </a>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}
