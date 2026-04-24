import type { AnalysisResponse, JobMatch, MarketConfidence } from "../lib/types";
import { cleanDisplayText } from "../lib/text";

type JobMatchesTableProps = {
  jobs: JobMatch[];
  analysisContext?: AnalysisResponse["analysis_context"];
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

function describeJobSource(source?: string) {
  if (source === "role-baseline") {
    return "Calibration baseline";
  }
  if (source === "arbeitnow") {
    return "Arbeitnow";
  }
  if (source === "remoteok") {
    return "RemoteOK";
  }
  if (source === "remotive") {
    return "Remotive";
  }
  if (source === "themuse") {
    return "The Muse";
  }
  if (source === "greenhouse") {
    return "Greenhouse ATS";
  }
  if (source === "lever") {
    return "Lever ATS";
  }
  if (source === "jobicy") {
    return "Jobicy";
  }
  if (source === "jooble") {
    return "Jooble";
  }
  if (source === "indianapi") {
    return "Indian Jobs API";
  }
  if (source === "adzuna") {
    return "Adzuna";
  }
  if (source === "usajobs") {
    return "USAJobs";
  }
  return source ?? "Live listing";
}

function describeMarketConfidence(confidence?: MarketConfidence) {
  if (typeof confidence === "object" && confidence) {
    if (confidence.baseline_only) {
      return "Baseline-only market view";
    }
    if (confidence.blended_market) {
      return "Live sample widened by calibration";
    }
    if (typeof confidence.factor === "number") {
      if (confidence.factor >= 0.96) {
        return "High-confidence live sample";
      }
      if (confidence.factor >= 0.88) {
        return "Medium-confidence live sample";
      }
      if (confidence.factor >= 0.76) {
        return "Limited live market sample";
      }
      return "Low-confidence market view";
    }
  }
  if (confidence === "high") {
    return "High-confidence live sample";
  }
  if (confidence === "medium") {
    return "Medium-confidence live sample";
  }
  if (confidence === "baseline-assisted") {
    return "Live sample widened by calibration";
  }
  if (confidence === "low") {
    return "Low-confidence market view";
  }
  return "Market sample";
}

function getMarketConfidenceDetail(confidence?: MarketConfidence) {
  return typeof confidence === "object" && confidence ? confidence : undefined;
}

function metricLabel(label: string, value?: number) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return `${label} ${Math.round(value)}`;
}

function sourcePillClass(source?: string) {
  if (source === "role-baseline") {
    return "bg-amber-200 text-ink dark:bg-amber-300/30 dark:text-amber-50";
  }
  return "bg-mist text-ink dark:bg-[#132531] dark:text-slate-100";
}

export function JobMatchesTable({ jobs, analysisContext }: JobMatchesTableProps) {
  const liveJobs = jobs.filter((job) => job.source !== "role-baseline");
  const baselineJobs = jobs.filter((job) => job.source === "role-baseline");
  const displayedLiveJobs = liveJobs.slice(0, 8);
  const displayedBaselineJobs = baselineJobs.slice(0, liveJobs.length >= 8 ? 2 : 3);
  const liveSourceCounts = Object.entries(analysisContext?.live_source_counts ?? {}).sort((left, right) => right[1] - left[1]);
  const marketConfidenceDetail = getMarketConfidenceDetail(analysisContext?.market_confidence);
  const liveJobCount =
    typeof analysisContext?.live_job_count === "number" ? analysisContext.live_job_count : marketConfidenceDetail?.live_job_count;
  const liveCompanyCount =
    typeof analysisContext?.live_company_count === "number" ? analysisContext.live_company_count : marketConfidenceDetail?.live_company_count;
  const baselineConfidence = analysisContext?.baseline_confidence ?? marketConfidenceDetail?.baseline_confidence;
  const baselineAssisted =
    Boolean(analysisContext?.used_role_baseline) ||
    Boolean(marketConfidenceDetail?.baseline_only) ||
    Boolean(marketConfidenceDetail?.blended_market);

  function renderFitMetrics(job: JobMatch) {
    const metrics = job.normalized_data.fit_metrics;
    const entries = [
      metricLabel("Title fit", metrics?.title_alignment),
      metricLabel("Role fit", metrics?.role_fit),
      metricLabel("Market quality", metrics?.market_quality),
      metricLabel("Skill overlap", metrics?.skill_overlap),
    ].filter(Boolean) as string[];

    if (!entries.length) {
      return null;
    }

    return (
      <div className="mt-4 flex flex-wrap gap-2">
        {entries.map((entry) => (
          <span
            key={`${job.source}-${job.external_id ?? job.title}-${entry}`}
            className="rounded-full border border-ink/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate transition-colors duration-300 dark:border-[#294250] dark:text-slate-300"
          >
            {entry}
          </span>
        ))}
      </div>
    );
  }

  function renderSelectionReasons(job: JobMatch) {
    const reasons = Array.isArray(job.normalized_data.selection_reasons) ? job.normalized_data.selection_reasons.slice(0, 3) : [];
    if (!reasons.length) {
      return null;
    }
    return (
      <div className="mt-4 rounded-[1.1rem] border border-ink/10 bg-mist/60 p-4 transition-colors duration-300 dark:border-[#223543] dark:bg-[#0c1821]">
        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">Why This Listing Was Used</p>
        <div className="mt-3 grid gap-2">
          {reasons.map((reason, index) => (
            <p
              key={`${job.source}-${job.external_id ?? job.title}-reason-${index}`}
              className="text-sm leading-6 text-slate-700 dark:text-slate-200"
            >
              {cleanDisplayText(reason)}
            </p>
          ))}
        </div>
      </div>
    );
  }

  function renderJob(job: JobMatch) {
    const matchStrength = job.normalized_data.match_strength_label;
    const description = cleanDisplayText(job.preview ?? job.description) || "Description preview was not available for this listing.";

    return (
      <article
        key={`${job.source}-${job.external_id ?? job.title}-${job.company}-${job.location}`}
        className="rounded-[1.5rem] border border-ink/10 p-5 transition-colors duration-300 dark:border-[#223543] dark:bg-[#0f1d27]"
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h4 className="text-lg font-semibold text-ink dark:text-slate-50">{job.title}</h4>
            <p className="text-sm text-slate-700 dark:text-slate-200">
              {job.company} - {job.location}
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <span className={`rounded-full px-3 py-1 text-xs font-semibold ${sourcePillClass(job.source)}`}>
              {describeJobSource(job.source)}
            </span>
            {matchStrength ? (
              <span className="rounded-full bg-sea/15 px-3 py-1 text-xs font-semibold text-ink dark:text-slate-100">
                {matchStrength}
              </span>
            ) : null}
            {job.remote ? (
              <span className="rounded-full bg-sea/15 px-3 py-1 text-xs font-semibold text-ink dark:text-slate-100">Remote</span>
            ) : null}
            {typeof job.relevance_score === "number" ? (
              <span className="rounded-full bg-ember/15 px-3 py-1 text-xs font-semibold text-ink dark:text-slate-100">
                Relevance {Math.round(job.relevance_score)}%
              </span>
            ) : null}
            {typeof job.normalized_data.skill_extraction_mode === "string" ? (
              <span className="rounded-full bg-mist px-3 py-1 text-xs font-semibold text-ink transition-colors duration-300 dark:bg-[#132531] dark:text-slate-100">
                {describeExtractionMode(job.normalized_data.skill_extraction_mode)}
              </span>
            ) : null}
          </div>
        </div>

        {renderFitMetrics(job)}

        <p className="mt-4 text-sm leading-6 text-slate-700 dark:text-slate-200">{description}</p>

        <div className="mt-4 flex flex-wrap gap-2">
          {(job.normalized_data.skills ?? []).slice(0, 8).map((skill) => (
            <span
              key={`${job.source}-${job.title}-${skill}`}
              className="rounded-full bg-mist px-3 py-1 text-xs font-semibold text-ink transition-colors duration-300 dark:bg-[#132531] dark:text-slate-100"
            >
              {skill}
            </span>
          ))}
        </div>

        {renderSelectionReasons(job)}

        {Array.isArray(job.normalized_data.skill_evidence) && job.normalized_data.skill_evidence.length ? (
          <div className="mt-4 grid gap-2">
            {job.normalized_data.skill_evidence.slice(0, 2).map((item, index) => (
              <p
                key={`${job.source}-${job.external_id ?? job.title}-${item.skill}-${index}`}
                className="text-xs leading-5 text-slate-600 dark:text-slate-300"
              >
                <span className="font-semibold text-ink dark:text-slate-100">{item.skill}:</span> {cleanDisplayText(item.snippet)}
              </p>
            ))}
          </div>
        ) : null}

        {job.source === "role-baseline" ? (
          <p className="mt-4 text-xs leading-5 text-slate-600 dark:text-slate-300">
            This card is a modeled role benchmark used only to widen the market sample when live listings were too sparse.
          </p>
        ) : (
          <a
            className="mt-4 inline-flex rounded-full border border-ink/15 px-4 py-2 text-sm font-semibold text-ink transition hover:border-sea hover:bg-sea/10 dark:border-[#294250] dark:bg-[#132531] dark:text-slate-100 dark:hover:border-sea dark:hover:bg-[#17303d]"
            href={job.url}
            target="_blank"
            rel="noreferrer"
          >
            Open listing
          </a>
        )}
      </article>
    );
  }

  return (
    <section className="glass-panel rounded-[2.25rem] p-5 sm:p-7">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Job Matches</p>
          <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">Market sample behind this score</h3>
        </div>
        <p className="text-sm text-slate-700 dark:text-slate-200">
          Showing {displayedLiveJobs.length} of {liveJobs.length} live listings used for ranking. Listings are sorted by role fit and requirement quality before they influence scoring.
        </p>
      </div>

      {analysisContext ? (
        <div className="mb-6 rounded-[1.4rem] border border-ink/10 bg-mist/70 p-4 transition-colors duration-300 dark:border-[#223543] dark:bg-[#0d1a24]">
          <div className="flex flex-wrap gap-2">
            <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
              {describeMarketConfidence(analysisContext.market_confidence)}
            </span>
            {analysisContext.target_location ? (
              <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
                Market {analysisContext.target_location}
              </span>
            ) : null}
            {typeof liveJobCount === "number" ? (
              <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
                {liveJobCount} live jobs
              </span>
            ) : null}
            {typeof liveCompanyCount === "number" ? (
              <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
                {liveCompanyCount} companies
              </span>
            ) : null}
            {typeof marketConfidenceDetail?.factor === "number" ? (
              <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
                Market confidence {Math.round(marketConfidenceDetail.factor * 100)}%
              </span>
            ) : null}
            {baselineAssisted ? (
              <span className="rounded-full bg-amber-200 px-3 py-1 text-xs font-semibold text-ink dark:bg-amber-300/30 dark:text-amber-50">
                Baseline assisted
              </span>
            ) : null}
            {baselineConfidence ? (
              <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-ink dark:bg-[#132531] dark:text-slate-100">
                Baseline confidence {baselineConfidence}
              </span>
            ) : null}
          </div>
          {liveSourceCounts.length ? (
            <div className="mt-4 flex flex-wrap gap-2">
              {liveSourceCounts.map(([source, count]) => (
                <span
                  key={`source-${source}`}
                  className="rounded-full border border-ink/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate transition-colors duration-300 dark:border-[#294250] dark:text-slate-300"
                >
                  {describeJobSource(source)} x{count}
                </span>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      {displayedLiveJobs.length ? <div className="grid gap-4">{displayedLiveJobs.map((job) => renderJob(job))}</div> : null}

      {displayedBaselineJobs.length ? (
        <div className={displayedLiveJobs.length ? "mt-6" : ""}>
          <div className="mb-4 rounded-[1.25rem] border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-6 text-amber-900 transition-colors duration-300 dark:border-amber-400/25 dark:bg-amber-400/10 dark:text-amber-100">
            <span className="font-semibold">Calibration baselines:</span> live listings were too sparse for a complete market map, so these role benchmarks were added only to widen skill coverage.
          </div>
          <div className="grid gap-4">{displayedBaselineJobs.map((job) => renderJob(job))}</div>
        </div>
      ) : null}
    </section>
  );
}
