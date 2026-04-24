import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { cleanDisplayText } from "../lib/text";
import type { SkillDetail } from "../lib/types";

type SkillGapChartProps = {
  missingSkills: { skill: string; share: number; signal_source?: string }[];
  weakSkillProofs: { skill: string; share: number; signal_source?: string }[];
  matchedSkills: string[];
  matchedSkillDetails: SkillDetail[];
  missingSkillDetails: SkillDetail[];
  weakSkillProofDetails: SkillDetail[];
  detectedSkills?: string[];
};

export function SkillGapChart({
  missingSkills,
  weakSkillProofs,
  matchedSkills,
  matchedSkillDetails,
  missingSkillDetails,
  weakSkillProofDetails,
  detectedSkills = [],
}: SkillGapChartProps) {
  const legacyWeakMissingSkills = missingSkills.filter((detail) => detail.signal_source === "weak-resume-proof");
  const trueMissingSkills = missingSkills.filter((detail) => detail.signal_source !== "weak-resume-proof");
  const legacyWeakMissingDetails = missingSkillDetails.filter((detail) => detail.signal_source === "weak-resume-proof");
  const trueMissingSkillDetails = missingSkillDetails.filter((detail) => detail.signal_source !== "weak-resume-proof");
  const rankedMissingDetails = [...trueMissingSkillDetails].sort((left, right) => {
    const leftSource = left.primary_source === "role-baseline" ? 0 : left.primary_source && left.primary_source !== "unknown" ? 2 : 1;
    const rightSource = right.primary_source === "role-baseline" ? 0 : right.primary_source && right.primary_source !== "unknown" ? 2 : 1;
    if (leftSource !== rightSource) {
      return rightSource - leftSource;
    }
    return right.market_share - left.market_share;
  });
  const missingDetailMap = new Map(rankedMissingDetails.map((detail) => [detail.skill, detail]));
  const mergedMissingDetails = [
    ...rankedMissingDetails,
    ...trueMissingSkills
      .filter((detail) => !missingDetailMap.has(detail.skill))
      .map((detail) => ({
        skill: detail.skill,
        market_share: detail.share,
        primary_source: "unknown",
        signal_source: "unknown",
        job_evidence: [],
      })),
  ];
  const weakProofDetailMap = new Map(weakSkillProofDetails.map((detail) => [detail.skill, detail]));
  const mergedWeakProofDetailCandidates = [
    ...weakSkillProofDetails,
    ...legacyWeakMissingDetails.filter((detail) => !weakProofDetailMap.has(detail.skill)),
    ...weakSkillProofs
      .filter((detail) => !weakProofDetailMap.has(detail.skill))
      .map((detail) => ({
        skill: detail.skill,
        market_share: detail.share,
        primary_source: "resume",
        signal_source: detail.signal_source ?? "weak-resume-proof",
        resume_evidence: [],
        job_evidence: [],
      })),
    ...legacyWeakMissingSkills
      .filter((detail) => !weakProofDetailMap.has(detail.skill))
      .map((detail) => ({
        skill: detail.skill,
        market_share: detail.share,
        primary_source: "resume",
        signal_source: "weak-resume-proof",
        resume_evidence: [],
        job_evidence: [],
      })),
  ];
  const mergedWeakProofDetails = mergedWeakProofDetailCandidates.filter((detail, index, items) => {
    const normalizedSkill = detail.skill.trim().toLowerCase();
    return items.findIndex((item) => item.skill.trim().toLowerCase() === normalizedSkill) === index;
  });
  const liveMissingDetails = mergedMissingDetails.filter((detail) => detail.primary_source && detail.primary_source !== "role-baseline" && detail.primary_source !== "unknown");
  const calibratedMissingDetails = mergedMissingDetails.filter((detail) => detail.primary_source === "role-baseline");
  const unsupportedMissingDetails = mergedMissingDetails.filter((detail) => !detail.primary_source || detail.primary_source === "unknown");
  const matchedDetails = [...matchedSkillDetails].sort((left, right) => {
    const leftSource = left.primary_source === "role-baseline" ? 0 : 1;
    const rightSource = right.primary_source === "role-baseline" ? 0 : 1;
    if (leftSource !== rightSource) {
      return rightSource - leftSource;
    }
    return right.market_share - left.market_share;
  });
  const detectedSkillList = Array.from(new Set(detectedSkills.map((skill) => skill.trim().toLowerCase()).filter(Boolean))).sort();
  const chartData = (mergedMissingDetails.length ? mergedMissingDetails : trueMissingSkills)
    .slice(0, 8)
    .map((detail) => ({
      skill: detail.skill,
      share: "market_share" in detail ? detail.market_share : detail.share,
    }));
  const hasMissingSkills = chartData.length > 0;

  function describeSource(source?: string) {
    if (source === "role-baseline") {
      return "Calibrated baseline";
    }
    if (source && source !== "unknown") {
      return "Live listing";
    }
    return "Market sample";
  }

  return (
    <section className="grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
      <div className="glass-panel rounded-[2.25rem] p-5 sm:p-7">
        <div className="mb-5">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Market Gaps</p>
          <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
            {hasMissingSkills ? "Missing role-specific tools" : "No major missing tools found"}
          </h3>
          <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
            {liveMissingDetails.length && calibratedMissingDetails.length
              ? "The strongest gaps are backed by live job listings, with additional role-family calibration added so narrow live samples do not underreport missing tools."
              : liveMissingDetails.length
              ? "These gaps are backed by live job listings from the current market sample."
              : calibratedMissingDetails.length
                ? "Live listings were too thin for a reliable skills map, so calibrated role baselines widened the missing-skill view."
                : unsupportedMissingDetails.length
                  ? "Some gaps were detected from the broader market sample even when the strongest evidence snippet was not preserved for every skill."
                : mergedWeakProofDetails.length
                  ? "The parser found the core tools on the resume. The cards below focus on where the resume needs stronger proof, not missing keywords."
                  : "The current market sample did not expose a strong missing-skill cluster."}
          </p>
        </div>
        {hasMissingSkills ? (
          <div className="h-[320px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} layout="vertical" margin={{ left: 20, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
                <XAxis type="number" tick={{ fill: "var(--chart-axis)" }} unit="%" />
                <YAxis type="category" dataKey="skill" width={95} tick={{ fill: "var(--tooltip-text)", fontSize: 12 }} />
                <Tooltip
                  contentStyle={{
                    background: "var(--tooltip-bg)",
                    border: "1px solid var(--tooltip-border)",
                    borderRadius: "16px",
                    color: "var(--tooltip-text)",
                  }}
                  cursor={{ fill: "rgba(94, 194, 183, 0.12)" }}
                  labelStyle={{ color: "var(--tooltip-text)" }}
                />
                <Bar dataKey="share" radius={[0, 12, 12, 0]} fill="var(--chart-bar)" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="flex min-h-[320px] items-center justify-center rounded-[1.5rem] border border-dashed border-ink/10 bg-mist px-8 text-center transition-colors duration-300 dark:border-[#294250] dark:bg-[#0f1d27]">
            <div className="max-w-md">
              <p className="font-semibold text-ink dark:text-slate-50">No major high-demand gaps were found in the sampled market set.</p>
              <p className="mt-3 text-sm leading-6 text-slate-700 dark:text-slate-200">
                Detected resume skills are not treated as missing. If a detected skill needs stronger evidence, it appears in the proof section below.
              </p>
            </div>
          </div>
        )}
        {mergedWeakProofDetails.length ? (
          <div className="mt-5 grid gap-3">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate dark:text-slate-300">Needs stronger proof</p>
            {mergedWeakProofDetails.slice(0, 4).map((detail) => (
              <article
                key={`weak-proof-${detail.skill}`}
                className="rounded-[1.25rem] border border-sea/30 bg-sea/10 p-4 transition-colors duration-300 dark:border-sea/25 dark:bg-sea/10"
              >
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink dark:text-slate-50">{detail.skill}</h4>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-sea/15 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-ink dark:text-slate-100">
                      Found in resume
                    </span>
                    <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">
                      {Math.round(detail.market_share)}% demand
                    </span>
                  </div>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-ink dark:text-slate-100">Resume proof:</span>{" "}
                  {cleanDisplayText(detail.resume_evidence?.[0]) || "This skill is listed, but stronger project or experience evidence would help."}
                </p>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-ink dark:text-slate-100">Market expectation:</span>{" "}
                  {cleanDisplayText(detail.job_evidence?.[0]?.snippet) || "Live or calibrated role signals expect practical evidence for this skill."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
        {liveMissingDetails.length ? (
          <div className="mt-5 grid gap-3">
            {liveMissingDetails.slice(0, 3).map((detail) => (
              <article key={detail.skill} className="soft-card rounded-[1.25rem] p-4">
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink dark:text-slate-50">{detail.skill}</h4>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-sea/15 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-ink dark:text-slate-100">
                      {describeSource(detail.primary_source)}
                    </span>
                    <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">
                      {Math.round(detail.market_share)}% demand
                    </span>
                  </div>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  {cleanDisplayText(detail.job_evidence?.[0]?.snippet) || "This skill appeared repeatedly in the market sample for the chosen role."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
        {calibratedMissingDetails.length ? (
          <div className="mt-5 grid gap-3">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate dark:text-slate-300">Calibration-only gaps</p>
            {calibratedMissingDetails.slice(0, 3).map((detail) => (
              <article
                key={`calibrated-${detail.skill}`}
                className="rounded-[1.25rem] border border-amber-200 bg-amber-50 p-4 transition-colors duration-300 dark:border-amber-400/25 dark:bg-amber-400/10"
              >
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink dark:text-slate-50">{detail.skill}</h4>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-amber-200 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-ink dark:bg-amber-300/30 dark:text-amber-50">
                      {describeSource(detail.primary_source)}
                    </span>
                    <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">
                      {Math.round(detail.market_share)}% demand
                    </span>
                  </div>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  {cleanDisplayText(detail.job_evidence?.[0]?.snippet) || "This gap was inferred from the calibration baseline for this role."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
        {unsupportedMissingDetails.length ? (
          <div className="mt-5 grid gap-3">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate dark:text-slate-300">Additional market gaps</p>
            {unsupportedMissingDetails.slice(0, 3).map((detail) => (
              <article
                key={`unsupported-${detail.skill}`}
                className="rounded-[1.25rem] border border-ink/10 bg-mist/70 p-4 transition-colors duration-300 dark:border-[#223543] dark:bg-[#0f1d27]"
              >
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink dark:text-slate-50">{detail.skill}</h4>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-mist px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-ink transition-colors duration-300 dark:bg-[#132531] dark:text-slate-100">
                      Market sample
                    </span>
                    <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">
                      {Math.round(detail.market_share)}% demand
                    </span>
                  </div>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  This skill was recurrent in the market-skill frequency map even though a high-confidence snippet was not preserved for the card view.
                </p>
              </article>
            ))}
          </div>
        ) : null}
      </div>
      <div className="glass-panel rounded-[2.25rem] p-5 sm:p-7">
        <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-300">Matched Skills</p>
        <h3 className="mt-2 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">What is already working</h3>
        <div className="mt-6 flex flex-wrap gap-3">
          {matchedSkills.length ? (
            matchedSkills.map((skill) => (
              <span key={skill} className="rounded-full bg-sea/15 px-4 py-2 text-sm font-semibold text-ink dark:text-slate-100">
                {skill}
              </span>
            ))
          ) : (
            <p className="text-sm leading-6 text-slate-700 dark:text-slate-200">No strong market-skill overlaps yet. That is fixable once we target the missing skills above.</p>
          )}
        </div>
        {matchedDetails.length ? (
          <div className="mt-6 grid gap-3">
            {matchedDetails.slice(0, 4).map((detail) => (
              <article key={detail.skill} className="soft-card rounded-[1.25rem] p-4">
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink dark:text-slate-50">{detail.skill}</h4>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-mist px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-ink transition-colors duration-300 dark:bg-white/[0.06] dark:text-slate-100">
                      {describeSource(detail.primary_source)}
                    </span>
                    <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate dark:text-slate-300">
                      {Math.round(detail.market_share)}% demand
                    </span>
                  </div>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-ink dark:text-slate-100">Resume proof:</span>{" "}
                  {cleanDisplayText(detail.resume_evidence?.[0]) || "Direct resume evidence was limited."}
                </p>
                <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-ink dark:text-slate-100">Market evidence:</span>{" "}
                  {cleanDisplayText(detail.job_evidence?.[0]?.snippet) || "Sampled jobs also mentioned this skill."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
        {detectedSkillList.length ? (
          <div className="mt-6 rounded-[1.5rem] border border-sea/25 bg-sea/10 p-4 transition-colors duration-300 dark:border-sea/20 dark:bg-sea/10">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate dark:text-slate-300">Detected in resume</p>
            <p className="mt-2 text-sm leading-6 text-slate-700 dark:text-slate-200">
              These canonical skills were found in the parsed resume. If one appears under proof, it means "show stronger usage," not "you do not have it."
            </p>
            <div className="mt-4 flex flex-wrap gap-2">
              {detectedSkillList.slice(0, 18).map((skill) => (
                <span key={`detected-${skill}`} className="rounded-full bg-white/70 px-3 py-1 text-xs font-semibold text-ink shadow-sm transition-colors duration-300 dark:bg-white/[0.08] dark:text-slate-100">
                  {skill}
                </span>
              ))}
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
