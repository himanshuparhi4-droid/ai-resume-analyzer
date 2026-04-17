import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { SkillDetail } from "../lib/types";

type SkillGapChartProps = {
  missingSkills: { skill: string; share: number }[];
  matchedSkills: string[];
  matchedSkillDetails: SkillDetail[];
  missingSkillDetails: SkillDetail[];
};

export function SkillGapChart({ missingSkills, matchedSkills, matchedSkillDetails, missingSkillDetails }: SkillGapChartProps) {
  const hasMissingSkills = missingSkills.length > 0;

  return (
    <section className="grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
      <div className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft md:p-8">
        <div className="mb-5">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">Market Gaps</p>
          <h3 className="mt-2 font-display text-3xl text-ink">Missing role-specific tools</h3>
        </div>
        {hasMissingSkills ? (
          <div className="h-[320px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={missingSkills.slice(0, 8)} layout="vertical" margin={{ left: 20, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#dbe5e3" />
                <XAxis type="number" tick={{ fill: "#52606b" }} unit="%" />
                <YAxis type="category" dataKey="skill" width={95} tick={{ fill: "#06131c", fontSize: 12 }} />
                <Tooltip />
                <Bar dataKey="share" radius={[0, 12, 12, 0]} fill="#ff8a5b" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="flex min-h-[320px] items-center justify-center rounded-[1.5rem] border border-dashed border-ink/10 bg-mist px-8 text-center">
            <div className="max-w-md">
              <p className="font-semibold text-ink">No major high-demand gaps were found in the sampled market set.</p>
              <p className="mt-3 text-sm leading-6 text-slate-700">
                That does not mean the resume is perfect. It means the current job sample did not surface a clear missing-tool cluster for this role.
              </p>
            </div>
          </div>
        )}
        {missingSkillDetails.length ? (
          <div className="mt-5 grid gap-3">
            {missingSkillDetails.slice(0, 3).map((detail) => (
              <article key={detail.skill} className="rounded-[1.25rem] bg-mist p-4">
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink">{detail.skill}</h4>
                  <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate">
                    {Math.round(detail.market_share)}% demand
                  </span>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700">
                  {detail.job_evidence?.[0]?.snippet ?? "This skill appeared repeatedly in the market sample for the chosen role."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
      </div>
      <div className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft md:p-8">
        <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">Matched Skills</p>
        <h3 className="mt-2 font-display text-3xl text-ink">What is already working</h3>
        <div className="mt-6 flex flex-wrap gap-3">
          {matchedSkills.length ? (
            matchedSkills.map((skill) => (
              <span key={skill} className="rounded-full bg-sea/15 px-4 py-2 text-sm font-semibold text-ink">
                {skill}
              </span>
            ))
          ) : (
            <p className="text-sm leading-6 text-slate-700">No strong market-skill overlaps yet. That is fixable once we target the missing skills above.</p>
          )}
        </div>
        {matchedSkillDetails.length ? (
          <div className="mt-6 grid gap-3">
            {matchedSkillDetails.slice(0, 4).map((detail) => (
              <article key={detail.skill} className="rounded-[1.25rem] bg-mist p-4">
                <div className="flex items-center justify-between gap-3">
                  <h4 className="font-semibold text-ink">{detail.skill}</h4>
                  <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate">
                    {Math.round(detail.market_share)}% demand
                  </span>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-700">
                  <span className="font-semibold text-ink">Resume proof:</span>{" "}
                  {detail.resume_evidence?.[0] ?? "Direct resume evidence was limited."}
                </p>
                <p className="mt-2 text-sm leading-6 text-slate-700">
                  <span className="font-semibold text-ink">Market evidence:</span>{" "}
                  {detail.job_evidence?.[0]?.snippet ?? "Sampled jobs also mentioned this skill."}
                </p>
              </article>
            ))}
          </div>
        ) : null}
      </div>
    </section>
  );
}
