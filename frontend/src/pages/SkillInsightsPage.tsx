import { Link } from "react-router-dom";
import { SkillGapChart } from "../components/SkillGapChart";
import type { AnalysisResponse } from "../lib/types";

type SkillInsightsPageProps = {
  result: AnalysisResponse | null;
};

function EmptyResultState() {
  return (
    <section className="glass-panel rounded-[2rem] p-8 text-center">
      <p className="eyebrow">Skill evidence</p>
      <h1 className="mt-3 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">
        Analyze a resume to see skill gaps and proof.
      </h1>
      <p className="mx-auto mt-3 max-w-2xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
        Market-backed skill gaps, matched skills, and weak proof signals appear here after analysis.
      </p>
      <Link className="primary-button mt-6" to="/upload">
        Analyze My Resume
      </Link>
    </section>
  );
}

export function SkillInsightsPage({ result }: SkillInsightsPageProps) {
  if (!result) {
    return <EmptyResultState />;
  }

  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <p className="eyebrow">Skill evidence</p>
            <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
              Skill gaps and market proof.
            </h1>
            <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Review which skills are already proven, which ones need stronger evidence, and which role-specific tools appear in the market sample.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Link className="primary-button" to="/suggestions">
              View Suggested Fixes
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/jobs">
              View Live Jobs
            </Link>
            <Link className="ghost-button min-h-[3.35rem] px-6" to="/dashboard">
              Back to Dashboard
            </Link>
          </div>
        </div>
      </section>

      <SkillGapChart
        missingSkills={result.missing_skills}
        weakSkillProofs={result.weak_skill_proofs ?? []}
        matchedSkills={result.matched_skills}
        matchedSkillDetails={result.matched_skill_details ?? []}
        missingSkillDetails={result.missing_skill_details ?? []}
        weakSkillProofDetails={result.weak_skill_proof_details ?? []}
        detectedSkills={result.analysis_context?.parsed_resume_skills ?? []}
      />
    </div>
  );
}
