import type { UploadInput } from "../components/UploadPanel";
import { UploadPanel } from "../components/UploadPanel";

const CHECKLIST = ["Resume file", "Target role", "Optional job description", "Market location"];

type UploadPageProps = {
  loading: boolean;
  error: string | null;
  onAnalyze: (payload: UploadInput) => Promise<void>;
};

export function UploadPage({ loading, error, onAnalyze }: UploadPageProps) {
  return (
    <div className="grid gap-6">
      <section className="grid grid-cols-1 items-start gap-6 lg:grid-cols-[0.8fr_1.2fr]">
        <aside className="signal-panel rounded-[2rem] p-6 sm:p-8">
          <p className="eyebrow">Upload resume</p>
          <h1 className="mt-4 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
            Upload your resume and choose the role.
          </h1>
          <p className="mt-5 text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
            Upload your resume to receive feedback on clarity, formatting, ATS compatibility, keyword relevance, and recruiter readability.
          </p>

          <div className="mt-7 grid gap-3">
            {CHECKLIST.map((item) => (
              <div key={item} className="flex items-center gap-3 rounded-[1.1rem] border border-slate-900/10 bg-white/50 p-4 dark:border-white/10 dark:bg-white/[0.04]">
                <span className="h-2.5 w-2.5 rounded-full bg-cyan-400" />
                <span className="text-sm font-bold text-slate-700 dark:text-slate-300">{item}</span>
              </div>
            ))}
          </div>

          <div className="mt-7 rounded-[1.2rem] border border-cyan-300/20 bg-cyan-300/10 p-5">
            <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-200">
              Your resume is processed securely and used only to generate personalized feedback. Login is optional and not required for analysis.
            </p>
          </div>
        </aside>

        <div className="grid gap-5">
          {error ? (
            <div className="rounded-[1.4rem] border border-red-300/70 bg-red-50 p-4 text-sm font-semibold leading-6 text-red-800 shadow-soft dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-100">
              {error}
            </div>
          ) : null}

          <UploadPanel loading={loading} onSubmit={onAnalyze} />

          <div className="rounded-2xl border border-slate-900/10 bg-white/50 p-5 dark:border-white/10 dark:bg-white/[0.04]">
            <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Want to save this report? Create a free account after analysis to access it later.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
