import type { UploadInput } from "../components/UploadPanel";
import { UploadPanel } from "../components/UploadPanel";

type UploadPageProps = {
  loading: boolean;
  error: string | null;
  onAnalyze: (payload: UploadInput) => Promise<void>;
};

export function UploadPage({ loading, error, onAnalyze }: UploadPageProps) {
  return (
    <div className="grid gap-6">
      <section className="grid grid-cols-1 items-start gap-8 lg:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-8 shadow-xl">
          <p className="eyebrow">Resume upload</p>
          <h1 className="mt-4 font-display text-4xl font-bold leading-tight tracking-tight text-ink md:text-5xl dark:text-slate-50">
            Upload your resume for instant AI-powered feedback
          </h1>
          <p className="mt-5 text-base leading-relaxed text-slate-700 md:text-lg dark:text-slate-300">
            Get a detailed analysis of your resume's clarity, structure, ATS compatibility, keyword relevance, and recruiter appeal.
          </p>

          <div className="mt-8 grid gap-4">
            {["ATS compatibility check", "Keyword and skills review", "Formatting analysis", "Recruiter readability feedback"].map((item) => (
              <div key={item} className="flex items-center gap-4 rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                <span className="h-3 w-3 rounded-full bg-cyan-300 shadow-[0_0_18px_rgba(103,232,249,0.45)]" />
                <span className="text-base font-semibold text-slate-800 dark:text-slate-200">{item}</span>
              </div>
            ))}
          </div>

          <div className="mt-8 rounded-2xl border border-cyan-300/20 bg-cyan-300/10 p-5">
            <p className="text-base font-semibold leading-relaxed text-slate-700 dark:text-slate-200">
              Your resume is processed securely and used only to generate personalized feedback.
            </p>
          </div>
        </div>

        <div className="grid gap-5">
          {error ? (
            <div className="rounded-[1.4rem] border border-red-300/70 bg-red-50 p-4 text-sm font-semibold leading-6 text-red-800 shadow-soft dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-100">
              {error}
            </div>
          ) : null}

          <UploadPanel loading={loading} onSubmit={onAnalyze} />

          <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
            <p className="text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Want to save this report? Create a free account to access it later.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
