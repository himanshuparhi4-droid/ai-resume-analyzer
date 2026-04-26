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
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="eyebrow">Resume upload</p>
          <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
            Upload your resume securely.
          </h1>
          <p className="mt-4 text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
            Upload your resume securely and receive a detailed analysis designed to help you improve structure, readability, keyword relevance, and recruiter appeal.
          </p>
          <div className="mt-5 flex flex-wrap gap-2">
            <span className="pill">PDF and DOCX supported</span>
            <span className="pill">Best under 5 MB</span>
            <span className="pill">No account required</span>
          </div>
        </div>
      </section>

      {error ? (
        <div className="rounded-[1.4rem] border border-red-300/70 bg-red-50 p-4 text-sm font-semibold leading-6 text-red-800 shadow-soft dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-100">
          {error}
        </div>
      ) : null}

      <UploadPanel loading={loading} onSubmit={onAnalyze} />

      <section className="signal-panel rounded-[1.75rem] p-5 sm:p-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="eyebrow">Privacy note</p>
            <p className="mt-2 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
              Your resume is processed securely and used only to generate personalized feedback.
            </p>
          </div>
          <p className="max-w-xl text-sm font-semibold leading-7 text-slate-600 dark:text-slate-400">
            Want to save this report? You can create a free account after the analysis and access it later.
          </p>
        </div>
      </section>
    </div>
  );
}
