import { FormEvent, useMemo, useState } from "react";

export type UploadInput = {
  file: File;
  roleQuery: string;
  location: string;
  limit: number;
};

type UploadPanelProps = {
  loading: boolean;
  onSubmit: (payload: UploadInput) => Promise<void>;
};

export function UploadPanel({ loading, onSubmit }: UploadPanelProps) {
  const [file, setFile] = useState<File | null>(null);
  const [roleQuery, setRoleQuery] = useState("Data Analyst");
  const [location, setLocation] = useState("India");
  const [limit, setLimit] = useState(12);
  const fileLabel = useMemo(() => (file ? file.name : "Upload PDF, DOCX, or TXT"), [file]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      return;
    }
    await onSubmit({ file, roleQuery, location, limit });
  }

  return (
    <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft transition-colors duration-300 md:p-8 dark:border-white/10 dark:bg-white/[0.04]">
      <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
        <div className="space-y-2">
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate dark:text-slate-400">Analyzer</p>
          <h2 className="font-display text-3xl text-ink dark:text-slate-50">Run a role-fit review</h2>
        </div>
        <div className="rounded-full border border-ink/10 bg-mist/85 px-4 py-2 text-sm text-slate-700 transition-colors duration-300 dark:border-white/10 dark:bg-white/[0.04] dark:text-slate-300">
          Location-aware live job sampling with evidence-backed scoring
        </div>
      </div>
      <form className="grid gap-5" onSubmit={handleSubmit}>
        <label className="group flex cursor-pointer flex-col gap-3 rounded-[1.6rem] border border-dashed border-sea/60 bg-mist/80 p-6 transition hover:border-sea hover:bg-white dark:border-sea/40 dark:bg-[#0f1c25] dark:hover:bg-[#12212b]">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="text-sm font-semibold text-ink dark:text-slate-100">Resume File</span>
            <span className="rounded-full border border-ink/10 bg-white/80 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-700 dark:border-white/10 dark:bg-white/[0.04] dark:text-slate-300">
              One file per run
            </span>
          </div>
          <span className="text-lg font-medium text-ink dark:text-slate-100">{fileLabel}</span>
          <span className="text-sm leading-6 text-slate-600 dark:text-slate-400">
            Use a clean PDF, DOCX, or TXT export so the parser can keep sections, dates, and skill evidence intact.
          </span>
          <input
            className="hidden"
            type="file"
            accept=".pdf,.docx,.txt"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>

        <div className="grid gap-4 xl:grid-cols-[1.15fr_1fr_0.95fr]">
          <label className="flex h-full flex-col rounded-[1.5rem] border border-ink/10 bg-mist/65 p-4 transition-colors duration-300 dark:border-white/10 dark:bg-white/[0.03]">
            <span className="text-sm font-semibold text-ink dark:text-slate-100">Target Role</span>
            <input
              className="mt-3 rounded-2xl border border-ink/10 bg-white px-4 py-3 text-ink outline-none ring-0 transition focus:border-sea dark:border-white/10 dark:bg-[#0f1c25] dark:text-slate-50 dark:placeholder:text-slate-500"
              value={roleQuery}
              onChange={(event) => setRoleQuery(event.target.value)}
              placeholder="Data Analyst"
            />
            <span className="mt-3 min-h-[3.5rem] text-sm leading-6 text-slate-600 dark:text-slate-400">
              Start with the exact hiring title you want the review calibrated against so the market sample stays role-specific.
            </span>
          </label>

          <label className="flex h-full flex-col rounded-[1.5rem] border border-ink/10 bg-mist/65 p-4 transition-colors duration-300 dark:border-white/10 dark:bg-white/[0.03]">
            <span className="text-sm font-semibold text-ink dark:text-slate-100">Location</span>
            <input
              className="mt-3 rounded-2xl border border-ink/10 bg-white px-4 py-3 text-ink outline-none transition focus:border-sea dark:border-white/10 dark:bg-[#0f1c25] dark:text-slate-50 dark:placeholder:text-slate-500"
              value={location}
              onChange={(event) => setLocation(event.target.value)}
              placeholder="India"
            />
            <span className="mt-3 min-h-[3.5rem] text-sm leading-6 text-slate-600 dark:text-slate-400">
              We prefer local or region-fit jobs first, then widen to global remote only if the live market is sparse.
            </span>
          </label>

          <label className="flex h-full flex-col rounded-[1.5rem] border border-ink/10 bg-mist/65 p-4 transition-colors duration-300 dark:border-white/10 dark:bg-white/[0.03]">
            <span className="text-sm font-semibold text-ink dark:text-slate-100">Listings to sample</span>
            <input
              className="mt-3 rounded-2xl border border-ink/10 bg-white px-4 py-3 text-ink outline-none transition focus:border-sea dark:border-white/10 dark:bg-[#0f1c25] dark:text-slate-50 dark:placeholder:text-slate-500"
              type="number"
              min={5}
              max={20}
              value={limit}
              onChange={(event) => setLimit(Number(event.target.value) || 12)}
            />
            <span className="mt-3 min-h-[3.5rem] text-sm leading-6 text-slate-600 dark:text-slate-400">
              Target live-market depth for ranking. Dense roles can surface up to 6 live matches in the review.
            </span>
          </label>
        </div>

        <button
          className="inline-flex min-h-[3.75rem] items-center justify-center rounded-full bg-ink px-6 py-3 text-lg font-semibold text-white transition hover:bg-sea hover:text-ink disabled:cursor-not-allowed disabled:opacity-60 dark:bg-sea dark:text-ink dark:hover:bg-[#81ddd3]"
          type="submit"
          disabled={loading || !file}
        >
          {loading ? "Reviewing..." : "Analyze Resume"}
        </button>
      </form>
    </section>
  );
}
