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
    <section className="rounded-[2rem] border border-ink/10 bg-white p-6 shadow-soft md:p-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.35em] text-slate">Analyzer</p>
          <h2 className="mt-2 font-display text-3xl text-ink">Run a role-fit review</h2>
        </div>
        <div className="rounded-full bg-mist px-4 py-2 text-sm text-slate-700">Location-aware live job sampling with evidence-backed scoring</div>
      </div>
      <form className="grid gap-5" onSubmit={handleSubmit}>
        <label className="group flex cursor-pointer flex-col gap-3 rounded-[1.5rem] border border-dashed border-ink/20 bg-mist/80 p-5 transition hover:border-sea hover:bg-white">
          <span className="text-sm font-semibold text-ink">Resume File</span>
          <span className="text-sm text-slate-700">{fileLabel}</span>
          <input
            className="hidden"
            type="file"
            accept=".pdf,.docx,.txt"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>

        <div className="grid gap-4 md:grid-cols-3">
          <label className="grid gap-2">
            <span className="text-sm font-semibold text-ink">Target Role</span>
            <input
              className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 outline-none ring-0 transition focus:border-sea"
              value={roleQuery}
              onChange={(event) => setRoleQuery(event.target.value)}
              placeholder="Data Analyst"
            />
          </label>
          <label className="grid gap-2">
            <span className="text-sm font-semibold text-ink">Location</span>
            <input
              className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 outline-none transition focus:border-sea"
              value={location}
              onChange={(event) => setLocation(event.target.value)}
              placeholder="India"
            />
            <span className="text-xs leading-5 text-slate-500">We prefer local or region-fit jobs first, then widen to global remote only if the live market is sparse.</span>
          </label>
          <label className="grid gap-2">
            <span className="text-sm font-semibold text-ink">Listings to sample</span>
            <input
              className="rounded-2xl border border-ink/10 bg-mist px-4 py-3 outline-none transition focus:border-sea"
              type="number"
              min={5}
              max={20}
              value={limit}
              onChange={(event) => setLimit(Number(event.target.value) || 12)}
            />
            <span className="text-xs leading-5 text-slate-500">Target live-market depth for ranking. Dense roles can surface up to 6 live matches in the review.</span>
          </label>
        </div>

        <button
          className="inline-flex items-center justify-center rounded-full bg-ink px-6 py-3 font-semibold text-white transition hover:bg-sea hover:text-ink disabled:cursor-not-allowed disabled:opacity-60"
          type="submit"
          disabled={loading || !file}
        >
          {loading ? "Reviewing..." : "Analyze Resume"}
        </button>
      </form>
    </section>
  );
}
