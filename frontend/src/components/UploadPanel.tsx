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

const ROLE_SUGGESTIONS = ["Data Analyst", "Web Developer", "SOC Analyst", "Frontend Developer"];

export function UploadPanel({ loading, onSubmit }: UploadPanelProps) {
  const [file, setFile] = useState<File | null>(null);
  const [roleQuery, setRoleQuery] = useState("Data Analyst");
  const [location, setLocation] = useState("Global");
  const [limit, setLimit] = useState(12);
  const fileLabel = useMemo(() => (file ? file.name : "Drop a resume export here"), [file]);
  const fileMeta = file ? `${(file.size / 1024 / 1024).toFixed(2)} MB selected` : "PDF, DOCX, or TXT";

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      return;
    }
    await onSubmit({ file, roleQuery, location, limit });
  }

  return (
    <section className="glass-panel overflow-hidden rounded-[2.25rem]">
      <div className="grid lg:grid-cols-[0.9fr_1.1fr]">
        <div className="ink-panel relative p-6 sm:p-8">
          <div className="absolute -bottom-20 -left-20 h-56 w-56 rounded-full bg-sea/20 blur-3xl" />
          <div className="relative">
            <p className="font-mono text-xs font-bold uppercase tracking-[0.34em] text-white/65">Run Review</p>
            <h2 className="mt-3 font-display text-4xl font-extrabold leading-none tracking-[-0.055em] text-white sm:text-5xl">Calibrate against the role you actually want.</h2>
            <p className="mt-4 text-sm leading-7 text-white/74">
              The role field is normalized for casing, spacing, and aliases, but exact intent still matters. Better role input creates better provider planning and better missing-skill judgment.
            </p>
            <div className="mt-7 grid gap-3">
              {["Role meaning normalization", "Live provider diagnostics", "Weak proof vs true missing", "Parser confidence scoring"].map((item) => (
                <div key={item} className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.07] p-3">
                  <span className="h-2.5 w-2.5 rounded-full bg-sea" />
                  <span className="text-sm font-semibold text-white/86">{item}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <form className="grid gap-5 p-5 sm:p-7 lg:p-8" onSubmit={handleSubmit}>
          <label className="group relative flex min-h-[13rem] cursor-pointer flex-col justify-between overflow-hidden rounded-[1.75rem] border border-dashed border-sea/60 bg-sea/10 p-6 transition hover:border-sea hover:bg-sea/15 dark:bg-sea/10">
            <div className="absolute -right-16 -top-16 h-40 w-40 rounded-full bg-sea/20 blur-2xl transition group-hover:scale-125" />
            <div className="relative flex flex-wrap items-start justify-between gap-4">
              <div>
                <span className="pill">Resume file</span>
                <p className="mt-5 max-w-xl font-display text-3xl font-extrabold tracking-[-0.045em] text-ink dark:text-slate-50">{fileLabel}</p>
              </div>
              <span className="rounded-full bg-ink px-4 py-2 text-xs font-black uppercase tracking-[0.18em] text-white dark:bg-sea dark:text-ink">
                {fileMeta}
              </span>
            </div>
            <p className="relative mt-5 max-w-2xl text-sm leading-6 text-slate-700 dark:text-slate-300">
              Cleaner exports improve parser confidence, but the analyzer now separates parse risk from resume strength so a good multi-column resume is not punished like a broken file.
            </p>
            <input className="hidden" type="file" accept=".pdf,.docx,.txt" onChange={(event) => setFile(event.target.files?.[0] ?? null)} />
          </label>

          <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr_0.7fr]">
            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Target role</span>
              <input className="field-control mt-3" value={roleQuery} onChange={(event) => setRoleQuery(event.target.value)} placeholder="Data Analyst" />
              <div className="mt-3 flex flex-wrap gap-2">
                {ROLE_SUGGESTIONS.map((role) => (
                  <button key={role} className="rounded-full border border-ink/10 px-3 py-1 text-xs font-bold text-slate-700 transition hover:border-sea hover:bg-sea/10 dark:border-white/10 dark:text-slate-300" onClick={() => setRoleQuery(role)} type="button">
                    {role}
                  </button>
                ))}
              </div>
            </label>

            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Market location</span>
              <input className="field-control mt-3" value={location} list="location-suggestions" onChange={(event) => setLocation(event.target.value)} placeholder="Global" />
              <datalist id="location-suggestions">
                <option value="Global" />
                <option value="India" />
                <option value="Remote" />
                <option value="United States" />
                <option value="Europe" />
                <option value="APAC" />
              </datalist>
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-600 dark:text-slate-400">Global keeps the sample broad. Use India or Remote when you want a sharper market bias.</p>
            </label>

            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Sample size</span>
              <input className="field-control mt-3" type="number" min={5} max={20} value={limit} onChange={(event) => setLimit(Number(event.target.value) || 12)} />
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-600 dark:text-slate-400">Higher targets help dense roles, while sparse roles show confidence warnings.</p>
            </label>
          </div>

          <button className="primary-button w-full" type="submit" disabled={loading || !file}>
            {loading ? "Building signal map..." : "Analyze Resume"}
          </button>
        </form>
      </div>
    </section>
  );
}
