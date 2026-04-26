import { DragEvent, FormEvent, useMemo, useState } from "react";

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
const LOCATION_OPTIONS = ["Global", "India", "Remote", "United States", "Europe", "APAC"];

export function UploadPanel({ loading, onSubmit }: UploadPanelProps) {
  const [file, setFile] = useState<File | null>(null);
  const [roleQuery, setRoleQuery] = useState("Data Analyst");
  const [location, setLocation] = useState("India");
  const [limit, setLimit] = useState(18);
  const [isDragging, setIsDragging] = useState(false);
  const fileLabel = useMemo(() => (file ? file.name : "Upload your resume"), [file]);
  const fileMeta = file ? `${(file.size / 1024 / 1024).toFixed(2)} MB selected` : "PDF or DOCX";

  function selectFile(nextFile?: File | null) {
    if (!nextFile) {
      return;
    }
    setFile(nextFile);
  }

  function handleDrop(event: DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    setIsDragging(false);
    selectFile(event.dataTransfer.files?.[0]);
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      return;
    }
    await onSubmit({ file, roleQuery, location, limit });
  }

  return (
    <section className="glass-panel overflow-hidden rounded-[2rem]">
      <div className="grid lg:grid-cols-[0.9fr_1.1fr]">
        <div className="ink-panel relative overflow-hidden border-b border-white/10 p-6 sm:p-8 lg:border-b-0 lg:border-r">
          <div className="absolute -left-24 top-10 h-56 w-56 rounded-full bg-cyan-300/10 blur-3xl" />
          <div className="absolute bottom-0 right-0 h-px w-2/3 bg-gradient-to-l from-cyan-300/35 to-transparent lg:h-2/3 lg:w-px lg:bg-gradient-to-b" />
          <div className="relative">
            <p className="font-mono text-xs font-bold uppercase tracking-[0.34em] text-cyan-100/65">Start Review</p>
            <h2 className="mt-3 font-display text-4xl font-extrabold leading-none tracking-[-0.055em] text-white sm:text-5xl">Review your resume for the role you want.</h2>
            <p className="mt-4 text-sm leading-7 text-white/74">
              Choose a target role so the review can compare your resume with relevant skills, experience expectations, and job market patterns.
            </p>
            <div className="mt-7 grid gap-3">
              {["Role matching", "Job market comparison", "Skill evidence review", "ATS readability check"].map((item) => (
                <div key={item} className="flex items-center gap-3 rounded-2xl border border-white/10 bg-slate-900/55 p-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.05)] transition hover:border-cyan-300/20 hover:bg-slate-900/75">
                  <span className="h-2.5 w-2.5 rounded-full border border-cyan-200/30 bg-cyan-300/70 shadow-[0_0_14px_rgba(103,232,249,0.18)]" />
                  <span className="text-sm font-semibold text-white/86">{item}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <form className="grid gap-5 p-5 sm:p-7 lg:p-8" onSubmit={handleSubmit}>
          <label
            className={`group relative flex min-h-[13rem] cursor-pointer flex-col justify-between overflow-hidden rounded-[1.6rem] border border-dashed p-6 shadow-[inset_0_1px_0_rgba(255,255,255,0.65)] transition dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] ${
              isDragging
                ? "border-cyan-500/70 bg-cyan-50/80 dark:border-cyan-300/60 dark:bg-cyan-300/10"
                : "border-slate-300/80 bg-white/55 hover:border-cyan-500/45 hover:bg-white/70 dark:border-slate-700/80 dark:bg-slate-950/35 dark:hover:border-cyan-300/35 dark:hover:bg-slate-900/45"
            }`}
            onDragLeave={() => setIsDragging(false)}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDrop={handleDrop}
          >
            <div className="absolute -right-16 -top-16 h-40 w-40 rounded-full bg-cyan-300/10 blur-2xl transition group-hover:scale-110" />
            <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-cyan-300/35 to-transparent opacity-70" />
            <div className="relative flex flex-wrap items-start justify-between gap-4">
              <div>
                <span className="pill">Resume file</span>
                <p className="mt-5 max-w-xl font-display text-3xl font-extrabold tracking-[-0.045em] text-ink dark:text-slate-50">{fileLabel}</p>
              </div>
              <span className="rounded-full border border-slate-900/10 bg-slate-950 px-4 py-2 text-xs font-black uppercase tracking-[0.18em] text-white shadow-sm dark:border-cyan-300/20 dark:bg-cyan-300/10 dark:text-cyan-100">
                {fileMeta}
              </span>
            </div>
            <p className="relative mt-5 max-w-2xl text-sm leading-6 text-slate-700 dark:text-slate-300">
              Drag and drop a resume here, or click to choose a file. A clean PDF or DOCX helps the system read sections, skills, experience, and projects more accurately.
            </p>
            <input className="hidden" type="file" accept=".pdf,.docx" onChange={(event) => selectFile(event.target.files?.[0])} />
          </label>

          <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr_0.7fr]">
            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Target role</span>
              <input className="field-control mt-3" value={roleQuery} onChange={(event) => setRoleQuery(event.target.value)} placeholder="Data Analyst" />
              <div className="mt-3 flex flex-wrap gap-2">
                {ROLE_SUGGESTIONS.map((role) => (
                  <button key={role} className="rounded-full border border-slate-900/10 bg-white/35 px-3 py-1 text-xs font-bold text-slate-700 transition hover:border-cyan-500/35 hover:bg-cyan-50 dark:border-white/10 dark:bg-white/[0.03] dark:text-slate-300 dark:hover:border-cyan-300/25 dark:hover:bg-cyan-300/10 dark:hover:text-cyan-50" onClick={() => setRoleQuery(role)} type="button">
                    {role}
                  </button>
                ))}
              </div>
            </label>

            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Market location</span>
              <select className="field-control mt-3 cursor-pointer" value={location} onChange={(event) => setLocation(event.target.value)} aria-label="Market location">
                {LOCATION_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-600 dark:text-slate-400">Choose a location to compare your resume with the most relevant job market.</p>
            </label>

            <label className="soft-card rounded-[1.5rem] p-4">
              <span className="text-sm font-extrabold text-ink dark:text-slate-100">Job listings to compare</span>
              <input className="field-control mt-3" type="number" min={5} max={20} value={limit} onChange={(event) => setLimit(Number(event.target.value) || 18)} />
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-600 dark:text-slate-400">More listings can improve market coverage, though live sources may return fewer results at times.</p>
            </label>
          </div>

          <button className="primary-button w-full" type="submit" disabled={loading || !file}>
            {loading ? "Reviewing resume..." : "Start Resume Analysis"}
          </button>
        </form>
      </div>
    </section>
  );
}
