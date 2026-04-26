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
  const [jobDescription, setJobDescription] = useState("");
  const [isDragging, setIsDragging] = useState(false);
  const fileLabel = useMemo(() => (file ? file.name : "No resume uploaded yet"), [file]);
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
    <section className="rounded-2xl border border-white/10 bg-white/5 p-6 shadow-xl sm:p-8">
        <form className="grid gap-6" onSubmit={handleSubmit}>
          <label
            className={`group relative grid min-h-[18rem] cursor-pointer place-items-center overflow-hidden rounded-2xl border-2 border-dashed p-10 text-center transition ${
              isDragging
                ? "border-cyan-300/70 bg-cyan-300/10"
                : "border-white/15 bg-white/[0.03] hover:border-cyan-300/45 hover:bg-white/[0.06]"
            }`}
            onDragLeave={() => setIsDragging(false)}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDrop={handleDrop}
          >
            <div className="absolute -right-16 -top-16 h-44 w-44 rounded-full bg-cyan-300/10 blur-3xl transition group-hover:scale-110" />
            <div className="relative">
              <span className="mx-auto grid h-14 w-14 place-items-center rounded-2xl border border-cyan-300/20 bg-cyan-300/10 text-xl font-black text-cyan-100">
                +
              </span>
              <p className="mt-5 font-display text-3xl font-extrabold tracking-[-0.045em] text-white">{fileLabel}</p>
              <p className="mt-3 text-base leading-relaxed text-slate-300">
                Drag and drop your resume here, or choose a file to upload.
              </p>
              <p className="mt-4 text-sm font-bold uppercase tracking-[0.16em] text-cyan-100/80">{fileMeta}</p>
            </div>
            <input className="hidden" type="file" accept=".pdf,.docx" onChange={(event) => selectFile(event.target.files?.[0])} />
          </label>

          <div className="grid gap-4">
            <label className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
              <span className="text-sm font-extrabold text-slate-100">Target role</span>
              <input className="field-control mt-3" value={roleQuery} onChange={(event) => setRoleQuery(event.target.value)} placeholder="Data Analyst" />
              <div className="mt-3 flex flex-wrap gap-2">
                {ROLE_SUGGESTIONS.map((role) => (
                  <button key={role} className="rounded-full border border-white/10 bg-white/[0.05] px-3 py-1 text-xs font-bold text-slate-300 transition hover:border-cyan-300/35 hover:bg-cyan-300/10 hover:text-cyan-50" onClick={() => setRoleQuery(role)} type="button">
                    {role}
                  </button>
                ))}
              </div>
            </label>

            <label className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
              <span className="text-sm font-extrabold text-slate-100">Optional job description comparison</span>
              <textarea
                className="field-control mt-3 min-h-28 resize-y"
                value={jobDescription}
                onChange={(event) => setJobDescription(event.target.value)}
                placeholder="Paste a job description here if you want to compare your resume against a specific role."
              />
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-400">Optional for now. The role field still drives the analysis.</p>
            </label>

            <div className="grid gap-4 sm:grid-cols-[1fr_0.75fr]">
            <label className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
              <span className="text-sm font-extrabold text-slate-100">Market location</span>
              <select className="field-control mt-3 cursor-pointer" value={location} onChange={(event) => setLocation(event.target.value)} aria-label="Market location">
                {LOCATION_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-400">Choose a relevant job market.</p>
            </label>

            <label className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
              <span className="text-sm font-extrabold text-slate-100">Listings</span>
              <input className="field-control mt-3" type="number" min={5} max={20} value={limit} onChange={(event) => setLimit(Number(event.target.value) || 18)} />
              <p className="mt-3 text-xs font-semibold leading-5 text-slate-400">More listings can improve coverage.</p>
            </label>
            </div>
          </div>

          <button className="h-12 w-full rounded-xl bg-cyan-300 px-6 text-sm font-extrabold uppercase tracking-[0.14em] text-slate-950 shadow-xl shadow-cyan-950/20 transition hover:-translate-y-0.5 hover:bg-cyan-200 disabled:cursor-not-allowed disabled:opacity-55" type="submit" disabled={loading || !file}>
            {loading ? "Reviewing resume..." : "Start Resume Analysis"}
          </button>
        </form>
    </section>
  );
}
