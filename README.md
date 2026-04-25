# AI Resume Analyzer

AI Resume Analyzer is a full-stack resume review app that parses an uploaded resume, detects skills and experience signals, compares them against live job listings, and returns ATS-style scoring, skill gaps, recommendations, and relevant job matches.

The project is intentionally trimmed to runtime essentials:

- `backend/` contains the FastAPI API, resume parser, scoring engine, job providers, and recommendation logic.
- `frontend/` contains the Vite React UI.
- `render.yaml` defines the Render backend service and optional cron sync.
- `.env` and `frontend/.env` are kept for local configuration.

## Current Behavior

- Uses a rules/evidence-based analysis pipeline by default.
- LLM usage is disabled unless explicitly configured with environment variables.
- Live job matching targets accurate listings first and caps display output at 15 jobs.
- Production is designed to stay lightweight enough for free Render limits.
- Job providers include Jooble, Adzuna, Greenhouse, Jobicy, Remotive, The Muse, Lever, RemoteOK, USAJobs, and Arbeitnow, depending on credentials/configuration.

## Project Layout

```text
.
|-- README.md
|-- render.yaml
|-- backend/
|   |-- requirements.txt
|   `-- app/
|       |-- main.py
|       |-- api/
|       |-- core/
|       |-- models/
|       |-- schemas/
|       |-- services/
|       `-- utils/
`-- frontend/
    |-- package.json
    |-- package-lock.json
    |-- vite.config.ts
    |-- tailwind.config.ts
    `-- src/
```

## Requirements

- Python 3.12.x for backend production parity.
- Node.js 18+ for frontend development/builds.
- Render environment variables for production backend deployment.

## Backend Setup

From the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r backend\requirements.txt
```

Run the API:

```powershell
$env:PYTHONPATH = "backend"
uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port 8000
```

Health check:

```text
http://localhost:8000/healthz
```

## Frontend Setup

```powershell
cd frontend
npm install
npm run dev
```

Default local frontend:

```text
http://localhost:5173
```

The frontend reads API URL from `frontend/.env`:

```env
VITE_API_BASE_URL=http://localhost:8000/api/v1
```

## Important Environment Variables

Backend reads `.env` through Pydantic settings. Key production variables:

```env
DATABASE_URL=
SECRET_KEY=
CORS_ORIGINS=["https://your-frontend-domain"]
FRONTEND_BASE_URL=https://your-frontend-domain
JOOBLE_API_KEY=
ADZUNA_APP_ID=
ADZUNA_APP_KEY=
LLM_PROVIDER=disabled
ENABLE_EMBEDDINGS=false
ENABLE_INTERNAL_SCHEDULER=false
```

Optional LLM settings if summaries/rewrite features are enabled later:

```env
LLM_PROVIDER=openai
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini
```

By default, LLM should remain disabled for low-cost deployment.

## Render Deployment

`render.yaml` deploys the backend from `backend/`:

```yaml
buildCommand: pip install -r requirements.txt
startCommand: uvicorn app.main:app --host 0.0.0.0 --port $PORT
healthCheckPath: /healthz
```

Required Render secrets/config:

- `DATABASE_URL`
- `CORS_ORIGINS`
- `FRONTEND_BASE_URL`
- `JOOBLE_API_KEY`
- `ADZUNA_APP_ID`
- `ADZUNA_APP_KEY`

Render can generate:

- `SECRET_KEY`
- `SYNC_SECRET`

## Accuracy And Performance Notes

- Do not reduce provider coverage, live job floor, candidate limits, or precision guards without revalidating job listing quality.
- Safe optimizations are mostly memoization, dedupe improvements, and compact diagnostics.
- Avoid training/fine-tuning on user resumes unless there is explicit consent and anonymization.
- The app does not automatically learn from repeated calls.
- Cache is useful for reliability, but live role accuracy should stay the priority.

## Useful Commands

Backend syntax check:

```powershell
python -m compileall -q backend\app
```

Minimal smoke test:

```powershell
python smoke_test.py
```

Frontend build:

```powershell
cd frontend
npm run build
```

Git status:

```powershell
git status --short
```

## Repository Hygiene

The repo intentionally excludes local/generated files:

- `.venv/`
- `node_modules/`
- `dist/`
- `__pycache__/`
- `*.db`
- `.env`
- `*.tsbuildinfo`

Keep only runtime source, dependency manifests, deployment config, and this README unless there is a clear reason to add development artifacts back.
