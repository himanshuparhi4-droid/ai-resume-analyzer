# Vercel Frontend Deploy

This project deploys best with:

- frontend on Vercel
- backend on Render or Railway
- database on Supabase

That split is the most reliable path for the current stack because the frontend is a Vite app, while the backend is a long-running FastAPI analysis service with file uploads, model work, and optional OCR.

## 1. Deploy the backend first

Use Render or Railway for the backend.

Required backend environment variables:

```env
DATABASE_URL=postgresql://postgres.[project-ref]:YOUR_PASSWORD@aws-0-ap-south-1.pooler.supabase.com:6543/postgres?sslmode=require
CORS_ORIGINS=["https://YOUR-FRONTEND.vercel.app"]
FRONTEND_BASE_URL=https://YOUR-FRONTEND.vercel.app
SECRET_KEY=replace-with-a-long-random-secret
ENVIRONMENT=production
LLM_PROVIDER=disabled
ENABLE_OCR=false
```

Recommended production notes:

- start with `LLM_PROVIDER=disabled`
- start with `ENABLE_OCR=false`
- add `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `USAJOBS_API_KEY`, and `USAJOBS_USER_AGENT` only after the core deployment is stable
- verify the backend works before touching Vercel:

```text
https://YOUR-BACKEND/api/v1
https://YOUR-BACKEND/docs
```

## 2. Deploy the frontend to Vercel

### Dashboard path

1. Push the repo to GitHub.
2. In Vercel, click `Add New Project`.
3. Import the repository.
4. Set `Root Directory` to:

```text
frontend
```

5. Framework Preset should be detected as `Vite`.
6. Add this environment variable in Vercel:

```env
VITE_API_BASE_URL=https://YOUR-BACKEND/api/v1
```

7. Deploy.

### Important Vercel setting

If you later move to a monorepo-style multi-service Vercel setup, do not do that until the current split deployment is stable. For this project today, the frontend-only Vercel deployment is the safer production path.

## 3. Post-deploy checks

After Vercel finishes:

1. Open the Vercel site.
2. Register a new account.
3. Upload a resume.
4. Confirm the browser network request for `/analyses/resume` goes to your backend domain, not localhost.
5. Confirm the backend allows the Vercel domain in `CORS_ORIGINS`.

## 4. Vercel CLI option

If you prefer CLI:

```powershell
cd C:\Users\KIIT\Desktop\codex\ai-resume-analyzer\frontend
vercel login
vercel
```

For production:

```powershell
vercel --prod
```

When prompted:

- link the correct Vercel account/team
- choose the imported project
- keep the project root as `frontend`

## 5. Common issues

### Frontend loads but analysis fails

Check:

- `VITE_API_BASE_URL` is set to the real backend URL
- backend `CORS_ORIGINS` includes the Vercel domain
- backend is not still pointing to SQLite for production

### Site still talks to localhost

That means the Vercel environment variable was missing during build. Add `VITE_API_BASE_URL`, then redeploy.

### Backend is too slow

Start with:

```env
LLM_PROVIDER=disabled
ENABLE_OCR=false
```

Then optimize before turning advanced features back on.
