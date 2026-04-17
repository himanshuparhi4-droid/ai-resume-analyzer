# Beginner Setup Guide

## 1. Install these on your laptop

### Required
- Python 3.11 or 3.12
- Node.js 20 LTS
- Git
- VS Code

### Optional but recommended
- PostgreSQL or Supabase account
- Ollama for free local AI
- Tesseract OCR if you want scanned resume support
- Poppler if you want OCR on PDF pages

## 2. Python packages

Inside `backend` install:
- `requirements.txt`
- `requirements-ml.txt`

## 3. Frontend packages

Inside `frontend` run `npm install`

## 4. If you want free local AI

Install Ollama and run:
- `ollama pull llama3.2:3b`

Then in `backend/.env` set:
- `LLM_PROVIDER=ollama`
- `OLLAMA_MODEL=llama3.2:3b`

## 5. If you want OCR

Install Tesseract OCR.
On Windows, also install Poppler if PDF OCR is enabled.
Then set in `backend/.env`:
- `ENABLE_OCR=true`
- `TESSERACT_CMD=C:\\Program Files\\Tesseract-OCR\\tesseract.exe`
- `POPPLER_PATH=C:\\path\\to\\poppler\\Library\\bin`

## 6. If you want stronger job coverage

Create API credentials for:
- Adzuna
- USAJOBS (optional)

Add them to `backend/.env`.

## 7. Local run order

### Terminal 1
- `cd backend`
- `python -m venv .venv`
- `. .venv/Scripts/Activate.ps1`
- `pip install -r requirements.txt`
- `pip install -r requirements-ml.txt`
- `copy .env.example .env`
- `uvicorn app.main:app --reload`

### Terminal 2
- `cd frontend`
- `npm install`
- `copy .env.example .env`
- `npm run dev`

## 8. Open in browser
- Frontend: `http://localhost:5173`
- Backend docs: `http://localhost:8000/docs`

## 9. What you should do first as a beginner
- First run it with SQLite and `LLM_PROVIDER=disabled`
- Then enable Ollama
- Then move database to Supabase/Postgres
- Then deploy frontend to Vercel and backend to Render or Railway

## 10. If you want a real hosted product with Supabase
- Create a Supabase project
- Go to `Project Settings -> Database`
- Copy the pooled connection string
- Put it in `DATABASE_URL`
- Keep `sslmode=require` in that URL
- Install backend packages again after pulling the latest code because Postgres now uses `psycopg`
- For deployment, do not use local Ollama. Set `LLM_PROVIDER=disabled` or switch to a hosted model provider
- Set `CORS_ORIGINS` to your real frontend URL after deployment

## 11. Why local Ollama is not enough for a global website
- Ollama runs on your own computer
- A public website on Vercel/Render cannot use the Ollama model from your laptop
- For worldwide users, either:
- use rule-based summaries first
- use OpenAI or another hosted inference API
- or host your own GPU/model server later
