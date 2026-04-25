# Benchmark Template

This folder is for evaluation, not production serving.

Included now:
- `benchmark_cases.json`: seeded evaluation set with labeled role-fit expectations
- `run_benchmark.py`: async runner that checks score ranges plus required matched/missing skills
- `run_live_fetch_matrix.py`: production-mode fetch harness that reports provider plan, per-provider attempt rollups, raw/matched counts, stage outcomes, underfill cause, and final live titles for representative role families
- `run_resume_gold_eval.py`: public Hugging Face resume-row parser/skill eval harness for building a 50+ resume calibration set without scraping private resumes
- `human_review.csv`: still recommended next for faculty or recruiter review

Suggested columns for human review:
- analysis_id
- reviewer_name
- role_query
- parser_accuracy_1_to_5
- skill_match_quality_1_to_5
- recommendation_quality_1_to_5
- notes

Run the seeded benchmark locally with:

```powershell
.\.venv\Scripts\python.exe .\evals\run_benchmark.py
```

Run the live-fetch matrix locally with:

```powershell
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --location Global --limit 10
```

Useful live-fetch matrix variants:

```powershell
# Compact shell-friendly summary for the universal role sweep
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --preset universal --location Global --limit 10 --format pretty

# Production target sweep: 30 role types, 18 requested listings, flag anything under 10 live listings
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --preset universal --location India --limit 18 --min-live 10 --format pretty

# Broad market sweep: 50+ tech, business, finance, people, customer, marketing, and operations roles
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --preset broad50 --location India --limit 18 --min-live 10 --format pretty --write .\evals\latest-live-fetch-matrix-broad50-india.json

# Alias-heavy sweep to stress normalization and family recovery
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --preset aliases --location Global --limit 10 --format pretty

# Write the full JSON payload for later analysis
.\.venv\Scripts\python.exe .\evals\run_live_fetch_matrix.py --preset canonical --location Global --limit 10 --write .\evals\latest-live-fetch-matrix.json
```

Available presets:
- `core`: original representative matrix
- `canonical`: universal canonical role families
- `aliases`: spacing/casing/alias stress cases such as `DataScientist` and `SOC Analyst`
- `universal`: canonical + alias roles together
- `broad50`: 50+ tech and business role families used to guard the 10+ listing target
- `all`: `core` plus the universal sweep

The harness output is designed to answer three questions quickly:
- Did providers return enough raw listings?
- Did timeouts or budget skips happen before enough providers responded?
- Did the selector keep or reject the family-safe matches that upstream returned?

Run the 50-resume public parser/skill calibration eval with:

```powershell
.\.venv\Scripts\python.exe .\evals\run_resume_gold_eval.py --limit 50 --format pretty --write .\evals\latest-resume-gold-eval.json
```

The default source is `Majinuub/Resume_Parsing` on Hugging Face, which currently reports `apache-2.0` metadata via the Hub API. The script ignores the dataset's reasoning text and evaluates only resume input text against structured output fields.
