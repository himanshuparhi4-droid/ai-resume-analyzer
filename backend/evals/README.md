# Benchmark Template

This folder is for evaluation, not production serving.

Included now:
- `benchmark_cases.json`: seeded evaluation set with labeled role-fit expectations
- `run_benchmark.py`: async runner that checks score ranges plus required matched/missing skills
- `run_live_fetch_matrix.py`: production-mode fetch harness that reports provider plan, per-provider raw/matched counts, underfill cause, and final live titles for representative role families
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
