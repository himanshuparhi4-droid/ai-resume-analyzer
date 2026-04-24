# Resume Analyzer Competitive Research

Reviewed: 2026-04-24

This note captures product patterns from major resume analysis tools and maps them to concrete improvements for this project.

## Sources Reviewed

- Jobscan: https://www.jobscan.co/
- SkillSyncer: https://skillsyncer.com/
- Enhancv Resume Checker: https://enhancv.com/resources/resume-checker/
- Kickresume Resume Checker: https://www.kickresume.com/en/resume-checker/
- Rezi AI Keyword Targeting: https://www.rezi.ai/rezi-docs/ai-keyword-targeting-explained
- Teal job matching docs: https://help.tealhq.com/en/articles/9923251-using-job-matching-resume-curation

## Common Patterns

1. Job-specific matching is the main scoring frame.
   Tools compare a resume against a target job description or role and surface missing keywords, skills, title alignment, education fit, and format risks.

2. Scores are guidance, not a real ATS truth.
   Strong tools warn that no universal ATS score exists. The better pattern is to show match rate, parse quality, and confidence separately.

3. Missing keywords are not enough.
   Rezi and SkillSyncer-style flows add placement guidance: skills section, summary, or bullet points. This maps well to our weak-proof vs missing-skill split.

4. Parser quality matters.
   Enhancv explicitly frames part of the score around how much resume content can be parsed. This supports our parser-confidence layer.

5. Human/recruiter criteria still matter.
   Kickresume-style checks include content completeness, space usage, action verbs, overused phrases, and recruiter readability.

6. Auto-tailoring needs guardrails.
   Teal-style curation can activate/deactivate relevant resume content. For this project, that should be evidence-based and avoid stuffing.

## Implementation Direction

- Keep facts rule-grounded: parsed skills, sections, dates, bullets, and live job evidence.
- Use confidence labels for uncertainty:
  - Parser confidence
  - Market sample confidence
  - Skill evidence confidence
- Separate present skills from weakly proven skills.
- Show evidence snippets for every important recommendation.
- Learn calibration from labeled examples after enough real cases exist.
- Treat provider underfill on Render as market uncertainty, not as a fully confident sample.

## Implemented In This Pass

- Added parser confidence in analysis context.
- Added market confidence in analysis context.
- Reduced ATS over-penalty when a multi-column PDF still has strong recovered structure.
- Fixed merged-line section splitting so education does not get swallowed by experience.
- Added regression tests for multi-column ATS calibration and education section recovery.

## Next Implementation Targets

1. UI confidence panel for parser and market confidence.
2. Skill evidence graph with `skills_only`, `project`, `experience`, and `missing` states.
3. Feedback buttons on recommendations: `correct`, `wrong`, `already present`, `not relevant`.
4. Gold evaluation dataset with labeled parse, skill, recommendation, and ATS-band expectations.
5. Adaptive provider health scoring for Render based on recent timeout and success diagnostics.
