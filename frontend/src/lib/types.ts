export type ScoreBreakdown = {
  skill_match: number;
  semantic_match: number;
  experience_match: number;
  market_demand: number;
  resume_quality: number;
  ats_compliance: number;
};

export type RecommendationItem = {
  title: string;
  detail: string;
  impact: string;
};

export type JobMatch = {
  source: string;
  title: string;
  company: string;
  location: string;
  remote: boolean;
  url: string;
  description: string;
  preview?: string;
  relevance_score?: number;
  tags: string[];
  normalized_data: {
    skills?: string[];
    skill_evidence?: {
      skill: string;
      matched_text?: string;
      snippet: string;
      source?: string;
      mode?: string;
    }[];
    skill_extraction_mode?: string;
    [key: string]: unknown;
  };
};

export type SkillDetail = {
  skill: string;
  market_share: number;
  resume_evidence?: string[];
  job_evidence?: {
    title: string;
    company: string;
    snippet: string;
  }[];
};

export type AnalysisResponse = {
  analysis_id: string;
  role_query: string;
  overall_score: number;
  breakdown: ScoreBreakdown;
  matched_skills: string[];
  missing_skills: { skill: string; share: number }[];
  matched_skill_details?: SkillDetail[];
  missing_skill_details?: SkillDetail[];
  market_skill_frequency?: { skill: string; count: number; share: number }[];
  top_job_matches: JobMatch[];
  analysis_context?: {
    market_source?: string;
    live_job_count?: number;
    used_role_baseline?: boolean;
    message?: string;
  };
  resume_archetype?: {
    type?: string;
    label?: string;
    confidence?: number;
    reasons?: string[];
  };
  component_feedback?: Partial<Record<keyof ScoreBreakdown, string[]>>;
  recommendations: RecommendationItem[];
  ai_summary: {
    mode?: string;
    target_role?: string;
    strengths?: string[];
    weaknesses?: string[];
    next_steps?: string[] | Record<string, string> | string;
  };
  resume_sections: Record<string, string>;
  resume_preview: string;
  share_token?: string | null;
  created_at?: string | null;
};

export type User = {
  id: string;
  email: string;
  full_name: string;
  created_at: string;
};

export type AuthResponse = {
  access_token: string;
  token_type: string;
  user: User;
};

export type HistoryItem = {
  analysis_id: string;
  role_query: string;
  overall_score: number;
  created_at: string;
  share_token?: string | null;
};

export type ComparisonResponse = {
  current_analysis_id: string;
  previous_analysis_id?: string | null;
  score_delta: number;
  component_deltas: Record<string, number>;
  summary: string;
};
