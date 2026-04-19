from functools import lru_cache

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "AI Resume Analyzer"
    api_prefix: str = "/api/v1"
    database_url: str = "sqlite:///./resume_analyzer.db"
    cors_origins: list[str] = ["http://localhost:5173"]
    frontend_base_url: str = "http://localhost:5173"

    secret_key: str = "change-me-in-production"
    access_token_expire_minutes: int = 10080

    default_job_source: str = "auto"
    remotive_base_url: str = "https://remotive.com/api/remote-jobs"
    remoteok_base_url: str = "https://remoteok.com/api"
    themuse_base_url: str = "https://www.themuse.com/api/public/jobs"
    themuse_api_key: str | None = None
    indianapi_jobs_base_url: str = "https://jobs.indianapi.in/jobs"
    indianapi_api_key: str | None = None
    jooble_base_url: str = "https://jooble.org/api"
    jooble_api_key: str | None = None
    jobicy_base_url: str = "https://jobicy.com/api/v2/remote-jobs"
    arbeitnow_base_url: str = "https://www.arbeitnow.com/api/job-board-api"
    adzuna_base_url: str = "https://api.adzuna.com/v1/api/jobs"
    adzuna_app_id: str | None = None
    adzuna_app_key: str | None = None
    adzuna_country: str = "in"
    usajobs_base_url: str = "https://data.usajobs.gov/api/search"
    usajobs_api_key: str | None = None
    usajobs_user_agent: str | None = None

    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    enable_embeddings: bool = True
    spacy_model: str = "en_core_web_sm"

    llm_provider: str = "disabled"
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:3b"
    llm_summary_timeout_seconds: float = 15.0
    job_request_timeout_seconds: float = 8.0
    job_fetch_timeout_seconds: float = 25.0
    enable_live_market_fetch: bool = True
    production_live_fetch_minimum: int = 6
    production_live_fetch_maximum: int = 8
    production_live_display_minimum: int = 6
    production_live_candidate_fetch: int = 60
    production_live_cache_ttl_minutes: int = 180

    fetch_limit: int = 12
    job_cache_ttl_minutes: int = 360
    enable_job_cache: bool = True
    sync_default_queries: list[str] = ["python developer", "data analyst", "full stack developer"]
    sync_default_locations: list[str] = ["India", "Remote"]
    sync_interval_minutes: int = 360
    enable_internal_scheduler: bool = False
    sync_secret: str = "local-sync-secret"

    enable_ocr: bool = False
    tesseract_cmd: str | None = None
    poppler_path: str | None = None
    enable_pdf_reports: bool = True

    log_level: str = "INFO"
    environment: str = "development"

    @computed_field
    @property
    def cors_origin_regex(self) -> str | None:
        # Vercel preview deployments get unique *.vercel.app hostnames.
        # Allowing that family keeps production + preview builds working
        # without manually updating CORS for every redeploy.
        origins = [self.frontend_base_url, *self.cors_origins]
        if any("vercel.app" in origin for origin in origins):
            return r"^https://.*\.vercel\.app$"
        return None

    @computed_field
    @property
    def has_adzuna_credentials(self) -> bool:
        return bool(self.adzuna_app_id and self.adzuna_app_key)

    @computed_field
    @property
    def has_indianapi_credentials(self) -> bool:
        return bool(self.indianapi_api_key)

    @computed_field
    @property
    def has_jooble_credentials(self) -> bool:
        return bool(self.jooble_api_key)

    @computed_field
    @property
    def has_usajobs_credentials(self) -> bool:
        return bool(self.usajobs_api_key and self.usajobs_user_agent)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
