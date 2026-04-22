from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    port: int = Field(default=8001, validation_alias="PORT")
    host: str = Field(default="0.0.0.0", validation_alias="HOST")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    internal_secret: str = Field(
        default="titlis-ai-internal-secret-dev",
        validation_alias="TITLIS_AI_INTERNAL_SECRET",
    )

    titlis_api_url: str = Field(
        default="http://titlis-api:8080",
        validation_alias="TITLIS_API_URL",
    )

    rag_enabled: bool = Field(default=True, validation_alias="RAG_ENABLED")
    rag_top_k: int = Field(default=3, validation_alias="RAG_TOP_K")

    # Langfuse — LLM observability (feature flag)
    langfuse_enabled: bool = Field(default=False, validation_alias="LANGFUSE_ENABLED")
    langfuse_public_key: str = Field(default="", validation_alias="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: str = Field(default="", validation_alias="LANGFUSE_SECRET_KEY")
    langfuse_base_url: str = Field(
        default="https://cloud.langfuse.com",
        validation_alias="LANGFUSE_BASE_URL",
    )

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
