from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "SakunaGraPH API"
    app_version: str = "0.1.0"

    cors_origins: list[str] = ["*"]

    local_llm_base_url: str = "http://localhost:11434"
    local_llm_model: str = "llama3.2"
    local_llm_timeout: float = 120.0
    local_llm_keep_alive: str = "5m"

    graphdb_endpoint: str = "http://localhost:7200/repositories/SakunaGraph"


settings = Settings()
