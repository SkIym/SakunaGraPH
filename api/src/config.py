from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "SakunaGraPH API"
    app_version: str = "0.1.0"

    cors_origins: list[str] = ["*"]

    local_llm_base_url: str = "http://127.0.0.1:1234"
    local_llm_chat_path: str = "/api/v1/chat"
    local_llm_model: str = "google/gemma-4-e4b"
    local_llm_timeout: float = 120.0
    local_llm_store: bool = False

    graphdb_endpoint: str = "http://localhost:7200/repositories/sakunagraph"


settings = Settings()
