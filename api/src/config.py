from pydantic import Field, SecretStr
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
    graphdb_read_only_username: str | None = None
    graphdb_read_only_password: SecretStr | None = None
    graphdb_query_timeout_seconds: float = Field(default=30.0, gt=0, le=300)

    ask_sparql_max_length: int = Field(default=30_000, ge=1_000, le=100_000)
    ask_sparql_max_triples: int = Field(default=80, ge=1, le=500)
    ask_sparql_max_optionals: int = Field(default=30, ge=0, le=200)
    ask_sparql_max_unions: int = Field(default=12, ge=0, le=100)
    ask_sparql_max_subqueries: int = Field(default=12, ge=0, le=100)
    ask_result_row_limit: int = Field(default=100, ge=1, le=1_000)


settings = Settings()
