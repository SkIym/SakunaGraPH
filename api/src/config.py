from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "SakunaGraPH API"
    app_version: str = "0.1.0"

    cors_origins: list[str] = ["*"]

    gemini_api_key: str = ""
    gemini_model: str = ""

    graphdb_endpoint: str = ""


settings = Settings()
