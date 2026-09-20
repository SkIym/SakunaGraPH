from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


API_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = API_DIR / ".env"

# Boto3 reads credentials from the process environment, while Pydantic's
# ``env_file`` support only populates Settings fields. Load the API-local file
# first so AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or the Bedrock bearer token
# participate in the normal AWS SDK credential chain as well.
load_dotenv(ENV_FILE, override=False)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "SakunaGraPH API"
    app_version: str = "0.1.0"

    cors_origins: list[str] = ["*"]

    aws_region: str = "ap-southeast-1"
    bedrock_model_id: str = ""
    bedrock_endpoint_url: str | None = None
    bedrock_connect_timeout: float = Field(default=10.0, gt=0, le=120)
    bedrock_read_timeout: float = Field(default=120.0, gt=0, le=600)
    bedrock_max_attempts: int = Field(default=3, ge=1, le=10)
    bedrock_max_concurrency: int = Field(default=8, ge=1, le=100)
    bedrock_max_tokens: int = Field(default=4_096, ge=1, le=32_768)
    bedrock_temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    bedrock_top_p: float = Field(default=1.0, gt=0.0, le=1.0)
    bedrock_streaming: bool = True

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
