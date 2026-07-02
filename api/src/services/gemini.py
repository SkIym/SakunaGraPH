from google import genai

from src.config import settings
from src.services.common import ServiceError

_client: genai.Client | None = None


def get_gemini_model() -> str:
    if not settings.gemini_model:
        raise ServiceError(503, "GEMINI_MODEL is not configured.")
    return settings.gemini_model


def get_gemini_client() -> genai.Client:
    if not settings.gemini_api_key:
        raise ServiceError(503, "GEMINI_API_KEY is not configured.")

    global _client
    if _client is None:
        _client = genai.Client(api_key=settings.gemini_api_key)
    return _client
