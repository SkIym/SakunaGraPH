# syntax=docker/dockerfile:1

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

RUN groupadd --system --gid 10001 sakuna \
    && useradd --system --uid 10001 --gid sakuna --home-dir /app --no-create-home sakuna

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt
COPY --chown=sakuna:sakuna src ./src

USER sakuna
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
