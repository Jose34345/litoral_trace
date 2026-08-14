# Dockerfile Multi-Stage de Producción - Litoral Trace API + Satellite Worker
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --prefix=/install \
    -r requirements.txt


# ---------------------------------------------------------------------------
# Runtime image shared by:
#   - FastAPI API
#   - Durable satellite worker
#
# Service-specific commands and healthchecks belong in Docker Compose.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runner

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    FASTAPI_PORT=8000 \
    FASTAPI_HOST=0.0.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY . .

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Documentation only. Docker Compose decides which ports are reachable
# by each concrete service.
EXPOSE 8000 9108

# API remains the default image command.
# The worker service overrides this command in docker-compose.prod.yml.
CMD [
    "uvicorn",
    "main:app",
    "--host",
    "0.0.0.0",
    "--port",
    "8000",
    "--workers",
    "4"
]