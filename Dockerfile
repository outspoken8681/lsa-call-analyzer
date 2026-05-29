FROM python:3.12-slim

WORKDIR /app

# Minimal system packages needed before playwright --with-deps runs
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies (cached layer — only rebuilds when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium and every OS-level library it needs
RUN playwright install chromium --with-deps

# Application code
COPY . .

# Railway injects $PORT at runtime; fall back to 8000 for local Docker runs
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
