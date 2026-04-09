FROM python:3.11-slim

WORKDIR /app

# ── System deps for Playwright / Chromium headless browser ───────────────────
# Required for auto daily token refresh (headless Kite login)
RUN apt-get update && apt-get install -y \
    wget curl gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps + Playwright browser (same interpreter via python -m) ────────
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "pyotp==2.9.0" \
    && python -m playwright install --with-deps chromium

# ── App source ────────────────────────────────────────────────────────────────
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# Ensure data dir exists (overlaid by Fly volume / Railway volume at runtime)
RUN mkdir -p /app/backend/data

ENV HOST=0.0.0.0
ENV PORT=8080

WORKDIR /app/backend

EXPOSE 8080

CMD ["python", "main.py"]
