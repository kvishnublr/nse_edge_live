FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend + frontend
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# Ensure data directory exists (will be overlaid by fly volume at /data)
RUN mkdir -p /app/backend/data

ENV HOST=0.0.0.0
ENV PORT=8080

WORKDIR /app/backend

EXPOSE 8080

CMD ["python", "main.py"]
