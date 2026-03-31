FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application configuration and code
COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/pipeline_orchestrator.py"]
