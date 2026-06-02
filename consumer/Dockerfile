FROM python:3.10-slim

WORKDIR /app

# Runtime deps for clickhouse-connect; rocksdict ships prebuilt wheels
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create Faust data directory (tables stored under datadir/v1/tables/)
RUN mkdir -p /app/faust-data

# Run Faust worker
CMD ["faust", "-A", "app.main", "worker", "-l", "info"]