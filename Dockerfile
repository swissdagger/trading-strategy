# Single image for the trading system. Both processes (feed_process,
# signal_process) run from the SAME image — they share all the packages
# (feeds/, producer/, strategy/, execution/) and config.py. docker-compose
# selects which one each container runs via `command`.

FROM python:3.12-slim

# System deps: build tools for any wheels that need compiling (asyncpg, etc.).
# Kept minimal; remove build-essential after pip if image size matters.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer caching: deps change less often than code).
# From the hashed lock (scripts/lock.sh), so every dependency, transitive ones
# included, is the exact version CI tested; a tampered download fails.
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

# Then the application code.
COPY . .

# No default CMD — docker-compose sets `command` per service so one image
# serves both processes. Running the image directly does nothing useful on
# purpose; always launch via compose (or override the command).
#
# Unbuffered stdout so logs stream to `docker logs` / journald in real time.
ENV PYTHONUNBUFFERED=1
