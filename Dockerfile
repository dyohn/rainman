FROM python:3.13-slim

WORKDIR /app

# Install package dependencies before copying source so that this layer
# is cached across code-only rebuilds.
COPY pyproject.toml ./
COPY src/ ./src/
COPY config/ ./config/

RUN pip install --no-cache-dir .

# WAL is written to /data so the host can mount a volume there.
# CLUSTER_CONFIG points to the config file baked into the image.
ENV WAL_PATH=/data/wal.jsonl
ENV CLUSTER_CONFIG=/app/config/cluster.json

VOLUME ["/data"]
EXPOSE 8000

CMD ["uvicorn", "rainman.node.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
