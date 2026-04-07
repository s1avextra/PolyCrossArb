FROM python:3.14-slim AS base

# Install Rust toolchain for the latency engine
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential pkg-config libssl-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /app

# Install Python deps first (cached layer)
COPY pyproject.toml ./
RUN pip install --no-cache-dir uv && uv pip install --system ".[execution,dev]"

# Build Rust engine
COPY rust_engine/ rust_engine/
RUN cd rust_engine && cargo build --release

# Copy source
COPY src/ src/
COPY scripts/ scripts/
COPY tests/ tests/

# Logs directory
RUN mkdir -p logs/sessions

# Health check endpoint
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default: run WebSocket pipeline in paper mode
# Override with: docker run ... --mode live
ENTRYPOINT ["python", "scripts/run_ws_pipeline.py"]
CMD ["--mode", "paper"]
