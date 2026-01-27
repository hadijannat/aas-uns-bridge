# Multi-stage Dockerfile for AAS-UNS Bridge
# Build stage
FROM python:3.12-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/
COPY proto/ ./proto/

# Generate protobuf bindings
RUN mkdir -p src/aas_uns_bridge/proto && \
    protoc --python_out=src/aas_uns_bridge/proto proto/sparkplug_b.proto

# Install the package
RUN pip install --no-cache-dir --prefix=/install .


# Runtime stage
FROM python:3.12-slim

LABEL org.opencontainers.image.title="AAS-UNS Bridge"
LABEL org.opencontainers.image.description="Asset Administration Shell to Unified Namespace Bridge"
LABEL org.opencontainers.image.vendor="AAS-UNS Bridge Contributors"

# Create non-root user
RUN groupadd -r bridge && useradd -r -g bridge bridge

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy configuration templates
COPY config/ ./config/

# Create directories for runtime
RUN mkdir -p /app/watch /app/state && \
    chown -R bridge:bridge /app

# Switch to non-root user
USER bridge

# Environment variables
ENV AAS_BRIDGE_CONFIG_FILE=/app/config/config.yaml
ENV AAS_BRIDGE_MAPPINGS_FILE=/app/config/mappings.yaml

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Entry point
ENTRYPOINT ["aas-uns-bridge"]
CMD ["run"]
