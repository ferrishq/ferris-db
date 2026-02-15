# Build stage
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY ferris-server ./ferris-server
COPY ferris-core ./ferris-core
COPY ferris-protocol ./ferris-protocol
COPY ferris-commands ./ferris-commands
COPY ferris-network ./ferris-network
COPY ferris-persistence ./ferris-persistence
COPY ferris-replication ./ferris-replication
COPY ferris-crdt ./ferris-crdt
COPY ferris-queue ./ferris-queue
COPY ferris-lock ./ferris-lock
COPY ferris-test-utils ./ferris-test-utils

# Build release binary
RUN cargo build --release --package ferris-server

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false ferris

# Copy binary from builder
COPY --from=builder /app/target/release/ferris-db /usr/local/bin/ferris-db

# Create data directory
RUN mkdir -p /data && chown ferris:ferris /data

# Switch to non-root user
USER ferris

# Set working directory
WORKDIR /data

# Expose default port
EXPOSE 6380

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD echo "PING" | nc -w 1 localhost 6380 | grep -q "PONG" || exit 1

# Default command
ENTRYPOINT ["ferris-db"]
CMD ["--bind", "0.0.0.0:6380"]
