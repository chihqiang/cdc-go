# ---------- Build stage ----------
# Use Go Alpine image as builder for smaller image and faster build
# NOTE: keep in sync with the `go` directive in go.mod (go 1.25.14)
FROM golang:1.25.14-bookworm AS builder

# Set build-time argument for version
ARG APP_VERSION=main

# Install dependencies for building Go project
RUN apt-get update && apt-get install -y git make gcc libc6-dev && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy project files to container
COPY . .

# Build the Go binary with the specified version
RUN GOOS=linux make build APP_VERSION=${APP_VERSION}

# ---------- Runtime stage ----------
# Use minimal Debian image for runtime
FROM debian:bookworm-slim

# Set default timezone to Shanghai
ENV TZ=Asia/Shanghai

# Install runtime dependencies in a single layer
# - ca-certificates, curl, wget, jq, tzdata, mysql client
# - configure timezone
# - clean apt cache to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl wget jq tzdata default-mysql-client && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*


# No configuration is baked in as environment variables: /app/config.yml carries
# the defaults with ${VAR:-fallback}, and a variable set with `docker run -e ...`
# still overrides them.

# Create a non-root user for security
RUN useradd --system --no-create-home --shell /usr/sbin/nologin cdcgo

# Copy the built binary and the configuration file from builder stage
COPY --from=builder /app/cdc-go /usr/local/bin/cdc-go
COPY --from=builder /app/config.yml /app/config.yml

# Directory the file store writes its offsets into (the default is the relative
# "runtime", i.e. /app/runtime with the WORKDIR below)
RUN mkdir -p /app/runtime

# Set ownership to the non-root user
RUN chown -R cdcgo:cdcgo /usr/local/bin/cdc-go /app

# Switch to non-root user
USER cdcgo

# Set working directory
WORKDIR /app

# Default command to run the binary
CMD ["cdc-go","-c","/app/config.yml"]