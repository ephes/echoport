# mc for tests_integration/test_minio_client_backends.py: the release the
# backup runners pin (ops-library echoport_backup_mc_version), checked against
# the upstream sha256sum.
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
ARG MC_SHA256_AMD64=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891
ARG MC_SHA256_ARM64=14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN set -eu; arch="$(dpkg --print-architecture)"; \
    case "$arch" in amd64) sum="$MC_SHA256_AMD64" ;; arm64) sum="$MC_SHA256_ARM64" ;; *) echo "unsupported: $arch" >&2; exit 1 ;; esac; \
    curl -fsSL -o /usr/local/bin/mc "https://github.com/minio/mc/releases/download/${MC_VERSION}/mc.linux-${arch}.${MC_VERSION}"; \
    echo "${sum}  /usr/local/bin/mc" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/mc
