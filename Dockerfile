# Build the manager binary
FROM golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source (relies on .dockerignore to filter)
COPY . .

# Build
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
#
# Bug 169: stamp `version.LinstorGitHash` / `version.LinstorBuildTime`
# at link time via `-ldflags -X`. Pre-fix the constants in
# `pkg/version/version.go` shipped as the literal placeholders
# `"blockstor"` and `"2026-01-01T00:00:00+00:00"`, so every image
# carried the same fake identity on /v1/controller/version and
# operators could not correlate a wire bug to a commit. The vars are
# also propagated for legacy `version.Version` / `version.GitCommit`
# (logger banners).
#
# Bug 171: callers (`stand/build-images.sh`, top-level Makefile)
# resolve the SHA on the HOST and pass it via --build-arg, because
# `.dockerignore` excludes `.git` — the in-container `git rev-parse`
# fallback below only kicks in for the (rare) case where someone runs
# `docker build .` directly without going through the wrappers and
# `.dockerignore` happens to be permissive. Default is the structured
# sentinel `sha-build-arg-missing` so a misconfigured CI is obvious
# (not the previous `unknown`, which is indistinguishable from a real
# tarball build).
ARG GIT_HASH=sha-build-arg-missing
ARG BUILD_TIME
ENV LDFLAGS="-X github.com/cozystack/blockstor/pkg/version.LinstorGitHash=${GIT_HASH} \
             -X github.com/cozystack/blockstor/pkg/version.GitCommit=${GIT_HASH} \
             -X github.com/cozystack/blockstor/pkg/version.LinstorBuildTime=${BUILD_TIME}"
RUN if [ -d .git ]; then GIT_HASH=$(git rev-parse HEAD 2>/dev/null || echo "${GIT_HASH}"); fi; \
    BUILD_TIME=${BUILD_TIME:-$(date -u +%FT%TZ)}; \
    LDFLAGS="-X github.com/cozystack/blockstor/pkg/version.LinstorGitHash=${GIT_HASH} \
             -X github.com/cozystack/blockstor/pkg/version.GitCommit=${GIT_HASH} \
             -X github.com/cozystack/blockstor/pkg/version.LinstorBuildTime=${BUILD_TIME}"; \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -ldflags "${LDFLAGS}" -o controller ./cmd/controller && \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -ldflags "${LDFLAGS}" -o apiserver  ./cmd/apiserver && \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -ldflags "${LDFLAGS}" -o satellite  ./cmd/satellite

# The satellite needs to shell out to drbdadm/lvs/zfs/cryptsetup, none
# of which are in distroless:static. We use debian-slim for it (and let
# the controller image stay distroless for the smaller surface). The
# manager is what ships under "blockstor:dev"; the satellite ships under
# "blockstor-satellite:dev".
FROM gcr.io/distroless/static:nonroot AS controller
WORKDIR /
COPY --from=builder /workspace/controller .
USER 65532:65532
ENTRYPOINT ["/controller"]

# The apiserver is the LINSTOR-compatible REST front-end split out
# of the controller (Phase 11). It runs N replicas behind a Service
# and serves linstor-csi / `linstor` CLI / piraeus-operator. Same
# distroless surface as the controller — no shell-outs.
FROM gcr.io/distroless/static:nonroot AS apiserver
WORKDIR /
COPY --from=builder /workspace/apiserver .
USER 65532:65532
ENTRYPOINT ["/apiserver"]

FROM debian:trixie-slim AS satellite
# zfsutils-linux is in contrib on trixie — enable it so we can pull
# zpool/zfs alongside drbd-utils, lvm2, cryptsetup. Note: the kernel
# module is provided by the host (Talos siderolabs/zfs extension);
# this image only ships the userspace tools.
RUN sed -i 's|^Components: main$|Components: main contrib|' /etc/apt/sources.list.d/debian.sources && \
    apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        drbd-utils lvm2 cryptsetup-bin zfsutils-linux gdisk parted ca-certificates \
        iptables && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=builder /workspace/satellite /usr/local/bin/satellite
ENTRYPOINT ["/usr/local/bin/satellite"]
