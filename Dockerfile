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
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager ./cmd
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o satellite ./cmd/satellite

# The satellite needs to shell out to drbdadm/lvs/zfs/cryptsetup, none
# of which are in distroless:static. We use debian-slim for it (and let
# the controller image stay distroless for the smaller surface). The
# manager is what ships under "blockstor:dev"; the satellite ships under
# "blockstor-satellite:dev".
FROM gcr.io/distroless/static:nonroot AS controller
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]

FROM debian:trixie-slim AS satellite
# zfsutils-linux is in contrib (not in main on trixie). Stand uses
# LVM-thin so we can ship without zfs for now; we add it back when the
# satellite needs to drive a ZFS pool. drbd-utils + lvm2 + cryptsetup
# are the runtime tools the wrappers shell out to.
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        drbd-utils lvm2 cryptsetup-bin ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=builder /workspace/satellite /usr/local/bin/satellite
ENTRYPOINT ["/usr/local/bin/satellite"]
