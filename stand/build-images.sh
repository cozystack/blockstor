#!/usr/bin/env bash
#
# usage: build-images.sh [REGISTRY]
#
# Build the controller + satellite container images from the current
# checkout and push them to the host's local Docker registry. The
# blockstor-deploy.yaml / blockstor-satellite-daemonset.yaml on the
# stand pull these via the per-cluster bridge gateway (see
# install-blockstor.sh) but the underlying registry is a single
# host-local one — so we push once and every parallel stand sees it.
#
# Bug 171: stamp the image with the real git SHA and build time at
# `docker build` time. Without these --build-args, the Dockerfile's
# ARG defaults leak through (`GIT_HASH=unknown`) because
# `.dockerignore` excludes `.git`, so the in-container `git rev-parse
# HEAD` fallback has no repo to read. We resolve the SHA on the host
# (where the .git tree DOES exist) and pass it via --build-arg.
# Tarball builds (no .git) fall back to `sha-unavailable-tarball` so a
# bad build is distinguishable from a build that simply lacks VCS
# context.
#
# Post-mortem Run 26-35: each `docker push` line emits
#   <tag>: digest: sha256:<hex> size: <n>
# We parse the digest off that line and write it to
#   .work/_factory/digest-<image>.txt   (one file per image)
# so `install-blockstor.sh` can substitute the floating `:dev` tag
# with `@sha256:<digest>` before `kubectl apply`. Floating tags do
# not change the Deployment spec on rebuild — kubelet only re-pulls
# on pod restart, and even then containerd cache can return a stale
# digest under load. Pinning the digest makes each build's manifest
# spec-different, so `kubectl apply` triggers a real rollout and
# Kubernetes treats the image as immutable.

set -euo pipefail

REGISTRY=${1:-localhost:5000}
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

# GIT_HASH / BUILD_TIME can be overridden by callers (CI, dev shims)
# that already know the right values; otherwise resolve from the host
# checkout. Tarball builds (no .git) fall back to a structured
# sentinel so a bad build is distinguishable from a missing VCS
# context.
if [ -z "${GIT_HASH:-}" ]; then
    if [ -d .git ] && command -v git >/dev/null 2>&1; then
        GIT_HASH=$(git rev-parse HEAD)
    else
        GIT_HASH="sha-unavailable-tarball"
    fi
fi
BUILD_TIME="${BUILD_TIME:-$(date -u +%FT%TZ)}"

echo ">> stamping GIT_HASH=$GIT_HASH BUILD_TIME=$BUILD_TIME"

BUILD_ARGS=(
    --build-arg "GIT_HASH=$GIT_HASH"
    --build-arg "BUILD_TIME=$BUILD_TIME"
)

# Where digest files land. install-blockstor.sh reads these to pin
# the manifest image refs by digest.
DIGEST_DIR="$REPO_ROOT/.work/_factory"
mkdir -p "$DIGEST_DIR"

# push <short-name>  — pushes $REGISTRY/<short-name>:dev, captures
# the per-push "digest: sha256:<hex>" line, writes the bare
# "sha256:<hex>" string to $DIGEST_DIR/digest-<short-name>.txt.
# Fails loudly if no digest can be parsed (registry unreachable,
# auth required, push refused) — we'd rather break the build than
# silently leave a stale digest file behind for the installer to
# pick up.
push() {
    local short="$1"
    local tag="$REGISTRY/$short:dev"
    local log
    log=$(mktemp)
    echo ">> docker push $tag"
    if ! docker push "$tag" 2>&1 | tee "$log"; then
        rm -f "$log"
        echo "FAIL: docker push $tag" >&2
        exit 1
    fi
    local digest
    digest=$(grep -oE 'sha256:[a-f0-9]{64}' "$log" | head -1)
    rm -f "$log"
    if [ -z "$digest" ]; then
        echo "FAIL: could not parse digest from push output for $tag" >&2
        exit 1
    fi
    echo "$digest" > "$DIGEST_DIR/digest-$short.txt"
    echo ">> $short pinned: $digest"
}

echo ">> docker build $REGISTRY/blockstor:dev (controller stage)"
docker build "${BUILD_ARGS[@]}" --target controller -t "$REGISTRY/blockstor:dev" .

echo ">> docker build $REGISTRY/blockstor-apiserver:dev (apiserver stage)"
docker build "${BUILD_ARGS[@]}" --target apiserver -t "$REGISTRY/blockstor-apiserver:dev" .

echo ">> docker build $REGISTRY/blockstor-satellite:dev (satellite stage)"
docker build "${BUILD_ARGS[@]}" --target satellite  -t "$REGISTRY/blockstor-satellite:dev" .

push blockstor
push blockstor-apiserver
push blockstor-satellite

echo ">> images ready (digests in $DIGEST_DIR/)"
