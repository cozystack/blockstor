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

echo ">> docker build $REGISTRY/blockstor:dev (controller stage)"
docker build "${BUILD_ARGS[@]}" --target controller -t "$REGISTRY/blockstor:dev" .

echo ">> docker build $REGISTRY/blockstor-apiserver:dev (apiserver stage)"
docker build "${BUILD_ARGS[@]}" --target apiserver -t "$REGISTRY/blockstor-apiserver:dev" .

echo ">> docker build $REGISTRY/blockstor-satellite:dev (satellite stage)"
docker build "${BUILD_ARGS[@]}" --target satellite  -t "$REGISTRY/blockstor-satellite:dev" .

echo ">> docker push $REGISTRY/blockstor:dev"
docker push "$REGISTRY/blockstor:dev"

echo ">> docker push $REGISTRY/blockstor-apiserver:dev"
docker push "$REGISTRY/blockstor-apiserver:dev"

echo ">> docker push $REGISTRY/blockstor-satellite:dev"
docker push "$REGISTRY/blockstor-satellite:dev"

echo ">> images ready"
