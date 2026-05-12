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

set -euo pipefail

REGISTRY=${1:-localhost:5000}
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

echo ">> docker build $REGISTRY/blockstor:dev (controller stage)"
docker build --target controller -t "$REGISTRY/blockstor:dev" .

echo ">> docker build $REGISTRY/blockstor-apiserver:dev (apiserver stage)"
docker build --target apiserver -t "$REGISTRY/blockstor-apiserver:dev" .

echo ">> docker build $REGISTRY/blockstor-satellite:dev (satellite stage)"
docker build --target satellite  -t "$REGISTRY/blockstor-satellite:dev" .

echo ">> docker push $REGISTRY/blockstor:dev"
docker push "$REGISTRY/blockstor:dev"

echo ">> docker push $REGISTRY/blockstor-apiserver:dev"
docker push "$REGISTRY/blockstor-apiserver:dev"

echo ">> docker push $REGISTRY/blockstor-satellite:dev"
docker push "$REGISTRY/blockstor-satellite:dev"

echo ">> images ready"
