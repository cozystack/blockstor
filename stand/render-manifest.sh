#!/usr/bin/env bash
#
# usage: render-manifest.sh <work-dir> <yaml-path>
#
# Reads stand/blockstor-*.yaml on stdin via $2, substitutes:
#   __REGISTRY__                              → <bridge-gateway>:5000
#   __REGISTRY__/blockstor:dev                → <reg>/blockstor@sha256:<digest>
#   __REGISTRY__/blockstor-apiserver:dev      → <reg>/blockstor-apiserver@sha256:<digest>
#   __REGISTRY__/blockstor-satellite:dev      → <reg>/blockstor-satellite@sha256:<digest>
# and writes the result to stdout. Digests come from
# .work/_factory/digest-<name>.txt, written by stand/build-images.sh
# after each `docker push`.
#
# Post-mortem Run 26-35: floating `:dev` tags meant `kubectl apply`
# saw no spec change on rebuild, kubelet never noticed, pod kept
# running an image digest from a previous build, and 10 successive
# e2e runs silently tested against pre-Bug-326 apiserver code.
# Pinning by digest makes each build's manifest spec-different so
# every `kubectl apply` triggers a real rolling update.
#
# If a digest file is missing (fresh checkout, local dev that
# hasn't run build-images, tests with fake images), this script
# falls back to the floating `:dev` tag and emits a WARN to stderr
# — those code paths don't need digest-pinning correctness and
# should not be broken.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
YAML=${2:?yaml path required}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DIGEST_DIR="$REPO_ROOT/.work/_factory"

export KUBECONFIG="$WORK_DIR/kubeconfig"

# REGISTRY may be pre-set by the caller (e.g. DRY_RUN sanity checks
# with no live cluster — see install-blockstor.sh). Default path is
# to read the bridge gateway off any node's InternalIP, since Talos
# VMs all live in the same /24 as the host bridge.
if [ -z "${REGISTRY:-}" ]; then
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    REGISTRY="${NODE_IP%.*}.1:5000"
fi

render_image() {
    local short="$1"
    local f="$DIGEST_DIR/digest-$short.txt"
    if [ -s "$f" ]; then
        local d
        d=$(cat "$f")
        echo "$REGISTRY/$short@$d"
    else
        echo "WARN: $f missing — falling back to floating :dev tag for $short" >&2
        echo "$REGISTRY/$short:dev"
    fi
}

CONTROLLER_IMG=$(render_image blockstor)
APISERVER_IMG=$(render_image blockstor-apiserver)
SATELLITE_IMG=$(render_image blockstor-satellite)

# Order matters: the more-specific `:dev` substitutions run first so
# the catch-all `__REGISTRY__` doesn't rewrite the image line before
# the digest can attach. After the first match the image line no
# longer contains `__REGISTRY__` and the second sed is a no-op.
sed -e "s|__REGISTRY__/blockstor:dev|$CONTROLLER_IMG|g" \
    -e "s|__REGISTRY__/blockstor-apiserver:dev|$APISERVER_IMG|g" \
    -e "s|__REGISTRY__/blockstor-satellite:dev|$SATELLITE_IMG|g" \
    -e "s|__REGISTRY__|$REGISTRY|g" \
    "$YAML"
