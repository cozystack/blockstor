#!/usr/bin/env bash
#
# usage: install-blockstor.sh WORK_DIR
#
# Wires the blockstor controller + satellite onto a freshly-created
# Talos+QEMU cluster (`make up NAME=<n>`). Idempotent: re-running on a
# cluster that already has blockstor returns 0 with the rolling-update
# bringing the latest images.
#
# Steps:
#   1. apply CRDs from config/crd/bases/
#   2. apply blockstor-deploy.yaml (controller + RBAC + namespace)
#   3. apply blockstor-satellite-daemonset.yaml
#   4. wait for controller + satellites Running
#
# Assumes the host registry on 10.164.0.1:5000 is reachable from the
# cluster (Talos config-patch trusts http for that mirror — see
# stand/up.sh).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo ">> apply CRDs"
kubectl apply -f "$REPO_ROOT/config/crd/bases/" 2>&1 | tail -5

echo ">> apply controller + RBAC"
kubectl apply -f "$REPO_ROOT/stand/blockstor-deploy.yaml" 2>&1 | tail -5

echo ">> apply satellite DaemonSet"
kubectl apply -f "$REPO_ROOT/stand/blockstor-satellite-daemonset.yaml" 2>&1 | tail -5

echo ">> wait for controller Running"
kubectl -n blockstor-system rollout status deploy/blockstor-controller --timeout=120s

echo ">> wait for satellites (3 workers)"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    ready=$(kubectl -n blockstor-system get pods -l app=blockstor-satellite --no-headers 2>/dev/null \
        | awk '{print $2}' | grep -c '^1/1$' || true)
    if [[ "$ready" == "3" ]]; then
        break
    fi
    sleep 5
done

if [[ "$ready" != "3" ]]; then
    echo "FAIL: only $ready/3 satellites Running"
    kubectl -n blockstor-system get pods -l app=blockstor-satellite
    exit 1
fi

echo ">> blockstor stack ready on $(basename "$WORK_DIR")"
