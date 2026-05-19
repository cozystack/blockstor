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
#   2. apply blockstor Node CRs from k8s worker nodes (depends only
#      on the Node CRD definition from step 1 — done before the
#      Deployments so a broken controller/apiserver image cannot
#      gate Node-CR bootstrap)
#   3. apply blockstor-deploy.yaml (controller + RBAC + namespace)
#   4. apply blockstor-apiserver-deploy.yaml
#   5. apply blockstor-satellite-daemonset.yaml
#   6. wait for controller + apiserver + satellites Running
#
# Assumes the host registry is reachable from the cluster on the
# bridge gateway (.1 of this cluster's NET_CIDR). Talos config-patch
# trusts http for that mirror — see stand/up.sh. The deploy manifests
# carry an `__REGISTRY__` placeholder which `stand/render-manifest.sh`
# substitutes with the actual bridge IP, computed from the first
# node's InternalIP — this is how parallel stands on the same host
# all see the same registry-on-host without colliding on a single IP.
#
# Post-mortem Run 26-35: each rendered manifest pins the image by
# digest (`<reg>/<name>@sha256:<digest>`) read from
# .work/_factory/digest-<name>.txt, written by stand/build-images.sh
# after each `docker push`. Floating `:dev` tags never changed the
# Deployment spec on rebuild, kubelet never re-pulled, and 10
# successive iters silently ran against stale apiserver code.
# Digest pinning makes every rebuild a real rolling update.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# render <yaml> emits the manifest with __REGISTRY__ substituted and
# every `image: ...:dev` replaced by `image: ...@sha256:<digest>`
# (or left as `:dev` with a WARN if the digest file is missing).
render() {
    "$REPO_ROOT/stand/render-manifest.sh" "$WORK_DIR" "$1"
}

# DRY_RUN=1 prints the would-apply YAML to stdout and exits, so
# `DRY_RUN=1 ./stand/install-blockstor.sh .work/foo | grep image:`
# is a stand-free sanity check that digest pinning is wired up.
# Pass a sentinel REGISTRY through to the renderer so it doesn't try
# to hit a kubectl that may not have a live cluster behind it. All
# log output goes to stderr so a `grep image:` over stdout sees
# only the rendered YAML.
if [[ "${DRY_RUN:-}" == "1" ]]; then
    export REGISTRY="${REGISTRY:-10.0.0.1:5000}"
    echo ">> [DRY_RUN] using sentinel registry at $REGISTRY" >&2
    echo "--- blockstor-deploy.yaml ---" >&2
    render "$REPO_ROOT/stand/blockstor-deploy.yaml"
    echo "--- blockstor-apiserver-deploy.yaml ---" >&2
    render "$REPO_ROOT/stand/blockstor-apiserver-deploy.yaml"
    echo "--- blockstor-satellite-daemonset.yaml ---" >&2
    render "$REPO_ROOT/stand/blockstor-satellite-daemonset.yaml"
    exit 0
fi

# Bridge gateway = .1 of the cluster CIDR. Read it off any node's
# InternalIP (Talos VMs all live in the same /24 as the host bridge).
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
REGISTRY="${NODE_IP%.*}.1:5000"
echo ">> using host registry at $REGISTRY"

echo ">> apply CRDs"
kubectl apply -f "$REPO_ROOT/config/crd/bases/" 2>&1 | tail -5

# Bootstrap blockstor Node CRDs from k8s worker nodes so the
# satellite reconciler's peer-resolution path has an address per
# node — otherwise multi-replica .res files render a 0.0.0.0
# placeholder for any peer this satellite hasn't directly seen.
# Cluster-scoped CRD; metadata.name == k8s node name.
#
# Done BEFORE the Deployments so a broken controller/apiserver
# image (crashloop on bad flag, missing env, etc.) does not gate
# Node-CRD installation. The Node CRs only depend on the Node CRD
# definition itself, which was just applied above. Re-running this
# script after the operator patches a broken Deployment is still
# safe — `kubectl apply` is idempotent.
echo ">> register blockstor Node CRDs from k8s workers"
for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}'); do
    ip=$(kubectl get node "$node" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Node
metadata: {name: $node}
spec:
  type: SATELLITE
  netInterfaces:
    - {name: default, address: $ip}
EOF
done

echo ">> apply controller + RBAC"
render "$REPO_ROOT/stand/blockstor-deploy.yaml" | kubectl apply -f - 2>&1 | tail -5

echo ">> apply apiserver + RBAC"
render "$REPO_ROOT/stand/blockstor-apiserver-deploy.yaml" | kubectl apply -f - 2>&1 | tail -5

echo ">> apply satellite DaemonSet"
render "$REPO_ROOT/stand/blockstor-satellite-daemonset.yaml" | kubectl apply -f - 2>&1 | tail -5

echo ">> wait for controller Running"
kubectl -n blockstor-system rollout status deploy/blockstor-controller --timeout=120s

echo ">> wait for apiserver Running"
kubectl -n blockstor-system rollout status deploy/blockstor-apiserver --timeout=120s

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
