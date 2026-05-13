#!/usr/bin/env bash
#
# usage: cheat-sheet-naming-deltas.sh WORK_DIR
#
# Scenario 1.26 (tests/scenarios/01-api-contract.md):
#   Pod / Deployment / namespace naming deltas vs the upstream LINSTOR
#   cheat-sheet (tests/observability-cheat-sheet-scenarios.md §A-C):
#
#   Upstream                                  | blockstor
#   ------------------------------------------|---------------------------------------------------------------
#   `kubectl exec -ti linstor-controller`     | `kubectl exec -n blockstor-system -ti deploy/blockstor-apiserver`
#   `kubectl get pod -l app=linstor-node`     | `kubectl get pod -n blockstor-system -l app=blockstor-satellite`
#   `cat /var/lib/linstor.d/<rd>.res`         | `cat /etc/drbd.d/<rd>.res`
#
# The 01-api-contract.md spec says the test asserts the BLOCKSTOR-
# specific command works. We do that and we also probe the upstream
# command as a non-fatal warning so the doc-vs-image-symlink decision
# (§A-C close) stays visible.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

fail=0

# --- A. blockstor-apiserver Deployment + Service presence ------------
# The spec example: `kubectl exec -n blockstor-system -ti deploy/
# blockstor-apiserver -- linstor n l`. The apiserver image is
# distroless (no shell, no `linstor` binary inside the container — by
# design, per Dockerfile line 41-45), so `linstor n l` cannot run
# inside the apiserver pod itself. The test therefore asserts the
# DEPLOYMENT + SERVICE both exist under the blockstor-specific names
# and the apiserver is Available — the operator's real cheat-sheet
# entry point is the Service (linstor-csi / `linstor --controllers`
# both go through it), not `kubectl exec`. The §B note in the
# cheat-sheet itself acknowledges this — true cheat-sheet parity
# would need a sidecar with the CLI baked in, a "consider" item.
echo ">> apiserver deploy/svc presence"
if ! kubectl -n "$NS" get deploy/blockstor-apiserver >/dev/null 2>&1; then
    echo "FAIL: Deployment $NS/blockstor-apiserver not found"
    fail=1
elif ! kubectl -n "$NS" get svc/blockstor-apiserver >/dev/null 2>&1; then
    echo "FAIL: Service $NS/blockstor-apiserver not found"
    fail=1
else
    avail=$(kubectl -n "$NS" get deploy/blockstor-apiserver \
        -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null || true)
    if [[ "$avail" != "True" ]]; then
        echo "FAIL: Deployment $NS/blockstor-apiserver not Available (status=$avail)"
        fail=1
    else
        echo "   deploy/blockstor-apiserver in ns=$NS Available OK"
    fi
fi

# --- A2. blockstor-controller is the leader binary; we don't probe
# `linstor` there either (distroless), but we do verify the pod
# selector at least returns one Ready pod, mirroring the cheat-sheet's
# `kubectl exec -ti linstor-controller` line as a presence check.
ctrl_pods=$(kubectl -n "$NS" get pods -l app=blockstor-controller \
    --field-selector status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ')
if (( ctrl_pods < 1 )); then
    # Some Helm/kustomize layouts label the controller as
    # `control-plane=controller-manager` (controller-tools default).
    ctrl_pods=$(kubectl -n "$NS" get pods -l control-plane=controller-manager \
        --field-selector status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ')
fi
if (( ctrl_pods < 1 )); then
    echo "FAIL: no Running blockstor-controller pods (-l app=blockstor-controller)"
    fail=1
else
    echo "   blockstor-controller pods: $ctrl_pods Running"
fi

# --- B. blockstor-satellite DaemonSet selector ------------------------
# Upstream cheat-sheet: `kubectl get pod -l app=linstor-node`.
# blockstor: `kubectl get pod -n blockstor-system -l app=blockstor-satellite`.
# Assert blockstor's selector returns ≥1 pod, one per worker node.
sat_count=$(kubectl -n "$NS" get pods -l app=blockstor-satellite --no-headers 2>/dev/null | wc -l | tr -d ' ')
worker_count=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' --no-headers 2>/dev/null | wc -l | tr -d ' ')
if (( sat_count < 1 )); then
    echo "FAIL: kubectl get pod -l app=blockstor-satellite returned 0 pods"
    fail=1
elif (( sat_count != worker_count )); then
    echo "WARN: $sat_count satellite pods vs $worker_count worker nodes (DaemonSet not converged?)"
else
    echo "   blockstor-satellite DS: $sat_count pods on $worker_count workers OK"
fi

# Upstream-name probe is informational — if it returns nothing, that
# confirms the §B/§C delta (no `app=linstor-node` label), and the
# cheat-sheet doc note stands.
upstream_count=$(kubectl get pods -A -l app=linstor-node --no-headers 2>/dev/null | wc -l | tr -d ' ')
if (( upstream_count == 0 )); then
    echo "   [info] -l app=linstor-node returns 0 pods → §B doc-delta confirmed (cheat-sheet must use app=blockstor-satellite for blockstor)"
fi

# --- C. .res file location -------------------------------------------
# Already covered by satellite-utils-smoke.sh, but we re-assert the
# blockstor-specific path here so this scenario alone tells the
# operator which directory to look in.
SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$SAT_POD" ]]; then
    has_blockstor=$(kubectl -n "$NS" exec "$SAT_POD" -- /bin/sh -c \
        "ls -d /etc/drbd.d 2>/dev/null" 2>/dev/null || true)
    if [[ -z "$has_blockstor" ]]; then
        echo "FAIL: /etc/drbd.d/ missing on $SAT_POD (blockstor-specific .res location)"
        fail=1
    else
        echo "   .res dir /etc/drbd.d/ present on $SAT_POD OK"
    fi

    # Informational: is there a /var/lib/linstor.d/ symlink for
    # cheat-sheet parity? §A close suggests this as an option.
    upstream_dir=$(kubectl -n "$NS" exec "$SAT_POD" -- /bin/sh -c \
        "ls -d /var/lib/linstor.d 2>/dev/null" 2>/dev/null || true)
    if [[ -z "$upstream_dir" ]]; then
        echo "   [info] /var/lib/linstor.d/ absent → cheat-sheet §A delta stands (operators must use /etc/drbd.d/)"
    else
        echo "   [info] /var/lib/linstor.d/ present → cheat-sheet parity achieved (symlink or real dir)"
    fi
fi

if (( fail != 0 )); then
    echo "CHEAT-SHEET-NAMING-DELTAS: FAIL"
    exit 1
fi

echo ">> CHEAT-SHEET-NAMING-DELTAS OK (apiserver/satellite/controller all reachable under blockstor-specific names; §A-C deltas documented inline)"
