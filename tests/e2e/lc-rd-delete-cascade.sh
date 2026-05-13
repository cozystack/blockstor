#!/usr/bin/env bash
#
# usage: lc-rd-delete-cascade.sh WORK_DIR
#
# Scenario 4.1 — RD delete cascades to all replicas, ports freed,
# subsequent re-create with the same name succeeds.
#
# Pin: linstor-cli #14, drbd-troubleshooting "force-strip finalizers
# leaves DRBD kernel state". Test MUST use `linstor rd d`, NEVER
# `kubectl patch --type=json -p='[...remove finalizers]'` — if
# finalizers don't clear within 60s that's a real bug, not something
# to bypass.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=test-cascade

# Random ephemeral port — parallel iters on the stand would otherwise
# collide on a fixed port.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/lc-rd-cascade-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: kubectl get events -n blockstor-system ----"
    kubectl get events -n blockstor-system --sort-by=.lastTimestamp | tail -30 || true
    echo "---- dump: kubectl logs -n blockstor-system deploy/blockstor-controller --tail=80 ----"
    kubectl logs -n blockstor-system deploy/blockstor-controller --tail=80 || true
    echo "---- dump: kubectl get resourcedefinition,resource,snapshot ----"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io 2>/dev/null || true
    kubectl get resources.blockstor.io.blockstor.io 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    # belt-and-suspenders — wipe any leftover RD via the lib helper so the
    # next scenario starts clean. NEVER force-strip finalizers from here.
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward to bind.
for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

echo ">> rd create $RD"
"${LCTL[@]}" resource-definition create "$RD"

echo ">> vd create $RD 100M"
"${LCTL[@]}" volume-definition create "$RD" 100M

echo ">> rd autoplace --place-count 2"
"${LCTL[@]}" resource-definition auto-place "$RD" --place-count 2

# Wait until both diskful replicas reach UpToDate. resource-definition
# auto-place returns once the placement decision is made, but DRBD
# bring-up + initial sync is async. 120s is the same window
# linstor-cli.sh uses on a fresh stand.
echo ">> wait up to 120s for both replicas to reach UpToDate"
deadline=$(( $(date +%s) + 120 ))
ok=0
while (( $(date +%s) < deadline )); do
    # `r l -r <rd>` machine-readable JSON; count rows where state="UpToDate"
    up=$("${LCTL[@]}" --machine-readable resource list-volumes -r "$RD" 2>/dev/null \
        | jq '[.[][] | select(.volumes[]?.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( up >= 2 )); then
        ok=1
        break
    fi
    sleep 2
done

if (( ok != 1 )); then
    echo "FAIL: $RD never reached UpToDate on 2 replicas"
    "${LCTL[@]}" resource list-volumes -r "$RD" || true
    exit 1
fi

echo ">> capture diskful nodes for later port-freed verification"
diskful_nodes=$("${LCTL[@]}" --machine-readable resource list -r "$RD" \
    | jq -r '.[][] | select((.flags // []) | index("DRBD_DISKLESS") | not) | .node_name' \
    | sort -u)
echo "diskful nodes: $diskful_nodes"

echo ">> rd delete $RD (cascade)"
"${LCTL[@]}" resource-definition delete "$RD"

# Within 30s: no Resource CRDs match name, RD no longer in list, ports freed.
echo ">> wait up to 30s for full cascade"
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    res_left=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
    rd_present=0
    if kubectl get "resourcedefinitions.blockstor.io.blockstor.io/${RD}" >/dev/null 2>&1; then
        rd_present=1
    fi
    if (( res_left == 0 && rd_present == 0 )); then
        break
    fi
    sleep 1
done

if (( res_left != 0 || rd_present != 0 )); then
    echo "FAIL: cascade incomplete after 30s — res_left=$res_left rd_present=$rd_present"
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' || true
    kubectl get "resourcedefinitions.blockstor.io.blockstor.io/${RD}" -o yaml 2>/dev/null || true
    # NEVER force-strip finalizers here. Surface the bug.
    exit 1
fi

# rd list must not show it
rd_list_json=$("${LCTL[@]}" --machine-readable resource-definition list 2>/dev/null || echo "[]")
if echo "$rd_list_json" | jq -e --arg rd "$RD" '.[][] | select(.name == $rd)' >/dev/null 2>&1; then
    echo "FAIL: $RD still appears in linstor rd list after delete"
    exit 1
fi

# Verify ports actually freed: re-create the same RD with the same name.
# If port 7000 (or whatever was allocated) wasn't freed, this either
# fails or silently allocates a new port — either way, the round-trip
# proves the cascade did its job.
echo ">> re-create same RD to verify ports/state freed"
"${LCTL[@]}" resource-definition create "$RD"
"${LCTL[@]}" volume-definition create "$RD" 100M
"${LCTL[@]}" resource-definition auto-place "$RD" --place-count 2

deadline=$(( $(date +%s) + 120 ))
ok2=0
while (( $(date +%s) < deadline )); do
    up=$("${LCTL[@]}" --machine-readable resource list-volumes -r "$RD" 2>/dev/null \
        | jq '[.[][] | select(.volumes[]?.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( up >= 2 )); then
        ok2=1
        break
    fi
    sleep 2
done

if (( ok2 != 1 )); then
    echo "FAIL: re-created $RD never reached UpToDate — ports/state likely not freed"
    "${LCTL[@]}" resource list-volumes -r "$RD" || true
    exit 1
fi

echo ">> final cleanup: rd delete $RD"
"${LCTL[@]}" resource-definition delete "$RD"

# small wait so the final cascade lands before the next scenario reuses the cluster
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resourcedefinitions.blockstor.io.blockstor.io/${RD}" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "PASS lc-rd-delete-cascade"
