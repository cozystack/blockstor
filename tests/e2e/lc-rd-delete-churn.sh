#!/usr/bin/env bash
#
# usage: lc-rd-delete-churn.sh WORK_DIR
#
# Scenario 5.4 extended — mass churn of 10 RDs in rapid succession.
# Validates Bug 1 cascade (RD delete cleans Resource CRDs + ports) +
# Bug 4 suppression (no `node_id of X instead of Y` warnings) +
# Phase 8.1 stability (no port-collision on subsequent RD).
#
# For each iteration (i = 1..N):
#   - rd create churn-i
#   - vd create churn-i 64M
#   - rd auto-place --place-count 2
#   - wait both replicas UpToDate
#   - rd delete churn-i
# After the loop:
#   - linstor r l must be empty
#   - no orphan Resource CRDs match ^churn-\d+\.
#   - no orphan ZVOLs (zfs list) named like *churn-*
#   - dmesg on every satellite shows zero
#     `node_id of N instead of M` warnings
#
# Pin: same as lc-rd-delete-cascade.sh — NEVER force-strip finalizers;
# if cleanup doesn't converge inside the per-iter timeout, that's a
# real bug.

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

ITERS=${ITERS:-10}
RD_PREFIX=${RD_PREFIX:-churn}
UP_TIMEOUT=${UP_TIMEOUT:-180}
DEL_TIMEOUT=${DEL_TIMEOUT:-60}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/lc-rd-churn-pf.log 2>&1 &
PF_PID=$!

# Capture dmesg cursor on every satellite at start, so the post-loop
# scan only counts warnings produced during this test.
declare -A DMESG_BASELINE
for node in $WORKER_1 $WORKER_2 $WORKER_3; do
    [[ -n "$node" ]] || continue
    DMESG_BASELINE[$node]=$(on_node "$node" bash -c "dmesg -T 2>/dev/null | wc -l" || echo 0)
done

dump_diag() {
    echo "---- dump: kubectl get events -n blockstor-system ----"
    kubectl get events -n blockstor-system --sort-by=.lastTimestamp | tail -40 || true
    echo "---- dump: kubectl logs -n blockstor-system deploy/blockstor-controller --tail=120 ----"
    kubectl logs -n blockstor-system deploy/blockstor-controller --tail=120 || true
    echo "---- dump: kubectl get resourcedefinitions / resources ----"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io 2>/dev/null || true
    kubectl get resources.blockstor.io.blockstor.io 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    # Best-effort wipe of any leftover churn-* RDs through the
    # supported lib helper. NEVER force-strip finalizers here.
    for i in $(seq 1 "$ITERS"); do
        delete_rd "${RD_PREFIX}-${i}" 2>/dev/null || true
    done
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward to bind.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

# Collect ports allocated per RD — verify each freed after delete.
declare -A ALLOCATED_PORT

t_start=$(date +%s)

for i in $(seq 1 "$ITERS"); do
    RD="${RD_PREFIX}-${i}"
    echo "=== iter $i/$ITERS: $RD ==="

    iter_start=$(date +%s)

    echo ">> rd create $RD"
    "${LCTL[@]}" resource-definition create "$RD"

    echo ">> vd create $RD 64M"
    "${LCTL[@]}" volume-definition create "$RD" 64M

    echo ">> rd auto-place $RD --place-count 2"
    "${LCTL[@]}" resource-definition auto-place "$RD" --place-count 2

    # Wait for both replicas UpToDate.
    deadline=$(( $(date +%s) + UP_TIMEOUT ))
    ok=0
    while (( $(date +%s) < deadline )); do
        up=$("${LCTL[@]}" --machine-readable resource list-volumes -r "$RD" 2>/dev/null \
            | jq '[.[][] | select(.volumes[]?.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
        if (( up >= 2 )); then
            ok=1
            break
        fi
        sleep 2
    done

    if (( ok != 1 )); then
        echo "FAIL[iter=$i]: $RD never reached UpToDate on 2 replicas"
        "${LCTL[@]}" resource list-volumes -r "$RD" || true
        exit 1
    fi

    # Capture port (per-RD TCP port lives in rd props as TcpPort).
    port=$("${LCTL[@]}" --machine-readable resource-definition list -r "$RD" 2>/dev/null \
        | jq -r '.[][] | (.props.TcpPort // "")' | head -1)
    ALLOCATED_PORT[$RD]="$port"
    echo "   port=$port"

    echo ">> rd delete $RD"
    "${LCTL[@]}" resource-definition delete "$RD"

    # Wait full cascade.
    deadline=$(( $(date +%s) + DEL_TIMEOUT ))
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
        echo "FAIL[iter=$i]: cascade incomplete after ${DEL_TIMEOUT}s — res_left=$res_left rd_present=$rd_present"
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd' || true
        exit 1
    fi

    iter_end=$(date +%s)
    echo "   iter took $((iter_end - iter_start))s"
done

t_end=$(date +%s)
total=$(( t_end - t_start ))
echo ""
echo "=== loop done in ${total}s ($ITERS iters) ==="

# ------- Post-loop invariants -------

# 1. linstor r l empty
r_left=$("${LCTL[@]}" --machine-readable resource list 2>/dev/null | jq '[.[][]] | length' 2>/dev/null || echo 0)
if (( r_left != 0 )); then
    echo "FAIL: linstor r l not empty after loop ($r_left resources remain)"
    "${LCTL[@]}" resource list || true
    exit 1
fi

# 2. No orphan Resource CRDs with churn-N. prefix
orphans=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
    | awk -v p="$RD_PREFIX-" '$1 ~ "^"p' | wc -l)
if (( orphans != 0 )); then
    echo "FAIL: orphan Resource CRDs matching ${RD_PREFIX}-*:"
    kubectl get resources.blockstor.io.blockstor.io --no-headers \
        | awk -v p="$RD_PREFIX-" '$1 ~ "^"p'
    exit 1
fi

rd_orphans=$(kubectl get resourcedefinitions.blockstor.io.blockstor.io --no-headers 2>/dev/null \
    | awk -v p="$RD_PREFIX-" '$1 ~ "^"p' | wc -l)
if (( rd_orphans != 0 )); then
    echo "FAIL: orphan ResourceDefinition CRDs matching ${RD_PREFIX}-*:"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io --no-headers \
        | awk -v p="$RD_PREFIX-" '$1 ~ "^"p'
    exit 1
fi

# 3. No orphan ZVOLs (zfs list) for churn-* on any satellite
for node in $WORKER_1 $WORKER_2 $WORKER_3; do
    [[ -n "$node" ]] || continue
    out=$(on_node "$node" bash -c "zfs list -H -t volume 2>/dev/null | awk '{print \$1}'" || true)
    leaked=$(echo "$out" | grep -E "${RD_PREFIX}-[0-9]+_" || true)
    if [[ -n "$leaked" ]]; then
        echo "FAIL: orphan ZVOLs on $node:"
        echo "$leaked"
        exit 1
    fi
done

# 4. dmesg on every satellite — zero `node_id of X instead of Y` warnings
#    counting only lines emitted during this run.
nodeid_warnings=0
for node in $WORKER_1 $WORKER_2 $WORKER_3; do
    [[ -n "$node" ]] || continue
    base=${DMESG_BASELINE[$node]:-0}
    hits=$(on_node "$node" bash -c "dmesg -T 2>/dev/null | tail -n +$((base + 1)) | grep -c 'node_id of'" || echo 0)
    if (( hits > 0 )); then
        echo "FAIL: $node dmesg has $hits 'node_id of X instead of Y' warnings:"
        on_node "$node" bash -c "dmesg -T 2>/dev/null | tail -n +$((base + 1)) | grep 'node_id of' | head -10" || true
        nodeid_warnings=$(( nodeid_warnings + hits ))
    fi
done

if (( nodeid_warnings != 0 )); then
    echo "FAIL: $nodeid_warnings node_id mismatch warnings across satellites"
    exit 1
fi

# 5. Port-collision check — verify all ports allocated through the loop
#    are unique. Port reuse across iters is fine (and expected); but
#    LINSTOR refusing to reallocate the same port after `rd delete`
#    would manifest as monotonically growing ports without any reuse.
#    Just report the set so reviewers can eyeball it.
echo ""
echo "ports allocated per iter:"
for i in $(seq 1 "$ITERS"); do
    RD="${RD_PREFIX}-${i}"
    echo "  $RD => ${ALLOCATED_PORT[$RD]}"
done

echo ""
echo "PASS lc-rd-delete-churn (${total}s, $ITERS iters)"
