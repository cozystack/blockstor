#!/usr/bin/env bash
#
# usage: lc-connection-cleanup.sh WORK_DIR
#
# Scenarios 1.17 + 5.4 — `linstor r d <node> <rd>` removes the peer's
# entry from the surviving replicas' Connections column within ~10s.
# Pre-bug: observer didn't handle `change connection action:destroy`,
# so the deleted peer lingered forever as a ghost StandAlone(<peer>)
# entry on the survivors.
#
# Test path:
#   - 2-replica autoplace on a 3-worker cluster → 2 diskful + 1
#     auto-tiebreaker = 3 Resource rows.
#   - Wait for both diskful replicas UpToDate.
#   - Identify the tiebreaker worker (State=TieBreaker / flags
#     include TIE_BREAKER+DISKLESS).
#   - `linstor r d <tiebreaker> <rd>`.
#   - Poll the surviving peers' Connections[] (JSON r l + REST view)
#     and assert the removed peer disappears within 10s — no ghost
#     StandAlone(<tiebreaker>) / Unconnected(<tiebreaker>) entries.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=test-conn

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/lc-conn-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    linstor --controllers "http://localhost:$PF_PORT" resource list -r "$RD" || true
    echo "---- dump: linstor --machine-readable r l -r $RD ----"
    linstor --controllers "http://localhost:$PF_PORT" --machine-readable resource list -r "$RD" || true
    echo "---- dump: GET /v1/view/resources?resource=$RD ----"
    curl -sf -m5 "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" || true
    echo "---- dump: kubectl get events -n blockstor-system ----"
    kubectl get events -n blockstor-system --sort-by=.lastTimestamp | tail -30 || true
    echo "---- dump: kubectl logs -n blockstor-system deploy/blockstor-controller --tail=80 ----"
    kubectl logs -n blockstor-system deploy/blockstor-controller --tail=80 || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

echo ">> rd create $RD + vd 100M + autoplace --place-count 2 (expects 2 diskful + 1 tiebreaker)"
"${LCTL[@]}" resource-definition create "$RD"
"${LCTL[@]}" volume-definition create "$RD" 100M
"${LCTL[@]}" resource-definition auto-place "$RD" --place-count 2

echo ">> wait up to 120s for both diskful replicas to reach UpToDate"
deadline=$(( $(date +%s) + 120 ))
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
    echo "FAIL: $RD never reached UpToDate on 2 replicas"
    "${LCTL[@]}" resource list-volumes -r "$RD" || true
    exit 1
fi

echo ">> wait up to 60s for tiebreaker to be auto-stamped (3 total replicas)"
deadline=$(( $(date +%s) + 60 ))
tiebreaker=""
while (( $(date +%s) < deadline )); do
    # Tiebreaker stamp: flags contain both TIE_BREAKER and DRBD_DISKLESS
    # in the upstream-LINSTOR resource flags.
    tiebreaker=$("${LCTL[@]}" --machine-readable resource list -r "$RD" 2>/dev/null \
        | jq -r '.[][] | select((.flags // []) | index("TIE_BREAKER")) | .node_name' \
        | head -1)
    if [[ -n "$tiebreaker" ]]; then
        break
    fi
    sleep 2
done

if [[ -z "$tiebreaker" ]]; then
    echo "FAIL: no TIE_BREAKER replica found after 60s"
    "${LCTL[@]}" resource list -r "$RD" || true
    exit 1
fi

echo ">> tiebreaker is on: $tiebreaker"

# Collect survivor names so we can scope the Connections check.
mapfile -t survivors < <(
    "${LCTL[@]}" --machine-readable resource list -r "$RD" \
        | jq -r --arg tb "$tiebreaker" '.[][] | select(.node_name != $tb) | .node_name'
)
echo ">> survivors: ${survivors[*]}"

echo ">> r d $tiebreaker $RD"
t0=$(date +%s)
"${LCTL[@]}" resource delete "$tiebreaker" "$RD"

# Within 10s: no survivor's Connections[] still references the tiebreaker.
# We poll both the JSON r l and /v1/view/resources to cover both surfaces.
echo ">> within 10s, survivors' Connections[] must not reference $tiebreaker"
deadline=$(( t0 + 10 ))
ghost=""
clean=0
while (( $(date +%s) < deadline )); do
    ghost=""

    # JSON view from `linstor r l -r <rd>` — newer schemas put per-peer
    # state in .layer_object.drbd.drbd_resource_definition or in
    # .state.connection_to[]. Be permissive: just check the entire
    # JSON blob for the tiebreaker hostname appearing anywhere on a
    # surviving node's row.
    json=$("${LCTL[@]}" --machine-readable resource list -r "$RD" 2>/dev/null || echo "[]")
    for s in "${survivors[@]}"; do
        # Extract that survivor's full sub-tree, then check if tiebreaker
        # name still appears (it shouldn't — peer was deleted).
        survivor_blob=$(echo "$json" | jq -c --arg s "$s" '.[][] | select(.node_name == $s)')
        if [[ -z "$survivor_blob" || "$survivor_blob" == "null" ]]; then
            continue
        fi
        if echo "$survivor_blob" | grep -q "\"$tiebreaker\""; then
            ghost="$s references $tiebreaker"
            break
        fi
    done

    # REST view sanity-check: /v1/view/resources with the rd filter
    # must not list a row keyed on the tiebreaker after delete.
    if [[ -z "$ghost" ]]; then
        view=$(curl -sf -m5 "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" 2>/dev/null || echo "[]")
        if echo "$view" | jq -e --arg tb "$tiebreaker" '.[]? | select(.node_name == $tb)' >/dev/null 2>&1; then
            ghost="view still lists tiebreaker $tiebreaker"
        fi
    fi

    if [[ -z "$ghost" ]]; then
        clean=1
        break
    fi

    sleep 1
done

elapsed=$(( $(date +%s) - t0 ))

if (( clean != 1 )); then
    echo "FAIL: ghost connection persisted ${elapsed}s after delete — $ghost"
    "${LCTL[@]}" resource list -r "$RD" || true
    curl -sf -m5 "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" \
        | jq . || true
    exit 1
fi

echo ">> connection cleanup OK in ${elapsed}s — no ghost reference to $tiebreaker"

# Cleanup
echo ">> rd delete $RD"
"${LCTL[@]}" resource-definition delete "$RD"

deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resourcedefinitions.blockstor.io.blockstor.io/${RD}" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "PASS lc-connection-cleanup"
