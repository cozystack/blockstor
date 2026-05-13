#!/usr/bin/env bash
#
# usage: node-restore.sh WORK_DIR
#
# Scenario 4.22 — `linstor node restore <node>` clears the EVICTED
# flag so the autoplacer is willing to land replicas on it again.
# Existing resources that already migrated off during the evacuate
# stay on their new homes — no auto-balance-back.
#
# Steps:
#   1. Spawn `test-restore` (place-count=2) with worker-3 deliberately
#      included via explicit `linstor resource create`; wait UpToDate.
#   2. `linstor node evacuate worker-3`; the NodeReconciler triggers
#      a replacement (placer fills the place-count=2 gap on a survivor
#      if WORKER_3's replica is dropped, or stays at 2 if WORKER_3 was
#      the third diskful). Wait until the EVICTED flag is observed.
#   3. `linstor node restore worker-3`. Within 30 s the EVICTED flag
#      is gone and connection_status flips back to ONLINE.
#   4. Pin "no auto-rebalance back": list the resources on each node
#      after restore. None of the resources that the placer moved off
#      during evacuate should be re-spawned on worker-3 without an
#      explicit operator action.
#   5. Spawn `test-after-restore` with place-count=3; the third replica
#      lands on worker-3 (the autoplacer is willing to use it again).

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

RD=test-restore
PROOF_RD=test-after-restore
RG=test-restore-rg

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/node-restore-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor n l ----"
    linstor --controllers "http://localhost:$PF_PORT" node list || true
    echo "---- dump: linstor r l ----"
    linstor --controllers "http://localhost:$PF_PORT" resource list || true
    echo "---- dump: kubectl get nodes.blockstor.io.blockstor.io -o yaml ----"
    kubectl get nodes.blockstor.io.blockstor.io -o yaml | tail -80 || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/restore" >/dev/null 2>&1 || true
    # Belt-and-suspenders: also strip the EVICTED flag at the CRD
    # level, in case the port-forward died before the curl above.
    kubectl patch "nodes.blockstor.io.blockstor.io/$WORKER_3" --type=merge \
        -p '{"spec":{"flags":null}}' >/dev/null 2>&1 || true

    delete_rd "$RD" 2>/dev/null || true
    delete_rd "$PROOF_RD" 2>/dev/null || true
    "${LCTL[@]}" resource-group delete "$RG" 2>/dev/null || true

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
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

echo ">> create RG $RG with place-count=2 so NodeReconciler's gap-fill knows the target"
# NodeReconciler.migrateResource reads PlaceCount from the RD's
# RG link (filter.PlaceCount defaults to 1 if no RG). Without an
# RG link, evacuating worker-3 leaves the surviving worker-1
# replica at PlaceCount=1 and no replacement is spawned.
"${LCTL[@]}" resource-group create "$RG" --place-count 2 --storage-pool stand >/dev/null

echo ">> spawn $RD via RG $RG and pin onto $WORKER_1+$WORKER_3, wait UpToDate"
"${LCTL[@]}" resource-definition create "$RD" --resource-group "$RG" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 100M >/dev/null
# Disable the auto-tiebreaker on this RD so the 3rd worker doesn't
# absorb the place_count=2 slot via a DISKLESS+TIE_BREAKER replica.
# Without this, the placer sees 1 diskful (worker-1) + 1 diskless
# tiebreaker (worker-2) as place_count=2 satisfied and never spawns
# a 2nd diskful when worker-3 is evicted.
"${LCTL[@]}" resource-definition set-property "$RD" \
    DrbdOptions/AutoAddQuorumTiebreaker false >/dev/null
"${LCTL[@]}" resource create "$WORKER_1" "$RD" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$WORKER_3" "$RD" --storage-pool stand >/dev/null

wait_uptodate "$RD" "$WORKER_1" "$WORKER_3"

echo ">> POST /v1/nodes/$WORKER_3/evacuate"
# Bypass the linstor CLI: blockstor's success envelope uses a
# non-upstream `maskInfo` (0x0001_0000_0000 vs upstream
# 0x0040_0000_0000_0000) which the python-linstor client treats as
# an error and then crashes parsing the JSON body as XML. The REST
# layer itself returns 200 + valid JSON.
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/evacuate" >/dev/null

echo ">> wait up to 30s for EVICTED flag to land"
deadline=$(( $(date +%s) + 30 ))
got_evicted=0
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" == *"EVICTED"* ]]; then
        got_evicted=1
        break
    fi
    sleep 1
done
if (( got_evicted != 1 )); then
    echo "FAIL: EVICTED flag never appeared (flags=$flags)"
    exit 1
fi

echo ">> wait up to 90s for placer to spawn replacement on a survivor"
deadline=$(( $(date +%s) + 90 ))
moved=0
moved_to=""
while (( $(date +%s) < deadline )); do
    # Look for a NEW diskful replica on a node that's neither
    # WORKER_3 nor WORKER_1 (the original pair). Placer needs to
    # bring the surviving diskful set back to place-count=2.
    survivors=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '[.[][] | select(.node_name != $w3) | select((.flags // []) | index("DISKLESS") | not) | .node_name] | unique | length')
    if (( survivors >= 2 )); then
        moved=1
        moved_to=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
            | jq -r --arg w3 "$WORKER_3" --arg w1 "$WORKER_1" \
            '.[][] | select(.node_name != $w3 and .node_name != $w1) | select((.flags // []) | index("DISKLESS") | not) | .node_name' | head -1)
        break
    fi
    sleep 2
done
if (( moved != 1 )); then
    echo "FAIL: placer never spawned a 2nd diskful survivor after evacuate"
    echo "---- view/resources ----"
    curl -fsS "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" 2>/dev/null | jq .
    echo "---- Resource CRDs ----"
    kubectl get resources.blockstor.io.blockstor.io -o yaml | grep -E "name:|nodeName|flags:|StorPoolName" | head -40
    exit 1
fi
echo "   replacement landed on: $moved_to"

echo ">> POST /v1/nodes/$WORKER_3/restore"
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/restore" >/dev/null

echo ">> within 30s: EVICTED clears AND connection_status=ONLINE"
deadline=$(( $(date +%s) + 30 ))
restored=0
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    conn=$(kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" \
        -o jsonpath='{.status.connectionStatus}' 2>/dev/null || true)
    if [[ "$flags" != *"EVICTED"* && "$conn" == "ONLINE" ]]; then
        restored=1
        break
    fi
    sleep 1
done
if (( restored != 1 )); then
    echo "FAIL: restore did not land (flags=$flags conn=$conn)"
    exit 1
fi

echo ">> pin: post-restore, replaced replica must STAY on $moved_to"
# Allow 10 s — if the NodeReconciler had an auto-rebalance back, it
# would re-spawn on WORKER_3 within one reconcile cycle. We assert
# the opposite: $moved_to keeps its diskful replica.
sleep 10
still_there=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
    | jq -r --arg mt "$moved_to" \
    '.[][] | select(.node_name == $mt) | select((.flags // []) | index("DISKLESS") | not) | .node_name' | head -1)
if [[ "$still_there" != "$moved_to" ]]; then
    echo "FAIL: $moved_to lost its replica post-restore — looks like auto-rebalance-back"
    "${LCTLJ[@]}" resource list -r "$RD" || true
    exit 1
fi

# Optional: pin that WORKER_3 didn't get auto-spawned BACK either
# (it may still have the old replica with DELETE flag pending the
# satellite cleanup — that's fine; what matters is the *replacement*
# stays where it is).

echo ">> spawn $PROOF_RD with --place-count 3; expect WORKER_3 to participate"
"${LCTL[@]}" resource-definition create "$PROOF_RD" >/dev/null
"${LCTL[@]}" volume-definition create "$PROOF_RD" 100M >/dev/null
"${LCTL[@]}" resource-definition auto-place "$PROOF_RD" --place-count 3 >/dev/null

deadline=$(( $(date +%s) + 180 ))
landed=0
while (( $(date +%s) < deadline )); do
    on_w3=$("${LCTLJ[@]}" resource list -r "$PROOF_RD" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '.[][] | select(.node_name == $w3) | select((.flags // []) | index("DISKLESS") | not) | .node_name' | head -1)
    if [[ "$on_w3" == "$WORKER_3" ]]; then
        landed=1
        break
    fi
    sleep 3
done
if (( landed != 1 )); then
    echo "FAIL: 3-replica autoplace did not place on $WORKER_3 — restore didn't actually re-enable it"
    "${LCTLJ[@]}" resource list -r "$PROOF_RD" || true
    exit 1
fi

echo ">> NODE-RESTORE OK"
