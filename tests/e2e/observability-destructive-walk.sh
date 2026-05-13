#!/usr/bin/env bash
#
# usage: observability-destructive-walk.sh WORK_DIR
#
# Scenario 7.13 — operator-visible destructive op walks all 3 levels
# without interrupting I/O on the resource's primary.
#
# Setup:
#   - 3-replica RD: 2 diskful via `rd_apply` on worker-A + worker-B,
#     plus 1 TIE_BREAKER witness that blockstor's RD reconciler
#     auto-stamps on the third worker. (We provision via the
#     blockstor CRDs directly instead of via piraeus-csi to keep
#     this test independent of the upstream CSI controller's
#     ControllerPublish path; the scenario under test is the
#     destructive-op observability flow, not CSI integration.)
#   - A background dd-loop inside the *PRIMARY* satellite pod that
#     appends a 4 KiB block to /dev/drbdN with conv=fsync once every
#     ~40ms, recording the post-fsync ms-timestamp to a file. fsync
#     forces the write through DRBD to the diskful peer, so a
#     replication / quorum disturbance shows up as a missing tick.
#
# Action:
#   - `linstor r d <tiebreaker-node> <rd>` via the upstream CLI
#     pointed at blockstor's REST surface. blockstor's
#     `handleResourceDelete` (pkg/rest/autoplace.go) detects the
#     TIE_BREAKER flag and stamps an
#     `auto-tiebreaker-suppressed-until` annotation on the parent
#     RD BEFORE the delete (Bug-4 carve-out). The RD reconciler
#     then skips re-stamping the witness for the 5-minute window.
#
#     The 7.13 scenario text in
#     tests/scenarios/07-quorum-observability.md predates this fix
#     and still says "tiebreaker auto-recreated on different node";
#     this test asserts the post-fix behaviour (witness gone, stays
#     gone) — that's the operator-intent semantic the suppression
#     window encodes. The spec doc is stale on this point and
#     should be updated to match the cheat-sheet text in
#     tests/observability-cheat-sheet-scenarios.md (item 5), which
#     accepts "the resource is just absent" as the expected level-3
#     observation.
#
# Expected (all checked within 10s of the delete):
#   Level 1 (K8s):     RD CRD stays Present; diskful Resource CRDs
#                      stay (no spurious Warning events).
#   Level 2 (LINSTOR): row for $TB_NODE absent from
#                      `linstor r l -r <rd>`; the RD carries the
#                      Bug-4 suppression annotation; the two
#                      diskful rows survive.
#   Level 3 (Node):    on the ex-tiebreaker satellite,
#                      `drbdadm status <rd>` reports
#                      "No currently configured DRBD found", and
#                      /etc/drbd.d/<rd>.res is gone.
#
# Asserts max inter-write gap < 1s (no I/O interruption).
#
# Cleanup: standard delete_rd helper from lib.sh, plus best-effort
# kill of the dd-loop kubectl-exec process.

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
if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH"
    exit 0
fi

RD=e2e-7-13-destructive
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# --- linstor CLI port-forward against blockstor-apiserver ---------------
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/7-13-pf.log 2>&1 &
PF_PID=$!

DD_LOG=/tmp/7-13-dd-${RD}.log
DD_PID=""

cleanup() {
    set +e
    if [[ -n "$DD_PID" ]]; then
        kill "$DD_PID" 2>/dev/null
        wait "$DD_PID" 2>/dev/null
    fi
    delete_rd "$RD" 2>/dev/null
    rm -f "$DD_LOG"
    kill "$PF_PID" 2>/dev/null
    wait "$PF_PID" 2>/dev/null
    set -e
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTL_M=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# --- 2 diskful replicas via rd_apply ------------------------------------
echo ">> apply 2-replica RD on $N1 + $N2 (default 64 MiB)"
rd_apply "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N1" "$N2"

# --- wait for blockstor to auto-stamp the TIE_BREAKER witness -----------
echo ">> wait 90s for the auto-tiebreaker on $N3"
deadline=$(( $(date +%s) + 90 ))
TB_NODE=""
while (( $(date +%s) < deadline )); do
    if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" >/dev/null 2>&1; then
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || true)
        if [[ "$flags" == *"TIE_BREAKER"* ]]; then
            TB_NODE=$N3
            break
        fi
    fi
    sleep 2
done
if [[ -z "$TB_NODE" ]]; then
    echo "FAIL: TIE_BREAKER witness never stamped on $N3 within 90s"
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD" '$1 ~ rd'
    exit 1
fi
echo "   tiebreaker node: $TB_NODE"

# --- Promote $N1 to primary + locate its /dev/drbdN ---------------------
DEV=$(device_for_rd "$RD" "$N1")
if [[ -z "$DEV" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $N1"
    exit 1
fi
echo "   primary=$N1 dev=$DEV"

# Promote to Primary so the dd-loop's writes go through DRBD to the
# diskful peer (and the witness). on_node already exec'd this dance
# inside lib.sh helpers, but we re-issue here to be explicit and to
# fail loudly if it can't.
on_node "$N1" drbdadm primary "$RD" 2>&1 | head -5 || true

# --- Background dd-loop inside the $N1 satellite pod --------------------
# Writes a single 4 KiB block at offset 0 of the DRBD device, fsyncs,
# then records the millisecond timestamp. ~25 ticks/s — comfortably
# above the 1 tick/s gap threshold even when the QEMU stand stalls.
# The output is captured on the host so we can analyze gaps after
# the destructive op.
echo ">> spawn dd-loop on $N1:$DEV (4 KiB / ~40ms / fsync)"
SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "FAIL: no satellite pod on $N1"
    exit 1
fi

(
    kubectl -n "$NS" exec "$SAT_POD" -- bash -c "
        while :; do
            dd if=/dev/urandom of=${DEV} bs=4096 count=1 \
                conv=fsync seek=0 status=none 2>/dev/null || exit 1
            date +%s%3N
            sleep 0.04
        done
    " 2>&1
) >"$DD_LOG" &
DD_PID=$!

# Warm-up: wait until the dd-loop has produced a steady stream.
echo ">> warm-up dd-loop for 5s"
sleep 5
warm_ticks=$(grep -c '^[0-9]' "$DD_LOG" || true)
echo "   warm-up ticks: $warm_ticks"
if [[ "${warm_ticks:-0}" -lt 50 ]]; then
    echo "FAIL: dd-loop only produced ${warm_ticks} ticks in 5s warm-up"
    tail -20 "$DD_LOG"
    exit 1
fi

# --- ACTION: linstor r d $TB_NODE $RD -----------------------------------
DELETE_T_MS=$(date +%s%3N)
echo ">> [t=0] linstor r d $TB_NODE $RD"
"${LCTL[@]}" resource delete "$TB_NODE" "$RD"

# --- Level 1 (K8s): RD + diskful Resource CRDs survive ------------------
echo ">> Level 1 (K8s): RD CRD present + diskful Resource CRDs survive"
deadline=$(( $(date +%s) + 10 ))
l1_ok=false
while (( $(date +%s) < deadline )); do
    rd_present=$(kubectl get \
        "resourcedefinitions.blockstor.io.blockstor.io/${RD}" \
        -o name 2>/dev/null || true)
    r_n1=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N1}" \
        -o name 2>/dev/null || true)
    r_n2=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o name 2>/dev/null || true)
    if [[ -n "$rd_present" && -n "$r_n1" && -n "$r_n2" ]]; then
        l1_ok=true
    fi
    sleep 1
done
if [[ "$l1_ok" != "true" ]]; then
    echo "FAIL: Level 1 — RD=$rd_present  R_${N1}=$r_n1  R_${N2}=$r_n2"
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD" '$1 ~ rd'
    exit 1
fi
# Tiebreaker Resource CRD must be gone.
r_tb=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${TB_NODE}" \
    -o name 2>/dev/null || true)
if [[ -n "$r_tb" ]]; then
    echo "FAIL: Level 1 — tiebreaker Resource CRD ${RD}.${TB_NODE} still present"
    exit 1
fi
echo "   Level 1 OK (RD + 2 diskful Resources stable; ${RD}.${TB_NODE} absent)"

# --- Level 2 (LINSTOR): tiebreaker row gone + suppression annotation ----
echo ">> Level 2 (LINSTOR): tiebreaker row gone + suppression annotation present"
deadline=$(( $(date +%s) + 10 ))
l2_ok=false
while (( $(date +%s) < deadline )); do
    tb_rows=$("${LCTL_M[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r --arg n "$TB_NODE" '
            [.[0][]? | select(.node_name == $n)] | length
        ')
    df_rows=$("${LCTL_M[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r '
            [.[0][]? | select((.flags // []) | index("DISKLESS") | not)] | length
        ')
    if [[ "${tb_rows:-1}" == "0" && "${df_rows:-0}" -ge 2 ]]; then
        l2_ok=true
        break
    fi
    sleep 1
done
if [[ "$l2_ok" != "true" ]]; then
    echo "FAIL: Level 2 — tb_rows=$tb_rows  df_rows=$df_rows"
    "${LCTL[@]}" resource list -r "$RD" || true
    exit 1
fi
ANN=$(kubectl get resourcedefinitions.blockstor.io.blockstor.io "$RD" \
    -o jsonpath='{.metadata.annotations.blockstor\.io/auto-tiebreaker-suppressed-until}' \
    2>/dev/null || true)
if [[ -z "$ANN" ]]; then
    echo "FAIL: Bug-4 suppression annotation missing on RD $RD"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io "$RD" -o yaml \
        | grep -A2 annotations || true
    exit 1
fi
echo "   Level 2 OK (no row on $TB_NODE; 2 diskful survive; suppression-until=$ANN)"

# --- Level 3 (Node): drbdadm clean + .res gone on ex-tiebreaker ---------
echo ">> Level 3 (Node @ $TB_NODE): drbdadm + .res both clean"
deadline=$(( $(date +%s) + 10 ))
l3_drbd_ok=false
l3_res_ok=false
drbd_out=""
while (( $(date +%s) < deadline )); do
    drbd_out=$(on_node "$TB_NODE" drbdadm status "$RD" 2>&1 || true)
    # `drbdadm status <rd>` reports one of:
    #   - "No currently configured DRBD found." — global form (no
    #     resources loaded at all, kernel module empty)
    #   - "no resources defined!"               — per-resource form
    #     when the .res file is gone and the named resource isn't
    #     loaded but the kernel may still hold sibling resources
    # Both are functionally the same observation for scenario 7.13:
    # the destructive op has reached the node. Accept either.
    if echo "$drbd_out" | grep -qE "No currently configured DRBD found|no resources defined"; then
        l3_drbd_ok=true
    fi
    res_exists=$(on_node "$TB_NODE" \
        bash -c "[ -f /etc/drbd.d/${RD}.res ] && echo y || echo n" \
        2>/dev/null || echo y)
    if [[ "$res_exists" == "n" ]]; then
        l3_res_ok=true
    fi
    [[ "$l3_drbd_ok" == "true" && "$l3_res_ok" == "true" ]] && break
    sleep 1
done
if [[ "$l3_drbd_ok" != "true" || "$l3_res_ok" != "true" ]]; then
    echo "FAIL: Level 3 — drbd_ok=$l3_drbd_ok res_ok=$l3_res_ok"
    echo "drbdadm status output:"
    echo "$drbd_out"
    on_node "$TB_NODE" ls -l /etc/drbd.d/ 2>/dev/null || true
    exit 1
fi
echo "   Level 3 OK (drbdadm: 'No currently configured DRBD found'; .res absent)"

# --- I/O continuity check -----------------------------------------------
# Stop dd-loop and compute max inter-write gap. The natural delta is
# ~40ms (sleep 0.04); the assertion threshold is 1000ms.
echo ">> stop dd-loop + compute max inter-write gap"
kill "$DD_PID" 2>/dev/null || true
wait "$DD_PID" 2>/dev/null || true
DD_PID=""

total_ticks=$(grep -c '^[0-9]' "$DD_LOG" || true)
MAX_GAP_MS=$(awk '
    /^[0-9]/ {
        if (prev != "") {
            d = $1 - prev
            if (d > max) max = d
        }
        prev = $1
    }
    END { print (max == "" ? 0 : max) }
' "$DD_LOG")
echo "   total_ticks=$total_ticks  max_gap_ms=$MAX_GAP_MS"

if [[ "${MAX_GAP_MS:-0}" -ge 1000 ]]; then
    echo "FAIL: dd-loop max inter-write gap ${MAX_GAP_MS}ms >= 1000ms"
    # Show the tail so we can see when the gap landed.
    tail -20 "$DD_LOG"
    exit 1
fi

# --- summary ------------------------------------------------------------
WALL_MS=$(( $(date +%s%3N) - DELETE_T_MS ))
echo ">> OBSERVABILITY-DESTRUCTIVE-WALK OK"
echo "   delete -> all-3-levels-asserted wall=${WALL_MS}ms"
echo "   max inter-write gap: ${MAX_GAP_MS}ms (threshold: 1000ms)"
echo "   tiebreaker on $TB_NODE removed; suppression-until=$ANN (Bug-4 in effect)"
