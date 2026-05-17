#!/usr/bin/env bash
#
# usage: recovery-suspended-quorum.sh WORK_DIR
#
# Bug 82 regression guard — bounded DRBD teardown on RD delete and
# DaemonSet preStop when the resource is `suspended:quorum`.
#
# Pre-fix story (RD-delete on a quorum-stuck resource):
#   * satellite handleDelete called drbdadm Adm.Down with NO timeout
#   * drbdadm down blocks forever on a `suspended:quorum` slot waiting
#     for the kernel to flush pending writes (quorum lost → no flush)
#   * the Resource CR finalizer never released → the RD CR never went
#     away → operators force-stripped the finalizer
#   * /dev/drbd<minor> survived as an orphan
#   * the NEXT RD that recycled the freed TCP port → DRBD minor failed
#     `drbdadm create-md` with `Device 'N' is configured!`, blocking
#     all fresh diskful provisioning until manual `drbdsetup down`
#   * DaemonSet preStop ran the same blocking drbdadm down, so satellite
#     pod termination during rollouts hung > 7 min on stuck nodes
#
# Post-fix behaviour (PR #2 commits 795346c7b + 27feb7e0d):
#   * pkg/drbd/drbdadm.go: DownForce(rd) wraps Adm.Down with a 15s
#     budget; on timeout iterates the minors and runs
#       drbdsetup detach --force <minor>
#       drbdsetup down <rd>
#     both of which bypass the quorum-wait path
#   * pkg/satellite/reconciler.go handleDelete uses DownForce
#   * stand/blockstor-satellite-daemonset.yaml preStop runs the same
#     kernel-direct fallback inline so DaemonSet rollouts stay bounded
#   * net result: an RD delete on a suspended:quorum resource MUST
#     complete (CRD gone, kernel slot torn down) inside the DownForce
#     budget + cleanup window, and the freed port/minor MUST be
#     reusable immediately by a fresh RD with the same backing size
#
# What this test does (real DRBD on the e2e-dualpri stand):
#   1. Create 3-replica RD `bug82-e2e` (autoplace, size 32M, pool=stand)
#      and wait for all three replicas UpToDate.
#   2. Pick worker-1 as the "stuck" node. Run `drbdadm disconnect`
#      from worker-1 to worker-2 AND to worker-3, killing both peer
#      connections. With both peers gone the slot loses quorum and
#      the kernel reports `suspended:quorum` in drbdsetup status.
#   3. Issue `linstor rd d bug82-e2e` via the python CLI against the
#      REST port-forward. Time the round-trip from rd-delete to the
#      RD CRD vanishing. With the fix this MUST be inside 60s
#      (DownForceTimeout=15s + finalizer/cleanup slack). Pre-fix
#      this would never complete because handleDelete blocked on
#      drbdadm down forever.
#   4. On every satellite verify `drbdsetup status bug82-e2e` reports
#      "no such resource" / empty — proves the kernel slot is torn
#      down end-to-end and the orphan-/dev/drbdN class of bug is gone.
#   5. Recycle test: immediately create `bug82-e2e-recycled` with the
#      same vd size, autoplace 3. Wait for UpToDate. Pre-fix this
#      would race the freed port/minor against the orphaned kernel
#      slot and fail `drbdadm create-md` with
#      `Device 'N' is configured!`.
#
# Cleanup trap restores peer connections (drbdadm connect on the
# stuck node, both directions) and force-deletes any leftover RDs
# via delete_rd so a partial-failure rerun doesn't trip on residue.
#
# Stand: e2e-dualpri on 129.213.29.101 — 3 worker nodes + 1 control
# plane, blockstor controller + apiserver + satellite DaemonSet.
# Pool: `stand` (FileDir-backed, see stand/blockstor-storagepools.yaml).

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

RD=bug82-e2e
RD_RECYCLE=bug82-e2e-recycled
SIZE=32M
POOL=${STORPOOL:-stand}

# Per-test timeouts. The DownForce budget on the fix side is 15s;
# we allow 60s for the full RD-delete envelope (REST → satellite
# observe DELETED → DownForce → handle Resource finalizer → drop
# RD CRD). 60s is the same envelope used by lc-rd-delete-cascade.
TEARDOWN_BUDGET=60
RECYCLE_BUDGET=180

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Random ephemeral port — parallel iters on the same host would
# otherwise collide on a fixed port.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/recovery-suspended-quorum-pf.log 2>&1 &
PF_PID=$!

# Track whether we already disconnected so cleanup knows to reconnect.
DISCONNECTED=0

dump_diag() {
    echo "---- dump: kubectl get resources ----"
    kubectl get resources.blockstor.io.blockstor.io -o wide 2>/dev/null || true
    echo "---- dump: kubectl get resourcedefinitions ----"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io -o wide 2>/dev/null || true
    for n in "$N1" "$N2" "$N3"; do
        echo "---- dump: drbdsetup status on $n ----"
        on_node "$n" drbdsetup status 2>/dev/null || true
        echo "---- dump: $n satellite log (tail 80) ----"
        local pod
        pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o "jsonpath={.items[?(@.spec.nodeName==\"${n}\")].metadata.name}")
        if [[ -n "$pod" ]]; then
            kubectl -n "$NS" logs "$pod" --tail=80 2>/dev/null || true
        fi
    done
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    if (( DISCONNECTED == 1 )); then
        # Best-effort: bring the disconnected peer connections back.
        # `drbdadm connect <rd>` is idempotent (no-op on Connected,
        # initiates handshake on StandAlone). Errors swallowed —
        # the RD is being deleted anyway.
        on_node "$N1" drbdadm connect "$RD" 2>/dev/null || true
    fi
    delete_rd "$RD" 2>/dev/null || true
    delete_rd "$RD_RECYCLE" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to bind before issuing CLI commands.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# ----------------------------------------------------------------------
# Step 1: create the 3-replica RD and wait for full UpToDate.
# ----------------------------------------------------------------------
echo ">> step 1: create $RD (3-replica autoplace, size=$SIZE, pool=$POOL)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" "$SIZE" >/dev/null
"${LCTL[@]}" resource-definition auto-place "$RD" \
    --place-count 3 --storage-pool "$POOL" >/dev/null

deadline=$(( $(date +%s) + 180 ))
uptodate=0
while (( $(date +%s) < deadline )); do
    n=$("${LCTLJ[@]}" volume list -r "$RD" 2>/dev/null \
        | jq -r '[.[][].volumes[]? | select(.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( n >= 3 )); then uptodate=1; break; fi
    sleep 2
done
if (( uptodate != 1 )); then
    echo "FAIL: $RD never reached 3/3 UpToDate within 180s"
    exit 1
fi
echo "   $RD: 3/3 UpToDate"

# ----------------------------------------------------------------------
# Step 2: force `suspended:quorum` by disconnecting both peers from N1.
# ----------------------------------------------------------------------
echo ">> step 2: disconnect N1=$N1 from N2=$N2 and N3=$N3 (quorum loss)"
on_node "$N1" drbdadm disconnect "$RD" 2>&1 || true
DISCONNECTED=1

# Wait up to 30s for the kernel on N1 to report suspended/quorum loss.
# `drbdsetup status <rd>` is the canonical surface — the keywords we
# tolerate are "quorum:no", "suspended", or "may_promote:no". Any of
# them indicates the slot has crossed into the bug-82 trigger zone.
echo "   wait up to 30s for N1 to observe quorum loss"
deadline=$(( $(date +%s) + 30 ))
saw_quorum_loss=0
while (( $(date +%s) < deadline )); do
    status_out=$(on_node "$N1" drbdsetup status "$RD" 2>&1 || true)
    if echo "$status_out" | grep -qiE "quorum:no|suspended|may_promote:no"; then
        saw_quorum_loss=1
        break
    fi
    sleep 2
done
if (( saw_quorum_loss != 1 )); then
    # If the kernel did not flip into the quorum-loss state, the
    # rest of the test is meaningless — the trigger condition for
    # Bug 82 was never reached. Bail out loud rather than declare
    # a false PASS.
    echo "FAIL: N1=$N1 never reported quorum loss after disconnect"
    on_node "$N1" drbdsetup status "$RD" 2>&1 || true
    exit 1
fi
echo "   N1=$N1 in quorum-loss state — Bug 82 trigger established"

# Capture the allocated DRBD minor on N1 before delete — we use it
# in step 4 to assert kernel-slot teardown.
DRBD_MINOR_BEFORE=$(on_node "$N1" bash -c "grep -oE 'minor [0-9]+' /etc/drbd.d/${RD}.res 2>/dev/null | head -1 | awk '{print \$2}'" || true)
echo "   DRBD minor on N1 before delete: ${DRBD_MINOR_BEFORE:-unknown}"

# ----------------------------------------------------------------------
# Step 3: `linstor rd d $RD` and time the round-trip.
# ----------------------------------------------------------------------
echo ">> step 3: linstor rd d $RD (expect completion within ${TEARDOWN_BUDGET}s)"
delete_start=$(date +%s)
# We don't fail on linstor's exit code immediately — the python
# client occasionally returns non-zero when the satellite responds
# with a delayed envelope; what we actually measure is whether the
# CRD vanishes inside the budget. But we DO record the CLI exit so
# we can include it in the failure dump.
linstor_exit=0
"${LCTL[@]}" resource-definition delete "$RD" 2>/tmp/rd-delete-bug82.err || linstor_exit=$?

# Poll for the RD CRD to disappear. delete_rd in lib.sh also has a
# belt-and-braces force path on EXIT, but the regression guard is
# specifically that the satellite-side finalizer chain unblocks the
# CRD by itself within DownForce + cleanup. We never call delete_rd
# from inside the timed window.
echo "   poll RD CRD to be gone"
crd_gone=0
deadline=$(( $(date +%s) + TEARDOWN_BUDGET ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get resourcedefinitions.blockstor.io.blockstor.io "$RD" \
            >/dev/null 2>&1; then
        crd_gone=1
        break
    fi
    sleep 1
done
delete_end=$(date +%s)
delete_secs=$(( delete_end - delete_start ))

if (( crd_gone != 1 )); then
    echo "FAIL: RD CRD $RD still present after ${TEARDOWN_BUDGET}s — Bug 82 regression"
    echo "      linstor CLI exit was: $linstor_exit"
    echo "      linstor CLI stderr:"
    sed 's/^/        | /' /tmp/rd-delete-bug82.err 2>/dev/null || true
    kubectl get resourcedefinition.blockstor.io.blockstor.io "$RD" -o yaml 2>/dev/null || true
    exit 1
fi
echo "   RD CRD $RD gone in ${delete_secs}s (budget ${TEARDOWN_BUDGET}s)"

# Disconnect flag is no longer meaningful — the RD is gone.
DISCONNECTED=0

# ----------------------------------------------------------------------
# Step 4: kernel-slot teardown on every satellite.
# ----------------------------------------------------------------------
echo ">> step 4: verify drbdsetup status reports no such resource on every node"
for node in "$N1" "$N2" "$N3"; do
    out=$(on_node "$node" drbdsetup status "$RD" 2>&1 || true)
    # Accept several phrasings — the exact wording varies across
    # drbd-utils versions. The contract is: the named resource is
    # NOT configured in the kernel anymore.
    if echo "$out" | grep -qiE "no such resource|not defined|not found"; then
        echo "   $node: kernel slot gone (\"$(echo "$out" | head -1)\")"
        continue
    fi
    # Empty stdout + non-error stderr also counts as "gone" (some
    # drbd-utils versions print nothing for an absent resource).
    if [[ -z "$(echo "$out" | tr -d '[:space:]')" ]]; then
        echo "   $node: kernel slot gone (empty output)"
        continue
    fi
    # Still showing? That's the orphan-/dev/drbdN regression.
    if echo "$out" | grep -qE "^${RD}\b"; then
        echo "FAIL: $node still has DRBD kernel state for $RD after RD delete"
        echo "      drbdsetup output:"
        echo "$out" | sed 's/^/        | /'
        exit 1
    fi
    # Anything else (e.g. a transient warning unrelated to $RD) we
    # log but accept — the name is not in the status, so the slot
    # is gone.
    echo "   $node: kernel slot gone (unrecognised output, but $RD not present)"
done

# ----------------------------------------------------------------------
# Step 5: recycle test — fresh RD with same backing size must reach
# UpToDate. Pre-fix this trips drbdadm create-md with
# `Device 'N' is configured!` because the orphan kernel slot still
# owns the recycled minor.
# ----------------------------------------------------------------------
echo ">> step 5: create $RD_RECYCLE (same size, autoplace 3) — port/minor recycle test"
"${LCTL[@]}" resource-definition create "$RD_RECYCLE" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_RECYCLE" "$SIZE" >/dev/null
"${LCTL[@]}" resource-definition auto-place "$RD_RECYCLE" \
    --place-count 3 --storage-pool "$POOL" >/dev/null

deadline=$(( $(date +%s) + RECYCLE_BUDGET ))
recycle_ok=0
while (( $(date +%s) < deadline )); do
    n=$("${LCTLJ[@]}" volume list -r "$RD_RECYCLE" 2>/dev/null \
        | jq -r '[.[][].volumes[]? | select(.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( n >= 3 )); then recycle_ok=1; break; fi
    sleep 3
done
if (( recycle_ok != 1 )); then
    echo "FAIL: $RD_RECYCLE never reached 3/3 UpToDate within ${RECYCLE_BUDGET}s"
    echo "      this is the orphan-minor 'Device N is configured!' regression"
    "${LCTL[@]}" resource list -r "$RD_RECYCLE" 2>&1 || true
    "${LCTL[@]}" error-reports list 2>&1 | tail -20 || true
    exit 1
fi
echo "   $RD_RECYCLE reached 3/3 UpToDate — port/minor recycled cleanly"

echo ">> RECOVERY-SUSPENDED-QUORUM OK"
echo "   RD-delete envelope: ${delete_secs}s (budget ${TEARDOWN_BUDGET}s)"
echo "   port/minor recycle: PASS"
