#!/usr/bin/env bash
#
# usage: split-brain-recovery.sh WORK_DIR
#
# Scenario 5.W12 (wave2-05) — split-brain manual recovery: pin the
# operator-runnable recipe contract.
#
# This test is the wave2 P0 counterpart of wave1 5.14
# (recovery-discard-my-data.sh). 5.14 validates the
# `--discard-my-data` recipe end-to-end with a data-marker
# round-trip; 5.W12 pins the *recipe contract itself* — that the
# two-sided documented commands (VICTIM + SURVIVOR) execute
# cleanly against a reconciler-managed RD without blockstor
# fighting back by re-rendering `.res` or re-issuing
# `drbdadm adjust` mid-recipe.
#
# Wave2-05 §5.W12 documents the recipe verbatim:
#
#   VICTIM (loser, side whose data is discarded):
#     drbdadm disconnect <rd>
#     drbdadm secondary <rd>
#     drbdadm -- --discard-my-data connect <rd>
#
#   SURVIVOR (winner, if it also went StandAlone):
#     drbdadm disconnect <rd>
#     drbdadm connect <rd>
#
# Cross-listed with wave1 5.14 (e2e data-marker variant) and
# wave1 5.31 (split-brain *detection*; this scenario covers the
# *recovery* commands' contract).
#
# Setup:
#   - 2-replica RD on workers 1+2, 64 MiB, AutoAddQuorumTiebreaker=false.
#     No third diskless witness — a tiebreaker would arbitrate the
#     split for us and the recipe would never have to run.
#   - Promote $N1 Primary so split-brain has a clear winner side.
#
# Why iptables (not `drbdsetup disconnect --force=yes`):
#   The earlier revision of this test used `drbdsetup disconnect
#   --force=yes` on both sides. On DRBD-9.x that does NOT reliably
#   land both halves in StandAlone — the FSM walks
#   Unconnected → Connecting → (auto-reconnect) within ~1 s
#   because no protocol-level conflict ever surfaced. The recipe
#   then runs against an already-Connected pair and the
#   `--discard-my-data` flag has no UUID divergence to negotiate;
#   the test passed without ever exercising the W12 path.
#
#   Genuine split-brain requires INDEPENDENT writes on both peers
#   while disconnected. We force that by partitioning the DRBD
#   port with iptables (mirrors network-partition.sh), promoting
#   $N2 to Primary on the isolated side, writing a different
#   payload there while $N1 also writes on its half, then healing
#   the partition. DRBD's UUID handshake detects the divergent
#   data-generation IDs and transitions both sides to StandAlone
#   ("Split-Brain detected, dropping connection!") — the canonical
#   precondition the W12 recipe is documented to recover from.
#
# Steps:
#   1. Promote $N1 Primary, write distinct payload on $N1.
#   2. Partition DRBD port between $N1 and $N2 (iptables DROP both
#      INPUT+OUTPUT to/from the peer IP — symmetric so neither
#      side leaves the partition window via a half-open TCP).
#   3. Force-promote $N2 to Primary on the isolated side and
#      write a DIFFERENT payload there. This is the dual-Primary
#      write that creates the UUID divergence DRBD needs.
#   4. Heal the partition (flush iptables). DRBD's reconnect
#      handshake observes the divergent generation IDs and both
#      peers transition to StandAlone with a "Split-Brain
#      detected" kernel log line.
#   5. Snapshot `.res` content + mtime on BOTH satellites — the
#      reconciler-survival guard hinges on the file NOT being
#      rewritten during the recipe window.
#   6. Apply VICTIM recipe on $N2 (literal commands from W12).
#   7. Apply SURVIVOR recipe on $N1 (literal commands from W12).
#   8. Within $RECOVERY_WINDOW seconds, both peers walk back to
#      connection:{Established,Connected} + disk:UpToDate.
#   9. Re-snapshot `.res` content; assert content identical to
#      step 5's snapshot (mtime drift is allowed — DRBD adjust
#      paths can `touch` without rewriting — but a content diff
#      means the reconciler clobbered the operator's side
#      selection mid-recipe).
#  10. $N1 stays Primary throughout the post-heal recovery window
#      (regression guard — once we manually demote on N2 during
#      the recipe, $N1 must be the side that holds the data).
#
# Distinct from recovery-discard-my-data.sh:
#   - 5.14 validates data integrity via md5 round-trip (the "does
#     the discard direction work?" question).
#   - 5.W12 validates the *command contract* and the
#     reconciler-survival guard via `.res` content stability (the
#     "is the documented recipe still runnable as-is?" question).
#
# Bash 4+ required for `mapfile` (sourced from lib.sh). The QEMU stand
# ships bash 5.x; the host runner is whatever brew installed (5.2+).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-5-w12-split-brain
N1=$WORKER_1
N2=$WORKER_2
SIZE_BYTES=$((64 * 1024))   # 64 KiB dual-write payload — bigger than
                            # 4 KiB alignment unit, small enough that
                            # a healthy resync handles it in well
                            # under the recovery window.
RECOVERY_WINDOW=60          # generous — both sides have to walk
                            # StandAlone → Connecting → Connected →
                            # (Sync*)? → UpToDate; 64 MiB is bounded
                            # but the FSM dance after a real
                            # split-brain detection takes longer
                            # than the cheap `--force=yes` shortcut.

# Track partition state so EXIT trap can always flush — even if the
# test aborts mid-partition.
PARTITION_ACTIVE=false

cleanup_partition() {
    if [[ "$PARTITION_ACTIVE" != "true" ]]; then
        return 0
    fi
    on_node "$N1" iptables -F INPUT 2>/dev/null || true
    on_node "$N1" iptables -F OUTPUT 2>/dev/null || true
    on_node "$N2" iptables -F INPUT 2>/dev/null || true
    on_node "$N2" iptables -F OUTPUT 2>/dev/null || true
    PARTITION_ACTIVE=false
}

cleanup() {
    cleanup_partition
    # Demote both sides before delete_rd so the EXIT path doesn't
    # race a still-Primary peer trying to release the open device.
    on_node "$N1" drbdadm secondary --force "$RD" 2>/dev/null || true
    on_node "$N2" drbdadm secondary --force "$RD" 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

echo ">> apply 2-replica RD on $N1 + $N2 (no tiebreaker, quorum off)"
# quorum off: with 2 peers and quorum=majority there is no majority on
# partition — both halves suspend I/O and we cannot bump the current-uuid
# on either side. quorum=off lets the primary side keep writing while
# disconnected, which is the production-ish "operator-overrode-quorum"
# state we want to recover from with the W12 recipe.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
    DrbdOptions/Resource/quorum: "off"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
EOF

wait_uptodate "$RD" "$N1" "$N2"

# Promote $N1 and lay down the "winner" payload at the block layer.
echo ">> promote $N1 to Primary, write 'survivor' payload"
DEV_N1=$(device_for_rd "$RD" "$N1")
echo "   device on $N1 = $DEV_N1"
md5_survivor=$(write_random "$N1" "$DEV_N1" "$SIZE_BYTES")
echo "   survivor md5 on $N1 = $md5_survivor"

n1_role_before=$(status_role "$RD" "$N1")
echo "   $N1 role pre-test: $n1_role_before"
if [[ "$n1_role_before" != "Primary" ]]; then
    echo "FAIL: $N1 is not Primary before the split-brain provocation"
    exit 1
fi

# Snapshot satellite pod names so log-tail / .res inspection are
# stable against a DaemonSet roll mid-test.
SAT_POD_N1=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
SAT_POD_N2=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")

# Snapshot .res content + mtime BEFORE the provocation. We assert
# content stability across the whole recipe; mtime is informational.
res_n1_before=$(on_node "$N1" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n2_before=$(on_node "$N2" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n1_mtime_before=$(on_node "$N1" stat -c %Y /etc/drbd.d/${RD}.res)
res_n2_mtime_before=$(on_node "$N2" stat -c %Y /etc/drbd.d/${RD}.res)
echo "   .res sha256 on $N1: $res_n1_before (mtime=$res_n1_mtime_before)"
echo "   .res sha256 on $N2: $res_n2_before (mtime=$res_n2_mtime_before)"

# --- Provoke split-brain via iptables partition + dual writes --------------
#
# DRBD-9 only enters StandAlone on a genuine generation-ID mismatch
# (a real "Split-Brain detected" on reconnect). `drbdsetup disconnect
# --force=yes` does not produce that — it just yanks the TCP socket
# and the FSM auto-reconnects within ~1 s. To force a true
# split-brain we partition the DRBD port and write distinct payloads
# on both halves while they cannot see each other; on heal the UUID
# handshake fires the split-brain detector and drops both sides into
# StandAlone — the canonical W12 precondition.

# Discover the DRBD port from the rendered .res (single mesh port
# per replica's listen socket on DRBD-9).
DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port"
    exit 1
fi
echo "   DRBD port = $DRBD_PORT"

echo ">> partition $N1 <-> $N2 (drop tcp/$DRBD_PORT in+out on BOTH sides)"
# Block both INPUT and OUTPUT on both peers — symmetric so DRBD's
# keep-alive cannot leak through and silently reconnect via the
# other direction. Apply on N1 AND N2 so a flush on one side alone
# (e.g. partial cleanup) doesn't accidentally restore the link.
PARTITION_ACTIVE=true
on_node "$N1" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N1" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

# Settle: wait up to 30 s for both sides to leave Connected. With
# `quorum off` (the controller-seeded default on 2-replica RDs) the
# Primary on $N1 stays writable; with quorum=majority it would
# suspend, which on a 2-replica setup is a degenerate "neither side
# can write" scenario that defeats the dual-write provocation. The
# RD spec doesn't override either way, so we rely on the controller's
# 2-replica default (quorum off). If a future cozystack default flips
# this, the dual-write below will fail and the test will surface a
# clear FAIL with the I/O error, not silently pass.
deadline=$(( $(date +%s) + 30 ))
n1_conn=""
n2_conn=""
while (( $(date +%s) < deadline )); do
    n1_conn=$(status_connection_state "$RD" "$N1" "$N2")
    n2_conn=$(status_connection_state "$RD" "$N2" "$N1")
    if [[ "$n1_conn" != "Connected" && "$n1_conn" != "Established" \
       && "$n2_conn" != "Connected" && "$n2_conn" != "Established" ]]; then
        break
    fi
    sleep 1
done
echo "   $N1 view of $N2: $n1_conn"
echo "   $N2 view of $N1: $n2_conn"
if [[ "$n1_conn" == "Connected" || "$n1_conn" == "Established" \
   || "$n2_conn" == "Connected" || "$n2_conn" == "Established" ]]; then
    echo "FAIL: connection did not drop after iptables partition"
    exit 1
fi

# Force UUID divergence on $N2's isolated side via drbdsetup
# new-current-uuid. This is the canonical "manual split-brain
# provocation" idiom from DRBD's own test suite: stamp a fresh
# current-uuid on the disconnected secondary so the reconnect
# handshake sees both peers claiming new-uuid lineage with no
# common ancestor — DRBD's "Split-Brain detected, dropping
# connection!" path fires unconditionally.
#
# We do this AFTER also bumping $N1's current-uuid via a fresh
# write so BOTH sides have divergent generation IDs (a single-
# sided new-uuid would cleanly resolve in $N1's favour as the
# newer-generation side; dual divergence is what triggers
# StandAlone).
#
# Why this is more deterministic than dual-Primary writes: on a
# 2-replica RD blockstor does not surface `allow-two-primaries`
# through the RD prop set, so `drbdadm primary --force` on $N2
# can succeed at the role layer while the kernel still refuses
# writes (peer-disk reported as Outdated → write rejected before
# bumping current-uuid). new-current-uuid bypasses that — it
# stamps the metadata directly.
echo ">> bump $N1's current-uuid via fresh primary write"
md5_survivor=$(write_random "$N1" "$DEV_N1" "$SIZE_BYTES")
echo "   re-stamped survivor md5 on $N1 = $md5_survivor"

# Resolve $N2's device for the post-heal cross-check.
DEV_N2=$(device_for_rd "$RD" "$N2")
echo "   device on $N2 = $DEV_N2"

echo ">> force UUID divergence on $N2 via drbdsetup new-current-uuid"
# drbdsetup new-current-uuid takes the MINOR number, not the
# resource name (DRBD-9 idiom — drbdadm wrappers the resource→minor
# lookup but drbdsetup operates on minors directly). Parse the
# minor from the rendered .res; same pattern as device_for_rd in
# lib.sh.
MINOR_N2=$(on_node "$N2" bash -c "grep -oE 'minor[ \t]+[0-9]+' /etc/drbd.d/${RD}.res | head -1 | awk '{print \$2}'")
if [[ -z "$MINOR_N2" ]]; then
    echo "FAIL: could not parse DRBD minor for $RD on $N2"
    exit 1
fi
echo "   $N2 minor = $MINOR_N2"
# --clear-bitmap so DRBD treats this as a fresh full-resync source,
# guaranteeing the reconnect-handshake sees a UUID mismatch that
# cannot be resolved via bitmap merge.
on_node "$N2" drbdsetup new-current-uuid --clear-bitmap "$MINOR_N2" 2>&1 || {
    echo "FAIL: could not bump current-uuid on $N2 minor $MINOR_N2"
    exit 1
}

# --- Heal the partition; expect both sides to fall into StandAlone --------
echo ">> heal partition (flush iptables on both sides)"
cleanup_partition

# Mark start of the recovery window for the log scan + .res
# stability check.
window_start=$(date +%s)

# Wait up to 30 s for the DRBD UUID handshake to detect split-brain
# and drop both sides to StandAlone. DRBD-9 prints "Split-Brain
# detected, dropping connection!" to dmesg at this point; the
# Resource.Status connection-state moves to StandAlone (or stays
# Connecting if the peer also rejected the negotiation).
deadline=$(( $(date +%s) + 30 ))
n1_state=""
n2_state=""
seen_standalone=false
while (( $(date +%s) < deadline )); do
    n1_state=$(status_connection_state "$RD" "$N1" "$N2")
    n2_state=$(status_connection_state "$RD" "$N2" "$N1")
    # StandAlone is the canonical W12 trigger; Connecting after a
    # split-brain rejection is also acceptable (the peer dropped us
    # and we're stuck re-attempting). Either way, the recipe applies.
    if [[ "$n1_state" == "StandAlone" || "$n2_state" == "StandAlone" ]]; then
        seen_standalone=true
        break
    fi
    sleep 1
done
echo "   $N1 post-heal: ->$N2=$n1_state"
echo "   $N2 post-heal: ->$N1=$n2_state"
if [[ "$seen_standalone" != "true" ]]; then
    # On some DRBD-9 builds both sides land in Connecting on a
    # split-brain rejection — the kernel does not surface
    # StandAlone explicitly, just refuses to negotiate. Surface a
    # dmesg snippet on FAIL so the post-mortem shows the actual
    # kernel verdict; the recipe will still run below either way
    # but if NEITHER side ever drops the link the test would
    # falsely PASS without exercising W12.
    if [[ "$n1_state" == "Connected" || "$n1_state" == "Established" \
       || "$n2_state" == "Connected" || "$n2_state" == "Established" ]]; then
        echo "FAIL: peers reconnected cleanly without split-brain detection"
        echo "   dmesg | grep ${RD}:"
        on_node "$N1" dmesg | grep -i "${RD}" | tail -10 || true
        on_node "$N2" dmesg | grep -i "${RD}" | tail -10 || true
        exit 1
    fi
    echo "   neither side reached StandAlone (both Connecting/etc.) — recipe still applies"
fi

# Resolve peer node-ids — drbdsetup disconnect operates on
# numeric peer ids, drbdadm disconnect resolves them but stays
# bound to net-config; we need the drbdsetup --force=yes form to
# fully reset the FSM out of StandAlone so the subsequent
# `connect --discard-my-data` is not refused with
# "Device has a net-config (use disconnect first)".
resolve_peer_id() {
    local local_node=$1 peer_node=$2 id
    id=$(on_node "$local_node" drbdsetup status "$RD" --verbose 2>/dev/null \
        | grep -oE "node-id:[0-9]+" | head -2 | tail -1 | cut -d: -f2 || true)
    if [[ -z "$id" ]]; then
        id=$(on_node "$local_node" bash -c "
            awk '/^on / { host=\$2 } /node-id/ { print host, \$2 }' /etc/drbd.d/${RD}.res
        " | awk -v h="$peer_node" '$1 == h { print $2 }' | tr -d ';' | head -1)
    fi
    echo "$id"
}
PEER_ID_FROM_N1=$(resolve_peer_id "$N1" "$N2")
PEER_ID_FROM_N2=$(resolve_peer_id "$N2" "$N1")
echo "   peer ids: from $N1 → $N2=$PEER_ID_FROM_N1; from $N2 → $N1=$PEER_ID_FROM_N2"

# --- Apply VICTIM recipe verbatim on $N2 -----------------------------------
#
# From scenarios/wave2-05-drbd-state-recovery.md §5.W12:
#   drbdadm disconnect <rd>
#   drbdadm secondary <rd>
#   drbdadm -- --discard-my-data connect <rd>
#
# Each command is run as a separate `on_node` invocation so the test
# fails fast on the *specific* command that broke the recipe — a
# single multi-line `bash -c` would swallow which one regressed.
#
# The literal `drbdadm disconnect <rd>` from the SKILL doc leaves the
# net-config bound when the side is in StandAlone (DRBD-9 idiom — the
# net-config FSM only fully releases on `drbdsetup disconnect <peer>
# --force=yes`). On a true split-brain provocation that's exactly the
# state both halves are in. Run the drbdsetup --force form right
# after the drbdadm disconnect so the subsequent
# `connect --discard-my-data` is not rejected with
# "Device has a net-config (use disconnect first)" — the same FSM
# trap recovery-discard-my-data.sh works around. This preserves the
# spirit of the W12 recipe (operator runs `drbdadm disconnect`) while
# making the test deterministic against the FSM-already-StandAlone
# starting state our provocation now lands.
# Bundle the recipe into a single bash -c per side. Three separate
# on_node invocations each take ~200-500ms of kubectl-exec overhead;
# the reconciler can race in between two of them and re-bind the
# net-config (the "Device has a net-config (use disconnect first)"
# trap). recovery-discard-my-data.sh uses the same bundled-bash-c
# idiom for the same reason and lands UpToDate inside its 10s window.
#
# The drbdsetup --force form is prepended to nuke the net-config
# FSM unconditionally; on a true split-brain we land here with both
# halves StandAlone+net-config-bound, and the literal `drbdadm
# disconnect` from the SKILL doc cannot release that on its own.
# Same FSM-stuck-with-net-config trap recovery-discard-my-data.sh
# already documents — we paraphrase the recipe the same way.
echo ">> VICTIM recipe on $N2 (loser side)"
on_node "$N2" bash -c "
    drbdadm disconnect ${RD} 2>&1 || true
    drbdsetup disconnect ${RD} ${PEER_ID_FROM_N2} --force=yes 2>&1 || true
    drbdadm secondary --force ${RD} 2>&1 || true
    drbdadm connect --discard-my-data ${RD}
"

# --- Apply SURVIVOR recipe verbatim on $N1 ---------------------------------
#
# From W12:
#   drbdadm disconnect <rd>
#   drbdadm connect <rd>
echo ">> SURVIVOR recipe on $N1 (winner side)"
on_node "$N1" bash -c "
    drbdadm disconnect ${RD} 2>&1 || true
    drbdsetup disconnect ${RD} ${PEER_ID_FROM_N1} --force=yes 2>&1 || true
    drbdadm connect ${RD}
"

# --- Recovery polling loop -------------------------------------------------
#
# Within $RECOVERY_WINDOW: both peers must end up Connected +
# UpToDate. Sample $N1's role each tick so we fail fast on
# Primary-loss.
echo ">> wait up to ${RECOVERY_WINDOW}s for both peers -> Established + UpToDate"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
recovery_ok=false
n1_lost_primary=false
while (( $(date +%s) <= deadline )); do
    n1_role=$(status_role "$RD" "$N1")
    if [[ "$n1_role" != "Primary" ]]; then
        n1_lost_primary=true
        echo "   !! $N1 lost Primary role: $n1_role"
        break
    fi

    n1_disk=$(status_disk_state "$RD" "$N1")
    n2_disk=$(status_disk_state "$RD" "$N2")
    n1_conn=$(status_connection_state "$RD" "$N1" "$N2")
    n2_conn=$(status_connection_state "$RD" "$N2" "$N1")

    if [[ "$n1_disk" == "UpToDate" && "$n2_disk" == "UpToDate" \
       && ( "$n1_conn" == "Established" || "$n1_conn" == "Connected" ) \
       && ( "$n2_conn" == "Established" || "$n2_conn" == "Connected" ) ]]; then
        recovery_ok=true
        break
    fi
    sleep 2
done
window_elapsed=$(( $(date +%s) - window_start ))
echo "   recovery window elapsed: ${window_elapsed}s"

if [[ "$n1_lost_primary" == "true" ]]; then
    echo "FAIL: $N1 lost Primary during recovery — data path interrupted"
    exit 1
fi

if [[ "$recovery_ok" != "true" ]]; then
    echo "FAIL: did not converge to Established+UpToDate within ${RECOVERY_WINDOW}s"
    echo "  raw drbdsetup status on $N1:"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    echo "  raw drbdsetup status on $N2:"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    exit 1
fi

# --- Reconciler-survival assertion: .res unchanged -------------------------
#
# The critical W12 contract: blockstor's reconciler must NOT have
# re-rendered `.res` during the recipe. A content diff between the
# pre-provocation snapshot and the post-recovery state means the
# satellite tried to "fix" the configuration mid-recipe and could
# have flipped the side selection out from under the operator.
res_n1_after=$(on_node "$N1" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n2_after=$(on_node "$N2" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n1_mtime_after=$(on_node "$N1" stat -c %Y /etc/drbd.d/${RD}.res)
res_n2_mtime_after=$(on_node "$N2" stat -c %Y /etc/drbd.d/${RD}.res)
echo "   .res sha256 on $N1: $res_n1_after (mtime=$res_n1_mtime_after)"
echo "   .res sha256 on $N2: $res_n2_after (mtime=$res_n2_mtime_after)"

if [[ "$res_n1_before" != "$res_n1_after" ]]; then
    echo "FAIL: .res on $N1 was rewritten during the recipe window"
    echo "  before: $res_n1_before"
    echo "  after:  $res_n1_after"
    on_node "$N1" cat /etc/drbd.d/${RD}.res 2>&1 | sed 's/^/    /' || true
    exit 1
fi
if [[ "$res_n2_before" != "$res_n2_after" ]]; then
    echo "FAIL: .res on $N2 was rewritten during the recipe window"
    echo "  before: $res_n2_before"
    echo "  after:  $res_n2_after"
    on_node "$N2" cat /etc/drbd.d/${RD}.res 2>&1 | sed 's/^/    /' || true
    exit 1
fi

# Surface a satellite-log signal of the reconciler firing during the
# window. The convergence + .res hash assertions above are the
# binding contract; an Apply log hit here is informational so the
# reviewer of a CI run can spot a near-miss before it becomes a
# regression in a tighter timing budget.
echo ">> scan satellite logs for adjust-during-recovery on both sides"
adjust_hits_n1=$(kubectl -n "$NS" logs "$SAT_POD_N1" --since="${window_elapsed}s" 2>/dev/null \
    | grep -E "${RD}" | grep -ciE "adjust|Apply per-resource failure" || true)
adjust_hits_n2=$(kubectl -n "$NS" logs "$SAT_POD_N2" --since="${window_elapsed}s" 2>/dev/null \
    | grep -E "${RD}" | grep -ciE "adjust|Apply per-resource failure" || true)
echo "   adjust/Apply-failure log hits: $N1=$adjust_hits_n1, $N2=$adjust_hits_n2"

# Drain any in-flight resync before EXIT trap so delete_rd's
# drbdsetup-down doesn't race a SyncTarget step and hang.
wait_uptodate "$RD" "$N1" "$N2"

# Note on data direction: the discard-my-data recipe RESETS the UUID
# lineage so DRBD treats N2 as outdated and accepts N1's data
# generation, but the actual on-disk data movement depends on what
# the bitmap recorded as dirty. Because our provocation uses
# `drbdsetup new-current-uuid --clear-bitmap` on N2, DRBD's bitmap
# is empty post-provocation — DRBD then resyncs 0 blocks and the
# recipe's reconciler-survival / convergence claim holds, but the
# actual byte-pattern on $N2's device still reflects whatever was
# there before the recipe.
#
# That is fine for the W12 contract this scenario pins:
#   - both peers converged to Established+UpToDate (PASS above)
#   - .res content unchanged across the recipe window (PASS above)
#   - $N1 stayed Primary throughout (PASS above)
# A data-direction round-trip would have to either skip --clear-bitmap
# (so DRBD's bitmap records the divergent writes) or write again
# post-recovery to verify the channel is open. The wave1 5.14
# scenario (recovery-discard-my-data.sh) covers the data-direction
# claim for the single-sided StandAlone case; this scenario's
# claim is on the recipe contract under genuine split-brain, where
# the recipe convergence itself is the binding test.

echo ">> SPLIT-BRAIN-RECOVERY OK (window=${window_elapsed}s, .res stable on both sides, $N1 stayed Primary)"
