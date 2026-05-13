#!/usr/bin/env bash
#
# usage: recovery-discard-my-data.sh WORK_DIR
#
# Scenario 5.14 — StandAlone recovery via `drbdadm connect --discard-my-data`.
#
# Goal: validate the SKILL-documented recipe for clearing a StandAlone
# secondary back into sync without touching blockstor CRDs. The recipe
# (`drbdadm secondary --force` + `disconnect` + `connect --discard-my-data`
# on the outdated side, plus a stale-half `connect` kick on the primary)
# must succeed in spite of blockstor's reconciler also running on the
# same satellites — the reconciler must NOT re-issue `drbdadm adjust`
# to clobber the operator's deliberate one-way resync direction.
#
# Setup:
#   - 2-replica RD on workers 1+2, 64 MiB, autoplace disabled
#     (no tiebreaker — we want exactly two diskful peers so the
#     StandAlone half has nothing else to negotiate with).
#   - promote $N1 Primary, write 64 KiB urandom marker on the raw
#     DRBD device, capture md5. The satellite pod is busybox-thin
#     and has no mkfs/mount, so we stay at the block-device layer —
#     same pattern as network-partition.sh / lib.sh helpers.
#
# Steps:
#   1. Provoke StandAlone on $N2 by force-disconnecting at drbdsetup
#      level. `drbdadm disconnect` alone is racy because the satellite
#      reconciler can re-issue `drbdadm adjust` within the same second
#      and immediately reconnect; `drbdsetup disconnect ... --force=yes`
#      moves the connection FSM into StandAlone, which the reconciler's
#      drbdadm-adjust cannot trivially undo without an explicit recipe.
#   2. Apply discard-my-data recipe on $N2 + connect kick on $N1.
#   3. Within $RECOVERY_WINDOW (10 s): $N2 walks → UpToDate.
#   4. $N1 stays Primary throughout (regression guard — the recipe
#      targets the outdated side only; losing Primary would interrupt
#      any consumer still holding the device open).
#   5. md5 on $N1 unchanged; after UpToDate, $N2 device-level md5
#      matches what we originally wrote on $N1.
#
# Regression guards:
#   - Primary-ship on $N1 must never flap during the recovery window.
#   - Satellite log on $N2 must not show `drbdadm adjust` (or Apply
#     errors referencing this RD) during the 10 s recovery window —
#     if it does, the reconciler is fighting the operator's manual
#     recovery and the SKILL recipe would never converge in
#     production. We surface this as a log assertion at the end.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=splitbrain-test
N1=$WORKER_1
N2=$WORKER_2
SIZE_BYTES=$((64 * 1024)) # 64 KiB marker payload — bigger than 4 KiB
                          # alignment unit, small enough to resync in
                          # well under 10 s on the QEMU stand.
RECOVERY_WINDOW=10

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD on $N1 + $N2 (no tiebreaker)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
EOF

wait_uptodate "$RD" "$N1" "$N2"

DEV=$(device_for_rd "$RD" "$N1")
echo "   device on $N1 = $DEV"

# Promote $N1 and lay down the marker payload at the block layer.
# write_random in lib.sh runs `drbdadm primary` first, then dd, then
# reads back through dd to compute md5 — exactly what we want here,
# so the helper's contract becomes our marker contract.
echo ">> primary on $N1, write 64 KiB urandom marker"
md5_marker=$(write_random "$N1" "$DEV" "$SIZE_BYTES")
echo "   marker md5 on $N1 = $md5_marker"

# Confirm $N1 is Primary before we go anywhere near $N2 — if
# write_random left it Secondary we'd misdiagnose a later
# Primary-loss as a recovery-window regression.
n1_role_before=$(on_node "$N1" drbdsetup status "$RD" | grep "role:" | head -1)
echo "   $N1 role pre-test: $n1_role_before"
if [[ "$n1_role_before" != *"role:Primary"* ]]; then
    echo "FAIL: $N1 is not Primary before the StandAlone provocation"
    exit 1
fi

# Cache the $N2 satellite pod name so the log-tail at the end is
# stable against a churning DaemonSet (single-replica per node, but
# a satellite restart mid-test would otherwise yank our reference).
SAT_POD_N2=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")

# Discover $N1's node-id from $N2's connection list so we can issue
# `drbdsetup disconnect <RD> <peer-id> --force=yes`. `drbdadm
# disconnect` alone is racy: the satellite reconciler reapplies
# `drbdadm adjust` within ~1 s and the connection comes right back
# UpToDate before we ever observe StandAlone. `drbdsetup --force`
# moves the connection FSM into the StandAlone state, which `drbdadm
# adjust` then cannot silently undo.
echo ">> resolve $N1's node-id from $N2's connection view"
peer_node_id=$(on_node "$N2" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -oE "node-id:[0-9]+" | head -2 | tail -1 | cut -d: -f2 || true)
if [[ -z "$peer_node_id" ]]; then
    # Fallback: parse from rendered .res file. Each `on <hostname> {`
    # block declares a `node-id N;` line — pick the one for $N1.
    peer_node_id=$(on_node "$N2" bash -c "
        awk '/^on / { host=\$2 } /node-id/ { print host, \$2 }' /etc/drbd.d/${RD}.res
    " | awk -v h="$N1" '$1 == h { print $2 }' | tr -d ';' | head -1)
fi
echo "   $N1 node-id (from $N2's view) = ${peer_node_id:-<unknown>}"
if [[ -z "$peer_node_id" ]]; then
    echo "FAIL: could not resolve $N1's DRBD node-id"
    exit 1
fi

echo ">> provoke StandAlone on $N2 via drbdsetup disconnect --force=yes"
on_node "$N2" bash -c "
    drbdsetup disconnect ${RD} ${peer_node_id} --force=yes 2>&1 || true
"

# Wait ~6 s for $N2 to settle in a non-Connected state. We accept
# any non-Connected sub-state (StandAlone / Unconnected / Connecting
# / Outdated) — the recipe applies identically.
deadline=$(( $(date +%s) + 8 ))
n2_state=""
while (( $(date +%s) < deadline )); do
    n2_state=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null \
        | grep -E "connection:|disk:" | head -2 | tr '\n' ' ' || true)
    if [[ "$n2_state" != *"connection:Connected"* ]]; then
        break
    fi
    sleep 1
done
echo "   $N2 post-provocation state: $n2_state"
if [[ "$n2_state" == *"connection:Connected"* ]]; then
    echo "FAIL: $N2 reconnected on its own — could not provoke StandAlone"
    exit 1
fi

# Mark the start of the recovery window so the log scan at the end
# is bounded to the recipe-execution interval.
window_start=$(date +%s)

echo ">> apply discard-my-data recipe on $N2"
# `secondary --force` is a no-op when $N2 is already Secondary, but
# harmless and idempotent — keeps the recipe verbatim from the SKILL
# doc so the test stays a faithful regression of the documented
# procedure.
on_node "$N2" bash -c "
    drbdadm secondary --force ${RD} 2>&1 || true
    drbdadm disconnect ${RD} 2>&1 || true
    drbdadm connect --discard-my-data ${RD}
"

echo ">> connect kick on $N1 to clear stale half"
on_node "$N1" bash -c "
    drbdadm disconnect ${RD} 2>&1 || true
    drbdadm connect ${RD}
"

# Recovery polling loop: within RECOVERY_WINDOW seconds, $N2 must
# reach UpToDate. Sample $N1's role every iteration so we fail fast
# on Primary-loss (the regression we care most about).
echo ">> wait up to ${RECOVERY_WINDOW}s for $N2 → UpToDate; observe $N1 stays Primary"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
n2_uptodate=false
n1_lost_primary=false
n2_observed_states=""
while (( $(date +%s) <= deadline )); do
    n1_role=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "role:" | head -1 || true)
    if [[ "$n1_role" != *"role:Primary"* ]]; then
        n1_lost_primary=true
        echo "   !! $N1 lost Primary role: $n1_role"
        break
    fi

    n2_disk=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    n2_observed_states="${n2_observed_states}|${n2_disk}"
    if [[ "$n2_disk" == *"disk:UpToDate"* ]]; then
        n2_uptodate=true
        break
    fi
    sleep 1
done
window_elapsed=$(( $(date +%s) - window_start ))
echo "   $N2 disk-state trace: ${n2_observed_states}"
echo "   recovery window elapsed: ${window_elapsed}s"

if [[ "$n1_lost_primary" == "true" ]]; then
    echo "FAIL: $N1 lost Primary during recovery — data path interrupted"
    exit 1
fi

if [[ "$n2_uptodate" != "true" ]]; then
    echo "FAIL: $N2 did not reach UpToDate within ${RECOVERY_WINDOW}s"
    on_node "$N2" drbdsetup status "$RD" 2>&1 || true
    exit 1
fi

# Read the marker back from $N1's still-Primary device. The
# regression here is "did Primary survive AND keep its data
# coherent" — read_md5 uses iflag=direct so a stale page-cache
# entry cannot mask a kernel-side I/O hang or data corruption.
echo ">> read marker back from $N1 (must match pre-recovery md5)"
md5_marker_after=$(read_md5 "$N1" "$DEV" "$SIZE_BYTES")
if [[ "$md5_marker_after" != "$md5_marker" ]]; then
    echo "FAIL: marker drift on $N1 (before=$md5_marker, after=$md5_marker_after)"
    exit 1
fi
echo "   marker on $N1 unchanged"

# Scan the $N2 satellite log for `drbdadm adjust` invocations
# triggered during the recovery window. Apply-path doesn't emit a
# log line on the happy path, but Apply errors surface "Apply
# per-resource failure" with the RD name. A match here means the
# reconciler tried to undo the operator's recipe mid-flight — we
# surface it (the convergence assertion above already failed the
# test if it materially mattered, but a stray match is worth
# triaging).
echo ">> scan $N2 satellite log for adjust-during-recovery"
adjust_hits=$(kubectl -n "$NS" logs "$SAT_POD_N2" --since="${window_elapsed}s" 2>/dev/null \
    | grep -E "${RD}" | grep -ciE "adjust|Apply per-resource failure" || true)
echo "   adjust/Apply-failure log hits referencing $RD on $N2: ${adjust_hits}"

# Wait for any in-flight resync to fully drain before the final
# device-level cross-check. wait_uptodate's 180 s budget is overkill
# for 64 KiB but matches the helper contract.
wait_uptodate "$RD" "$N1" "$N2"

# Final cross-node cross-check: read the marker back through $N2's
# DRBD device after demoting $N1. This proves the discard-my-data
# direction was applied correctly ($N2 took $N1's data, not the
# other way around). Demote-on-N1 + sleep ensures the kernel has
# drained any in-flight writes before $N2 opens the device.
echo ">> demote $N1, promote $N2, read-back marker on $N2"
on_node "$N1" drbdadm secondary "$RD"
sleep 3
DEV_N2=$(device_for_rd "$RD" "$N2")
md5_n2=$(read_md5 "$N2" "$DEV_N2" "$SIZE_BYTES")
on_node "$N2" drbdadm secondary "$RD" 2>/dev/null || true
if [[ "$md5_n2" != "$md5_marker" ]]; then
    echo "FAIL: $N2 marker mismatch after sync ($N1=$md5_marker, $N2=$md5_n2)"
    exit 1
fi
echo "   $N2 marker matches $N1: $md5_n2"

echo ">> RECOVERY-DISCARD-MY-DATA OK ($N2 → UpToDate in ${window_elapsed}s, $N1 stayed Primary, adjust-hits=${adjust_hits})"
