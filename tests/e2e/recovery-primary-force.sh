#!/usr/bin/env bash
#
# usage: recovery-primary-force.sh WORK_DIR
#
# Scenario 5.30 — emergency-recovery `drbdadm primary --force` from
# the satellite shell must NOT be auto-undone by the reconciler.
#
# When both halves of a 2-replica RD are reachable but the operator
# has decided (out-of-band) to force-promote the Secondary side —
# e.g. the actual Primary's userspace is wedged but the kernel is
# still serving Established, so a normal `drbdadm primary` would be
# refused by DRBD — the recovery recipe is `drbdadm primary --force`
# directly on the satellite. After this, both peers will report
# role:Primary (dual-primary) until the operator resolves the
# original wedge and demotes one side.
#
# Regression target: the blockstor satellite reconciler must NOT
# observe "peer role doesn't match my desired state" and silently
# call `drbdadm secondary` on the force-promoted side. Doing so
# would defeat the entire purpose of the recipe — it's an
# *emergency* override and the human operator owns the demote step.
#
# Setup:
#   - 2-replica RD on workers 1+2, 64 MiB, autoplace disabled.
#   - DRBD reaches UpToDate on both peers.
#   - Identify which side is Secondary (the reconciler does not
#     auto-promote without the auto-primary RD-prop, so the post-
#     wait state should be Secondary/Secondary; we just pick one).
#
# Steps:
#   1. ssh into the Secondary side's satellite pod and run
#      `drbdadm primary <rd> --force`.
#   2. Confirm `drbdadm status` shows role:Primary on the forced
#      side. We also attempt `drbdadm primary --force` on the OTHER
#      side — DRBD's default config refuses this with "Multiple
#      primaries not allowed" (exit 11) so we end up single-Primary
#      in practice; the assertion target is "reconciler accepts an
#      externally-driven role flip", which holds either way. On a
#      config with allow-two-primaries=yes the second promote would
#      succeed and the scenario would naturally extend to dual-
#      Primary without any other change.
#   3. Watch for $OBSERVE_WINDOW seconds. The reconciler must NOT
#      issue `drbdadm secondary` on either side. We watch role
#      directly (sampling every second) AND scrape satellite logs
#      for the substring `drbdadm secondary` referencing this RD.
#   4. Prove the forced-Primary side is actually writable: dd a
#      small marker through the raw /dev/drbdN device.
#   5. Cleanup: demote both sides via `drbdadm secondary --force`
#      from the operator side (test cleanup mirrors what the human
#      operator would do post-recovery), then trap-delete the RD.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=primary-force-test
N1=$WORKER_1
N2=$WORKER_2
SIZE_BYTES=$((64 * 1024))  # 64 KiB writability marker
OBSERVE_WINDOW=30          # reconciler must NOT demote within this window

# Cleanup trap demotes both sides before letting delete_rd run, so
# the EXIT path doesn't race a still-Primary peer trying to release
# the open device. `secondary --force` is idempotent — a peer that
# was already Secondary just no-ops.
cleanup() {
    on_node "$N1" drbdadm secondary --force "$RD" 2>/dev/null || true
    on_node "$N2" drbdadm secondary --force "$RD" 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

echo ">> apply 2-replica RD on $N1 + $N2"
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

# Identify the Secondary side (without auto-primary prop, both
# should be Secondary on settle — we pick N2 as the force-promote
# target, then also promote N1 to exercise dual-Primary).
n1_role=$(status_role "$RD" "$N1")
n2_role=$(status_role "$RD" "$N2")
echo "   pre-test roles: $N1=$n1_role  $N2=$n2_role"

# Cache satellite pods for log scanning later. Single-replica per
# node — but capture by name so a mid-test pod restart yanks the
# log scan rather than silently sampling a different generation.
SAT_POD_N1=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
SAT_POD_N2=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")

window_start=$(date +%s)

echo ">> drbdadm primary --force on $N2 (emergency-recovery promote)"
on_node "$N2" drbdadm primary --force "$RD"

# Attempt to also force-promote N1 to exercise the dual-Primary
# case. DRBD's default config rejects this with "Multiple primaries
# not allowed by config" (exit 11) — that's fine: the assertion we
# care about is "reconciler does not undo the N2 promote", and that
# holds regardless of whether N1 actually transitions. Logged as a
# best-effort step so the scenario remains valid on configs that DO
# allow dual-Primary (allow-two-primaries=yes) without changing the
# pass criteria.
echo ">> attempt drbdadm primary --force on $N1 (best-effort dual-Primary)"
on_node "$N1" drbdadm primary --force "$RD" 2>&1 || true

# Sample roles immediately after the promote pair — both should
# read role:Primary. If DRBD refused N1's promote (split-brain
# negotiation, etc.) we still continue; the test's primary
# assertion is "reconciler does not demote N2", which holds in
# either single- or dual-Primary mode.
sleep 1
n1_role_post=$(status_role "$RD" "$N1")
n2_role_post=$(status_role "$RD" "$N2")
echo "   post-promote: $N1=$n1_role_post  $N2=$n2_role_post"

if [[ "$n2_role_post" != "Primary" ]]; then
    echo "FAIL: $N2 did not become Primary after drbdadm primary --force"
    exit 1
fi

# Observe window: sample roles every second. If at any point N2
# (or N1, if dual-Primary was achieved) drops to Secondary, the
# reconciler has demoted us — emergency recovery defeated. Record
# the trace so the post-mortem shows exactly when the drop
# happened.
echo ">> observe ${OBSERVE_WINDOW}s — reconciler must NOT demote forced-Primary side"
demoted=false
demote_at=-1
role_trace=""
for i in $(seq 1 "$OBSERVE_WINDOW"); do
    r_n2=$(status_role "$RD" "$N2")
    r_n1=$(status_role "$RD" "$N1")
    role_trace="${role_trace}|t${i}:${r_n1}/${r_n2}"
    if [[ "$r_n2" != "Primary" ]]; then
        demoted=true
        demote_at=$i
        echo "   !! $N2 demoted at t=${i}s: $r_n2"
        break
    fi
    sleep 1
done

if [[ "$demoted" == "true" ]]; then
    echo "FAIL: reconciler demoted $N2 within ${demote_at}s of force-promote"
    echo "   trace: ${role_trace}"
    kubectl -n "$NS" logs "$SAT_POD_N2" --since="${OBSERVE_WINDOW}s" 2>/dev/null \
        | grep -E "${RD}|secondary|adjust" | tail -30 || true
    exit 1
fi
echo "   $N2 stayed Primary for full ${OBSERVE_WINDOW}s"

# Cross-check via satellite logs — even if role sampling didn't
# catch a demote (e.g. the reconciler demoted then DRBD raced
# back to Primary somehow), the log will record the drbdadm
# invocation. We grep for `drbdadm secondary` referencing this
# RD, scoped to the observe window.
window_elapsed=$(( $(date +%s) - window_start ))
echo ">> scan satellite logs for 'drbdadm secondary' on $RD"
sec_hits_n1=$(kubectl -n "$NS" logs "$SAT_POD_N1" --since="${window_elapsed}s" 2>/dev/null \
    | grep "${RD}" | grep -ciE "drbdadm secondary|Adm\\.Secondary" || true)
sec_hits_n2=$(kubectl -n "$NS" logs "$SAT_POD_N2" --since="${window_elapsed}s" 2>/dev/null \
    | grep "${RD}" | grep -ciE "drbdadm secondary|Adm\\.Secondary" || true)
echo "   secondary-log hits: $N1=${sec_hits_n1}  $N2=${sec_hits_n2}"

if (( sec_hits_n1 > 0 || sec_hits_n2 > 0 )); then
    echo "FAIL: satellite logged drbdadm secondary on $RD during recovery window"
    exit 1
fi

# Writability proof: dd through the forced-Primary device. If the
# reconciler had partially undone the promote (e.g. demote+repromote
# leaving a stale device handle), this dd would EIO out.
echo ">> prove $N2 is writable post-force-promote"
DEV_N2=$(device_for_rd "$RD" "$N2")
echo "   device on $N2 = $DEV_N2"
blocks=$(( (SIZE_BYTES + 4095) / 4096 ))
md5_written=$(on_node "$N2" bash -c "
    dd if=/dev/urandom of=${DEV_N2} bs=4096 count=${blocks} status=none oflag=direct
    dd if=${DEV_N2} bs=4096 count=${blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
")
echo "   write+readback md5 on $N2 = $md5_written"
if [[ -z "$md5_written" || "$md5_written" == "d41d8cd98f00b204e9800998ecf8427e" ]]; then
    # empty md5 == dd failed silently
    echo "FAIL: write to forced-Primary $N2 produced empty/failed md5"
    exit 1
fi

echo ">> RECOVERY-PRIMARY-FORCE OK (forced-Primary held ${OBSERVE_WINDOW}s, secondary-hits=$((sec_hits_n1 + sec_hits_n2)), writable)"
