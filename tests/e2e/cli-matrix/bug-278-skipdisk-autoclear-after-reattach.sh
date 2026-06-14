#!/usr/bin/env bash
#
# usage: bug-278-skipdisk-autoclear-after-reattach.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 278.
#
# Reproduction: after a satellite pod restart (simulating a Talos OS
# kernel upgrade), the existing defensive-stamp code path stamps
# `DrbdOptions/SkipDisk=True` onto Resource.Spec.Props. Pre-fix, the
# stamp survived the reattach forever — the satellite kept dispatching
# `drbdadm adjust --skip-disk` and the local volume stayed Diskless
# even though the kernel was healthy.
#
# Bug 278 fix (pkg/satellite/reconciler.go runAdjust + SkipDiskClearer):
# on EVERY reconcile, when the reconciler sees the observer-owned
# `SkipDisk=True` stamp AND the kernel probe reports the local volume
# as non-Diskless (HasDisklessVolume==false — UpToDate / Inconsistent /
# Outdated, backing storage attached), the satellite releases the
# observer's SSA claim on the SkipDisk key via SkipDiskClearer. The next
# dispatcher cycle re-resolves Spec.Props without SkipDisk, the FSM
# transitions PhaseSkipDisk → PhaseRunning, and the next reconcile
# dispatches plain `drbdadm adjust` to keep the lower disk attached.
#
# Two-sided contract this cell pins (BUG-046 triage corrected the
# previous revision — see below):
#
#   A. PERSISTENCE (positive control). On a replica whose kernel volume
#      really is Diskless (HasDisklessVolume==true), the auto-clear gate
#      is INTENTIONALLY closed: a defensive SkipDisk stamp there MUST
#      survive — that is the exact state SkipDisk is meant to guard. This
#      proves the clearer is selective (it does not blindly wipe every
#      stamp) and that the stamp wiring works at all.
#
#   B. AUTO-CLEAR. On a HEALTHY (UpToDate) replica the satellite auto-
#      clears the observer-owned defensive stamp, so the volume stays
#      attached after a Talos-upgrade reattach. We restart the satellite
#      pod on that node (the documented Talos kernel-restart shape), and
#      assert the stamp is gone and the disk is UpToDate.
#
# Why the previous revision could never pass (BUG-046 triage): it stamped
# SkipDisk on a HEALTHY UpToDate replica and then read the prop BACK as a
# setup precondition (`assert stamped == "True"`). But the auto-clear
# fires on EVERY reconcile of a healthy slot, so the satellite cleared
# the stamp between the SSA apply and the read-back — the precondition
# raced the product's correct behaviour and aborted with "stamp did not
# land". The stamped-state on a healthy disk is transient BY DESIGN; the
# cell now asserts the auto-clear directly (poll for ABSENCE, the steady
# state) and uses the genuinely-persistent Diskless replica as the
# positive control instead of a brittle read-back of a doomed stamp.
#
# The SSA stamp is applied under the OBSERVER's field manager
# (`blockstor-satellite-skipdisk`, pkg/satellite/controllers/observer.go)
# so it carries the exact SSA ownership the real defensive write has.
# The auto-clear RELEASES that owner's claim — that is the whole product
# contract: the satellite un-stamps only its own defensive writes, while
# an operator-set SkipDisk (different field manager) would survive. The
# apply doc carries the two +required immutable scalars
# (resourceDefinitionName / nodeName) — same SSA-validation rule the
# observer's writeSkipDiskProp follows; --force-conflicts mirrors the
# observer's ForceOwnership.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-278

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# stamp_skipdisk <node> — SSA-apply DrbdOptions/SkipDisk=True onto
# (RD, NODE).Spec.Props under the observer's field manager, mirroring
# the real defensive stamp's ownership shape.
stamp_skipdisk() {
    local node=$1
    kubectl apply --server-side --force-conflicts \
        --field-manager=blockstor-satellite-skipdisk -f - <<EOF >/dev/null
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata:
  name: ${RD}.${node}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${node}
  props:
    DrbdOptions/SkipDisk: "True"
EOF
}

# skipdisk_prop <node> — echo the current SkipDisk prop value on
# (RD, NODE).Spec.Props ("True" or empty).
skipdisk_prop() {
    local node=$1
    kubectl get "resources.blockstor.cozystack.io/${RD}.${node}" \
        -o jsonpath='{.spec.props.DrbdOptions/SkipDisk}' 2>/dev/null || echo ""
}

echo ">> [Bug 278] 2-replica diskful RD on $N1+$N2, diskless replica on $N3"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand >/dev/null
"${LCTL[@]}" resource create "$N3" "$RD" --diskless >/dev/null

echo ">> wait for both diskful UpToDate ($N1, $N2)"
wait_uptodate "$RD" "$N1" "$N2"

echo ">> wait for $N3 to settle Diskless"
if ! wait_status_diskless "$RD" "$N3" 60; then
    echo "FAIL (Bug 278 setup): $N3 never converged to Diskless; aborting" >&2
    exit 1
fi

# ---- Part A: PERSISTENCE on a genuinely-Diskless replica -----------------
#
# HasDisklessVolume==true on $N3, so the auto-clear gate is closed: the
# defensive stamp MUST survive there. This is the positive control that
# proves the clearer is selective (it does not blindly wipe every stamp).
echo ">> [Bug 278/A] stamp SkipDisk=True onto the DISKLESS $N3 (must persist)"
stamp_skipdisk "$N3"

echo ">> [Bug 278/A] confirm SkipDisk persists on Diskless $N3 (15s observation)"
# Poll for a sustained 15s: the stamp must stay pinned, not flicker away.
persist_ok=true
for _ in $(seq 1 8); do
    val=$(skipdisk_prop "$N3")
    if [[ "$val" != "True" ]]; then
        persist_ok=false
        break
    fi
    sleep 2
done
if [[ "$persist_ok" != "true" ]]; then
    echo "FAIL (Bug 278/A regression): SkipDisk was cleared on the Diskless $N3 — the clearer is over-eager (must only clear on a HEALTHY slot)" >&2
    kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
        -o json 2>/dev/null | jq '{props: .spec.props, status: .status}' >&2 || true
    exit 1
fi

# ---- Part B: AUTO-CLEAR on a HEALTHY replica after reattach --------------
#
# Stamp the same defensive prop onto the HEALTHY $N2. Restart its
# satellite pod (the documented Talos kernel-upgrade reattach shape),
# then assert the satellite auto-clears the observer-owned stamp and the
# disk stays UpToDate. We poll for ABSENCE — the steady state the
# product converges to — rather than the transient stamped state (which
# the clearer correctly wipes on every reconcile of a healthy slot, and
# which the previous revision wrongly tried to read back as a precondition).
echo ">> [Bug 278/B] stamp SkipDisk=True onto the HEALTHY $N2 (simulates pre-upgrade defensive stamp)"
stamp_skipdisk "$N2"

echo ">> [Bug 278/B] restart satellite pod on $N2 (simulates Talos kernel upgrade reattach)"
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    --field-selector "spec.nodeName=$N2" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [[ -z "$sat_pod" ]]; then
    echo "FAIL (Bug 278 setup): no satellite pod found on $N2; aborting" >&2
    exit 1
fi
kubectl -n "$NS" delete pod "$sat_pod" --wait=true >/dev/null

echo ">> wait for satellite back up on $N2"
new_pod=""
for _ in $(seq 1 60); do
    new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        --field-selector "spec.nodeName=$N2,status.phase=Running" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$new_pod" && "$new_pod" != "$sat_pod" ]]; then
        break
    fi
    sleep 2
done
if [[ -z "$new_pod" || "$new_pod" == "$sat_pod" ]]; then
    echo "FAIL (Bug 278 setup): new satellite pod did not start on $N2 within 120s" >&2
    exit 1
fi

echo ">> [Bug 278/B] wait up to 60s for the satellite to auto-clear SkipDisk on $N2"
# Poll Resource.Spec.Props.DrbdOptions/SkipDisk — Bug 278 contract:
# the satellite reconciler probes kernel state, sees healthy
# (HasDisklessVolume==false), and releases the observer's SSA claim on
# the SkipDisk key. After SSA release the apiserver removes the key from
# Spec.Props (no other owner claims it).
cleared=false
for _ in $(seq 1 30); do
    if [[ -z "$(skipdisk_prop "$N2")" ]]; then
        cleared=true
        break
    fi
    sleep 2
done

if [[ "$cleared" != "true" ]]; then
    echo "FAIL (Bug 278): SkipDisk did NOT auto-clear on the HEALTHY $N2 within 60s after satellite restart" >&2
    kubectl get "resources.blockstor.cozystack.io/${RD}.${N2}" \
        -o json 2>/dev/null | jq '{props: .spec.props, status: .status}' >&2 || true
    exit 1
fi

echo ">> [Bug 278/B] confirm $N2 disk state is UpToDate (stayed attached after clear)"
if ! wait_status_state "$RD" "$N2" "UpToDate" 60 0; then
    echo "FAIL (Bug 278 deep): SkipDisk cleared but $N2 did not hold UpToDate within 60s" >&2
    kubectl get "resources.blockstor.cozystack.io/${RD}.${N2}" \
        -o json 2>/dev/null | jq '{props: .spec.props, status: .status}' >&2 || true
    exit 1
fi

# Sibling check: $N1 must still be UpToDate (the restart on $N2 must
# NOT affect the sibling's disk state — that would mean either the
# auto-clear path was overzealous or the cluster has another
# instability).
n1_disk=$(status_disk_state "$RD" "$N1" 0)
if [[ "$n1_disk" != "UpToDate" ]]; then
    echo "FAIL (Bug 278 sibling regression): $N1 disk_state=$n1_disk after $N2 reattach (want UpToDate)" >&2
    exit 1
fi

echo ">> bug-278-skipdisk-autoclear-after-reattach OK (A: stamp persists on Diskless $N3; B: auto-clear fires on healthy $N2 reattach, $N2 stays UpToDate)"
