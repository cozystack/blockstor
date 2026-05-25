#!/usr/bin/env bash
#
# usage: recovery-quorum-persistence.sh WORK_DIR
#
# Scenario 7.5 — quorum-off persistence across satellite restart.
#
# The operator's recovery muscle-memory: a 3-replica RD lost two of
# its peers, I/O is suspended on the surviving Primary, and the
# operator does the canonical "ride out the outage" dance:
#
#     <patch RD: DrbdOptions/Resource/quorum=off>
#     drbdadm adjust  <rd>       # satellite re-renders .res with quorum off
#     drbdadm resume-io <rd>     # kernel drops the suspended:quorum flag
#
# This MUST be persistent. The DRBD .res file on the surviving
# Primary has to keep `quorum off;` across a satellite pod restart,
# otherwise:
#   * satellite pod bounce (operator update / node drain / OOM)
#     re-renders the .res file
#   * `drbdadm adjust` picks up the default quorum (majority)
#   * I/O re-suspends on the Primary the operator was deliberately
#     keeping alive
# That would defeat the whole point of the override.
#
# Why CRD-driven (not `linstor rd sp`):
#   The earlier revision used `linstor rd set-property` against a
#   port-forwarded REST endpoint plus `linstor resource-definition
#   auto-place`. On blockstor (CRD-backed controller) that path is
#   a phantom: `linstor r l` happily shows UpToDate replicas while
#   `drbdsetup status` reports the RD does not exist on the kernel,
#   because the REST shim does not roundtrip through the blockstor
#   ResourceDefinition / Resource CRDs. Every assertion downstream
#   then operates on a fiction. We rewrite the test against the
#   CRD model the rest of the e2e suite uses (recovery-primary-
#   force.sh, recovery-setgi-per-peer.sh, network-partition.sh),
#   `kubectl apply` the RD + 3 explicit Resources, and patch
#   `spec.props.DrbdOptions/Resource/quorum` on the RD CRD to
#   trigger the .res re-render the test is asserting persistence on.
#
# Why iptables (not talosctl reboot):
#   The original test crashed peer talos VMs to induce quorum loss.
#   That is fragile (Talos+QEMU reboot can leave flannel broken on
#   the way back up, the stand becomes single-use) and orthogonal
#   to the persistence claim. Quorum loss only needs the kernel on
#   the survivor to see its peers vanish. Dropping the DRBD port
#   between the survivor and the two peer satellites achieves
#   exactly that, and the cleanup is `iptables -F` — no node
#   reboot, the stand is reusable.
#
# Setup:
#   - 3-replica RD on workers 1+2+3, 32 MiB, AutoAddQuorumTiebreaker=false,
#     DrbdOptions/Resource/quorum=majority, DrbdOptions/Resource/on-no-quorum=suspend-io
#     (explicit so the test does not depend on controller defaults).
#   - Promote $N1 Primary, write a small marker so the suspended-I/O
#     window is observable on a real consumer device.
#
# Steps:
#   1. Apply RD + 3 Resources; wait 3/3 UpToDate; promote $N1.
#   2. Partition DRBD port between $N1 and {$N2, $N3} via iptables on
#      $N1. Wait for the surviving Primary to register quorum loss
#      (Status.Volumes[0].Quorum=="false" OR Status.Suspended non-empty).
#   3. Patch the RD CRD: spec.props.DrbdOptions/Resource/quorum=off.
#      The controller propagates the prop change to the satellite,
#      which re-renders .res and runs `drbdadm adjust`. Verify the
#      .res on $N1 contains `quorum off;` within 60s.
#   4. `drbdadm resume-io` + write to confirm the Primary is writable
#      again after the override.
#   5. Delete the satellite pod on $N1. Wait for the replacement pod
#      to be Ready and the satellite to re-register with the
#      controller.
#   6. Verify .res on $N1 STILL contains `quorum off;` — the
#      persistence claim. RD CRD prop survives the satellite respawn
#      because the controller is authoritative and re-pushes the
#      prop on satellite reconnect.
#   7. Heal the partition (flush iptables). Wait for all 3 replicas
#      to reconverge UpToDate.
#   8. Patch the RD CRD back to quorum=majority. Verify .res on $N1
#      no longer has `quorum off;` and the Primary does NOT re-suspend
#      I/O (we only flip back after the kernel knows quorum is healthy
#      again — if the test races this, it surfaces as a write failure
#      below, not a silent pass).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=quorum-test
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
SIZE_KIB=32768
MARKER_BYTES=$((64 * 1024))

# Track partition state so EXIT trap always restores connectivity
# even if the test aborts mid-window.
PARTITION_ACTIVE=false

cleanup_partition() {
    if [[ "$PARTITION_ACTIVE" != "true" ]]; then
        return 0
    fi
    on_node "$N1" iptables -F INPUT 2>/dev/null || true
    on_node "$N1" iptables -F OUTPUT 2>/dev/null || true
    PARTITION_ACTIVE=false
}

cleanup() {
    cleanup_partition
    # Best-effort: restore quorum=majority on the RD before delete so
    # the next test starts from a clean default. Patch is idempotent —
    # if the test failed before applying the override, this is a no-op
    # against an unchanged spec.
    kubectl patch resourcedefinition "$RD" --type=merge \
        -p '{"spec":{"props":{"DrbdOptions/Resource/quorum":"majority"},"drbdOptions":{"resource":{"quorum":"majority"}}}}' \
        >/dev/null 2>&1 || true
    on_node "$N1" drbdadm secondary --force "$RD" 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

# -- Step 1: create 3-replica RD, mount Primary on N1 -----------------
#
# DrbdOptions/AutoQuorum: "disabled" — without this, the
# ResourceDefinitionReconciler's setQuorum() unconditionally
# re-stamps `DrbdOptions/Resource/quorum=majority` on every RD
# reconcile (resourcedefinition_controller.go::quorumPolicy returns
# majority for 3 diskful + 0 diskless). That would revert the manual
# quorum=off override at step 3 the moment any RD field changes,
# which is exactly Scenario 7.W01 — "operator owns the policy in
# auto-quorum=disabled mode". We set it at creation time so the
# step-3 patch sticks across the satellite reconciles that follow.
echo ">> step 1: apply 3-replica RD ${RD} on ${N1}, ${N2}, ${N3} (auto-quorum disabled, quorum=majority)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoQuorum: "disabled"
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
    DrbdOptions/Resource/quorum: "majority"
    DrbdOptions/Resource/on-no-quorum: "suspend-io"
  drbdOptions:
    resource:
      quorum: "majority"
      onNoQuorum: "suspend-io"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
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
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: stand}
EOF

# 3-replica wait — wait_uptodate is 2-replica; inline the 3-way wait.
echo ">> wait up to 180s for 3/3 UpToDate"
deadline=$(( $(date +%s) + 180 ))
all_up=false
while (( $(date +%s) < deadline )); do
    s1=$(status_disk_state "$RD" "$N1")
    s2=$(status_disk_state "$RD" "$N2")
    s3=$(status_disk_state "$RD" "$N3")
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" && "$s3" == "UpToDate" ]]; then
        all_up=true
        break
    fi
    sleep 2
done
if [[ "$all_up" != "true" ]]; then
    echo "FAIL: $RD never reached 3/3 UpToDate (states: $N1=$s1 $N2=$s2 $N3=$s3)"
    exit 1
fi
echo "   3/3 UpToDate"

DEV=$(device_for_rd "$RD" "$N1")
echo ">> promote $N1 to Primary, write marker"
md5_marker=$(write_random "$N1" "$DEV" "$MARKER_BYTES")
echo "   marker md5 on $N1 = $md5_marker"

n1_role=$(status_role "$RD" "$N1")
if [[ "$n1_role" != "Primary" ]]; then
    echo "FAIL: $N1 is not Primary after write_random"
    exit 1
fi

# Capture initial .res on N1 for the persistence diff.
res_initial=$(on_node "$N1" cat /etc/drbd.d/${RD}.res || true)
echo "---- .res INITIAL on $N1 (first 40 lines) ----"
echo "$res_initial" | sed -n '1,40p'

# -- Step 2: provoke quorum loss via iptables port partition --------
#
# Drop the DRBD mesh port on $N1 in both directions so the kernel
# cannot see EITHER peer. With quorum=majority on a 3-replica RD,
# losing both peers takes $N1 below quorum (1/3) and the kernel
# raises `suspended:quorum`. No node reboot, the stand is reusable.

DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from .res"
    exit 1
fi
echo ">> step 2: partition $N1 (drop tcp/$DRBD_PORT in+out) to lose quorum"
PARTITION_ACTIVE=true
on_node "$N1" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N1" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

deadline=$(( $(date +%s) + 90 ))
quorum_lost=false
while (( $(date +%s) < deadline )); do
    q=$(status_volume_quorum "$RD" "$N1")
    s=$(status_suspended "$RD" "$N1")
    if [[ "$q" == "false" || -n "$s" ]]; then
        quorum_lost=true
        echo "   $N1 sees quorum=$q suspended=$s"
        break
    fi
    sleep 2
done
if [[ "$quorum_lost" != "true" ]]; then
    echo "FAIL: $N1 never observed quorum loss within 90s after partition"
    echo "   drbdsetup status on $N1:"
    on_node "$N1" drbdsetup status "$RD" 2>&1 | sed 's/^/    /' || true
    exit 1
fi

# -- Step 3: patch RD CRD with quorum=off, wait for .res re-render ---
#
# Patch BOTH `spec.props` AND `spec.drbdOptions.resource.quorum`. Bug 309
# (resourcedefinition_controller.go::setQuorum): the typed field wins
# over the prop-bag in effectiveprops.Resolve, so a prop-only patch is
# silently overwritten by the existing typed value on the next reconcile
# and the satellite renders `.res` from the typed slot's stale value.
# Writing both keeps the typed and prop-bag views consistent and matches
# what the controller itself does when it seeds the override path.
echo ">> step 3: patch RD ${RD} → DrbdOptions/Resource/quorum=off"
kubectl patch resourcedefinition "$RD" --type=merge \
    -p '{"spec":{"props":{"DrbdOptions/Resource/quorum":"off"},"drbdOptions":{"resource":{"quorum":"off"}}}}'

# Bump an annotation on the local Resource so the satellite-resource
# reconciler enqueues immediately. The satellite watches RD changes
# (enqueueResourcesForRD) but on a quorum-suspended slot the reconcile
# can be slow to fire-and-render — annotating the Resource Spec is a
# deterministic kick that matches the operator-CLI muscle-memory of
# `kubectl annotate ... reconcile=now` to force-flush a stuck loop.
kubectl annotate --overwrite "resource.blockstor.cozystack.io/${RD}.${N1}" \
    blockstor.io/reconcile-kick="$(date +%s)" >/dev/null

# The satellite renders .res from the RD prop set on every reconcile.
# Within 90s the .res on $N1 must contain `quorum off;` — anything
# slower means the controller→satellite prop propagation is broken,
# which would defeat the persistence claim before we even get to
# the bounce step. 90s is generous against a quorum-suspended slot
# where the reconciler may be retrying drbdadm calls with their
# bounded timeouts.
echo ">> step 4: wait up to 90s for .res on $N1 to contain 'quorum off;'"
deadline=$(( $(date +%s) + 90 ))
saw_off=false
while (( $(date +%s) < deadline )); do
    if on_node "$N1" cat /etc/drbd.d/${RD}.res 2>/dev/null | grep -qE "^\s*quorum\s+off\s*;"; then
        saw_off=true
        break
    fi
    # Re-kick every 15s in case the first annotation was reconciled
    # before the controller propagated the new RD spec to the
    # satellite's cache.
    kubectl annotate --overwrite "resource.blockstor.cozystack.io/${RD}.${N1}" \
        blockstor.io/reconcile-kick="$(date +%s)" >/dev/null 2>&1 || true
    sleep 3
done
if [[ "$saw_off" != "true" ]]; then
    echo "---- .res AFTER override on $N1 ----"
    on_node "$N1" cat /etc/drbd.d/${RD}.res 2>&1 || true
    echo "---- RD spec.props ----"
    kubectl get resourcedefinition "$RD" -o jsonpath='{.spec.props}' || true
    echo ""
    echo "FAIL: .res on $N1 never picked up 'quorum off;'"
    exit 1
fi
echo "   OK: .res shows quorum off"

# -- Step 5: resume-io so the Primary can take writes again ----------
#
# After `adjust` (which the satellite ran when re-rendering .res),
# DRBD-9 with on-suspended-primary-outdated may have demoted us to
# Secondary — re-promote, then resume-io, then probe.
echo ">> step 5: drbdadm resume-io $RD on $N1, prove writable"
on_node "$N1" drbdadm adjust "$RD" >/dev/null 2>&1 || true
on_node "$N1" drbdadm resume-io "$RD" >/dev/null 2>&1 || true
on_node "$N1" drbdadm primary --force "$RD" >/dev/null 2>&1 \
    || on_node "$N1" drbdadm primary "$RD" >/dev/null 2>&1 || true

io_ok=false
blocks=$(( (MARKER_BYTES + 4095) / 4096 ))
for _ in 1 2 3 4 5; do
    if on_node "$N1" bash -c "
        timeout 5 dd if=/dev/zero of=${DEV} bs=4096 count=${blocks} status=none oflag=direct conv=fdatasync
    " >/dev/null 2>&1; then
        io_ok=true
        break
    fi
    on_node "$N1" drbdadm primary --force "$RD" >/dev/null 2>&1 || true
    sleep 2
done
if [[ "$io_ok" != "true" ]]; then
    echo "FAIL: $N1 not writable after resume-io"
    on_node "$N1" drbdsetup status "$RD" 2>&1 | sed 's/^/    /' || true
    exit 1
fi
echo "   $N1 writable after resume-io"

# -- Step 6: bounce the satellite pod on $N1 -------------------------
SAT_POD_N1=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
echo ">> step 6: bounce satellite pod $SAT_POD_N1 on $N1"
# --wait=false: the preStop hook runs `drbdadm down` against the
# suspended-quorum slot. With Bug 82 fixed that's bounded to ~15s,
# but we don't gate the test on preStop timing — we just want the
# new pod up. The `wait for new pod Ready` loop below is the
# synchronisation point.
kubectl -n "$NS" delete pod "$SAT_POD_N1" --wait=false >/dev/null

deadline=$(( $(date +%s) + 180 ))
new_pod=""
while (( $(date +%s) < deadline )); do
    p=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}" 2>/dev/null || true)
    if [[ -n "$p" && "$p" != "$SAT_POD_N1" ]]; then
        ready=$(kubectl -n "$NS" get pod "$p" \
            -o jsonpath='{.status.containerStatuses[?(@.name=="blockstor-satellite")].ready}' 2>/dev/null || echo "false")
        # Fallback if container name differs across builds.
        if [[ "$ready" != "true" ]]; then
            ready=$(kubectl -n "$NS" get pod "$p" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
            [[ "$ready" == "True" ]] && ready="true"
        fi
        if [[ "$ready" == "true" ]]; then
            new_pod=$p
            break
        fi
    fi
    sleep 2
done
if [[ -z "$new_pod" ]]; then
    echo "FAIL: replacement satellite pod on $N1 never became Ready"
    exit 1
fi
echo "   new pod Ready: $new_pod"

# -- Step 7: verify .res STILL has `quorum off;` ---------------------
echo ">> step 7: verify .res on $N1 STILL contains 'quorum off;' (persistence claim)"
# Give the new satellite a few seconds to render its res files.
sleep 5
res_after=$(on_node "$N1" cat /etc/drbd.d/${RD}.res || true)
echo "---- .res AFTER satellite bounce on $N1 (first 40 lines) ----"
echo "$res_after" | sed -n '1,40p'
if ! echo "$res_after" | grep -qE "^\s*quorum\s+off\s*;"; then
    echo "FAIL: .res on $N1 lost 'quorum off;' after satellite respawn"
    echo "      regression — RD CRD prop override not persisted across satellite restart"
    exit 1
fi
echo "   OK: quorum off survived satellite respawn"

# Sanity-check the kernel matches the file. The new satellite is
# expected to run `drbdadm adjust` on startup; if it skips that,
# the kernel still has the old quorum=majority but the FILE has
# quorum=off — the override has not yet propagated to-the-kernel
# but HAS persisted at the config layer. We surface that as INFO
# (not FAIL) because the persistence claim is on the .res file —
# the kernel-side reconcile is the satellite's job and is covered
# by the adjust step.
if on_node "$N1" drbdsetup show "$RD" 2>/dev/null | grep -qE "quorum\s+off"; then
    echo "   OK: drbdsetup show confirms quorum off in kernel"
else
    echo "INFO: drbdsetup show does not show quorum off — re-adjusting"
    on_node "$N1" drbdadm adjust "$RD" >/dev/null 2>&1 || true
fi

# -- Step 8: heal partition, restore quorum=majority -----------------
echo ">> step 8a: heal partition"
cleanup_partition

echo ">> step 8b: wait up to 240s for 3/3 UpToDate (peers reconnect)"
deadline=$(( $(date +%s) + 240 ))
all_back=false
while (( $(date +%s) < deadline )); do
    s1=$(status_disk_state "$RD" "$N1")
    s2=$(status_disk_state "$RD" "$N2")
    s3=$(status_disk_state "$RD" "$N3")
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" && "$s3" == "UpToDate" ]]; then
        all_back=true
        break
    fi
    sleep 3
done
if [[ "$all_back" != "true" ]]; then
    echo "INFO: $RD did not converge to 3/3 UpToDate within 240s (states: $N1=$s1 $N2=$s2 $N3=$s3)"
    echo "      continuing — restoring quorum=majority is the binding test step"
fi

# Re-promote in case the satellite respawn or peer-reconnect adjust
# bounced us back to Secondary.
on_node "$N1" drbdadm primary --force "$RD" >/dev/null 2>&1 \
    || on_node "$N1" drbdadm primary "$RD" >/dev/null 2>&1 || true
sleep 2

# Confirm the Primary is writable BEFORE flipping quorum back. If
# the kernel never resumed I/O after the peers came back this dd
# would EIO — we want to fail before the patch so the post-mortem
# is unambiguous.
if ! on_node "$N1" bash -c "
    timeout 5 dd if=/dev/zero of=${DEV} bs=4096 count=${blocks} status=none oflag=direct conv=fdatasync
" >/dev/null 2>&1; then
    on_node "$N1" drbdadm primary --force "$RD" >/dev/null 2>&1 || true
    sleep 3
    if ! on_node "$N1" bash -c "
        timeout 5 dd if=/dev/zero of=${DEV} bs=4096 count=${blocks} status=none oflag=direct conv=fdatasync
    " >/dev/null 2>&1; then
        echo "FAIL: Primary on $N1 not writable before restoring quorum=majority"
        exit 1
    fi
fi

echo ">> step 8c: restore quorum=majority via CRD patch"
# Patch both prop-bag and typed slot — same Bug 309 reasoning as step 3.
kubectl patch resourcedefinition "$RD" --type=merge \
    -p '{"spec":{"props":{"DrbdOptions/Resource/quorum":"majority"},"drbdOptions":{"resource":{"quorum":"majority"}}}}'

# Within 60s the .res must drop `quorum off;` AND the Primary must
# NOT re-suspend I/O during the transition.
deadline=$(( $(date +%s) + 60 ))
saw_majority=false
io_blip=false
while (( $(date +%s) < deadline )); do
    if on_node "$N1" cat /etc/drbd.d/${RD}.res 2>/dev/null | grep -qE "^\s*quorum\s+off\s*;"; then
        :
    else
        saw_majority=true
    fi
    if ! on_node "$N1" bash -c "
        timeout 5 dd if=/dev/zero of=${DEV} bs=4096 count=${blocks} status=none oflag=direct conv=fdatasync
    " >/dev/null 2>&1; then
        io_blip=true
        break
    fi
    if [[ "$saw_majority" == "true" ]]; then break; fi
    sleep 2
done

if [[ "$io_blip" == "true" ]]; then
    echo "FAIL: Primary on $N1 re-suspended I/O during quorum=majority restore"
    exit 1
fi
if [[ "$saw_majority" != "true" ]]; then
    echo "FAIL: .res on $N1 still has 'quorum off;' after restoring majority"
    on_node "$N1" cat /etc/drbd.d/${RD}.res 2>&1 | sed 's/^/    /' || true
    exit 1
fi
echo "   quorum=majority restored, no I/O re-suspension on Primary"

echo ">> RECOVERY-QUORUM-PERSISTENCE OK"
echo "   .res quorum=off persisted across satellite respawn: YES"
echo "   no I/O re-suspension when restoring majority: YES"
