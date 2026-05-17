#!/usr/bin/env bash
#
# usage: disk-replace-internal-metadata.sh WORK_DIR
#
# Scenario 5.W09 — Replace failed disk (internal metadata).
#
# Cross-listed with:
#   - tests/scenarios/06-storage-backends.md §6.18 (StorPoolNameDrbdMeta
#     routes metadata to a separate pool — the EXTERNAL-metadata sibling
#     of this scenario; 5.W10 covers the external-metadata recipe).
#   - tests/scenarios/06-storage-backends.md §6.19 (Failed disk
#     replacement recovers via satellite reconnect — the StoragePool-
#     level recovery, not metadata-level).
#   - tests/drbd-troubleshooting-scenarios.md §7 (`r d` + `rd ap` —
#     the linstor-CLI-shaped variant of the same recovery).
#
# Source: drbd-troubleshooting §"Replacing a failed disk when using
# internal metadata" (lines 82-106).
#
# Background:
#   With INTERNAL metadata, the DRBD generation-id / activity-log /
#   dirty-bitmap live in the last few MiB of the backing block device.
#   When the disk fails physically (re-allocated sector, dropped cable,
#   smartctl-doomed pool), DRBD detaches automatically (on-io-error=detach)
#   and the replica goes Diskless. Operator-side recovery has two
#   shapes, both documented in upstream drbd-troubleshooting:
#
#     A) blockstor-managed (preferred):
#          # delete the bad replica via CRD — finalizers + reconciler
#          # teardown free the port + carry the .res file with them
#          kubectl delete resource <rd>.<bad-node>
#          # re-apply with the same node — autoplace or explicit
#          # Resource recreates LV + .md-created marker + drbdadm
#          # create-md + adjust → fresh metadata block, resync from
#          # surviving peer
#
#     B) raw drbdadm (operator-shell):
#          drbdadm detach --force <rd>        # kernel-level disk drop
#          # swap underlying LV/zvol/file out of band
#          drbdadm create-md --max-peers=N <rd>
#          drbdadm attach <rd>
#          # DRBD bitmap-resyncs from peer-disk:UpToDate side
#
#   Recipe B is what the upstream doc walks operators through verbatim.
#   The CRITICAL property under test for blockstor: a satellite that
#   sees recipe B happen on its shell MUST NOT overwrite the .res file
#   mid-recipe (rendering would race the operator's `drbdadm attach`
#   and roll back the just-installed kernel state to whatever the
#   controller thinks the desired state is). The reconciler picks up
#   the new metadata via its existing `HasMD` adopt-on-existing path
#   (pkg/satellite/reconciler.go:1008-1019) — no .res re-render needed
#   because the controller-side desired state didn't change.
#
# Methodology:
#   1. Bring up a 2-replica RD on workers 1+2 (LVM-THIN backing — the
#      cozystack stand default; internal metadata works on any block
#      device but LVM gives us the cleanest "swap the LV" simulation).
#   2. Promote $N1, write a 64 KiB urandom marker, capture md5. This
#      gives us a data-shape assertion at the end of the recipe.
#   3. Capture the sha256 of $N1's .res file before the recipe (the
#      "reconciler must not overwrite" invariant pins on this hash).
#   4. Simulate disk failure on $N1 via `drbdadm detach --force` —
#      moves the local disk:Diskless without provoking a kernel-level
#      teardown of the resource (drbdadm down would also work but is
#      a stricter assertion shape than the upstream recipe needs).
#   5. Apply recipe B on $N1:
#        drbdmeta --force <rd>/0 v09 <backing-dev> internal create-md <peers>
#        drbdadm attach <rd>
#      We deliberately use `drbdmeta create-md` directly (not `drbdadm
#      create-md`) so the test matches the upstream-doc command shape
#      and exercises the "operator bypassed blockstor entirely" path,
#      which is the strictest reconciler-survival case.
#   6. Within $RECOVERY_WINDOW (10 s) $N1 must walk Inconsistent →
#      SyncTarget → UpToDate via the bitmap diff against $N2.
#   7. Post-recovery assertions:
#        a. $N1 disk:UpToDate, $N2 disk:UpToDate
#        b. md5 readback from $N1 matches the original marker
#        c. .res sha256 unchanged across the recipe window — the
#           reconciler did not race the operator and re-render the
#           file (the regression this scenario pins)
#        d. $N1 keeps its Primary-ship if it was Primary; Secondary
#           otherwise. The recipe touches local disk state only and
#           must NOT flip role.
#
# Regression guards:
#   - $N2 must stay UpToDate throughout — the recipe targets $N1's
#     local disk only; if $N2's disk transitions during the window,
#     the test setup is interacting with something other than the
#     intended metadata-replacement code path.
#   - The satellite log on $N1 must not contain `create-md` or
#     `drbdadm adjust` for this RD during the recovery window. If it
#     does, the reconciler is fighting the operator's recovery —
#     either by re-rendering .res (would trip `firstActivation=false`
#     skip but the post-condition wouldn't change) or by re-running
#     create-md (would wipe the freshly-stamped metadata block and
#     orphan $N1 from the cluster).
#
# CI vs. operator-runbook duality:
#   This script doubles as an executable runbook for operators who
#   need to walk through the upstream recipe on a stand. The recipe
#   commands are issued verbatim against the satellite pod's shell,
#   so the test record IS the runbook. Failure modes (port re-use,
#   metadata-zone reuse, .res re-render) all surface as test failures
#   with the exact command-line that misbehaved.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=disk-replace-internal-meta
N1=$WORKER_1
N2=$WORKER_2
SIZE_KIB=65536
SIZE_BYTES=$((64 * 1024))   # marker payload — bigger than 4 KiB align
                            # unit, small enough to resync in well
                            # under 10 s on the QEMU stand
RECOVERY_WINDOW=${RECOVERY_WINDOW:-15}

trap 'delete_rd "$RD"' EXIT

# ---------- step 1: 2-replica RD on N1 + N2 (no tiebreaker) ----------
#
# Disable the auto-tiebreaker to keep the topology to exactly two
# diskful peers — a TIE_BREAKER witness on a third node would muddy
# the post-recipe peer-state assertions (`$N2 must stay UpToDate
# throughout` would have to grow into a 3-way invariant).
echo ">> apply 2-replica RD on $N1 + $N2 (no tiebreaker)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
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
echo "   /dev/drbd? on $N1 = $DEV"

# ---------- step 2: write marker payload from N1 ----------
echo ">> primary on $N1, write ${SIZE_BYTES}-byte urandom marker"
md5_marker=$(write_random "$N1" "$DEV" "$SIZE_BYTES")
echo "   marker md5 = $md5_marker"

# Confirm $N1 is Primary before the disk-replace provocation — if
# write_random somehow left it Secondary we'd misdiagnose a later
# role flip as a recipe regression.
n1_role_before=$(on_node "$N1" drbdsetup status "$RD" | grep "role:" | head -1)
echo "   $N1 role pre-recipe: $n1_role_before"
if [[ "$n1_role_before" != *"role:Primary"* ]]; then
    echo "FAIL: $N1 is not Primary before the disk-replace provocation"
    exit 1
fi

# ---------- step 3: capture .res sha256 + satellite pod for log scan ----------
res_sha_before=$(on_node "$N1" sha256sum "/etc/drbd.d/${RD}.res" | awk '{print $1}')
echo "   .res sha256 pre-recipe = $res_sha_before"

# Cache the $N1 satellite pod name so the log-tail at the end is
# stable against a churning DaemonSet (same trick as recovery-
# discard-my-data.sh §5.14).
SAT_POD_N1=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
echo "   $N1 satellite pod: $SAT_POD_N1"

# Discover N1's backing block device — what `drbdmeta create-md`
# needs as its `<device>` argument. Parse the `disk` line out of the
# rendered .res file (the only canonical place the satellite stamps
# the backing path).
BACK_DEV=$(on_node "$N1" bash -c "
    awk '/disk[[:space:]]+\/dev/ { print \$2; exit }' /etc/drbd.d/${RD}.res
" | tr -d ';')
echo "   $N1 backing device: $BACK_DEV"
if [[ -z "$BACK_DEV" ]]; then
    echo "FAIL: could not resolve $N1's backing device from .res"
    exit 1
fi

# ---------- step 4: simulate disk failure on N1 (detach → Diskless) ----------
#
# `drbdadm detach --force` drops the kernel's binding to the lower
# disk without taking the resource down. The replica goes Diskless,
# DRBD keeps Primary-ship (still UpToDate from $N2's perspective),
# I/O fails over the wire. This is the steady-state shape the
# upstream recipe expects when an operator arrives at a node that
# already detached itself via `on-io-error=detach`.
echo ">> simulate disk failure on $N1: drbdadm detach --force"
on_node "$N1" drbdadm detach --force "$RD"

# Wait briefly for the kernel to settle on disk:Diskless.
deadline=$(( $(date +%s) + 5 ))
n1_disk=""
while (( $(date +%s) < deadline )); do
    n1_disk=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null \
        | grep "disk:" | head -1 || true)
    if [[ "$n1_disk" == *"disk:Diskless"* ]]; then
        break
    fi
    sleep 1
done
echo "   $N1 post-detach disk-state: $n1_disk"
if [[ "$n1_disk" != *"disk:Diskless"* ]]; then
    echo "FAIL: $N1 did not transition to Diskless after detach (got: $n1_disk)"
    exit 1
fi

# Sanity guard: $N2 must remain UpToDate throughout. If the detach
# somehow flapped $N2 we'd misdiagnose post-recipe failures.
n2_disk=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$n2_disk" != *"disk:UpToDate"* ]]; then
    echo "FAIL: $N2 unexpected disk-state after $N1 detach: $n2_disk"
    exit 1
fi

# Mark the start of the recovery window so the log scan at the end
# is bounded to the recipe-execution interval.
window_start=$(date +%s)

# ---------- step 5: recipe — drbdmeta create-md + drbdadm attach ----------
#
# `drbdmeta --force <res>/<vol> v09 <dev> internal create-md <peers>`
# is the upstream-doc verbatim command. We use it directly (not via
# `drbdadm create-md`, which wraps the same call but pulls peer count
# from .res) so the test exercises the strictest "operator bypassed
# blockstor entirely" path. The `<peers>` arg is the max-peer count
# DRBD-9 needs for activity-log sizing; we use the same default the
# satellite uses (drbd.MaxPeers - 1 = 31).
#
# After create-md re-stamps the metadata block, `drbdadm attach`
# binds the kernel to the lower disk again. DRBD reads the freshly-
# created metadata, sees an empty bitmap + zeroed GI, hands the peer
# control of the resync direction → SyncTarget from $N2.
# --max-peers must match what the satellite originally stamped via
# `drbdadm create-md --max-peers=<N>` (pkg/drbd/drbdadm.go pins it
# to drbd.MaxPeers-1 = 15). The previous value 31 inflated the
# activity-log + bitmap metadata zone past what the kernel had
# already learned the device's data capacity to be, and `drbdadm
# attach` then failed `Low.dev. smaller than requested DRBD-dev.
# size. Current (diskless) capacity 130936, cannot attach smaller
# (130872) disk` — the operator-recipe doc value (31) is upstream
# DRBD-9's max but doesn't match blockstor's deployment shape.
echo ">> recipe: drbdmeta create-md + drbdadm attach on $N1"
on_node "$N1" bash -c "
    drbdmeta --force 0 v09 ${BACK_DEV} internal create-md 15
    drbdadm attach ${RD}
"

# ---------- step 6: wait for N1 → UpToDate via bitmap resync ----------
echo ">> wait up to ${RECOVERY_WINDOW}s for $N1 → UpToDate"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
n1_uptodate=false

while (( $(date +%s) < deadline )); do
    state=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null \
        | grep "disk:" | head -1 || true)
    if [[ "$state" == *"disk:UpToDate"* ]]; then
        n1_uptodate=true
        break
    fi
    sleep 1
done

if [[ "$n1_uptodate" != "true" ]]; then
    echo "FAIL: $N1 did not reach UpToDate within ${RECOVERY_WINDOW}s"
    on_node "$N1" drbdsetup status "$RD" || true
    exit 1
fi

window_end=$(date +%s)
echo "   $N1 reached UpToDate in $((window_end - window_start))s"

# ---------- step 7: assertions ----------

# (a) Both replicas UpToDate.
n2_disk=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$n2_disk" != *"disk:UpToDate"* ]]; then
    echo "FAIL: $N2 not UpToDate post-recovery: $n2_disk"
    exit 1
fi

# (b) marker md5 round-trip from N1 (which just resynced from N2).
md5_after=$(read_md5 "$N1" "$DEV" "$SIZE_BYTES")
echo "   marker md5 post-recovery on $N1 = $md5_after"
if [[ "$md5_after" != "$md5_marker" ]]; then
    echo "FAIL: marker md5 mismatch post-recovery (want=$md5_marker got=$md5_after)"
    exit 1
fi

# (c) .res sha256 unchanged — reconciler did NOT race the operator
# and re-render the file. This is the regression the scenario pins.
res_sha_after=$(on_node "$N1" sha256sum "/etc/drbd.d/${RD}.res" | awk '{print $1}')
echo "   .res sha256 post-recipe = $res_sha_after"
if [[ "$res_sha_after" != "$res_sha_before" ]]; then
    echo "FAIL: .res was re-rendered mid-recipe (sha changed: $res_sha_before → $res_sha_after)"
    exit 1
fi

# (d) N1 still Primary — recipe is local-disk-only, must not flip role.
n1_role_after=$(on_node "$N1" drbdsetup status "$RD" | grep "role:" | head -1)
echo "   $N1 role post-recipe: $n1_role_after"
if [[ "$n1_role_after" != *"role:Primary"* ]]; then
    echo "FAIL: $N1 lost Primary-ship across the disk-replace recipe"
    exit 1
fi

# (e) satellite log scan — the reconciler must NOT have re-run
# `drbdadm create-md` or `drbdadm adjust` for this RD during the
# recovery window. Either would mean the controller-side reconciler
# clobbered the operator's recipe.
window_since="${window_start}"
sat_log=$(kubectl -n "$NS" logs --since-time="@${window_since}" "$SAT_POD_N1" 2>/dev/null || true)
if echo "$sat_log" | grep -qE "drbdadm create-md.*${RD}\b"; then
    echo "FAIL: reconciler re-ran create-md during recovery window — would wipe operator-stamped metadata"
    echo "$sat_log" | grep -E "create-md.*${RD}" | head -5
    exit 1
fi
if echo "$sat_log" | grep -qE "drbdadm adjust ${RD}\b"; then
    echo "FAIL: reconciler ran adjust during recovery window — would race operator's attach"
    echo "$sat_log" | grep -E "adjust ${RD}" | head -5
    exit 1
fi

echo ">> DISK-REPLACE INTERNAL METADATA OK ($N1 recovered via raw drbdmeta + attach; .res unchanged; reconciler stayed quiet)"
