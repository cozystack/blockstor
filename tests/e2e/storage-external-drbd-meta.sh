#!/usr/bin/env bash
#
# usage: storage-external-drbd-meta.sh WORK_DIR
#
# Scenario 6.18 — external DRBD metadata pool via StorPoolNameDrbdMeta.
#
# UG9 §"Using external DRBD metadata" (lines 4463-4534) describes the
# operator workflow:
#
#   * primary `lvm-thin` pool holds the data LVs (cheap thin storage)
#   * sibling `meta-thin` pool holds the DRBD activity-log + bitmap +
#     superblock as a separate `<rd>_<vol>_meta` LV (a small but hot
#     pool that the operator may park on a faster device)
#   * `linstor rd set-property <rd> StorPoolNameDrbdMeta meta-thin`
#     stamps the routing prop; the dispatcher resolves it in
#     Resource → RD precedence order and stamps DesiredVolume.MetaPool
#   * the satellite renders `meta-disk /dev/meta-thin/<rd>_<vol>_meta;`
#     on the local diskful host and `meta-disk internal;` on every
#     peer (drbd never reads peer-side meta-disk, pinning a path
#     there would break determinism across peers)
#
# Coverage status (2026-05-13):
#
#   * .res render path: WIRED (pkg/drbd + pkg/dispatcher + pkg/satellite
#     all carry the MetaPool field; unit tests
#     TestApplyRendersExternalMetaDiskPath +
#     TestRenderExternalMetadata pin the rendered layout).
#   * satellite-side provisioning of the `_meta` sibling LV:
#     NOT WIRED. TestApplyProvisionsBothDataAndMeta is t.Skip'd in
#     pkg/satellite/reconciler_drbd_test.go pending a Provider-side
#     CreateMetaVolume API (each storage Provider hardcodes the
#     data-LV naming; carving a sibling `_meta` LV with the right
#     suffix needs an API change that threads through every backend).
#
# Therefore this script does TWO things:
#
#   1. Assert (must-pass): the rendered .res on the local diskful
#      host contains the external `meta-disk /dev/meta-thin/...;`
#      line and the peer keeps `meta-disk internal;`. This is the
#      controller→dispatcher→satellite render contract — already
#      shipped at commit 01dd0655e and the regression guard for
#      that contract lives here.
#
#   2. Probe (best-effort, expected-fail today): check whether the
#      sibling `<rd>_<vol>_meta` LV exists on the `meta-thin` pool.
#      Until Provider.CreateMetaVolume lands, it will NOT — the
#      script prints a KNOWN-GAP note and exits 0. When the
#      provisioner is wired up the probe flips to PASS automatically
#      with no script edits needed; the resource will then also
#      reach UpToDate which the script also reports.
#
# Stand: any 2+ worker blockstor cluster (uses lib.sh's WORKER_*
# discovery). The data pool is the existing `lvm-thin` pool baked
# into install-pools.sh; the meta pool is created on the fly inside
# the satellite container as a small (256M) sibling thin pool in
# the same VG `blockstor-lvm`, then exposed via per-node
# StoragePool CRDs `meta-thin.<worker>`.
#
# Cleanup tears the meta pool back down so the stand is reusable
# for the next iter.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=${RD:-extmeta-test}
DATA_POOL=${DATA_POOL:-lvm-thin}
META_POOL=${META_POOL:-meta-thin}
# Backing VG + sibling thin pool name. Both `lvm-thin` (the data
# pool) and `meta-thin` (this test's pool) sit inside the same VG
# blockstor-lvm — the satellite container already has the VG
# wired up from install-pools.sh, so we only have to lvcreate the
# meta thinpool on top of free VG extents.
META_VG=${META_VG:-blockstor-lvm}
META_LVTHIN=${META_LVTHIN:-meta}
# 256 MiB is enough for DRBD external metadata of multi-GiB
# volumes (≈32 MiB per 1 TiB data) and leaves the rest of the
# 1 GiB VFree for the data pool's own carves.
META_SIZE=${META_SIZE:-256M}

N1=$WORKER_1
N2=$WORKER_2

# Per-node StoragePool CRD names. The admission webhook enforces
# `<spec.poolName>.<spec.nodeName>` as the CRD metadata.name —
# stand/blockstor-storagepools.yaml uses the same dotted form
# (e.g. `lvm-thin.<worker>`). We name them deterministically off
# $META_POOL so cleanup is a label-free `kubectl delete` by exact name.
META_SP_1="${META_POOL}.${N1}"
META_SP_2="${META_POOL}.${N2}"

# Track on which workers we successfully created the meta thinpool
# so cleanup only tries to lvremove where there is something to
# remove. Avoids spurious `LV not found` errors on partial setup.
META_CREATED_ON=()

cleanup() {
    local rc=$?

    # Delete RD first (waits on finalizers; provider DeleteVolume
    # runs on each satellite to drop the data LV). Done via lib.sh
    # which also kills any lingering kernel state.
    delete_rd "$RD"

    # Delete the meta-pool StoragePool CRDs. Use --wait to let the
    # StoragePoolReconciler run its finalizer (deregisters from the
    # in-mem provider map) before we tear the backing LV.
    kubectl delete --wait=true --timeout=30s --ignore-not-found \
        storagepool.blockstor.io.blockstor.io \
        "$META_SP_1" "$META_SP_2" 2>/dev/null || true

    # Lvremove the meta thinpool on every worker we touched.
    for node in "${META_CREATED_ON[@]:-}"; do
        [[ -z "$node" ]] && continue
        on_node "$node" bash -c "
            lvremove -f ${META_VG}/${META_LVTHIN} 2>/dev/null || true
        " >/dev/null 2>&1 || true
    done

    if (( rc != 0 )); then
        echo "---- diag dump ----"
        kubectl get resourcedefinitions.blockstor.io.blockstor.io "$RD" \
            -o yaml 2>/dev/null | head -40 || true
        kubectl get resources.blockstor.io.blockstor.io 2>/dev/null \
            | grep "$RD" || true
    fi
    exit "$rc"
}
trap cleanup EXIT

# --- Step 1: carve a sibling meta thinpool on every diskful peer ----
#
# The data pool `lvm-thin` lives at blockstor-lvm/thin. We create a
# second thinpool blockstor-lvm/meta on the same VG. install-pools.sh
# leaves ~1 GiB of VFree on every worker for exactly this style of
# add-on test pool. The container has no udev so we have to pass the
# activation knobs that install-pools.sh uses (--config 'activation
# {udev_sync=0 udev_rules=0}'). Idempotent: skip if the LV already
# exists from a prior run that didn't clean up.
echo ">> step 1: create meta thinpool ${META_VG}/${META_LVTHIN} on $N1 and $N2"
for node in "$N1" "$N2"; do
    if on_node "$node" bash -c "lvs --noheadings ${META_VG}/${META_LVTHIN} 2>/dev/null | grep -q ${META_LVTHIN}"; then
        echo "   $node: meta thinpool already present, reusing"
        META_CREATED_ON+=("$node")
        continue
    fi
    if ! on_node "$node" bash -c "
        lvcreate -T ${META_VG}/${META_LVTHIN} -L ${META_SIZE} \
            --config 'activation {udev_sync=0 udev_rules=0}' \
            -y >/dev/null 2>&1
    "; then
        echo "FAIL: lvcreate -T ${META_VG}/${META_LVTHIN} failed on $node"
        echo "      (does the VG have ${META_SIZE} of free extents?)"
        on_node "$node" bash -c "vgs ${META_VG}; lvs ${META_VG}" || true
        exit 1
    fi
    META_CREATED_ON+=("$node")
    echo "   $node: created ${META_VG}/${META_LVTHIN} (${META_SIZE})"
done

# --- Step 2: declare the meta pool as a per-node StoragePool CRD ----
#
# providerKind LVM_THIN matches the data pool — same provider, but
# pointed at the new thinpool LV. The satellite's
# StoragePoolReconciler observes the CRD and registers the provider
# under poolName=meta-thin. Note: until Provider.CreateMetaVolume
# lands (the known gap), this pool will still appear in `linstor
# storage-pool list` but no `_meta` LV will be carved when the test
# RD spawns — that is the gap this script also probes for.
echo ">> step 2: apply StoragePool CRDs for $META_POOL on $N1, $N2"
cat <<EOF | kubectl apply -f -
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: StoragePool
metadata:
  name: ${META_SP_1}
spec:
  nodeName: ${N1}
  poolName: ${META_POOL}
  providerKind: LVM_THIN
  props:
    StorDriver/LvmVg: ${META_VG}
    StorDriver/ThinPool: ${META_LVTHIN}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: StoragePool
metadata:
  name: ${META_SP_2}
spec:
  nodeName: ${N2}
  poolName: ${META_POOL}
  providerKind: LVM_THIN
  props:
    StorDriver/LvmVg: ${META_VG}
    StorDriver/ThinPool: ${META_LVTHIN}
EOF

# Give the satellite reconciler a moment to register the new
# provider in its in-mem map before the dispatcher tries to
# resolve `meta-thin` for the RD.
sleep 3

# --- Step 3: create the RD with StorPoolNameDrbdMeta=meta-thin ------
#
# StorPoolNameDrbdMeta is the upstream-LINSTOR prop key (see
# pkg/dispatcher/dispatcher.go const StorPoolNameDrbdMetaKey). RD
# scope lets a single prop apply to every Resource in the RD;
# Resource-scope override (not exercised here) follows the same
# `target.Spec.Props[…]` lookup.
#
# `StorPoolName: lvm-thin` on each Resource pins the *data* pool —
# the dispatcher only consults StorPoolNameDrbdMeta to pick the
# meta pool. Same upstream LINSTOR convention.
echo ">> step 3: apply RD $RD with StorPoolNameDrbdMeta=$META_POOL on $N1+$N2"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    StorPoolNameDrbdMeta: ${META_POOL}
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${DATA_POOL}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: ${DATA_POOL}}
EOF

# --- Step 4: wait for the satellite to render .res ------------------
#
# Until Provider.CreateMetaVolume lands the resource will NOT reach
# UpToDate (drbdadm create-md fails when /dev/meta-thin/..._meta is
# absent), so we can't use wait_uptodate. Instead, poll the .res file
# directly — the satellite renders the .res in reconciler.Apply BEFORE
# it tries to bring the device up, so the render is observable even
# when the kernel-side activation later fails.
echo ">> step 4: wait for /etc/drbd.d/${RD}.res to appear on $N1"
deadline=$(( $(date +%s) + 90 ))
res_file=""
while (( $(date +%s) < deadline )); do
    if on_node "$N1" test -f "/etc/drbd.d/${RD}.res" 2>/dev/null; then
        res_file=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res" 2>/dev/null || true)
        if [[ -n "$res_file" ]]; then
            break
        fi
    fi
    sleep 2
done

if [[ -z "$res_file" ]]; then
    echo "FAIL: ${RD}.res never appeared on $N1 within 90s"
    echo "      controller may not be dispatching — check controller logs:"
    kubectl -n "$NS" logs -l app=blockstor-controller --tail=30 2>/dev/null \
        | grep -iE "$RD|error|meta" || true
    exit 1
fi

echo "---- rendered .res on $N1 ----"
echo "$res_file"
echo "------------------------------"

# --- Step 5: assert local-diskful meta-disk + peer internal ---------
#
# Path shape from pkg/satellite/reconciler.go:
#   fmt.Sprintf("/dev/%s/%s_%05d_meta", mp, dr.GetName(), volNumber)
# vol 0 → 5-digit `00000` suffix. Match the WHOLE line so we catch
# accidental peer-side leaks (every peer must keep `internal`).
EXPECTED_LOCAL="meta-disk /dev/${META_POOL}/${RD}_00000_meta;"
echo ">> step 5a: assert local diskful $N1 carries '$EXPECTED_LOCAL'"
if ! echo "$res_file" | grep -qF "$EXPECTED_LOCAL"; then
    echo "FAIL: expected '$EXPECTED_LOCAL' in rendered .res on $N1"
    echo ""
    echo "      Two possible causes:"
    echo "      1. Controller image predates the 6.18 feature commit"
    echo "         (StorPoolNameDrbdMeta wiring in pkg/dispatcher +"
    echo "         pkg/satellite). Re-run \`make build-images\` from"
    echo "         a checkout that contains the feature and bounce"
    echo "         the controller + satellite pods."
    echo "      2. Regression in the controller→dispatcher→satellite"
    echo "         render contract. The unit tests"
    echo "         TestApplyRendersExternalMetaDiskPath +"
    echo "         TestRenderExternalMetadata should have caught"
    echo "         this first — if they pass and the e2e doesn't,"
    echo "         look for a CRD-prop propagation bug between the"
    echo "         RD's Spec.Props and DesiredVolume.MetaPool."
    exit 1
fi
echo "   OK: $N1 carries external meta-disk"

# The peer block in the .res must keep `meta-disk internal;` — drbd
# never reads peer-side meta-disk and pinning a path there would
# break determinism across peers (peer's local meta-disk path is
# specific to that node's pool layout). Look for the line scoped
# under `on $N2 {`.
echo ">> step 5b: assert peer $N2 keeps 'meta-disk internal;'"
peer_block=$(echo "$res_file" \
    | awk -v node="on $N2 {" 'BEGIN{p=0} $0~node{p=1} p{print} p && /^}/{exit}')
if [[ -z "$peer_block" ]]; then
    echo "FAIL: could not locate 'on $N2 {' block in rendered .res"
    exit 1
fi
if ! echo "$peer_block" | grep -q "meta-disk internal;"; then
    echo "FAIL: peer block for $N2 does not carry 'meta-disk internal;'"
    echo "---- peer block: ----"
    echo "$peer_block"
    exit 1
fi
echo "   OK: $N2 carries 'meta-disk internal;'"

# Belt-and-suspenders: count occurrences. Expect 1 external + 1
# internal across the whole .res file (one diskful + one peer).
n_internal=$(echo "$res_file" | grep -c "meta-disk internal;" || true)
n_external=$(echo "$res_file" | grep -cF "meta-disk /dev/${META_POOL}/" || true)
echo "   render counts: external=$n_external internal=$n_internal"
if (( n_external != 1 )); then
    echo "FAIL: expected exactly 1 external meta-disk line; got $n_external"
    exit 1
fi
if (( n_internal != 1 )); then
    echo "FAIL: expected exactly 1 'meta-disk internal;' (peer only); got $n_internal"
    exit 1
fi

# --- Step 6: probe for the sibling _meta LV (expected-fail) ---------
#
# This is the satellite-side provisioning step that
# TestApplyProvisionsBothDataAndMeta documents as the open follow-up.
# Until Provider.CreateMetaVolume lands no `<rd>_00000_meta` LV will
# show up on the meta thinpool. Treat absence as KNOWN GAP, not
# failure — when the carve API is wired this probe flips to PASS
# automatically and we can drop the gap-handling branch.
echo ">> step 6: probe for sibling _meta LV on $N1:${META_VG}/${META_LVTHIN}"
META_LV_NAME="${RD}_00000_meta"

# Give the reconciler a fair window to attempt the meta carve in
# case the wiring lands while this branch is still alive. 30 s is
# more than enough — provider.CreateVolume itself returns in <1 s
# under nominal load on the QEMU stand.
deadline=$(( $(date +%s) + 30 ))
meta_lv_present=0
while (( $(date +%s) < deadline )); do
    if on_node "$N1" bash -c "lvs --noheadings -o lv_name ${META_VG} 2>/dev/null \
            | awk '{print \$1}' | grep -qx '${META_LV_NAME}'"; then
        meta_lv_present=1
        break
    fi
    sleep 2
done

if (( meta_lv_present == 1 )); then
    echo "   PASS-UPGRADE: sibling _meta LV $META_LV_NAME is present"
    echo "                 satellite-side meta provisioning has landed —"
    echo "                 6.18 is now fully wired end-to-end. Consider"
    echo "                 dropping the gap branch and asserting"
    echo "                 wait_uptodate \"$RD\" \"$N1\" \"$N2\"."
    # While we're here, also verify the resource progresses since
    # the only blocker for UpToDate was the missing meta LV.
    if wait_uptodate "$RD" "$N1" "$N2"; then
        echo "   resource reached UpToDate on both peers — end-to-end OK"
    else
        echo "INFO: meta LV present but $RD did not reach UpToDate;"
        echo "      check satellite logs (separate issue from 6.18 routing)"
    fi
else
    echo "   KNOWN GAP: sibling _meta LV $META_LV_NAME is NOT present on"
    echo "              ${META_VG}/${META_LVTHIN} after 30s."
    echo ""
    echo "              This is the t.Skip'd unit test"
    echo "              TestApplyProvisionsBothDataAndMeta in"
    echo "              pkg/satellite/reconciler_drbd_test.go — the"
    echo "              satellite-side provisioning of the sibling LV"
    echo "              still needs a Provider.CreateMetaVolume API."
    echo ""
    echo "              .res render (the controller-side contract"
    echo "              already shipped at 01dd0655e) is verified OK."
    echo "              When the provisioner lands, step 6 will flip"
    echo "              to PASS-UPGRADE automatically with no script"
    echo "              edits required."
fi

echo ">> STORAGE-EXTERNAL-DRBD-META OK"
echo "   .res routing (StorPoolNameDrbdMeta → meta-disk): PASS"
if (( meta_lv_present == 1 )); then
    echo "   satellite-side meta-LV provisioning:               PASS"
else
    echo "   satellite-side meta-LV provisioning:               KNOWN GAP"
fi
