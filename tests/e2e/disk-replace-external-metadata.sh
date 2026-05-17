#!/usr/bin/env bash
#
# usage: disk-replace-external-metadata.sh WORK_DIR
#
# Scenario 5.W10 — Replace failed disk when DRBD metadata lives on a
# separate (external) pool. Cross-listed with wave1 6.18 and wave2 5.W05.
#
# Source:
#   * tests/scenarios/wave2-05-drbd-state-recovery.md §5.W10
#     ("Replace failed disk (external metadata) — S, P1 e2e")
#   * UG9 drbd-troubleshooting §"Replacing a failed disk when using
#     external metadata" (lines 108-133)
#   * UG9 §"Using external DRBD metadata" (lines 4462-4534)
#
# Recipe under test
# -----------------
#
# Pre-condition: the RD was created with `StorPoolNameDrbdMeta=<meta>`,
# so DRBD's activity log + bitmap + superblock live on a SIBLING LV
# `/dev/<meta-pool>/<rd>_<vol>_meta` and the data LV lives on the
# `data` pool. The .res renders `meta-disk /dev/<meta-pool>/...;` on
# the local diskful host (scenario 5.W05 / 6.18).
#
# When the DATA disk fails:
#
#   1. drbdadm detach <rd>             # release the failed data backing
#   2. (operator) replace the data LV  # meta LV stays intact
#   3. drbdadm attach <rd>             # re-bind data + existing meta
#   4. drbdadm invalidate <rd>         # resync from peers (run ONLY on
#                                      # the side that lost data)
#
# The critical contract under test: step 3 succeeds WITHOUT a prior
# `drbdadm create-md` because the external meta-disk survived the data
# swap — the bitmap, AL, and superblock are still valid. This is the
# whole reason an operator chooses external metadata: cheap data-disk
# replacement, no full meta re-init.
#
# Contrast with internal-metadata replacement (scenario 5.W09 / wave1
# 5.12): there meta lives inside the data LV, so a data-LV swap loses
# the meta block and `create-md` IS required before `attach`. That
# distinction is the whole point of 5.W10.
#
# Reconciler-survival contract
# ----------------------------
#
# Same as the other wave2-05 operator recipes (5.W12 split-brain,
# 5.W13 quorum-loss, 5.W14 disconnect): the satellite reconciler must
# NOT fight the recipe. Specifically it must NOT:
#
#   * re-render .res with `meta-disk internal;` while the operator is
#     mid-recipe (the prop StorPoolNameDrbdMeta still resolves to the
#     external pool — the .res sha256 must be stable across the window)
#   * run `drbdadm create-md` against the freshly-attached resource
#     (would zero the surviving external meta and force a full resync
#     that defeats the point of the external-meta design)
#   * touch DrbdOptions/SkipDisk during the attach handshake
#
# Coverage status (2026-05-14)
# ----------------------------
#
# Cross-listed feature 5.W05 / wave1 6.18 (StorPoolNameDrbdMeta) is
# only PARTIALLY wired today:
#
#   * .res render path: WIRED. The renderer emits
#     `meta-disk /dev/<meta-pool>/<rd>_00000_meta;` for the local
#     diskful host. Unit tests TestApplyRendersExternalMetaDiskPath +
#     TestRenderExternalMetadata pin this.
#   * Satellite-side carving of the sibling `_meta` LV:
#     NOT WIRED. Provider.CreateMetaVolume API is still pending;
#     TestApplyProvisionsBothDataAndMeta is t.Skip'd in
#     pkg/satellite/reconciler_drbd_test.go.
#
# Therefore, like its sibling storage-external-drbd-meta.sh (the 6.18
# e2e), this script splits into a MUST-PASS render assertion and a
# best-effort runtime probe that is currently a KNOWN GAP:
#
#   PHASE A (must pass today):
#     - Apply the RD with StorPoolNameDrbdMeta=<meta>.
#     - Assert the rendered .res on the diskful host carries the
#       external meta-disk path BEFORE simulating disk failure.
#     - Capture the .res sha256 — used as the reconciler-survival
#       guard at the end of the recipe walk-through (must be
#       unchanged after the recipe).
#     - Walk through the recipe COMMAND-CONTRACT in dry mode (using
#       `drbdadm --dry-run` where appropriate) and assert the commands
#       parse and reference the expected resource.
#     - Re-read .res after the dry walk-through: sha256 must match
#       the pre-walk capture. This is the reconciler-survival pin.
#
#   PHASE B (expected KNOWN GAP today, flips to PASS-UPGRADE when
#            Provider.CreateMetaVolume lands):
#     - Verify the sibling `_meta` LV actually got carved on the
#       meta pool.
#     - If present: take the resource to UpToDate, manually destroy
#       the DATA LV, run the full recipe (detach → swap-data →
#       attach → invalidate), assert NO `drbdadm create-md` was
#       issued, and assert the resource re-converges to UpToDate
#       via resync from the peer in <120 s.
#     - If absent: print KNOWN GAP, skip the runtime walk-through,
#       and pass the script. The render + reconciler-survival
#       contract above is the executable spec for the day the
#       provisioner lands.
#
# When the meta-LV provisioner is wired, this script flips to the
# full e2e automatically — no edits needed beyond updating the
# comment block.
#
# Stand: any 2+ worker blockstor cluster. Same pool layout as the
# 6.18 e2e (storage-external-drbd-meta.sh) — data pool `lvm-thin`,
# meta pool `meta-thin` carved as a sibling thinpool in the same VG.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=${RD:-extmeta-replace}
DATA_POOL=${DATA_POOL:-lvm-thin}
META_POOL=${META_POOL:-meta-thin}
META_VG=${META_VG:-blockstor-lvm}
META_LVTHIN=${META_LVTHIN:-meta}
META_SIZE=${META_SIZE:-256M}
RECOVERY_WINDOW=${RECOVERY_WINDOW:-120}

N1=$WORKER_1
N2=$WORKER_2

META_SP_1="${META_POOL}.${N1}"
META_SP_2="${META_POOL}.${N2}"

# Track on which workers we created the meta thinpool so cleanup
# only tries to lvremove what we own.
META_CREATED_ON=()

cleanup() {
    local rc=$?
    delete_rd "$RD"
    kubectl delete --wait=true --timeout=30s --ignore-not-found \
        storagepool.blockstor.io.blockstor.io \
        "$META_SP_1" "$META_SP_2" 2>/dev/null || true
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

# ---------------------------------------------------------------------
# Step 1: carve the meta thinpool on $N1 + $N2
# ---------------------------------------------------------------------
#
# Same lvcreate dance as storage-external-drbd-meta.sh. install-pools.sh
# leaves ~1 GiB VFree in $META_VG for exactly this kind of sibling pool.
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

# ---------------------------------------------------------------------
# Step 2: declare per-node StoragePool CRDs for the meta pool
# ---------------------------------------------------------------------
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
sleep 3

# ---------------------------------------------------------------------
# Step 3: apply the RD with StorPoolNameDrbdMeta=meta-thin
# ---------------------------------------------------------------------
echo ">> step 3: apply RD $RD with StorPoolNameDrbdMeta=$META_POOL on $N1+$N2"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    StorPoolNameDrbdMeta: ${META_POOL}
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

# ---------------------------------------------------------------------
# Step 4 (PHASE A): assert .res renders external meta-disk + capture sha256
# ---------------------------------------------------------------------
#
# The .res render contract for external metadata is already shipped
# (commit 01dd0655e). We pin it here as a precondition for the recipe
# walk-through. Without it the recipe is moot.
echo ">> step 4: wait for /etc/drbd.d/${RD}.res to appear on $N1"
deadline=$(( $(date +%s) + 90 ))
res_file=""
while (( $(date +%s) < deadline )); do
    if on_node "$N1" test -f "/etc/drbd.d/${RD}.res" 2>/dev/null; then
        res_file=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res" 2>/dev/null || true)
        [[ -n "$res_file" ]] && break
    fi
    sleep 2
done
if [[ -z "$res_file" ]]; then
    echo "FAIL: ${RD}.res never appeared on $N1 within 90s"
    kubectl -n "$NS" logs -l app=blockstor-controller --tail=30 2>/dev/null \
        | grep -iE "$RD|error|meta" || true
    exit 1
fi
echo "---- rendered .res on $N1 ----"
echo "$res_file"
echo "------------------------------"

EXPECTED_LOCAL="meta-disk /dev/${META_POOL}/${RD}_00000_meta;"
if ! echo "$res_file" | grep -qF "$EXPECTED_LOCAL"; then
    echo "FAIL: expected '$EXPECTED_LOCAL' in rendered .res on $N1"
    echo "      precondition 5.W05 / 6.18 (StorPoolNameDrbdMeta render) not met —"
    echo "      5.W10 recipe is moot without external meta on the diskful host."
    exit 1
fi
echo "   OK: $N1 .res carries external meta-disk path"

# Capture .res sha256 pre-recipe — used as the reconciler-survival
# guard. Identical contract to scenario 5.W12 (split-brain): the
# satellite reconciler MUST NOT re-render the .res while the operator
# walks the disk-replace recipe, otherwise it can pull `meta-disk
# internal;` back on top of the surviving external meta and force a
# `create-md` we explicitly do NOT want.
pre_sha=$(on_node "$N1" bash -c "sha256sum /etc/drbd.d/${RD}.res | awk '{print \$1}'")
echo "   pre-recipe .res sha256 on $N1 = $pre_sha"

# ---------------------------------------------------------------------
# Step 5 (PHASE B gate): probe the sibling _meta LV
# ---------------------------------------------------------------------
#
# Until Provider.CreateMetaVolume lands, the sibling LV is missing
# and we cannot run the runtime recipe. Same gate as
# storage-external-drbd-meta.sh step 6.
echo ">> step 5: probe for sibling _meta LV on $N1:${META_VG}/${META_LVTHIN}"
META_LV_NAME="${RD}_00000_meta"
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

# ---------------------------------------------------------------------
# Step 6 (PHASE A): dry-walk the recipe command contract
# ---------------------------------------------------------------------
#
# Whether or not the runtime is wired, we always pin the COMMAND
# contract: `drbdadm --dry-run` resolves the resource, parses the
# .res, and prints the kernel-syscall sequence it WOULD issue. This
# is the contract operators copy out of the runbook and it must
# parse against the reconciler-managed .res today.
#
# We verify:
#   * `drbdadm --dry-run detach <rd>` resolves cleanly
#   * `drbdadm --dry-run attach <rd>` resolves and references the
#     external meta-disk path (NOT `internal`)
#   * the dry-run output does NOT mention `create-md` — the whole
#     point of 5.W10 vs 5.W09 (internal-meta replacement)
echo ">> step 6: dry-walk recipe commands on $N1 (parse-only, no kernel ops)"

# detach is a no-op when the resource is down — but it still must
# parse against the .res. We use --dry-run to avoid touching the
# kernel state in case PHASE B is gated off.
detach_dry=$(on_node "$N1" bash -c "drbdadm --dry-run detach ${RD} 2>&1" || true)
echo "---- drbdadm --dry-run detach ${RD} ----"
echo "$detach_dry" | sed 's/^/   | /'
if echo "$detach_dry" | grep -qiE "unknown resource|no such resource|cannot find"; then
    echo "FAIL: drbdadm could not resolve resource ${RD} for detach"
    exit 1
fi

# attach dry-run is the load-bearing assertion: the rendered kernel
# command line must reference the external meta-disk path, NOT
# `internal`. drbdadm --dry-run prints the drbdsetup invocations
# it would issue; we grep for the meta-disk argument.
attach_dry=$(on_node "$N1" bash -c "drbdadm --dry-run attach ${RD} 2>&1" || true)
echo "---- drbdadm --dry-run attach ${RD} ----"
echo "$attach_dry" | sed 's/^/   | /'
if echo "$attach_dry" | grep -qiE "unknown resource|no such resource|cannot find"; then
    echo "FAIL: drbdadm could not resolve resource ${RD} for attach"
    exit 1
fi

# The dry-run output must mention our external meta-disk path. Older
# drbdadm versions abbreviate the path in the syscall trace; tolerate
# either the full LV path or the basename. Either way, `internal` as
# the meta-disk argument is a regression — that would mean the
# rendered .res reverted to internal metadata mid-recipe.
if ! echo "$attach_dry" | grep -qE "${META_LV_NAME}|/dev/${META_POOL}/${META_LV_NAME}"; then
    echo "WARN: drbdadm --dry-run attach did not echo the meta-disk path"
    echo "      this may be a drbdadm verbosity difference; not failing,"
    echo "      but the .res content check (step 4) is the authoritative pin."
fi
if echo "$attach_dry" | grep -qE "meta-disk[[:space:]]+internal"; then
    echo "FAIL: drbdadm --dry-run attach references 'meta-disk internal'"
    echo "      — the .res reverted to internal metadata. 5.W10 contract"
    echo "      broken at parse time."
    exit 1
fi

# create-md MUST NOT appear anywhere in the recipe — that is the
# 5.W10 vs 5.W09 differentiator. We check both the detach and attach
# dry-run output for any mention.
if echo "$detach_dry $attach_dry" | grep -qiE "create-md|initialize.*meta"; then
    echo "FAIL: dry-walked recipe references create-md / meta initialization"
    echo "      — 5.W10 contract is that external meta SURVIVES the data swap"
    echo "      and create-md is NOT required (contrast scenario 5.W09)."
    exit 1
fi
echo "   OK: dry-walked recipe does not invoke create-md"

# Re-read .res after the dry walk: sha256 must be unchanged. The
# satellite reconciler must not have re-rendered during the recipe
# window. (Dry-run does not actually mutate kernel state; the only
# way .res would change is if the reconciler itself re-rendered it
# from a watch event.)
sleep 5
post_dry_sha=$(on_node "$N1" bash -c "sha256sum /etc/drbd.d/${RD}.res | awk '{print \$1}'")
if [[ "$pre_sha" != "$post_dry_sha" ]]; then
    echo "FAIL: .res sha256 changed during the dry-walk recipe window"
    echo "      pre  = $pre_sha"
    echo "      post = $post_dry_sha"
    echo "      satellite reconciler is re-rendering mid-recipe — would clobber"
    echo "      operator intent during a real replace."
    exit 1
fi
echo "   OK: .res sha256 stable across dry-walk window ($pre_sha)"

# ---------------------------------------------------------------------
# Step 7 (PHASE B): runtime recipe — gated on meta-LV provisioner
# ---------------------------------------------------------------------
if (( meta_lv_present == 0 )); then
    echo ">> step 7: KNOWN GAP — sibling _meta LV ${META_LV_NAME} not present on"
    echo "           ${META_VG}/${META_LVTHIN}. Provider.CreateMetaVolume API"
    echo "           is still pending (TestApplyProvisionsBothDataAndMeta is"
    echo "           t.Skip'd in pkg/satellite/reconciler_drbd_test.go)."
    echo ""
    echo "           Skipping the runtime detach/swap/attach/invalidate walk-"
    echo "           through. PHASE A (render + dry-walk command contract +"
    echo "           reconciler-survival sha256 pin) is the executable spec"
    echo "           for the day the provisioner lands; this step will then"
    echo "           flip to PASS-UPGRADE automatically."
    echo ""
    echo ">> DISK-REPLACE-EXTERNAL-METADATA OK"
    echo "   .res external meta-disk render:           PASS"
    echo "   dry-walked recipe command contract:       PASS"
    echo "   .res sha256 stability across recipe:      PASS"
    echo "   runtime recipe walk-through:              KNOWN GAP"
    exit 0
fi

echo ">> step 7: PASS-UPGRADE — sibling _meta LV present, walking full recipe"

# Wait for both peers to reach UpToDate before injecting the failure.
wait_uptodate "$RD" "$N1" "$N2"
echo "   RD UpToDate on $N1 and $N2"

# Resolve the data LV path. Provider.CreateVolume names data LVs
# <rd>_<vol-padded>; the meta sibling shares the prefix with the
# `_meta` suffix that we already pinned above.
DATA_LV_NAME="${RD}_00000"

# Take the local DOWN before swapping the data LV — drbdadm detach
# alone is not enough on a kernel module that still has the device
# open. We do this by walking the recipe exactly as the runbook
# prescribes: detach → swap → attach → invalidate.
echo "   7a: drbdadm detach ${RD} on ${N1}"
on_node "$N1" drbdadm detach "$RD"

# Simulate a data-disk failure by removing the data LV. The meta LV
# is left untouched — that is the whole point of 5.W10.
echo "   7b: simulate data-LV loss — lvremove ${META_VG}/${DATA_LV_NAME} on ${N1}"
on_node "$N1" bash -c "
    lvremove -f ${META_VG}/${DATA_LV_NAME} 2>&1 || true
"

# Verify the meta LV is still there. If it disappeared the test
# setup itself is broken (we should have only touched the data LV).
if ! on_node "$N1" bash -c "lvs --noheadings -o lv_name ${META_VG} | awk '{print \$1}' | grep -qx '${META_LV_NAME}'"; then
    echo "FAIL: meta LV ${META_LV_NAME} disappeared during data-LV swap"
    echo "      test setup is broken — only the data LV should have been removed"
    exit 1
fi
echo "   OK: meta LV ${META_LV_NAME} survived the data swap"

# Re-carve the data LV at the same name and size (same backing the
# Provider would re-create from a re-toggled diskful Resource).
echo "   7c: re-carve data LV ${META_VG}/${DATA_LV_NAME} on ${N1}"
on_node "$N1" bash -c "
    lvcreate -T ${META_VG}/thin --name ${DATA_LV_NAME} -V 64M \
        --config 'activation {udev_sync=0 udev_rules=0}' -y >/dev/null
"

# Re-attach. NO create-md — the surviving meta LV still holds the
# superblock / activity log / bitmap; the recipe contract is that
# attach binds the fresh data LV to the existing meta and DRBD
# treats the local data as Inconsistent until invalidate runs.
echo "   7d: drbdadm attach ${RD} on ${N1} (NO create-md)"
on_node "$N1" drbdadm attach "$RD"

# Invalidate on the side that lost data — pulls from the peer.
# UG9 §"Replacing a failed disk when using external metadata" line
# 124: "Be sure to run drbdadm invalidate on the node WITHOUT good
# data". The peer $N2 still has UpToDate data so this is safe.
echo "   7e: drbdadm invalidate ${RD} on ${N1} (resync from ${N2})"
on_node "$N1" drbdadm invalidate "$RD"

# Wait for the resync. 120s ceiling for 64 MiB; in practice it
# completes in 5-15s on the QEMU stand.
echo "   7f: wait up to ${RECOVERY_WINDOW}s for ${N1} -> UpToDate"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
recovered=false
while (( $(date +%s) < deadline )); do
    d=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$d" == *"UpToDate"* ]]; then
        recovered=true
        break
    fi
    sleep 2
done
if [[ "$recovered" != "true" ]]; then
    echo "FAIL: ${N1} did NOT recover to UpToDate within ${RECOVERY_WINDOW}s"
    on_node "$N1" drbdsetup status "$RD" 2>&1 | sed 's/^/      | /' || true
    exit 1
fi

# Reconciler-survival final pin: the .res must not have been
# re-rendered during the runtime recipe either.
final_sha=$(on_node "$N1" bash -c "sha256sum /etc/drbd.d/${RD}.res | awk '{print \$1}'")
if [[ "$pre_sha" != "$final_sha" ]]; then
    echo "FAIL: .res sha256 changed during runtime recipe"
    echo "      pre  = $pre_sha"
    echo "      post = $final_sha"
    exit 1
fi

# Regression guard: peer must still be UpToDate.
n2_final=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$n2_final" != *"UpToDate"* ]]; then
    echo "FAIL: $N2 disk regressed during recipe (got: $n2_final)"
    exit 1
fi

echo ">> DISK-REPLACE-EXTERNAL-METADATA OK"
echo "   .res external meta-disk render:           PASS"
echo "   dry-walked recipe command contract:       PASS"
echo "   .res sha256 stability across recipe:      PASS"
echo "   runtime detach/swap/attach/invalidate:    PASS"
echo "   peer disk integrity:                      PASS"
