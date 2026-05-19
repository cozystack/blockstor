#!/usr/bin/env bash
#
# usage: multi-volume-late-vd-create.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 332 (regression of Bug 79, P1).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor rd c test2
#   $ linstor vd c test2 1G                       # vol-0
#   $ linstor r c test2 --auto-place=3 -s lvm-thin
#   # wait until all 3 replicas reach UpToDate
#   $ linstor vd c test2 1G                       # vol-1 — late VD
#   $ linstor vd c test2 1G                       # vol-2 — late VD
#
#   $ drbdadm status test2
#   test2 role:Secondary suspended:quorum
#     volume:0 disk:UpToDate blocked:upper
#     volume:1 disk:Diskless quorum:no     ← Unintentional Diskless
#     volume:2 disk:Diskless quorum:no     ← Unintentional Diskless
#
# Expected: late-added vol-1 / vol-2 each get their backing LV
# allocated on every diskful replica, drbdmeta create-md fires
# per-volume, the kernel slot picks up the new volumes, and every
# (replica, volume) pair settles UpToDate within 60s.
#
# Unit pin: pkg/satellite/reconciler_drbd_test.go::
#   TestApplyDRBDAllocatesBackingForLateAddedVolume
# verifies the satellite's per-volume create-md gate via FakeExec.
# This L6 cell is the kernel-truth half — only the real stand can
# observe the actual `drbdadm status` output that surfaced the bug.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-332
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: 3 healthy SATELLITE nodes carrying the target pool.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — Bug 332 fixture not available"
    exit 0
fi

echo ">> [Bug 332] rd c + vd c (vol-0)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 332] r c --auto-place=3 -s $POOL"
"${LCTL[@]}" resource create --auto-place=3 --storage-pool "$POOL" "$RD" >/dev/null

echo ">> wait up to 120s for vol-0 to reach UpToDate on all 3 replicas"
deadline=$(( $(date +%s) + 120 ))
all_up=false
while (( $(date +%s) < deadline )); do
    # Per-replica volume-0 state, three rows expected. The wire
    # shape is `linstor r l --resources <rd>` → DiskState column.
    states=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .vlms[]? | select(.vlm_nr == 0) | .state.disk_state // "Unknown"] | join(",")' \
        2>/dev/null || echo "")
    if [[ "$states" == "UpToDate,UpToDate,UpToDate" ]]; then
        all_up=true
        break
    fi
    sleep 3
done

if [[ "$all_up" != "true" ]]; then
    echo "FAIL: vol-0 did not reach UpToDate on all 3 replicas within 120s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi

# THE BUG: add vol-1 and vol-2 AFTER vol-0 is UpToDate.
echo ">> [Bug 332] late vd c (vol-1)"
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 332] late vd c (vol-2)"
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> wait up to 60s for vol-1 + vol-2 to reach UpToDate on all 3 replicas"
deadline=$(( $(date +%s) + 60 ))
late_up=false
while (( $(date +%s) < deadline )); do
    # Pull every (replica, volume) disk_state. We need exactly:
    #   3 rows × 3 volumes = 9 disk_state strings, all UpToDate.
    # A Bug-332-bitten path will show vol-1/vol-2 stuck on
    # "Diskless" with the operator-set DISKLESS flag absent.
    states=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .vlms[]? | .state.disk_state // "Unknown"] | join(",")' \
        2>/dev/null || echo "")
    count_uptodate=$(awk -F, '{ for (i=1;i<=NF;i++) if ($i=="UpToDate") n++ } END { print n+0 }' <<<"$states")
    if (( count_uptodate == 9 )); then
        late_up=true
        break
    fi
    sleep 3
done

if [[ "$late_up" != "true" ]]; then
    echo "FAIL (Bug 332): late-added vol-1/vol-2 not UpToDate on all 3 replicas within 60s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -40 >&2
    # Surface the smoking gun: a Diskless line for a non-DISKLESS spec.
    echo "----- linstor r l --resources $RD (with flags) -----" >&2
    "${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '.[][]? | "\(.node_name) vol=\(.vlms[]?.vlm_nr) state=\(.vlms[]?.state.disk_state) flags=\(.rsc_flags//[])"' >&2 || true
    exit 1
fi

# Guard against the surface symptom: even if the wire-shape probe
# above tripped its happy path, drbdadm status on a diskful replica
# MUST NOT report any volume as Diskless when the spec lacks the
# DISKLESS flag. This is the kernel-truth assertion that distinguishes
# Bug 332 (Unintentional Diskless) from spec-pinned diskless replicas.
echo ">> [Bug 332] kernel-truth: drbdadm status on a diskful node"
satellite_node=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r '.[][]? | select((.rsc_flags//[]) | (map(. == "DISKLESS") | any | not)) | .node_name' \
    2>/dev/null | head -1)

if [[ -z "$satellite_node" ]]; then
    echo "SKIP-PARTIAL: could not resolve a diskful node for kernel-truth check"
    echo ">> multi-volume-late-vd-create OK (Bug 332 pinned at REST/state level)"
    exit 0
fi

if status_out=$(kubectl debug node/"$satellite_node" --image=alpine -- chroot /host drbdadm status "$RD" 2>&1); then
    echo "$status_out"
    if grep -E 'volume:[12].*disk:Diskless' <<<"$status_out" >/dev/null; then
        echo "FAIL (Bug 332): diskful node $satellite_node reports volume:1 or volume:2 as Diskless on kernel state" >&2
        echo "$status_out" >&2
        exit 1
    fi
else
    echo "SKIP-PARTIAL: kubectl debug to inspect drbdadm status failed (RBAC / image pull); REST-level pin still asserted"
fi

echo ">> multi-volume-late-vd-create OK (Bug 332 pinned: late vd c on $RD brought vol-1/vol-2 to UpToDate, no Unintentional Diskless)"
