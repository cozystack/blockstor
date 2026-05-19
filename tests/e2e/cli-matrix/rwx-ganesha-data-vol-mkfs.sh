#!/usr/bin/env bash
#
# usage: rwx-ganesha-data-vol-mkfs.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 311 NFS-Ganesha follow-up (kernel-truth pin
# of the multi-volume mkfs path on a real DRBD stack).
#
# Reproduction shape (from the piraeus NFS-Ganesha RWX scenario):
#
#   $ linstor rd c ganesha-test
#   $ linstor rd sp ganesha-test FileSystem/Type ext4
#   $ linstor vd c ganesha-test 256M       # vol-0 control / config
#   $ linstor vd c ganesha-test 512M       # vol-1 data (NFS export)
#   $ linstor r c ganesha-test --auto-place=2 -s <pool>
#   # wait until both volumes reach UpToDate on every replica
#
# Expected (Bug 311 fixed): BOTH /dev/drbd<m> AND /dev/drbd<m+1>
# carry a valid ext4 filesystem (blkid -o export reports TYPE=ext4 on
# each), and the Resource CRD carries FilesystemFormatted=True.
#
# Pre-fix shape: vol-0 (control) was mkfs'd, but vol-1 (data) stayed
# raw because the satellite's runAutoMkfs only formatted /dev/drbd<m>.
# NFS-ganesha then tried to export an unformatted block device and
# `mount-recovery@<rd>.service` died with `fsck.ext2: Bad magic
# number in super-block`.
#
# Unit pin: pkg/satellite/reconciler_drbd_test.go::
#   TestApplyAutoMkfsMultiVolumeFormatsAllVolumes
#   TestApplyAutoMkfsMultiVolumeSkipsPreFormattedVolume
#   TestApplyAutoMkfsMultiVolumeStampsConditionOnlyAfterAll
# verify the satellite formats every volume in the FakeExec world.
# This L6 cell is the kernel-truth half — only the real DRBD stack
# can observe blkid's actual filesystem-signature read on the
# replicated device.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-bug311-ganesha
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: 2 healthy SATELLITE nodes carrying the target pool. NFS-
# Ganesha-shaped RDs are 2-volume / 2-replica in production; we only
# need a 2-node pool here.
echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on 2 nodes (got $ok_nodes) — Bug 311 multi-volume fixture not available"
    exit 0
fi

echo ">> [Bug 311] rd c $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null

# FileSystem/Type drives the satellite's runAutoMkfs path. Set on the
# RD scope (not the RG) so the test stays self-contained — the
# effective-props resolver folds RD props over RG defaults, so an RD-
# level mkfs prop is sufficient to drive the auto-mkfs loop on every
# replica that becomes auto-primary.
echo ">> [Bug 311] rd sp $RD FileSystem/Type ext4"
"${LCTL[@]}" resource-definition set-property "$RD" FileSystem/Type ext4 >/dev/null

echo ">> [Bug 311] vd c $RD 256M (vol-0: control / NFS-ganesha config)"
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null

echo ">> [Bug 311] vd c $RD 512M (vol-1: data / NFS export target)"
"${LCTL[@]}" volume-definition create "$RD" 512M >/dev/null

echo ">> [Bug 311] r c $RD --auto-place=2 -s $POOL"
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

# Resolve the diskful nodes — auto-place=2 stages two of them. The
# mkfs path fires on whichever replica is elected auto-primary; both
# satellites observe the resulting filesystem via DRBD replication, so
# blkid on EITHER replica's local DRBD device sees TYPE=ext4 once
# initial sync completes.
echo ">> resolve diskful nodes"
mapfile -t diskful_nodes < <(linstor_diskful_nodes "$RD")
if (( ${#diskful_nodes[@]} < 2 )); then
    echo "FAIL: expected 2 diskful nodes, got ${#diskful_nodes[@]}" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   diskful nodes: ${diskful_nodes[*]}"

echo ">> wait up to 180s for vol-0 AND vol-1 to reach UpToDate on both replicas"
# Initial sync of a 768 MiB total RD on a busy QEMU stand takes time;
# add headroom over the single-volume catchers' 90s.
deadline=$(( $(date +%s) + 180 ))
all_up=false
while (( $(date +%s) < deadline )); do
    # 2 replicas × 2 volumes = 4 disk_state strings, all UpToDate.
    states=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .vlms[]? | .state.disk_state // "Unknown"] | join(",")' \
        2>/dev/null || echo "")
    count_uptodate=$(awk -F, '{ for (i=1;i<=NF;i++) if ($i=="UpToDate") n++ } END { print n+0 }' <<<"$states")
    if (( count_uptodate == 4 )); then
        all_up=true
        break
    fi
    sleep 3
done

if [[ "$all_up" != "true" ]]; then
    echo "FAIL: vol-0 + vol-1 did not reach UpToDate on both replicas within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi

# Resolve the DRBD minor for vol-0 from the .res file on any diskful
# replica. vol-1's device is minor+1 by construction (the satellite
# allocates contiguous minors per RD; the .res renderer emits one
# `volume <vol>` block per VD with a `device /dev/drbd<minor+vol>`
# line).
probe_node="${diskful_nodes[0]}"
echo ">> resolve DRBD minor on $probe_node"
minor_vol0=$(on_node "$probe_node" bash -c "
    grep -oE 'device /dev/drbd[0-9]+' /etc/drbd.d/${RD}.res 2>/dev/null \
        | head -1 | grep -oE '[0-9]+'
" 2>/dev/null || echo "")
if [[ -z "$minor_vol0" || ! "$minor_vol0" =~ ^[0-9]+$ ]]; then
    echo "FAIL: could not resolve vol-0 DRBD minor on $probe_node" >&2
    on_node "$probe_node" cat /etc/drbd.d/"${RD}".res 2>&1 | tail -40 >&2
    exit 1
fi
minor_vol1=$(( minor_vol0 + 1 ))
echo "   minor_vol0=/dev/drbd${minor_vol0}  minor_vol1=/dev/drbd${minor_vol1}"

# THE BUG ASSERTION: blkid -o export on BOTH /dev/drbd<minor_vol0> AND
# /dev/drbd<minor_vol1> MUST report TYPE=ext4 on every diskful replica
# once initial sync completes. The satellite's runAutoMkfs runs on the
# auto-primary replica; the ext4 superblock is then replicated to the
# peer via DRBD, so blkid on the secondary sees the same signature.
#
# Bug 311 pre-fix shape: blkid on /dev/drbd<minor_vol1> would show no
# TYPE= line (raw block device) — that's exactly what tripped
# `fsck.ext2: Bad magic number in super-block` inside NFS-Ganesha's
# mount-recovery service.
echo ">> [Bug 311] blkid -o export on /dev/drbd${minor_vol0} + /dev/drbd${minor_vol1} on every diskful replica"
deadline=$(( $(date +%s) + 120 ))
all_formatted=false
while (( $(date +%s) < deadline )); do
    failed=""
    for node in "${diskful_nodes[@]}"; do
        for minor in "$minor_vol0" "$minor_vol1"; do
            # blkid exit 2 = no signature → falsy here.
            out=$(on_node "$node" blkid -o export "/dev/drbd${minor}" 2>/dev/null || true)
            if ! grep -qE '^TYPE=ext4$' <<<"$out"; then
                failed="$failed ${node}:/dev/drbd${minor}"
            fi
        done
    done
    if [[ -z "$failed" ]]; then
        all_formatted=true
        break
    fi
    sleep 5
done

if [[ "$all_formatted" != "true" ]]; then
    echo "FAIL (Bug 311 regression): blkid did NOT report TYPE=ext4 on:$failed" >&2
    for node in "${diskful_nodes[@]}"; do
        for minor in "$minor_vol0" "$minor_vol1"; do
            echo "----- ${node} blkid -o export /dev/drbd${minor} -----" >&2
            on_node "$node" blkid -o export "/dev/drbd${minor}" 2>&1 >&2 || true
        done
    done
    exit 1
fi

# Belt-and-braces: the satellite stamps `FilesystemFormatted=True` on
# the Resource CRD once every diskful volume passes the mkfs gate
# (Phase 11.3 Stage 2). Assert at least one of the diskful replicas
# carries the Condition — the dispatcher then propagates this into
# the desired-state on every subsequent reconcile, short-circuiting
# the per-volume blkid round-trip.
echo ">> [Bug 311] Status.Conditions[FilesystemFormatted]=True stamped"
deadline=$(( $(date +%s) + 60 ))
stamped=""
while (( $(date +%s) < deadline )); do
    for node in "${diskful_nodes[@]}"; do
        cond=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${node}" \
            -o jsonpath='{.status.conditions[?(@.type=="FilesystemFormatted")].status}' \
            2>/dev/null || echo "")
        if [[ "$cond" == "True" ]]; then
            stamped="$node"
            break
        fi
    done
    if [[ -n "$stamped" ]]; then
        break
    fi
    sleep 3
done

if [[ -z "$stamped" ]]; then
    echo "FAIL (Bug 311): FilesystemFormatted=True Condition never stamped on any diskful replica within 60s" >&2
    for node in "${diskful_nodes[@]}"; do
        echo "----- ${RD}.${node} status.conditions -----" >&2
        kubectl get "resources.blockstor.io.blockstor.io/${RD}.${node}" \
            -o jsonpath='{.status.conditions}' 2>&1 >&2 || true
        echo "" >&2
    done
    exit 1
fi

echo ">> rwx-ganesha-data-vol-mkfs OK (Bug 311 pinned: BOTH /dev/drbd${minor_vol0} + /dev/drbd${minor_vol1} carry ext4 on ${diskful_nodes[*]}; FilesystemFormatted=True on $stamped)"
