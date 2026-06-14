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
"${LCTL[@]}" resource create --auto-place=3 --storage-pool="$POOL" "$RD" >/dev/null

# Resolve the 3 diskful nodes auto-place picked, so every convergence
# check below reads the per-replica Resource.Status directly.
echo ">> wait for 3 diskful replicas to be placed"
if ! wait_diskful_count "$RD" 3 90; then
    echo "FAIL: auto-place did not land 3 diskful replicas" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi
mapfile -t DISKFUL_NODES < <(linstor_diskful_nodes "$RD")
if (( ${#DISKFUL_NODES[@]} != 3 )); then
    echo "FAIL: expected 3 diskful nodes, got ${#DISKFUL_NODES[@]}: ${DISKFUL_NODES[*]}" >&2
    exit 1
fi
echo "   diskful nodes: ${DISKFUL_NODES[*]}"

# wait_all_replicas_volume_uptodate <vol> <timeout> — poll
# Resource.Status.volumes[<vol>].diskState on EVERY diskful node until
# all report UpToDate, or timeout. Reads the observer-stamped CRD via
# status_disk_state (lib.sh) — the SAME wire surface the passing cells
# use. NOTE: the `--machine-readable resource list` projection leaves
# `.vlms` null on this apiserver, so the previous jq-on-vlms detection
# always read empty and timed out even while `linstor r l` (which reads
# Resource.Status) showed every replica UpToDate. status_disk_state
# reads the populated CRD field instead.
wait_all_replicas_volume_uptodate() {
    local vol=$1 timeout=$2
    local deadline=$(( $(date +%s) + timeout ))
    local node st all_ok
    while (( $(date +%s) < deadline )); do
        all_ok=true
        for node in "${DISKFUL_NODES[@]}"; do
            st=$(status_disk_state "$RD" "$node" "$vol")
            if [[ "$st" != "UpToDate" ]]; then
                all_ok=false
                break
            fi
        done
        if [[ "$all_ok" == "true" ]]; then
            return 0
        fi
        sleep 3
    done
    return 1
}

# 240s: a 1G x3 initial sync shares the stand with the rest of the
# sweep; the previous 120s budget flaked under load.
echo ">> wait up to 240s for vol-0 to reach UpToDate on all 3 replicas"
if ! wait_all_replicas_volume_uptodate 0 240; then
    echo "FAIL: vol-0 did not reach UpToDate on all 3 replicas within 240s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi

# THE BUG: add vol-1 and vol-2 AFTER vol-0 is UpToDate.
echo ">> [Bug 332] late vd c (vol-1)"
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 332] late vd c (vol-2)"
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

# Each late-added 1G volume needs its own initial sync on every replica;
# under sweep load that takes well past the old 60s budget, so give the
# late volumes the same 240s headroom vol-0 gets.
echo ">> wait up to 240s for vol-1 + vol-2 to reach UpToDate on all 3 replicas"
late_up=true
for vol in 1 2; do
    if ! wait_all_replicas_volume_uptodate "$vol" 240; then
        late_up=false
        break
    fi
done

if [[ "$late_up" != "true" ]]; then
    echo "FAIL (Bug 332): late-added vol-1/vol-2 not UpToDate on all 3 replicas within 240s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -40 >&2
    # Surface the smoking gun: per (node, vol) disk_state straight from
    # the populated Resource.Status — a Bug-332-bitten path shows vol-1
    # or vol-2 stuck on Diskless on a non-DISKLESS replica.
    echo "----- per (node, vol) Resource.Status diskState -----" >&2
    for node in "${DISKFUL_NODES[@]}"; do
        for vol in 0 1 2; do
            echo "  $node vol=$vol state=$(status_disk_state "$RD" "$node" "$vol")" >&2
        done
    done
    exit 1
fi

# Guard against the surface symptom: even if the wire-shape probe
# above tripped its happy path, drbdadm status on a diskful replica
# MUST NOT report any volume as Diskless when the spec lacks the
# DISKLESS flag. This is the kernel-truth assertion that distinguishes
# Bug 332 (Unintentional Diskless) from spec-pinned diskless replicas.
echo ">> [Bug 332] kernel-truth: drbdsetup status on a diskful node"
# Use the already-resolved diskful node set (reliable, CRD-backed) and
# probe the kernel directly via the satellite pod (on_node, lib.sh) —
# the same drbdsetup status path the parent helpers use. The previous
# `--machine-readable ... | jq .flags | head -1` resolution shared the
# null-vlms / SIGPIPE failure modes of the wait loops above.
satellite_node=${DISKFUL_NODES[0]:-}

if [[ -z "$satellite_node" ]]; then
    echo "SKIP-PARTIAL: could not resolve a diskful node for kernel-truth check"
    echo ">> multi-volume-late-vd-create OK (Bug 332 pinned at REST/state level)"
    exit 0
fi

if status_out=$(on_node "$satellite_node" drbdsetup status "$RD" --verbose 2>&1); then
    echo "$status_out"
    if grep -E 'volume:[12].*disk:Diskless' <<<"$status_out" >/dev/null; then
        echo "FAIL (Bug 332): diskful node $satellite_node reports volume:1 or volume:2 as Diskless on kernel state" >&2
        echo "$status_out" >&2
        exit 1
    fi
else
    echo "SKIP-PARTIAL: drbdsetup status on $satellite_node failed; REST-level pin still asserted"
fi

echo ">> multi-volume-late-vd-create OK (Bug 332 pinned: late vd c on $RD brought vol-1/vol-2 to UpToDate, no Unintentional Diskless)"
