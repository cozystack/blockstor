#!/usr/bin/env bash
#
# usage: bug-384-late-vd-thin-converges.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 384 (P0, DATA INTEGRITY; regression of the
# Bug 79/332 family).
#
# Verbatim operator repro on a 2-diskful lvm-thin RD:
#
#   $ linstor rd c test
#   $ linstor vd c test 1G                 # vol-0
#   $ linstor r c <n1> test -s lvm-thin
#   $ linstor r c <n2> test -s lvm-thin
#   # wait until vol-0 is UpToDate on both replicas
#   $ linstor vd c test 1G                 # vol-1 — the LATE-added VD
#
#   $ linstor r l
#    test  <n1>  DRBD,STORAGE  Inconsistent
#    test  <n2>  DRBD,STORAGE  Inconsistent
#   $ linstor v l
#    test  <n1>  lvm-thin  vol 0  /dev/drbd20000  UpToDate
#    test  <n1>  lvm-thin  vol 1  /dev/drbd20001  Inconsistent   ← bug
#    test  <n2>  lvm-thin  vol 0  /dev/drbd20000  UpToDate
#    test  <n2>  lvm-thin  vol 1  /dev/drbd20001  Inconsistent   ← bug
#
# vol-0 stays UpToDate; the late-added vol-1 is stuck Inconsistent on
# EVERY replica with no SyncSource to recover from — neither replica
# was elected the initial-UpToDate winner for the new volume, so DRBD
# has no authority to drive either out of Inconsistent.
#
# Expected (post-fix): the satellite re-runs the lowest-node-id winner
# election locally per fresh volume (seedFreshVolumes / isLateAddWinner),
# the lowest-id replica seeds vol-1 Consistent+UpToDate, and BOTH
# replicas settle vol-1 UpToDate within 60s.
#
# Unit pin: pkg/satellite/reconciler_drbd_test.go::
#   TestApplyLateAddedVolumeWinnerSeedsUpToDate (winner seeds vol-1
#   Consistent+UpToDate) and ::TestApplyLateAddedVolumeNonWinnerTakes
#   SkipInitSync (non-winner stays case-A, no split-brain). This L6
#   cell is the kernel-truth half — only the real stand observes the
#   actual `linstor v l` / `drbdadm status` Inconsistent surface.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-384
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2

# Pre-flight: both target nodes must carry the thin pool — Bug 384's
# repro is thin-specific (the seed path only skips initial sync on
# thin/ZFS). Skip cleanly if the fixture pool is not present.
echo ">> pre-flight: $POOL SP on $N1 + $N2"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
have=$(jq -r --arg n1 "$N1" --arg n2 "$N2" \
    '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique
     | map(select(. == $n1 or . == $n2)) | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( have < 2 )); then
    echo "SKIP: $POOL SP not on both $N1 and $N2 (got $have) — Bug 384 fixture unavailable"
    exit 0
fi

echo ">> [Bug 384] rd c + vd c (vol-0)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 384] r c on $N1 + $N2 (-s $POOL)"
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for vol-0 UpToDate on both replicas"
wait_uptodate "$RD" "$N1" "$N2"

# THE BUG: add vol-1 AFTER vol-0 is UpToDate. The RD is already
# Initialized, so the first-activation winner election cannot fire.
echo ">> [Bug 384] late vd c (vol-1)"
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> wait up to 90s for vol-1 to reach UpToDate on BOTH replicas"
deadline=$(( $(date +%s) + 90 ))
late_up=false
while (( $(date +%s) < deadline )); do
    s1=$(status_disk_state "$RD" "$N1" 1)
    s2=$(status_disk_state "$RD" "$N2" 1)
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" ]]; then
        late_up=true
        break
    fi
    sleep 3
done

if [[ "$late_up" != "true" ]]; then
    echo "FAIL (Bug 384): late-added vol-1 did not reach UpToDate on both replicas within 90s" >&2
    echo "  $N1 vol-1: $(status_disk_state "$RD" "$N1" 1)   $N2 vol-1: $(status_disk_state "$RD" "$N2" 1)" >&2
    "${LCTL[@]}" volume list --resources "$RD" 2>&1 | tail -20 >&2 || true
    exit 1
fi

# Kernel-truth guard: drbdadm status on a diskful replica MUST NOT
# report vol-1 as Inconsistent (the exact operator-observed surface).
echo ">> [Bug 384] kernel-truth: drbdadm status on $N1"
if status_out=$(on_node "$N1" drbdadm status "$RD" 2>&1); then
    echo "$status_out"
    if grep -E 'volume:1.*disk:Inconsistent' <<<"$status_out" >/dev/null; then
        echo "FAIL (Bug 384): $N1 reports volume:1 as Inconsistent on kernel state" >&2
        echo "$status_out" >&2
        exit 1
    fi
else
    echo "SKIP-PARTIAL: kernel-truth probe on $N1 unavailable; Status-level pin still asserted"
fi

echo ">> bug-384-late-vd-thin-converges OK (late vd c on $RD brought vol-1 to UpToDate on both replicas, no stuck Inconsistent)"
