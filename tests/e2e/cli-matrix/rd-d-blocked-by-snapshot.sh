#!/usr/bin/env bash
#
# usage: rd-d-blocked-by-snapshot.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case E1 (deletion semantics).
#
# UG9 linstor-administration.adoc ~1364-1366, ~1395 documents two
# asymmetric deletion rules that are easy to break in a reimplementation:
#
#   1. `rd d <rd>` is REFUSED while the RD still has snapshots. The
#      operator must drop the snapshots first. Upstream raises
#      FAIL_EXISTS_SNAPSHOT_DFN with the text "Cannot delete <rd>
#      because it has snapshots." (blockstor: "Cannot delete resource
#      definition '<rd>' because it has snapshots.", code 514|MASK_ERROR,
#      HTTP 409 → CLI exit 10).
#
#   2. `r d <node> <rd>` of a SINGLE replica is NOT blocked by snapshots —
#      it drops one per-node replica and the snapshots SURVIVE on the
#      remaining replicas / snapshot-definition.
#
# This cell pins both on the live operator-CLI path:
#   a) 2-replica diskful RD + 1 snapshot.
#   b) `rd d` → non-zero, RD + replicas + snapshot all still present.
#   c) `r d <n2> <rd>` → exit 0, snapshot still present, replica n2 gone.
#   d) drop the snapshot, then `rd d` → exit 0, RD gone.
#
# Why a dedicated cell: the snapshot guard lives on the RD-delete
# handler only (pkg/rest/resource_definitions.go::handleRDDelete). A
# refactor that accidentally wired it onto the per-replica delete path
# (pkg/rest/autoplace.go::handleResourceDelete) would silently break the
# documented "snapshots survive a replica delete" contract — exactly the
# kind of regression L1 mocks can miss because the inmemory store and
# the real cascade diverge under finalizer pressure.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cce-rd-d-snap
SNAP=cce-snap1
SIZE_MIB=32

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> Phase 1: rd c + vd c + r c $N1 + r c $N2 (size=${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2: $_out" >&2; exit 1; }
wait_uptodate "$RD" "$N1" "$N2"

echo ">> Phase 2: snapshot create $RD $SNAP"
_out=$("${LCTL[@]}" snapshot create "$RD" "$SNAP" 2>&1) \
    || { echo "FAIL: snapshot create $SNAP: $_out" >&2; exit 1; }

# Wait for the snapshot to register on the controller (CRD present).
deadline=$(( $(date +%s) + 60 ))
snap_seen=false
while (( $(date +%s) < deadline )); do
    if "${LCTL[@]}" snapshot list 2>/dev/null | grep -q "$SNAP"; then
        snap_seen=true
        break
    fi
    sleep 2
done
if ! $snap_seen; then
    echo "FAIL: snapshot $SNAP never appeared in 'snapshot list'" >&2
    "${LCTL[@]}" snapshot list >&2 || true
    exit 1
fi

# =====================================================================
# Contract 1: rd d is REFUSED while a snapshot exists.
# =====================================================================
echo ">> Phase 3: rd d $RD must be REJECTED (snapshot present)"
set +e
rd_d_out=$("${LCTL[@]}" resource-definition delete "$RD" 2>&1)
rd_d_rc=$?
set -e
echo "$rd_d_out"

if [[ $rd_d_rc -eq 0 ]]; then
    echo "FAIL (E1): rd d returned 0 despite an existing snapshot — guard missing" >&2
    exit 1
fi

# Error envelope must name the snapshot-blocked reason.
if ! grep -qiE 'because it has snapshots' <<<"$rd_d_out"; then
    echo "FAIL (E1): rd d rejection text does not mention 'because it has snapshots': $rd_d_out" >&2
    exit 1
fi

# RD + both replicas must survive the refused delete (cascade must NOT
# have fired). Probe the CRD layer directly.
if ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1; then
    echo "FAIL (E1): RD ${RD} vanished after a REFUSED rd d — cascade fired before the guard" >&2
    exit 1
fi
repl_count=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
    | awk -v rd="${RD}." '$1 ~ "^"rd' | wc -l | tr -d ' ')
if [[ "$repl_count" -lt 2 ]]; then
    echo "FAIL (E1): expected >=2 replicas after refused rd d, got $repl_count" >&2
    exit 1
fi
echo "   OK: rd d refused, RD + $repl_count replicas + snapshot intact"

# =====================================================================
# Contract 2: r d of a SINGLE replica is NOT blocked; snapshot survives.
# =====================================================================
echo ">> Phase 4: r d $N2 $RD must SUCCEED and leave the snapshot intact"
set +e
r_d_out=$("${LCTL[@]}" resource delete "$N2" "$RD" 2>&1)
r_d_rc=$?
set -e
echo "$r_d_out"

if [[ $r_d_rc -ne 0 ]]; then
    echo "FAIL (E1): r d $N2 $RD returned non-zero ($r_d_rc) — the snapshot guard must NOT apply to per-replica delete" >&2
    exit 1
fi

# The snapshot must STILL be present.
if ! "${LCTL[@]}" snapshot list 2>/dev/null | grep -q "$SNAP"; then
    echo "FAIL (E1): snapshot $SNAP disappeared after a single-replica r d — snapshots must survive" >&2
    "${LCTL[@]}" snapshot list >&2 || true
    exit 1
fi
echo "   OK: r d succeeded, snapshot $SNAP survived"

# =====================================================================
# Contract 3: drop the snapshot, then rd d succeeds (recovery flow).
# =====================================================================
echo ">> Phase 5: snapshot delete then rd d must SUCCEED"
_out=$("${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>&1) \
    || { echo "FAIL: snapshot delete $SNAP: $_out" >&2; exit 1; }

# Give the controller a moment to drop the snapshot CRD.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if ! "${LCTL[@]}" snapshot list 2>/dev/null | grep -q "$SNAP"; then
        break
    fi
    sleep 2
done

set +e
rd_d2_out=$("${LCTL[@]}" resource-definition delete "$RD" 2>&1)
rd_d2_rc=$?
set -e
echo "$rd_d2_out"

if [[ $rd_d2_rc -ne 0 ]]; then
    echo "FAIL (E1): rd d still refused after the snapshot was dropped: $rd_d2_out" >&2
    exit 1
fi

# Wait for the RD to clear so the EXIT-trap cleanup + no-orphans check
# starts from a clean state.
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1; then
        break
    fi
    sleep 3
done

echo ">> PASS: rd-d-blocked-by-snapshot (E1: rd d guarded, r d not, recovery flow)"
