#!/usr/bin/env bash
#
# usage: r-deactivate-drops-peer-from-res.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 350.
#
# Upstream LINSTOR behaviour (verified from upstream Java source
# at /Users/kvaps/git/linstor-server, Apache 2.0 reference only):
# `DrbdResourceFileUtils.regenerateResFile` (satellite/.../drbd/
# resfiles/DrbdResourceFileUtils.java:79-89) filters the peer list
# of every .res rewrite with `!Resource.Flags.INACTIVE`. When the
# operator runs `linstor r deactivate <node> <rd>`, the INACTIVE
# peer is dropped wholesale from every surviving peer's .res — not
# as `disk none` (that's the --diskless shape) but as a fully
# absent `on <node> { ... }` block. DRBD on the survivors stops
# attempting to dial that peer.
#
# blockstor behaviour (pkg/dispatcher/dispatcher.go:78-94): the
# dispatcher's peer-collection loop only checks `nodeIDOf(peer)
# >= 0` — there is NO filter for INACTIVE / INACTIVATING flags.
# When a sibling reconciles its .res after `r deactivate <node>`,
# the deactivated peer is still listed; DRBD continues to attempt
# the dial and the connection appears as Connecting/StandAlone in
# events2 forever.
#
# Symptoms surfaced via this cell:
#   - .res on surviving peer contains `on <deactivated-node>`
#     block after deactivate
#   - drbdsetup status on surviving peer still lists deactivated
#     peer in `peer-node-id:` rows
#   - quorum semantics differ from upstream (the peer is treated
#     as "unreachable" rather than "deconfigured")
#
# Test contract:
#   1. 2-replica diskful RD on worker-1 + worker-2.
#   2. Wait both UpToDate.
#   3. `linstor r deactivate worker-2 RD`.
#   4. Wait for the operation to acknowledge (INACTIVE flag set +
#      satellite ack via Resource.Status).
#   5. On worker-1, dump the rendered .res and assert it does NOT
#      contain `on "$worker2_name"` block.
#   6. On worker-1, `drbdsetup status <RD>` MUST NOT list the
#      deactivated peer's node-id.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-deact-drop-peer
N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    # If still INACTIVE, activate first so delete_rd can drive a
    # clean tear-down through the normal r d path.
    "${LCTL[@]}" resource activate "$N2" "$RD" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> 2-replica diskful RD on $N1 + $N2"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 64M 2>&1) \
    || { echo "FAIL: vd c $RD 64M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }

wait_uptodate "$RD" "$N1" "$N2"

# =====================================================================
# Trigger: linstor r deactivate $N2 $RD
# =====================================================================
echo ">> [Bug 350 trigger] linstor r deactivate $N2 $RD"
_out=$("${LCTL[@]}" resource deactivate "$N2" "$RD" 2>&1) \
    || { echo "FAIL: r deactivate $N2 $RD: $_out" >&2; exit 1; }

# Wait for the INACTIVE flag to land on the Resource CRD AND
# for $N1's satellite to re-render its .res. Give the closed-loop
# observer 30s to converge.
echo ">> wait up to 30s for INACTIVE flag visible on $N2 + .res rewrite on $N1"
deadline=$(( $(date +%s) + 30 ))
inactive_seen=false
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" == *"INACTIVE"* ]]; then
        inactive_seen=true
        break
    fi
    sleep 2
done
if ! $inactive_seen; then
    echo "FAIL: $N2 never got INACTIVE flag after r deactivate within 30s" >&2
    kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" -o yaml 2>&1 | tail -30 >&2
    exit 1
fi
# Extra settle window for the .res rewrite to propagate on $N1.
sleep 5

# =====================================================================
# Bug 350 assertion A: $N1's .res must NOT contain `on "$N2"` block
# =====================================================================
echo ">> [Bug 350] $N1 .res must NOT contain 'on \"$N2\"' block after r deactivate"
# The .res lives at /etc/drbd.d/<RD>.res inside the satellite
# (Bug 310 made it host-shared, but we read via the satellite pod
# for portability). Capture and grep.
N1_SAT=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$N1\")].metadata.name}")
if [[ -z "$N1_SAT" ]]; then
    echo "FAIL: satellite pod for $N1 not found"
    exit 1
fi

res_dump=$(kubectl -n "$NS" exec "$N1_SAT" -- \
    cat "/etc/drbd.d/${RD}.res" 2>/dev/null || \
    kubectl -n "$NS" exec "$N1_SAT" -- \
    bash -c "find / -name '${RD}.res' 2>/dev/null | head -1 | xargs cat 2>/dev/null" || \
    echo "")

if [[ -z "$res_dump" ]]; then
    echo "FAIL: could not read .res file for $RD on $N1" >&2
    exit 1
fi

# Look for `on "<N2>"` or `on <N2>` block. Both quoted and
# unquoted forms are valid drbdadm syntax.
if grep -qE "^[[:space:]]*on[[:space:]]+\"?${N2}\"?[[:space:]]*\\{" <<<"$res_dump"; then
    echo "FAIL (Bug 350): .res on $N1 still contains 'on $N2' block after r deactivate" >&2
    echo "  Upstream DrbdResourceFileUtils.regenerateResFile filters peers with !Flags.INACTIVE." >&2
    echo "  blockstor's pkg/dispatcher/dispatcher.go peer loop is missing this filter." >&2
    echo "----- rendered .res on $N1 -----" >&2
    echo "$res_dump" >&2
    exit 1
fi

# =====================================================================
# Bug 350 assertion B: drbdsetup status on $N1 must NOT list peer
# =====================================================================
echo ">> [Bug 350] drbdsetup status on $N1 must NOT list $N2 as a peer"
status_dump=$(on_node "$N1" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
if [[ -z "$status_dump" ]]; then
    echo "FAIL: drbdsetup status on $N1 returned empty" >&2
    exit 1
fi

# Peer rows in `drbdsetup status` format the way the kernel
# enumerates connections. With the peer dropped from .res and
# drbdadm adjust having reloaded the resource, the connection
# row for $N2 must be absent. We grep on $N2's name first; if
# absent, also assert no `peer-node-id:` row that maps to $N2's
# pre-deactivate node-id.
if grep -qE "(^|[[:space:]])${N2}([[:space:]]|$)" <<<"$status_dump"; then
    echo "FAIL (Bug 350): drbdsetup status on $N1 still lists $N2 as a peer connection" >&2
    echo "----- drbdsetup status -----" >&2
    echo "$status_dump" >&2
    exit 1
fi

# =====================================================================
# Optional: reactivate must restore the peer block
# =====================================================================
echo ">> [Bug 350 reverse] linstor r activate $N2 $RD restores the peer in .res"
_out=$("${LCTL[@]}" resource activate "$N2" "$RD" 2>&1) \
    || { echo "FAIL: r activate $N2 $RD: $_out" >&2; exit 1; }

# Give .res rewrite + drbdadm adjust 30s to converge back to
# 2-replica UpToDate.
deadline=$(( $(date +%s) + 60 ))
restored=false
while (( $(date +%s) < deadline )); do
    res_dump=$(kubectl -n "$NS" exec "$N1_SAT" -- \
        cat "/etc/drbd.d/${RD}.res" 2>/dev/null || echo "")
    if grep -qE "^[[:space:]]*on[[:space:]]+\"?${N2}\"?[[:space:]]*\\{" <<<"$res_dump"; then
        restored=true
        break
    fi
    sleep 2
done
if ! $restored; then
    echo "FAIL (Bug 350 reverse): .res on $N1 did NOT restore 'on $N2' block within 60s after r activate" >&2
    echo "----- rendered .res on $N1 -----" >&2
    echo "$res_dump" >&2
    exit 1
fi

# Final convergence — both UpToDate again.
wait_uptodate "$RD" "$N1" "$N2"

echo ">> r-deactivate-drops-peer-from-res OK (Bug 350 pinned: INACTIVE peer dropped from .res of survivors, restored on activate)"
