#!/usr/bin/env bash
#
# usage: r-inactive-refills-active-redundancy.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 393 (P1, placer INACTIVE miscount).
#
# Symptom: the autoplacer counts an INACTIVE diskful replica toward
# place_count. An INACTIVE replica is `drbdadm down` (operator
# deactivation, Bug 350) — it serves no I/O and casts no quorum vote,
# and is dropped from every sibling's .res file. So on an RD sitting at
# 2 ACTIVE diskful + 1 INACTIVE diskful with place_count=3 the placer's
# `placed >= want` test was satisfied and it did NOT gap-fill a
# replacement active diskful — the RD silently ran with only 2 active
# replicas instead of the 3 the operator requested.
#
# Root cause: pkg/placer/placer.go `countDiskfulReplicas` excluded
# DISKLESS and disabled-node (EVICTED/LOST) replicas, but NOT INACTIVE.
# Fix: also skip INACTIVE so it never satisfies place_count — the same
# INACTIVE-miscount class as Bugs 387 / 390.
#
# Unit pin: pkg/placer/placer_test.go
# (TestPlacerDeficitExcludesInactiveDiskful).
#
# This L6 cell is the stand-side companion: drives the real
# python-linstor CLI on a 3-diskful → deactivate-one → re-autoplace=3
# shape and asserts the placer refills a replacement ACTIVE diskful on
# a 4th node, so the count of ACTIVE diskful replicas reaches 3 again
# (the INACTIVE replica stays put and does not count).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

# Need a 4th healthy worker as the gap-fill target: the 3 original
# diskful nodes are all "taken" (incl. the INACTIVE one), so the
# replacement active diskful must land on a node outside that set.
require_workers 4

linstor_cli_setup

RD=cli-matrix-393
SP=${SP:-stand}
# WORKER_4 is not exported by the parent lib.sh (it only ships
# WORKER_1..3); derive it from the same alphabetically-sorted worker
# list so the gap-fill target is deterministic.
mapfile -t _WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort
)
N4="${_WORKERS[3]:-}"
if [[ -z "$N4" ]]; then
    skip "scenario needs a 4th worker for the gap-fill target"
fi

# linstor_active_diskful_count <rd> — number of replicas that are
# diskful (no DISKLESS / TIE_BREAKER) AND active (no INACTIVE). This is
# exactly what place_count must count after Bug 393.
linstor_active_diskful_count() {
    local rd=$1
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local flags
            flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
                -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
            if [[ "$flags" != *"DISKLESS"* ]] \
                && [[ "$flags" != *"TIE_BREAKER"* ]] \
                && [[ "$flags" != *"INACTIVE"* ]]; then
                echo "${name#${rd}.}"
            fi
        done \
        | awk 'NF{c++} END{print c+0}'
}

cleanup() {
    # Reactivate the deactivated replica first so delete_rd can drive a
    # clean cascade tear-down. DEACT is only set once we've picked the
    # node, so guard with the default-empty form.
    if [[ -n "${DEACT:-}" ]]; then
        "${LCTL[@]}" resource activate "$DEACT" "$RD" 2>/dev/null || true
    fi
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 393] rd c + vd c + r c --auto-place=3 -s $SP"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=3 --storage-pool="$SP" "$RD" >/dev/null

echo ">> wait up to 180s for 3 active diskful UpToDate"
deadline=$(( $(date +%s) + 180 ))
ready=false
while (( $(date +%s) < deadline )); do
    if [[ "$(linstor_active_diskful_count "$RD")" == "3" ]] \
        && [[ "$(linstor_replica_count "$RD")" == "3" ]]; then
        ready=true
        break
    fi
    sleep 3
done
if [[ "$ready" != "true" ]]; then
    echo "FAIL: never reached 3 active diskful within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Capture the 3 diskful node names so we can deactivate one and verify
# the replacement lands OUTSIDE this set.
mapfile -t DISKFUL_NODES < <(linstor_diskful_nodes "$RD")
DEACT="${DISKFUL_NODES[0]}"
echo "   3 active diskful on: ${DISKFUL_NODES[*]}; deactivating $DEACT"

# Deactivate one diskful replica → INACTIVE flag. It is still a diskful
# replica (backing disk present) but `drbdadm down`: non-voting,
# non-serving. The placer must treat the RD as 1-short on active
# redundancy.
echo ">> linstor r deactivate $DEACT $RD  (→ INACTIVE)"
"${LCTL[@]}" resource deactivate "$DEACT" "$RD" >/dev/null 2>&1 || {
    echo "FAIL: r deactivate $DEACT $RD exited non-zero" >&2
    exit 1
}

echo ">> wait up to 30s for INACTIVE flag visible on $DEACT"
deadline=$(( $(date +%s) + 30 ))
inactive=false
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${DEACT}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" == *"INACTIVE"* ]]; then
        inactive=true
        break
    fi
    sleep 2
done
if [[ "$inactive" != "true" ]]; then
    echo "FAIL: INACTIVE flag never landed on $DEACT within 30s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   $DEACT is INACTIVE; active diskful is now 2 (want 3)"

# Re-trigger autoplace=3. Pre-fix the placer counts the INACTIVE
# replica as the 3rd and is satisfied → no replacement. Post-fix the
# INACTIVE replica does NOT count, so the placer gap-fills a fresh
# active diskful on the 4th node.
echo ">> linstor r c $RD --auto-place=3 -s $SP  (rebalance / gap-fill)"
"${LCTL[@]}" resource create --auto-place=3 --storage-pool="$SP" "$RD" >/dev/null 2>&1 || {
    echo "FAIL: re-autoplace=3 exited non-zero" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
}

echo ">> wait up to 180s for ACTIVE diskful count to reach 3 again"
deadline=$(( $(date +%s) + 180 ))
refilled=false
while (( $(date +%s) < deadline )); do
    if [[ "$(linstor_active_diskful_count "$RD")" == "3" ]]; then
        refilled=true
        break
    fi
    sleep 3
done
if [[ "$refilled" != "true" ]]; then
    n_active=$(linstor_active_diskful_count "$RD")
    echo "FAIL (Bug 393 regression): active diskful = $n_active, want 3" >&2
    echo "  an INACTIVE replica must NOT satisfy place_count — the placer" >&2
    echo "  must gap-fill a replacement active diskful." >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# The deactivated node must STILL be INACTIVE — the gap-fill must not
# have reactivated it; it must have placed a NEW replica elsewhere.
deact_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${DEACT}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
if [[ "$deact_flags" != *"INACTIVE"* ]]; then
    echo "FAIL (Bug 393): $DEACT lost its INACTIVE flag (flags=$deact_flags)" >&2
    echo "  the placer must refill on a fresh node, not reactivate the down one" >&2
    exit 1
fi

# The replacement active diskful must be on a node OUTSIDE the original
# 3-diskful set — i.e. the 4th worker. Confirm $N4 now carries an
# active diskful replica.
n4_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N4}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "__missing__")
if [[ "$n4_flags" == "__missing__" ]]; then
    echo "FAIL (Bug 393): no replacement replica on the 4th worker $N4" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
if [[ "$n4_flags" == *"INACTIVE"* ]] \
    || [[ "$n4_flags" == *"DISKLESS"* ]] \
    || [[ "$n4_flags" == *"TIE_BREAKER"* ]]; then
    echo "FAIL (Bug 393): replacement on $N4 is not an active diskful (flags=$n4_flags)" >&2
    exit 1
fi

echo ">> r-inactive-refills-active-redundancy OK (Bug 393 pinned: INACTIVE diskful does not satisfy place_count; placer refilled active redundancy on $N4)"
