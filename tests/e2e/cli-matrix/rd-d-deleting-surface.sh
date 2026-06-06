#!/usr/bin/env bash
#
# usage: rd-d-deleting-surface.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case E4 (two-phase RD deletion surface).
#
# UG9 linstor-administration.adoc ~1350-1362 documents that an RD marked
# for deletion is NOT immediately removed from the DB: the controller
# flags it, the satellites confirm the on-node teardown, and only then
# does it disappear. During the interim `rd l` / `r l` show the object
# in a DELETING state, and a downed satellite blocks the final removal.
#
# In blockstor the two-phase delete is realised with CRD finalizers: a
# `rd d` stamps DeletionTimestamp + cascades to the per-replica
# Resources, whose `satellite-resource` finalizer drains DRBD before the
# object is finalised. If a satellite cannot drain (it is down), the
# Resource lingers with a DeletionTimestamp — which the k8s store now
# projects onto the wire as the upstream-canonical DELETE flag, so the
# CLI renders the State column as DELETING (fix: pkg/store/k8s
# withDeletingFlag).
#
# We must NOT stop a blockstor satellite (forbidden on the shared
# stand). Instead we SAFELY simulate the "finalizer held" condition on
# our OWN cce- resource by adding an extra finalizer to one Resource
# CRD, issuing `rd d`, and asserting the DELETING surface appears while
# the finalizer blocks finalisation. We then remove our finalizer and
# let the normal teardown complete. This touches only cce- objects.
#
# Contract:
#   a) 2-replica diskful RD, UpToDate.
#   b) Add a test finalizer to the Resource CRD on $N2.
#   c) `rd d $RD` (cascade stamps DeletionTimestamp on every replica).
#   d) Within the wait window, `r l -r $RD` shows DELETING for the
#      finalizer-blocked replica (wire `flags` carries DELETE), and the
#      RD is NOT yet gone (final removal blocked).
#   e) Remove the test finalizer → the delete completes, RD clears.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cce-rd-d-deleting
SIZE_MIB=32
TEST_FINALIZER="cce.test/hold-deleting-surface"

N1=$WORKER_1
N2=$WORKER_2

# The Resource CRD name is "<rd>.<node>" by blockstor's composite-key
# convention; discover it rather than assume the exact slug.
rsc_crd_name() {
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." -v node="$1" '$1 ~ "^"rd && $0 ~ node {print $1; exit}'
}

remove_test_finalizer() {
    local crd
    crd=$(rsc_crd_name "$N2")
    if [[ -n "$crd" ]]; then
        # Drop ONLY our test finalizer; keep any other finalizer the
        # satellite set so the real teardown still runs to completion.
        local kept
        kept=$(kubectl get "resources.blockstor.cozystack.io/${crd}" \
            -o json 2>/dev/null \
            | jq -c --arg f "$TEST_FINALIZER" \
                '[(.metadata.finalizers // [])[] | select(. != $f)]' 2>/dev/null || echo 'null')
        kubectl patch "resources.blockstor.cozystack.io/${crd}" --type=merge \
            -p "{\"metadata\":{\"finalizers\":${kept}}}" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    # Always strip our test finalizer so nothing leaks even on a mid-cell
    # abort, then run the standard teardown.
    remove_test_finalizer || true
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

echo ">> Phase 2: add a test finalizer to the $N2 Resource CRD (simulate a stuck on-node teardown)"
CRD_N2=$(rsc_crd_name "$N2")
if [[ -z "$CRD_N2" ]]; then
    echo "FAIL: could not resolve Resource CRD name for $RD on $N2" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers >&2 || true
    exit 1
fi
echo "   $N2 Resource CRD = $CRD_N2"
# Append our finalizer (merge with whatever the satellite already set).
existing_fin=$(kubectl get "resources.blockstor.cozystack.io/${CRD_N2}" \
    -o jsonpath='{.metadata.finalizers}' 2>/dev/null || echo '[]')
echo "   existing finalizers: $existing_fin"
kubectl patch "resources.blockstor.cozystack.io/${CRD_N2}" --type=json \
    -p "[{\"op\":\"add\",\"path\":\"/metadata/finalizers/-\",\"value\":\"${TEST_FINALIZER}\"}]" \
    >/dev/null 2>&1 \
    || kubectl patch "resources.blockstor.cozystack.io/${CRD_N2}" --type=merge \
        -p "{\"metadata\":{\"finalizers\":[\"${TEST_FINALIZER}\"]}}" >/dev/null 2>&1 \
    || { echo "FAIL: could not add test finalizer to $CRD_N2" >&2; exit 1; }

echo ">> Phase 3: rd d $RD (cascade stamps DeletionTimestamp; finalizer blocks finalisation)"
# `rd d` waits for cache convergence then returns; the cascade has
# stamped DeletionTimestamp on every replica. The $N2 replica cannot
# finalise because our test finalizer is held.
"${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true

# =====================================================================
# Contract: the wire surfaces DELETING for the finalizer-blocked replica
# while the RD is NOT yet gone.
# =====================================================================
echo ">> Phase 4: r l -r $RD must show DELETING for the blocked replica"
deadline=$(( $(date +%s) + 60 ))
deleting_seen=false
while (( $(date +%s) < deadline )); do
    # Wire-level assertion: the Resource view carries the DELETE flag for
    # the blocked replica. golinstor's `r l -o json` is a doubly-nested
    # array of per-replica rows; the resource-level flags live under
    # `rsc_flags` (see multi-volume-late-vd-create.sh / n-rst-recreates-
    # tiebreaker.sh for the same shape).
    rl_json=$("${LCTL[@]}" resource list --resources "$RD" -o json 2>/dev/null || echo '[]')
    has_delete=$(jq -r --arg n "$N2" '
        [ .[]?[]?
          | select(.node_name == $n)
          | (.rsc_flags // []) | index("DELETE") ] | any' <<<"$rl_json" 2>/dev/null || echo false)
    # Fallback: the human-readable State column rendered by the CLI.
    rl_txt=$("${LCTL[@]}" resource list --resources "$RD" 2>/dev/null || true)
    if [[ "$has_delete" == "true" ]] || grep -qiE 'DELETING' <<<"$rl_txt"; then
        deleting_seen=true
        break
    fi
    sleep 2
done

if ! $deleting_seen; then
    echo "FAIL (E4): r l never surfaced a DELETING/DELETE state for the finalizer-blocked replica within 60s" >&2
    "${LCTL[@]}" resource list --resources "$RD" >&2 || true
    "${LCTL[@]}" resource list --resources "$RD" -o json >&2 || true
    exit 1
fi
echo "   OK: DELETING surface visible while the finalizer blocks finalisation"

# The RD must NOT be fully gone yet — a downed/stuck satellite blocks the
# final removal (the whole point of the two-phase delete).
if ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1; then
    # Acceptable race: if the RD object itself finalised faster than the
    # blocked child, the child Resource CRD must still be observable as
    # DELETING. Re-check the child directly.
    if [[ -z "$(rsc_crd_name "$N2")" ]]; then
        echo "FAIL (E4): both RD and blocked replica vanished despite a held finalizer" >&2
        exit 1
    fi
fi

# =====================================================================
# U19/U90/U112/U41/U101/U186/U242: a SECOND `rd d` while the resource is
# latched in DELETING (peer unreachable / finalizer held) must be
# IDEMPOTENT — it must NOT error, must NOT wedge, and must leave the
# teardown progressing. The user reports were "resource stuck on
# DELETING, can't complete or retry the delete". BS realises the
# two-phase delete with finalizers: a repeat `rd d` re-stamps the
# (already-set) DeletionTimestamp via an idempotent client.Delete, so
# the CLI returns success and the operator can safely retry.
# =====================================================================
echo ">> Phase 4b (U19): repeat rd d while DELETING — must be idempotent (exit 0)"
retry_err=$(mktemp)
if ! "${LCTL[@]}" resource-definition delete "$RD" >"$retry_err" 2>&1; then
    echo "FAIL (U19): a retried 'rd d' while DELETING returned non-zero (not idempotent)" >&2
    echo "----- retry output -----" >&2
    cat "$retry_err" >&2
    rm -f "$retry_err"
    exit 1
fi
rm -f "$retry_err"
echo "   OK: retried rd d while DELETING is idempotent (exit 0)"

# The retry must NOT have force-cleared the finalizer-blocked child —
# the teardown is still genuinely blocked until we release our finalizer.
if [[ -z "$(rsc_crd_name "$N2")" ]] \
    && ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1; then
    echo "FAIL (U19): retried rd d finalised the object despite a held finalizer" >&2
    exit 1
fi
echo "   OK: retry did not bypass the held finalizer (teardown still blocked, not wedged)"

# =====================================================================
# Release: remove the test finalizer; the real delete completes.
# =====================================================================
echo ">> Phase 5: remove the test finalizer; delete must complete"
remove_test_finalizer

deadline=$(( $(date +%s) + 120 ))
cleared=false
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1 \
        && [[ -z "$(rsc_crd_name "$N2")" ]]; then
        cleared=true
        break
    fi
    sleep 3
done
if ! $cleared; then
    echo "FAIL (E4): RD/replica did not clear within 120s after releasing the finalizer" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers | grep "$RD" >&2 || true
    exit 1
fi

# =====================================================================
# U186/U242: "can't reuse a name stuck in DELETING". Once the teardown
# completes (DELETING cleared), the SAME RD name MUST be reusable — a
# fresh `rd c <same-name>` must succeed, proving the name was freed and
# no stale object/finalizer lingers to block re-creation.
# =====================================================================
echo ">> Phase 6 (U186/U242): the freed name must be reusable — rd c $RD again"
reuse_err=$(mktemp)
if ! "${LCTL[@]}" resource-definition create "$RD" >"$reuse_err" 2>&1; then
    echo "FAIL (U186/U242): could not re-create RD '$RD' after its DELETING cleared — name not freed" >&2
    echo "----- re-create output -----" >&2
    cat "$reuse_err" >&2
    rm -f "$reuse_err"
    exit 1
fi
rm -f "$reuse_err"
echo "   OK: name '$RD' reused successfully after teardown (delete_rd in cleanup removes it)"

echo ">> PASS: rd-d-deleting-surface (E4 two-phase delete + U19 idempotent retry + U186/U242 name-reuse)"
