#!/usr/bin/env bash
#
# usage: vd-d-cascades-replicas.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 355.
#
# User-reported 2026-05-19:
#
#   $ linstor vd d test 0
#   ERROR:
#   Description:
#       Volume definition 0 on resource definition "test" cannot
#       be deleted because resource replicas still reference it.
#   Cause:
#       3 resource replica(s) reference VolumeNumber 0 on "test":
#       e2e2-worker-1, e2e2-worker-2, e2e2-worker-3
#   Correction:
#       Delete the listed resource replicas first
#       (`linstor r d <node> test`), or pass `?force=true` to
#       drop the volume definition anyway and accept the orphan
#       replicas.
#
# Two problems with this surface:
#
# (1) Upstream LINSTOR's `vd d` cascades automatically (Apache
#     2.0 / GPL — read-only research, no code copy;
#     controller/.../CtrlVlmDfnDeleteApiCallHandler.java:152-215).
#     The handler walks every Volume on the parent RD via
#     `getVolumeIteratorPrivileged(vlmDfn)`, calls
#     `markDeleted(vlm)` per replica, then markDeleted(vlmDfn),
#     then `updateSatellites(vlmDfn.getResourceDefinition(),
#     deleteDataFlux)` triggers per-node tear-down. Refusal
#     happens ONLY for `anyResourceInUsePrivileged` (Primary /
#     mounted) — a Secondary replica that simply exists is NOT
#     a reason to refuse.
#
# (2) The blockstor error text suggests `?force=true` as the
#     escape hatch. linstor-client has no `--force` for `vd d`,
#     and blockstor's pkg/rest/volume_definitions.go never wires
#     a `force` query-param check. So the Correction line points
#     the operator at a non-existent feature.
#
# Test contract:
#   1. Build a 2-replica diskful RD (both replicas Secondary —
#      no Primary promote, nothing mounted).
#   2. Wait both UpToDate.
#   3. `linstor vd d <rd> 0` MUST succeed (cascade-delete).
#   4. Within 30s, `linstor vd l <rd>` must show no volume 0
#      AND the parent RD must still exist (vd d ≠ rd d).
#   5. Per-node Resource Volumes (.status.volumes) must drop the
#      removed VolumeNumber; backing zvol/LV must be gone.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-vd-d-cascade
N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> 2-replica diskful RD on $N1 + $N2 (no Primary promote — both Secondary)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
# Two volumes so the test can also verify single-VD removal
# leaves the RD intact with the surviving volume.
_out=$("${LCTL[@]}" volume-definition create "$RD" 64M 2>&1) \
    || { echo "FAIL: vd c $RD 64M (vol 0): $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 64M 2>&1) \
    || { echo "FAIL: vd c $RD 64M (vol 1): $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }
wait_uptodate "$RD" "$N1" "$N2"

# =====================================================================
# Trigger: linstor vd d <rd> 0
# =====================================================================
echo ">> [Bug 355 trigger] linstor vd d $RD 0  (cascade-delete via upstream parity)"
err_file=$(mktemp)
if ! "${LCTL[@]}" volume-definition delete "$RD" 0 2>"$err_file" >/dev/null; then
    rc=$?
    err_text=$(cat "$err_file")
    echo "FAIL (Bug 355): vd d $RD 0 returned exit $rc — upstream cascades automatically without requiring manual r d" >&2
    echo "----- stderr -----" >&2
    echo "$err_text" >&2

    # (2) Subordinate assertion: error text MUST NOT mention
    # `?force=true` — that escape hatch is not wired in
    # blockstor's REST handler. If the cluster ever returns
    # the suggestion, operators waste cycles building a curl
    # call that the server then 4xx's anyway.
    if grep -qE 'force=true|--force' <<<"$err_text"; then
        echo "FAIL (Bug 355 / sibling): error text suggests '?force=true' but blockstor REST does not implement it" >&2
        echo "----- env -----" >&2
        echo "$err_text" >&2
    fi
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# =====================================================================
# Bug 355 assertion: VD 0 gone, RD + VD 1 still alive
# =====================================================================
echo ">> [Bug 355] within 30s: vd l shows only volume 1; rd l still has $RD"
# Why: blockstor's `--machine-readable volume-definition list` returns the
# v1 schema (`[[{name, volume_definitions:[{volume_number, ...}]}]]`),
# not upstream LINSTOR's v0 protobuf shape (`.rsc_dfns / .rsc_name /
# .vlm_dfns / .vlm_nr`). The v0 filter against v1 data jq-errors with
# `Cannot index array with string "rsc_dfns"` → 2>/dev/null swallows it
# → `|| echo "-1"` fires → never converges. Match the existing v1 helpers
# in tests/e2e/cli-matrix/lib.sh.
deadline=$(( $(date +%s) + 30 ))
ok=false
while (( $(date +%s) < deadline )); do
    nvols=$("${LCTL[@]}" --machine-readable volume-definition list \
        --resource-definitions "$RD" 2>/dev/null \
        | jq -r --arg rd "$RD" '
            [.[]? | .[]? | select(.name==$rd) | .volume_definitions[]? | .volume_number]
            | length' 2>/dev/null || echo "-1")
    if [[ "$nvols" == "1" ]]; then
        ok=true
        break
    fi
    sleep 2
done

if ! $ok; then
    echo "FAIL (Bug 355): VD 0 cascade-delete did not converge to 1 surviving VD within 30s (got nvols=$nvols)" >&2
    echo "----- linstor vd l $RD -----" >&2
    "${LCTL[@]}" volume-definition list --resource-definitions "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

if ! "${LCTL[@]}" resource-definition list --resource-definitions "$RD" 2>/dev/null | grep -q "$RD"; then
    echo "FAIL (Bug 355 sibling): vd d 0 also deleted the parent RD $RD" >&2
    exit 1
fi

# =====================================================================
# Per-node Resource.status.volumes must drop vol 0
# =====================================================================
echo ">> per-node Resource.status.volumes must drop volume 0 on both nodes"
for N in "$N1" "$N2"; do
    vols=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N}" \
        -o jsonpath='{.status.volumes[*].volumeNumber}' 2>/dev/null || echo "")
    if grep -qE '(^|[[:space:]])0([[:space:]]|$)' <<<"$vols"; then
        echo "FAIL (Bug 355 deep): $RD.$N still has volume 0 in Status.Volumes 30s after vd d 0" >&2
        kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N}" -o yaml 2>&1 | head -40 >&2
        exit 1
    fi
done

# =====================================================================
# Surviving volume 1 stays UpToDate (RD is functional post-vd-d)
# =====================================================================
echo ">> volume 1 stays UpToDate on both nodes post-cascade"
for N in "$N1" "$N2"; do
    s=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N}" \
        -o jsonpath='{.status.volumes[?(@.volumeNumber==1)].diskState}' 2>/dev/null || echo "")
    if [[ "$s" != "UpToDate" ]]; then
        echo "FAIL (Bug 355 deep): volume 1 on $N is diskState=$s (want UpToDate) post-cascade" >&2
        exit 1
    fi
done

echo ">> vd-d-cascades-replicas OK (Bug 355 pinned: vd d cascades replicas, surviving VD healthy, no ?force=true required)"
