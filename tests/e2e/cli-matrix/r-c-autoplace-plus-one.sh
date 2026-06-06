#!/usr/bin/env bash
#
# usage: r-c-autoplace-plus-one.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case campaign D2
# (UG9 linstor-administration.adoc ~1246-1278).
#
# Upstream LINSTOR contract: `r c --auto-place +1 <rd>` adds EXACTLY ONE
# replica to the resource's current diskful count, ignoring the parent
# RG's place-count. The placement constraints (storage-pool, provider,
# topology) still apply to the new replica.
#
# Reproduction (3-node cluster):
#
#   $ linstor resource-group create rg2 --place-count 2 -s stand
#   $ linstor resource-group spawn-resources rg2 ap 32M     # 2 diskful
#   $ linstor resource create --auto-place +1 ap            # -> 3 diskful
#
# This cell pins the extension half: a 2-replica RD grows to exactly 3
# diskful replicas (existing-diskful + 1), all UpToDate. A regression
# that re-applied the RG place-count (2) instead of (existing + 1), or
# that miscounted a tiebreaker/diskless peer toward the existing total,
# would land the wrong number of replicas.
#
# Unit pin: pkg/rest/autoplace_test.go
#   (TestAutoplaceAdditionalPlusOneAddsOneReplica + siblings).
# L7 replay: tests/operator-harness/replay/r-c-autoplace-plus-one.yaml.
#
# ORACLE NOTE (upstream LINSTOR 1.33.2, corner-campaign group D): the
# plan's companion claim that the `+N` shorthand is REJECTED for
# `rg create --place-count` was DISPROVEN against the upstream oracle —
# `rg create <rg> --place-count +1` is ACCEPTED (RG persists, exit 0),
# the `+N` delta only carries semantics on the `r c --auto-place` path.
# This cell therefore pins only the extension half (the real contract);
# there is no rejection-half assertion because upstream has none.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RG=cli-matrix-d2-rg
RD=cli-matrix-d2-ap
POOL=${POOL:-stand}

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [D2] rg create --place-count 2 + spawn -> 2 diskful replicas"
"${LCTL[@]}" resource-group create "$RG" --place-count 2 --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M >/dev/null

# Bug 391 cross-check: `32M` must spawn a 32768 KiB VD, not a 32 KiB
# one (a 32 KiB VD is below DRBD's create-md floor and the replicas
# below would never converge). Guards this workflow against a silent
# spawn-size regression that the replica-count assertion alone would
# mask.
if ! wait_vd_size "$RD" 0 32768 60; then
    got=$(linstor_vd_size_kib "$RD" 0)
    echo "FAIL (D2/Bug391): VD size_kib=${got}, want 32768 (32M must not be divided to 32 KiB)" >&2
    "${LCTL[@]}" volume-definition list --resource-definitions "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> spawned VD = 32768 KiB (32M) — OK"

# Wait for 2 diskful replicas UpToDate.
wait_replica_count "$RD" 2 90 || {
    echo "FAIL (D2 setup): RD did not reach 2 replicas" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
}
diskful2=$(linstor_diskful_count "$RD")
if [[ "$diskful2" != "2" ]]; then
    echo "FAIL (D2 setup): expected 2 diskful replicas, got $diskful2" >&2
    exit 1
fi
echo ">> baseline: 2 diskful replicas — OK"

echo ">> [D2] r c --auto-place +1 $RD MUST add exactly one diskful replica (-> 3)"
if ! "${LCTL[@]}" resource create --auto-place +1 "$RD"; then
    echo "FAIL (D2): --auto-place +1 exited non-zero" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Expect exactly 3 diskful replicas now (2 existing + 1).
deadline=$(( $(date +%s) + 90 ))
diskful=0
while (( $(date +%s) < deadline )); do
    diskful=$(linstor_diskful_count "$RD")
    if (( diskful >= 3 )); then
        break
    fi
    sleep 3
done

if [[ "$diskful" != "3" ]]; then
    echo "FAIL (D2): after --auto-place +1 expected 3 diskful replicas, got $diskful" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> exactly 3 diskful replicas after +1 — OK"

echo ">> wait all 3 diskful replicas UpToDate"
deadline=$(( $(date +%s) + 240 ))
all_up=false
while (( $(date +%s) < deadline )); do
    # Count diskful replicas whose disk_state is bare "UpToDate".
    up=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .volumes[]? | select(.provider_kind != "DISKLESS") | .state.disk_state]
                 | map(select(. == "UpToDate")) | length' 2>/dev/null || echo 0)
    if (( up >= 3 )); then
        all_up=true
        break
    fi
    sleep 3
done

if [[ "$all_up" != "true" ]]; then
    echo "FAIL (D2): not all 3 diskful replicas reached UpToDate after +1" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> r-c-autoplace-plus-one OK (D2 pinned: +1 grew 2 diskful to exactly 3, all UpToDate)"
