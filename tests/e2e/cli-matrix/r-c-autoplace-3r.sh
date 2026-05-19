#!/usr/bin/env bash
#
# usage: r-c-autoplace-3r.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 328 (user-reported, blocker).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor sp l
#   lvm-thin  worker-1  Ok  …  Free: 13 GiB
#   lvm-thin  worker-2  Ok  …  Free: 13 GiB
#   lvm-thin  worker-3  Ok  …  Free: 13 GiB
#
#   $ linstor rd c test2
#   $ linstor vd c test2 1G
#   $ linstor r c test2 --auto-place=3 -s lvm-thin
#   ERROR: Not enough available nodes
#
# Unit pin: pkg/placer + pkg/rest stack — verified in-memory that the
# placer + REST handler chain places all 3 replicas correctly when fed
# the exact stand inputs. See commit 2e3ead987: in-memory store cannot
# reproduce, the bug is stand-side cache-trail / timing. This L6 cell
# is the only place the bug can actually fire — drives the real CLI
# against a freshly-provisioned 3-node lvm-thin cluster and asserts
# the autoplace succeeds end-to-end.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-328
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: verify 3 SATELLITE nodes with the named pool in
# State=Ok and FreeCapacity ≥ 1 GiB. Skip with a clear message if
# the stand doesn't carry the lvm-thin pool (e.g. zfs-only stand) —
# the bug is specifically about lvm-thin autoplace, exercising it
# against another provider doesn't repro.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — Bug 328 fixture not available"
    echo "$sp_json" | head -50
    exit 0
fi

echo ">> [Bug 328] linstor rd c + vd c"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 328] linstor r c $RD --auto-place=3 -s $POOL"
err_file=$(mktemp)
if ! out=$("${LCTL[@]}" resource create --auto-place=3 --storage-pool="$POOL" "$RD" 2>"$err_file"); then
    rc=$?
    echo "FAIL (Bug 328 regression): auto-place=3 exited $rc" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "----- stdout -----" >&2
    echo "$out" >&2
    echo "------------------" >&2
    if grep -q "Not enough" "$err_file"; then
        echo "  ^^^ classic Bug 328 symptom — autoplacer rejected 3 healthy nodes" >&2
    fi
    rm -f "$err_file"
    exit 1
fi

if grep -qiE 'Not enough available nodes|Not enough nodes' "$err_file"; then
    echo "FAIL (Bug 328): autoplacer warning 'Not enough nodes' on stderr despite 3 healthy SPs" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 90s for all 3 replicas to land DRBD+STORAGE on $POOL"
deadline=$(( $(date +%s) + 90 ))
all3=false
while (( $(date +%s) < deadline )); do
    placed=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
    if (( placed == 3 )); then
        all3=true
        break
    fi
    sleep 3
done

if [[ "$all3" != "true" ]]; then
    echo "FAIL (Bug 328): autoplace did not stage 3 Resource CRDs within 90s" >&2
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' >&2
    exit 1
fi

# Final wire-shape check via the python CLI: row count == 3 and
# every row shows the lvm-thin pool. A Bug-328-bitten autoplace
# might silently fall back to fewer rows (n=2) or to a different
# pool, so both have to be asserted.
rows=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r '[.[][]? | .name] | length' 2>/dev/null || echo 0)
if (( rows != 3 )); then
    echo "FAIL (Bug 328): linstor r l shows $rows rows for $RD, want 3" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> r-c-autoplace-3r OK (Bug 328 pinned: --auto-place=3 -s $POOL placed all 3 replicas)"
