#!/usr/bin/env bash
#
# usage: r-c-on-shape-2r-tb.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 327 (user-reported 5x).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker
#
#   $ linstor r d worker-2 test         # delete
#   $ linstor r c worker-2 test         # re-create — NO --diskless
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  Diskless         ← WRONG: should be UpToDate
#   test  worker-3  …  TieBreaker
#
# Root cause: bare `r c <node> <rd>` carries no flags AND no
# StorPoolName. Pre-fix the REST handler persisted the wire body
# verbatim — the new replica then had no pool and the satellite
# brought it up Diskless on the DRBD layer. Upstream LINSTOR's
# CtrlRscCrtApiHelper resolves the pool from parent-RG's
# SelectFilter.StoragePool OR from a sibling diskful replica
# before staging. The fix mirrors that.
#
# Unit pin: pkg/rest/r_create_bug_327_test.go.
# This L6 cell is the stand-side companion: drives the real
# python-linstor CLI sequence on the 2r-tb shape and asserts the
# re-created replica lands UpToDate with DRBD+STORAGE layers,
# NOT Diskless.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-327
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 327] shape-2r-tb: 2-replica RD on $N1+$N2 + auto-tiebreaker on $N3"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool stand "$RD" >/dev/null

echo ">> wait both diskful replicas UpToDate (auto-tiebreaker on the 3rd node)"
deadline=$(( $(date +%s) + 180 ))
uptodate_pair=""
while (( $(date +%s) < deadline )); do
    pair=()
    for n in "$N1" "$N2" "$N3"; do
        d=$(status_disk_state "$RD" "$n" 0)
        if [[ "$d" == "UpToDate" ]]; then
            pair+=("$n")
        fi
    done
    if (( ${#pair[@]} >= 2 )); then
        uptodate_pair="${pair[0]} ${pair[1]}"
        break
    fi
    sleep 3
done
if [[ -z "$uptodate_pair" ]]; then
    echo "FAIL: 2-replica RD never reached UpToDate on any pair within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   diskful pair: $uptodate_pair"

# Pick a diskful node to delete + re-create. Prefer $N2 so the
# shape mirrors the user's reproduction case (`worker-2`).
RECREATE=""
for n in $uptodate_pair; do
    if [[ "$n" == "$N2" ]]; then
        RECREATE=$n
        break
    fi
done
RECREATE=${RECREATE:-$(echo "$uptodate_pair" | awk '{print $1}')}
echo "   will delete + re-create replica on: $RECREATE"

echo ">> linstor r d $RECREATE $RD"
"${LCTL[@]}" resource delete "$RECREATE" "$RD" >/dev/null

echo ">> wait for $RECREATE Resource CRD to be gone"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${RECREATE}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo ">> linstor r c $RECREATE $RD  (no --diskless, no --storage-pool — bare form, Bug 327 trigger)"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create "$RECREATE" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL: bare r c exited $rc — Bug 327 regression on the REST handler" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 120s for $RECREATE to land DRBD+STORAGE layered and UpToDate (NOT Diskless)"
deadline=$(( $(date +%s) + 120 ))
ok=false
last_flags=""
last_disk=""
while (( $(date +%s) < deadline )); do
    last_flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${RECREATE}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    last_disk=$(status_disk_state "$RD" "$RECREATE" 0)

    # The Bug 327 contract: the re-created replica must NOT carry the
    # DISKLESS flag, and Status.DiskState must converge to UpToDate.
    if [[ "$last_flags" != *"DISKLESS"* ]] && [[ "$last_disk" == "UpToDate" ]]; then
        ok=true
        break
    fi
    sleep 3
done

if [[ "$ok" != "true" ]]; then
    echo "FAIL (Bug 327 regression): ${RD}.${RECREATE} did not land diskful UpToDate" >&2
    echo "  last flags: $last_flags" >&2
    echo "  last disk_state: $last_disk" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Belt-and-suspenders: pull `linstor r l` JSON and verify the
# Layers column contains both DRBD and STORAGE (the python CLI reads
# them off `layer_object` walking the tree). A diskless replica
# carries only DRBD; a Bug-327-bitten replica would lack STORAGE.
# JSON shape from --machine-readable is `[[{rsc}, {rsc}, ...]]` —
# use `.[][]` to flatten before filtering.
recreate_layers=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r --arg rd "$RD" --arg n "$RECREATE" '
        .[][] | select(.name==$rd and .node_name==$n)
        | [.. | objects | .type? // empty] | unique | join(",")
    ' 2>/dev/null || echo "")

if [[ -n "$recreate_layers" ]] && [[ "$recreate_layers" != *"DRBD"* || "$recreate_layers" != *"STORAGE"* ]]; then
    echo "FAIL (Bug 327): ${RD}.${RECREATE} layers='$recreate_layers' — expected both DRBD and STORAGE" >&2
    exit 1
fi

echo ">> r-c-on-shape-2r-tb OK (Bug 327 pinned: re-created replica is diskful UpToDate, not Diskless)"
