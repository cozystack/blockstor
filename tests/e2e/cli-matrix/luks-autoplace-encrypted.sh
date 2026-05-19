#!/usr/bin/env bash
#
# usage: luks-autoplace-encrypted.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333 (3-replica encrypted autoplace).
#
# Audit gap: r-c-autoplace-3r.sh (Bug 328) pinned 3-replica
# autoplace on a plain DRBD,STORAGE stack. The Bug-328 cache-trail
# / timing window can interact differently with the LUKS layer
# because the satellite has to luksFormat on first reconcile —
# extra disk work that widens the staging window the placer is
# trying to close on. This cell exercises the worst case: 3
# replicas of an encrypted RD via the CLI.
#
# Setup:
#   - cluster passphrase set
#   - linstor rd c testluksN -l drbd,luks,storage
#   - linstor vd c testluksN 64M (keep small — 3x luksFormat on a
#     QEMU stand is the slow operation)
#   - linstor r c testluksN --auto-place=3 -s <POOL>
#
# Assertions:
#   - all 3 replicas reach UpToDate
#   - all 3 backing LVs have LUKS headers + the cluster passphrase
#     opens each of them
#   - linstor r l shows 3 rows for the RD (CLI's wire-shape view)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-333-ap3
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-333-ap3-pp!'

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

cleanup_encryption_state

echo ">> [Bug 333] pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — 3-replica encrypted autoplace fixture unavailable"
    exit 0
fi

echo ">> [Bug 333] set cluster passphrase"
"${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1 || true

echo ">> [Bug 333] linstor rd c $RD -l drbd,luks,storage + vd c 64M"
"${LCTL[@]}" resource-definition create "$RD" --layer-list drbd,luks,storage >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null

echo ">> [Bug 333] linstor r c $RD --auto-place=3 -s $POOL"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create --auto-place=3 --storage-pool "$POOL" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 333): encrypted auto-place=3 exited $rc" >&2
    cat "$err_file" >&2
    if grep -q "Not enough" "$err_file"; then
        echo "  ^^^ classic Bug 328 symptom interacting with LUKS layer" >&2
    fi
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 333] wait for 3 Resource CRDs to land"
deadline=$(( $(date +%s) + 90 ))
placed=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed[@]} == 3 )); then break; fi
    sleep 3
done
if (( ${#placed[@]} != 3 )); then
    echo "FAIL: 3-replica encrypted autoplace did not stage 3 Resource CRDs (got ${#placed[@]})" >&2
    exit 1
fi
echo "   placed on: ${placed[*]}"

echo ">> [Bug 333] wait all 3 replicas UpToDate"
# 3-replica initial sync over LUKS on a QEMU stand: each replica
# does luksFormat (~5s) before drbdadm create-md, then the cluster
# does pairwise sync. 240s safety margin (same as wait_sync_done
# precedent in lib.sh).
deadline=$(( $(date +%s) + 240 ))
all_up=false
while (( $(date +%s) < deadline )); do
    up_count=0
    for n in "${placed[@]}"; do
        if [[ "$(status_disk_state "$RD" "$n" 0)" == "UpToDate" ]]; then
            up_count=$(( up_count + 1 ))
        fi
    done
    if (( up_count == 3 )); then
        all_up=true
        break
    fi
    sleep 5
done
if [[ "$all_up" != "true" ]]; then
    echo "FAIL (Bug 333): not all 3 replicas reached UpToDate within 240s" >&2
    for n in "${placed[@]}"; do
        echo "  $n: $(status_disk_state "$RD" "$n" 0)" >&2
    done
    exit 1
fi

echo ">> [Bug 333] linstor r l shows 3 rows + cryptsetup luksDump on all 3 backings"
rows=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r '[.[][]? | .name] | length' 2>/dev/null || echo 0)
if (( rows != 3 )); then
    echo "FAIL (Bug 333): linstor r l shows $rows rows for $RD, want 3" >&2
    exit 1
fi

for node in "${placed[@]}"; do
    backing=$(luks_backing_device "$RD" "$node" 0)
    if [[ -z "$backing" ]]; then
        echo "FAIL: could not resolve backing for $RD on $node" >&2
        exit 1
    fi
    echo "   $node: backing=$backing"
    if ! wait_luks_header_present "$node" "$backing" 30; then
        echo "FAIL (Bug 333): missing LUKS header on $node:$backing" >&2
        exit 1
    fi
    if ! assert_luks_passphrase_opens "$node" "$backing" "$PASSPHRASE"; then
        echo "FAIL (Bug 333): passphrase does not open $node:$backing" >&2
        exit 1
    fi
done

echo ">> luks-autoplace-encrypted OK (Bug 333: 3-replica encrypted autoplace, all headers valid)"
