#!/usr/bin/env bash
#
# usage: luks-rd-create-encrypted.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333.
#
# Audit gap: existing tests/e2e/luks-layer.sh + drbd-luks-stack.sh
# drive the LUKS layer via kubectl-apply on the Resource* CRDs. The
# python-CLI path
#   linstor rd c <rd> -l drbd,luks,storage
#   linstor vd c <rd> 1G
#   linstor r c <rd> --auto-place=2 -s <pool>
# never ran on the stand. That path goes through
# pkg/rest/rd_layer_stack.go (layerStack list assembly from the `-l`
# flag) which has no other e2e coverage. Bug 175 (LUKS shell-
# injection) was unit-test-only.
#
# Setup:
#   - cluster passphrase set via real CLI
#   - linstor rd c testluks -l drbd,luks,storage
#   - linstor vd c testluks 128M (smaller than 1G to keep CI fast,
#     still big enough for a meaningful luksDump)
#   - linstor r c testluks --auto-place=2 -s <POOL>
#
# Assertions:
#   - both replicas reach UpToDate via observer-stamped Status
#   - drbdsetup status on at least one replica shows the LUKS layer
#     in the connection's `disk` line (= /dev/mapper/<rd>-0-luks)
#   - cryptsetup luksDump on the backing LV of EACH replica returns
#     a valid header (Bug 333 contract: every replica is individually
#     encrypted, header survives independent of DRBD ship)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-333-rd
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-333-rd-pp!'

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

cleanup_encryption_state

echo ">> [Bug 333] pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes) — encrypted-RD autoplace fixture unavailable"
    exit 0
fi

echo ">> [Bug 333] set cluster passphrase via real CLI"
if ! "${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1; then
    echo "FAIL: pre-flight create-passphrase failed" >&2
    exit 1
fi

echo ">> [Bug 333] linstor rd c $RD -l drbd,luks,storage"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource-definition create "$RD" \
        --layer-list drbd,luks,storage 2>"$err_file"; then
    echo "FAIL: rd create with -l drbd,luks,storage rejected" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 333] linstor vd c $RD 128M"
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null

echo ">> [Bug 333] linstor r c $RD --auto-place=2 -s $POOL"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 333): encrypted auto-place=2 exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 333] wait for 2 Resource CRDs to land"
deadline=$(( $(date +%s) + 60 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_nodes[@]} == 2 )); then
        break
    fi
    sleep 2
done

if (( ${#placed_nodes[@]} != 2 )); then
    echo "FAIL: autoplace did not stage 2 Resource CRDs within 60s (got ${#placed_nodes[@]})" >&2
    exit 1
fi
echo "   placed on: ${placed_nodes[*]}"

N1="${placed_nodes[0]}"
N2="${placed_nodes[1]}"

echo ">> [Bug 333] wait both replicas UpToDate"
wait_uptodate "$RD" "$N1" "$N2"

echo ">> [Bug 333] drbdsetup status shows LUKS layer on at least one peer"
# The .res `disk` line is the operator-visible proof that the LUKS
# mapper is in the stack between DRBD and storage. Same assertion
# the kubectl-driven drbd-luks-stack.sh makes, brought into the
# CLI path for symmetry.
res_body=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res" 2>/dev/null || true)
if ! grep -q "/dev/mapper/${RD}-0-luks" <<<"$res_body"; then
    echo "FAIL (Bug 333): .res does not point at LUKS mapper on $N1" >&2
    echo "$res_body" >&2
    exit 1
fi

echo ">> [Bug 333] cryptsetup luksDump on backing LV of each replica"
for node in "$N1" "$N2"; do
    backing=$(luks_backing_device "$RD" "$node" 0)
    if [[ -z "$backing" ]]; then
        echo "FAIL: could not resolve backing device for $RD on $node" >&2
        exit 1
    fi
    echo "   $node: backing=$backing"
    if ! wait_luks_header_present "$node" "$backing" 30; then
        echo "FAIL (Bug 333): no LUKS header on $node:$backing" >&2
        exit 1
    fi
    if ! assert_luks_passphrase_opens "$node" "$backing" "$PASSPHRASE"; then
        echo "FAIL (Bug 333): cluster passphrase does not unlock $node:$backing" >&2
        exit 1
    fi
done

echo ">> luks-rd-create-encrypted OK (Bug 333: -l drbd,luks,storage CLI path, 2 replicas, valid headers)"
