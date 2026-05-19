#!/usr/bin/env bash
#
# usage: luks-clone-encrypted.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333 (clone-encrypted branch).
#
# Audit gap: tests/e2e/clone.sh covers `linstor rd clone` only on
# plain DRBD,STORAGE. The encrypted case has a distinct concern:
# the clone target must inherit the source's layer-list AND its
# passphrase reference. If the rest/rd_clone.go path drops either,
# the cloned RD ends up plaintext (silent confidentiality
# downgrade) or with a fresh LUKS header that the source's
# passphrase can't open.
#
# Setup:
#   - encrypted source RD with [DRBD,LUKS,STORAGE] on 2 nodes,
#     known md5 written
# Steps:
#   1. linstor rd clone <src> <dst>
#   2. autoplace the clone
#   3. wait UpToDate on the clone
#   4. assertions:
#      - clone's RD has the same layer-list (drbd,luks,storage)
#      - same passphrase opens the clone's backing LV
#      - data round-trips (md5 matches)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD_SRC=cli-matrix-333-clone-src
RD_DST=cli-matrix-333-clone-dst
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-333-clone-pp!'

cleanup() {
    delete_rd "$RD_DST"
    delete_rd "$RD_SRC"
    assert_no_orphans "$RD_SRC"
    assert_no_orphans "$RD_DST"
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

cleanup_encryption_state

echo ">> [Bug 333] pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

echo ">> [Bug 333] set cluster passphrase + create source encrypted RD"
"${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1 || true
"${LCTL[@]}" resource-definition create "$RD_SRC" --layer-list drbd,luks,storage >/dev/null
"${LCTL[@]}" volume-definition create "$RD_SRC" 64M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool "$POOL" "$RD_SRC" >/dev/null

deadline=$(( $(date +%s) + 60 ))
placed_src=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_src < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD_SRC." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_src[@]} == 2 )); then break; fi
    sleep 2
done
if (( ${#placed_src[@]} != 2 )); then
    echo "FAIL: source RD did not autoplace 2 replicas" >&2
    exit 1
fi
N1="${placed_src[0]}"
N2="${placed_src[1]}"

wait_uptodate "$RD_SRC" "$N1" "$N2"
DEV=$(device_for_rd "$RD_SRC" "$N1")
RD=$RD_SRC
md5_src=$(write_random "$N1" "$DEV" 262144)

echo ">> [Bug 333] linstor rd clone $RD_SRC $RD_DST"
err_file=$(mktemp)
# Newer python-linstor: `rd clone <src> <dst>`. Older builds expose
# the same verb as `resource-definition clone`.
if ! "${LCTL[@]}" resource-definition clone "$RD_SRC" "$RD_DST" 2>"$err_file"; then
    echo "FAIL (Bug 333): rd clone CLI rejected" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# Some clone implementations create the RD shell + auto-place
# automatically; others leave placement to the operator. Trigger
# autoplace defensively and rely on the autoplace endpoint being
# idempotent if the clone already placed replicas.
"${LCTL[@]}" resource create --auto-place=2 --storage-pool "$POOL" "$RD_DST" \
    >/dev/null 2>&1 || true

deadline=$(( $(date +%s) + 120 ))
placed_dst=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_dst < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD_DST." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_dst[@]} == 2 )); then break; fi
    sleep 2
done
if (( ${#placed_dst[@]} != 2 )); then
    echo "FAIL: clone RD did not get 2 replicas within 120s (got ${#placed_dst[@]})" >&2
    exit 1
fi
M1="${placed_dst[0]}"
M2="${placed_dst[1]}"

wait_uptodate "$RD_DST" "$M1" "$M2"

echo ">> [Bug 333] clone's layer-list contains LUKS (confidentiality not silently downgraded)"
clone_layers=$("${LCTL[@]}" --machine-readable resource-definition list --resource-definitions "$RD_DST" 2>/dev/null \
    | jq -r '[.[]?[]? | .layer_data[]?.type // empty] | join(",") | ascii_upcase' 2>/dev/null || echo "")
if [[ "$clone_layers" != *"LUKS"* ]]; then
    echo "FAIL (Bug 333): clone's layer-list missing LUKS — silent confidentiality downgrade: '$clone_layers'" >&2
    exit 1
fi

echo ">> [Bug 333] same passphrase opens clone on each replica"
for node in "$M1" "$M2"; do
    backing=$(luks_backing_device "$RD_DST" "$node" 0)
    if [[ -z "$backing" ]]; then
        echo "FAIL: could not resolve backing for $RD_DST on $node" >&2
        exit 1
    fi
    if ! wait_luks_header_present "$node" "$backing" 30; then
        echo "FAIL (Bug 333): cloned LV $node:$backing has no LUKS header" >&2
        exit 1
    fi
    if ! assert_luks_passphrase_opens "$node" "$backing" "$PASSPHRASE"; then
        echo "FAIL (Bug 333): source passphrase does not open clone at $node:$backing" >&2
        exit 1
    fi
done

DEV_DST=$(device_for_rd "$RD_DST" "$M1")
RD=$RD_DST
md5_dst=$(read_md5 "$M1" "$DEV_DST" 262144)

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL (Bug 333): clone data differs from source (src=$md5_src dst=$md5_dst)" >&2
    exit 1
fi

echo ">> luks-clone-encrypted OK (Bug 333: rd clone preserves LUKS + passphrase + data)"
