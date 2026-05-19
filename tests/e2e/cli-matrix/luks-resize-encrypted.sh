#!/usr/bin/env bash
#
# usage: luks-resize-encrypted.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333 (resize-encrypted branch).
#
# Audit gap: tests/e2e/resize-luks.sh drives the resize via
# rest_put on /v1/resource-definitions/{rd}/volume-definitions/{vol}.
# The python-CLI surface (`linstor vd s <rd> <vol> <new-size>` →
# PUT same path) was never exercised end-to-end. The CLI path has
# its own arg-parse → REST-body translation in
# pkg/rest/volume_definition.go that no test pinned.
#
# Setup: encrypted RD with [DRBD,LUKS,STORAGE], 64 MiB.
# Write known pattern, capture md5.
# Steps:
#   1. linstor vd s <rd> 0 128M (or whichever CLI syntax our
#      version supports; --size 128M as fallback)
#   2. wait for satellite resize chain (LV grow → cryptsetup resize
#      → drbdadm resize). Same convergence semantics as resize-luks.sh
#   3. blockdev --getsize64 reports >= original + half-delta
#   4. md5 over original region still matches

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-333-resize
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-333-resize-pp!'
SIZE_INITIAL_KIB=65536    # 64 MiB
SIZE_GROWN_KIB=131072     # 128 MiB

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
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

echo ">> [Bug 333] set cluster passphrase"
"${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1 || true

echo ">> [Bug 333] linstor rd c $RD -l drbd,luks,storage + vd c 64M + r c --auto-place=2"
"${LCTL[@]}" resource-definition create "$RD" --layer-list drbd,luks,storage >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

# Resolve the placed pair so we can wait_uptodate against the actual
# nodes the autoplacer picked rather than $WORKER_1+$WORKER_2 by
# convention.
deadline=$(( $(date +%s) + 60 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_nodes[@]} == 2 )); then break; fi
    sleep 2
done
if (( ${#placed_nodes[@]} != 2 )); then
    echo "FAIL: autoplace did not stage 2 replicas" >&2
    exit 1
fi
N1="${placed_nodes[0]}"
N2="${placed_nodes[1]}"

wait_uptodate "$RD" "$N1" "$N2"
DEV=$(device_for_rd "$RD" "$N1")
echo ">> [Bug 333] write known 4 MiB pattern, capture md5"
md5_pre=$(write_random "$N1" "$DEV" 4194304)

echo ">> [Bug 333] linstor vd s $RD 0 128M"
err_file=$(mktemp)
# `linstor vd s` = `volume-definition set-size`. Some client builds
# accept the size as a positional arg, others want --size. Try the
# positional form first (newer client), fall back to --size.
if ! "${LCTL[@]}" volume-definition set-size "$RD" 0 128M 2>"$err_file"; then
    if ! "${LCTL[@]}" volume-definition set-size "$RD" 0 --size 128M 2>>"$err_file"; then
        echo "FAIL (Bug 333): vd set-size CLI exited non-zero" >&2
        cat "$err_file" >&2
        rm -f "$err_file"
        exit 1
    fi
fi
rm -f "$err_file"

echo ">> [Bug 333] wait for resize chain (LV → cryptsetup → drbd)"
# Same heuristic as resize-luks.sh: assert the device grew BEYOND
# the initial size by >= half the requested delta. DRBD --max-peers
# + LUKS header overhead carves the exact byte count down from the
# nominal request, so we can't tie the assertion to an exact number.
GROWTH_MIN_KIB=$(( (SIZE_GROWN_KIB - SIZE_INITIAL_KIB) / 2 ))
deadline=$(( $(date +%s) + 90 ))
cur_kib=0
while (( $(date +%s) < deadline )); do
    cur_kib=$(on_node "$N1" bash -c "blockdev --getsize64 ${DEV}" 2>/dev/null || true)
    cur_kib=$(( ${cur_kib:-0} / 1024 ))
    if (( cur_kib >= SIZE_INITIAL_KIB + GROWTH_MIN_KIB )); then
        break
    fi
    sleep 2
done

if (( cur_kib < SIZE_INITIAL_KIB + GROWTH_MIN_KIB )); then
    echo "FAIL (Bug 333): device size $cur_kib KiB not above $((SIZE_INITIAL_KIB + GROWTH_MIN_KIB)) KiB after 90s" >&2
    exit 1
fi

echo ">> [Bug 333] read first 4 MiB — md5 must still match"
md5_post=$(read_md5 "$N1" "$DEV" 4194304)
if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL (Bug 333): data corrupted by CLI-driven resize (pre=$md5_pre post=$md5_post)" >&2
    exit 1
fi

echo ">> [Bug 333] LUKS header still recognised after resize (blkid)"
# Resize-induced header corruption is the canonical regression here
# (cryptsetup resize on a mounted mapper that wasn't properly closed
# would smash byte 0). Belt-and-braces probe on the backing LV.
backing=$(luks_backing_device "$RD" "$N1" 0)
if [[ -n "$backing" ]]; then
    if ! on_node "$N1" blkid -t TYPE=crypto_LUKS "$backing" >/dev/null 2>&1; then
        echo "FAIL (Bug 333): backing $backing no longer reports TYPE=crypto_LUKS after resize" >&2
        exit 1
    fi
fi

echo ">> luks-resize-encrypted OK (Bug 333: linstor vd s grew encrypted vol, data + header preserved)"
