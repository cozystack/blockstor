#!/usr/bin/env bash
#
# usage: luks-snapshot-restore-encrypted.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333 (snapshot-restore-encrypted branch).
#
# Audit gap: tests/e2e/snapshot-restore-cross-node.sh covers
# snapshot+restore but only on a plain DRBD,STORAGE stack. The
# DRBD,LUKS,STORAGE stack has its own restore-side concern: the
# LUKS header on the cloned/restored LV must still open with the
# original passphrase. The python-CLI path
#   linstor snapshot c <rd> <snap>
#   linstor snapshot restore <rd> <snap> <new-rd>
# was never exercised end-to-end on an encrypted source.
#
# Setup:
#   - encrypted RD with [DRBD,LUKS,STORAGE] on 2 nodes
#   - write known pattern via the DRBD-9 plaintext mapper, capture md5
# Steps:
#   1. linstor snapshot create
#   2. linstor snapshot resource-definition restore (creates new RD
#      with the same layer-list + passphrase reference)
#   3. autoplace the restored RD
#   4. wait UpToDate
#   5. on the restored RD, the same passphrase must open the LUKS
#      mapper, and the md5 over the same offset must match.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD_SRC=cli-matrix-333-snap-src
RD_DST=cli-matrix-333-snap-dst
SNAP=cli-matrix-333-snap-$(date +%s)
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-333-snap-pp!'

cleanup() {
    # delete snapshots before RDs (delete_rd already does this, but
    # be explicit for the restored RD which may carry the source's
    # snapshot too).
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

echo ">> [Bug 333] set cluster passphrase"
"${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1 || true

echo ">> [Bug 333] create source encrypted RD"
"${LCTL[@]}" resource-definition create "$RD_SRC" --layer-list drbd,luks,storage >/dev/null
"${LCTL[@]}" volume-definition create "$RD_SRC" 64M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD_SRC" >/dev/null

# Resolve placed nodes.
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

echo ">> [Bug 333] linstor snapshot create $RD_SRC $SNAP"
err_file=$(mktemp)
if ! "${LCTL[@]}" snapshot create "$RD_SRC" "$SNAP" 2>"$err_file"; then
    echo "FAIL (Bug 333): snapshot create rejected" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 333] linstor snapshot resource-definition restore → $RD_DST"
# Restore-into-new-RD verb. Newer python-linstor uses
# `snapshot resource-definition restore` (RD-level), older
# clients used `snapshot resource restore`. Try the canonical
# RD-level form first.
err_file=$(mktemp)
if ! "${LCTL[@]}" snapshot resource-definition restore \
        --from-resource "$RD_SRC" --from-snapshot "$SNAP" \
        --to-resource "$RD_DST" 2>"$err_file"; then
    # Fallback: older client form.
    if ! "${LCTL[@]}" snapshot resource restore \
            --from-resource "$RD_SRC" --from-snapshot "$SNAP" \
            --to-resource "$RD_DST" 2>>"$err_file"; then
        echo "FAIL (Bug 333): snapshot restore (both forms) rejected" >&2
        cat "$err_file" >&2
        rm -f "$err_file"
        exit 1
    fi
fi
rm -f "$err_file"

echo ">> [Bug 333] linstor r c $RD_DST --auto-place=2"
# Restore creates the target RD shell but does NOT auto-deploy
# replicas — caller must place them. Same flow as the kubectl-
# driven snapshot-restore-cross-node.sh.
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD_DST" >/dev/null

deadline=$(( $(date +%s) + 60 ))
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
    echo "FAIL: restored RD did not autoplace 2 replicas" >&2
    exit 1
fi
M1="${placed_dst[0]}"
M2="${placed_dst[1]}"

wait_uptodate "$RD_DST" "$M1" "$M2"

echo ">> [Bug 333] passphrase opens restored backing LV on each replica"
for node in "$M1" "$M2"; do
    backing=$(luks_backing_device "$RD_DST" "$node" 0)
    if [[ -z "$backing" ]]; then
        echo "FAIL: could not resolve backing for $RD_DST on $node" >&2
        exit 1
    fi
    if ! wait_luks_header_present "$node" "$backing" 30; then
        echo "FAIL (Bug 333): restored LV $node:$backing has no LUKS header" >&2
        exit 1
    fi
    if ! assert_luks_passphrase_opens "$node" "$backing" "$PASSPHRASE"; then
        echo "FAIL (Bug 333): passphrase does not open restored $node:$backing" >&2
        exit 1
    fi
done

echo ">> [Bug 333] data round-trip via DRBD mapper"
DEV_DST=$(device_for_rd "$RD_DST" "$M1")
RD=$RD_DST
md5_dst=$(read_md5 "$M1" "$DEV_DST" 262144)

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL (Bug 333): restored data differs from source (src=$md5_src dst=$md5_dst)" >&2
    exit 1
fi

echo ">> luks-snapshot-restore-encrypted OK (Bug 333: snap/restore preserves LUKS header + data)"
