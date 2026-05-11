#!/usr/bin/env bash
#
# usage: resize-plain.sh WORK_DIR
#
# Tests Phase 8.2 "volume resize" without LUKS — same chain minus the
# cryptsetup layer.
# Setup:
#   - 2-replica RD on workers 1+2, 64 MiB initial
#   - write known pattern
# Steps: bump size to 128 MiB via REST, wait for resize, verify md5.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-resize-plain
N1=$WORKER_1
N2=$WORKER_2
SIZE_INITIAL_KIB=65536
SIZE_GROWN_KIB=131072

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD"
rd_apply "$RD" "$N1" "$N2" "$SIZE_INITIAL_KIB"
wait_uptodate "$RD" "$N1" "$N2"

DEV=$(device_for_rd "$RD" "$N1")
md5_pre=$(write_random "$N1" "$DEV" 4194304)

echo ">> resize via REST → $SIZE_GROWN_KIB KiB"
rest_put "/v1/resource-definitions/${RD}/volume-definitions/0" \
    "{\"size_kib\":${SIZE_GROWN_KIB}}"

# DRBD reserves ~44 KiB for internal metadata, so the visible /dev/drbdN
# size after resize is `requested - <small>`. Accept ≥99% of the
# requested size as a successful grow.
TOLERANCE_KIB=128
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    cur=$(on_node "$N1" bash -c "blockdev --getsize64 ${DEV}" 2>/dev/null || true)
    cur=$(( ${cur:-0} / 1024 ))
    if (( cur + TOLERANCE_KIB >= SIZE_GROWN_KIB )); then
        break
    fi
    sleep 2
done

if (( cur + TOLERANCE_KIB < SIZE_GROWN_KIB )); then
    echo "FAIL: device size $cur KiB < $SIZE_GROWN_KIB (tolerance ${TOLERANCE_KIB})"
    exit 1
fi

md5_post=$(read_md5 "$N1" "$DEV" 4194304)
if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: data lost over resize"
    exit 1
fi

echo ">> RESIZE-PLAIN OK"
