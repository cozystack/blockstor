#!/usr/bin/env bash
#
# usage: backing-device-fail.sh WORK_DIR
#
# Tests Phase 8.2 "events2 observer auto-detach on disk:Failed".
# Setup:
#   - 2-replica RD, both UpToDate
#   - kill the loop file on Primary so DRBD's lower disk goes Failed
# Expected:
#   - events2 observer sees disk:Failed
#   - satellite runs `drbdadm detach --force` on the Failed replica
#   - Primary stays Primary via the network leg (peer remains UpToDate)
#   - I/O continues without errors

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-disk-fail
PRIMARY=test-worker-1
PEER=test-worker-2

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD"
rd_apply "$RD" "$PRIMARY" "$PEER"
wait_uptodate "$RD" "$PRIMARY" "$PEER"

DEV=$(device_for_rd "$RD" "$PRIMARY")

echo ">> Primary write 256 KiB before fault"
md5_pre=$(write_random "$PRIMARY" "$DEV" 262144)

echo ">> simulate backing-device failure on $PRIMARY"
# loopfile pool puts each volume's image at /var/lib/blockstor-pool/<rd>_NNNNN.img
# Truncating to 0 + losetup detach makes DRBD see Failed I/O.
on_node "$PRIMARY" bash -c "
    losetup -j /var/lib/blockstor-pool/${RD}_00000.img | head -1 | cut -d: -f1 | xargs -r losetup -d
"

echo ">> wait 30s for events2 observer to detach"
sleep 30

# Detach success criteria: local disk in {Diskless, Failed}; peer still UpToDate.
prim_disk=$(on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$prim_disk" != *"Diskless"* && "$prim_disk" != *"Failed"* ]]; then
    echo "FAIL: $PRIMARY disk did not transition (got: $prim_disk)"
    exit 1
fi

peer_disk=$(on_node "$PEER" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$peer_disk" != *"UpToDate"* ]]; then
    echo "FAIL: $PEER disk no longer UpToDate (got: $peer_disk)"
    exit 1
fi

echo ">> read on $PRIMARY (network-leg) — md5 must match $md5_pre"
md5_after=$(read_md5 "$PRIMARY" "$DEV" 262144)
if [[ "$md5_after" != "$md5_pre" ]]; then
    echo "FAIL: post-detach read returned different data"
    exit 1
fi

echo ">> BACKING-DEVICE-FAIL OK (observer detached, peer covers reads via network)"
