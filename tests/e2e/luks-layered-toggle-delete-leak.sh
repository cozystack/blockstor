#!/usr/bin/env bash
#
# usage: luks-layered-toggle-delete-leak.sh WORK_DIR
#
# Bug-hunt v2 E.1b + E.1c regression catcher. Two interlocked bugs
# in the LUKS-stacked toggle-to-diskless + delete path:
#
#   E.1b: `linstor r td --diskless <node> <rd>` on a LUKS-stacked
#         Resource returns SUCCESS but does nothing on the
#         satellite. drbdsetup still reports `disk:UpToDate`, the
#         LUKS dm-crypt mapper is still present, the backing ZFS
#         zvol is still present.
#   E.1c: After E.1b, `linstor r d <node> <rd>` and then
#         `linstor rd d <rd>` leak the zvol. The Resource was
#         marked `DISKLESS` by the toggle so the full-RD delete
#         path skips storage-layer cleanup.
#
# Shared root cause: `applyStorageIfDiskful` (pkg/satellite/
# reconciler.go) ran `detachIfStillAttached` → `reclaim
# VolumesForDiskless` but never `cryptsetup luksClose` between
# them. The LUKS mapper held `/dev/zvol/...` open, ZFS DeleteVolume
# silently swallowed the leftover, and the leak compounded once
# `r d` arrived.
#
# Fix in pkg/satellite/reconciler.go around the toggle-to-diskless
# block (mirrors the existing `DeleteResource` cleanup ordering):
#   detach → cryptsetup luksClose → reclaim storage.
#
# This e2e walks the full leak path and asserts EVERY worker shows
# no `blockstor-zfs/<rd>_<volnum>` zvol after `rd d`.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "FAIL: linstor CLI not in PATH (apt install linstor-client)" >&2
    exit 1
fi

RD=luks-toggle-leak
N1=$WORKER_1
N2=$WORKER_2

CONVERGE=${CONVERGE:-60}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/luks-toggle-leak-pf.log 2>&1 &
PF_PID=$!

# Track passphrase state so cleanup doesn't strand
# DrbdOptions/EncryptPassphrase on a property the next scenario can't
# observe; harmless on shared stands but tidy.
SET_PASSPHRASE=0

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    "${LCTL[@]}" r l -r "$RD" 2>&1 || true
    echo "---- dump: zvol presence on each worker ----"
    for n in "$N1" "$N2" "$WORKER_3"; do
        echo "  -- $n --"
        on_node "$n" zfs list -t volume 2>/dev/null | grep -E "${RD}|NAME" || echo "    (no matching zvol)"
    done
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
    if (( SET_PASSPHRASE == 1 )); then
        "${LCTL[@]}" controller set-property DrbdOptions/EncryptPassphrase "" 2>/dev/null || true
    fi
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

delete_rd "$RD" 2>/dev/null || true

# Stamp the cluster-wide passphrase the LUKS layer needs. Best-effort:
# a previous scenario may already have it stamped and the second set
# is harmless on the apiserver but emits a warning on the CLI.
echo ">> stamping DrbdOptions/EncryptPassphrase via controller set-property"
"${LCTL[@]}" controller set-property DrbdOptions/EncryptPassphrase "bug-hunt-v2-luks-passphrase" 2>&1 | tail -5
SET_PASSPHRASE=1

# ---- STEP 1: 2 diskful with LUKS-stacked layer list ----------------

echo ">> create RD $RD with layer-list drbd,luks,storage"
"${LCTL[@]}" resource-definition create "$RD" --layer-list drbd,luks,storage
"${LCTL[@]}" volume-definition create "$RD" 64M

echo ">> create 2 diskful on $N1, $N2 with zfs-thin pool"
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool zfs-thin
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool zfs-thin

echo ">> wait for the zvol to appear on $N1 (LUKS-stacked RD bring-up may take longer)"
deadline=$(( $(date +%s) + 180 ))
ZVOL_NAME="${RD}_00000"
while (( $(date +%s) < deadline )); do
    if on_node "$N1" zfs list -t volume -o name -H 2>/dev/null | grep -qF "$ZVOL_NAME"; then
        break
    fi
    sleep 2
done

if ! on_node "$N1" zfs list -t volume -o name -H 2>/dev/null | grep -qF "$ZVOL_NAME"; then
    echo "FAIL: zvol $ZVOL_NAME never appeared on $N1; LUKS-stacked bring-up itself broken"
    dump_diag
    exit 1
fi

echo "   zvol $ZVOL_NAME present on $N1"

# ---- STEP 2: toggle worker-1 to diskless ---------------------------
#
# This is the exact path E.1b breaks: pre-fix the call returns
# SUCCESS but the satellite leaves the LUKS mapper + zvol behind.

echo ">> linstor r td --diskless $N1 $RD (the E.1b call)"
"${LCTL[@]}" resource toggle-disk "$N1" "$RD" --diskless

echo ">> wait up to ${CONVERGE}s for $N1 to release the zvol"
deadline=$(( $(date +%s) + CONVERGE ))
while (( $(date +%s) < deadline )); do
    if ! on_node "$N1" zfs list -t volume -o name -H 2>/dev/null | grep -qF "$ZVOL_NAME"; then
        break
    fi
    sleep 2
done

if on_node "$N1" zfs list -t volume -o name -H 2>/dev/null | grep -qF "$ZVOL_NAME"; then
    echo "ASSERT E.1b FAILED: zvol $ZVOL_NAME still present on $N1 ${CONVERGE}s after r td --diskless"
    on_node "$N1" zfs list -t volume 2>&1 | head -5
    on_node "$N1" dmsetup ls 2>&1 | grep -E "${RD}|^Name" || true
    exit 1
fi

echo "   E.1b: zvol released from $N1 after toggle ✓"

# ---- STEP 3: r d on the now-diskless worker-1, then full rd d -----

echo ">> linstor r d $N1 $RD (drop the now-diskless replica)"
"${LCTL[@]}" resource delete "$N1" "$RD" || true

echo ">> linstor rd d $RD (drop the whole RD)"
delete_rd "$RD"

# ---- STEP 4: assert NO zvol leak on any worker --------------------
#
# Pre-fix observed state: zvol survives on $N1 (the host that was
# toggled), absent on $N2 and $WORKER_3. Post-fix: empty everywhere.

echo ">> assert no $ZVOL_NAME zvol on any worker (E.1c leak gate)"

deadline=$(( $(date +%s) + CONVERGE ))
leak=""

while (( $(date +%s) < deadline )); do
    leak=""
    for n in "$N1" "$N2" "$WORKER_3"; do
        if on_node "$n" zfs list -t volume -o name -H 2>/dev/null | grep -qF "$ZVOL_NAME"; then
            leak="$n"
        fi
    done
    if [[ -z "$leak" ]]; then
        break
    fi
    sleep 2
done

if [[ -n "$leak" ]]; then
    echo "ASSERT E.1c FAILED: zvol $ZVOL_NAME still present on $leak after rd d"
    on_node "$leak" zfs list -t volume 2>&1 | head -5
    exit 1
fi

echo ">> PASS: LUKS-stacked toggle-to-diskless + delete leaves no zvol leak on any host"
