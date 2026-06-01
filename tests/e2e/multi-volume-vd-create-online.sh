#!/usr/bin/env bash
#
# usage: multi-volume-vd-create-online.sh WORK_DIR
#
# Regression catcher for Bug B.4 (P0 critical): adding a new
# volume to an already-running multi-replica RD via
# `linstor vd c <rd> N` hot-loops the satellite reconciler with
# `drbdadm create-md` against the ALREADY-ATTACHED vol-0 minor.
#
# Pre-fix surface (from /tmp/bug-hunt3-B4-vd-create-second-
# volume-wedges-satellite.md):
#
#   - `linstor v l` shows vol-1 with DeviceName=None on every
#     diskful node — satellite never reaches per-volume
#     granularity needed when vol-0 is already attached.
#   - `drbdsetup status` on the diskful peer shows:
#         hunt3-b4 role:Secondary suspended:quorum
#           volume:0 disk:UpToDate blocked:upper
#           volume:1 disk:Diskless quorum:no
#     The WHOLE RESOURCE is suspended (`susp-io( no -> quorum )`
#     in dmesg) because vol-1 cannot achieve quorum — vol-0 I/O
#     is BLOCKED.
#   - Satellite log loops at ~10 Hz with:
#         create-md hunt3-b4: drbdadm create-md: drbdadm create-md
#         --force --max-peers=15 hunt3-b4:
#           open(/dev/zvol/blockstor-zfs/hunt3-b4_00000) failed:
#           Device or resource busy
#           Device '20000' is configured!
#
# Root cause: `ensureMetadata` issued a per-RD `drbdadm dump-md
# <rd>` + `drbdadm create-md <rd>` pair on the diskless→diskful
# transition that the new kernel slot for vol-1 trips. With vol-0
# attached, the per-RD `dump-md` walks all volumes, reports
# "missing" because vol-1 has no metadata yet, and the follow-up
# `create-md <rd>` EBUSY-loops against vol-0's attached minor.
#
# Fix: scope HasMD + CreateMD per-volume (`<rd>/<volNumber>`),
# mirroring upstream LINSTOR DrbdLayer.adjustResource. Volumes
# that already have metadata are skipped; only the new volume's
# lower disk gets stamped.
#
# This scenario lives in tests/e2e/ (not unit) because:
#   - The trigger needs a real DRBD kernel attached to vol-0
#     (mock attach state can't reproduce EBUSY on the minor),
#   - The wedge surfaces as `suspended:quorum` which only a
#     real DRBD-9 kernel emits,
#   - The convergence path runs through a real `drbdadm adjust`
#     pulling in the new volume's minor.

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

RD=b4-multi-vd-online
N1=$WORKER_1
N2=$WORKER_2

# Per-step convergence budget. Worst observed re-converge for
# the per-volume create-md + adjust + UpToDate handshake on a
# healthy QEMU stand is ~30s; 90s gives 3x headroom for CI noise.
CONVERGE=${CONVERGE:-90}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/b4-multi-vd-online-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor v l -r $RD ----"
    "${LCTL[@]}" v l -r "$RD" 2>&1 || true
    echo "---- dump: per-node drbdsetup status ----"
    for n in "$N1" "$N2"; do
        echo "  -- $n --"
        on_node "$n" drbdsetup status "$RD" 2>&1 || true
    done
    echo "---- dump: kubectl logs -n $NS daemonset/blockstor-satellite --tail=80 ----"
    kubectl logs -n "$NS" daemonset/blockstor-satellite --tail=80 2>/dev/null \
        | grep -E "(create-md|vol|suspended|blocked|EBUSY|Device or resource busy)" \
        | tail -40 || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
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

# Clean any leftover state from a prior run.
delete_rd "$RD" 2>/dev/null || true

# ---- STEP 1: 2-replica drbd+storage RD, vol-0 100 MiB ------------

echo ">> create RD $RD with 2 diskful replicas on $N1, $N2 (vol-0 100M)"
"${LCTL[@]}" resource-definition create "$RD" -l drbd,storage
"${LCTL[@]}" volume-definition create "$RD" 100M
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool stand
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool stand

echo ">> wait both diskful replicas UpToDate (<=180s)"
wait_disk_state "$RD" "$N1" UpToDate 180 0
wait_disk_state "$RD" "$N2" UpToDate 180 0

# ---- STEP 2: late-add vol-1 ---------------------------------------

echo ">> linstor vd c $RD 100M (add vol-1 to a running RD)"
"${LCTL[@]}" volume-definition create "$RD" 100M

# ---- STEP 3: assert vol-1 reaches UpToDate, vol-0 stays unblocked --
#
# Pre-fix: vol-1 sits at `disk:Diskless quorum:no` forever, the
# whole resource is `suspended:quorum`, vol-0 reports
# `blocked:upper`, and the satellite reconciler hot-loops at
# ~10 Hz with the EBUSY create-md error against vol-0.

echo ">> wait vol-1 UpToDate on both diskful peers (<=${CONVERGE}s)"
wait_disk_state "$RD" "$N1" UpToDate "$CONVERGE" 1
wait_disk_state "$RD" "$N2" UpToDate "$CONVERGE" 1

# Resource MUST NOT be `suspended:quorum`. The per-volume
# create-md + adjust path lets vol-1's quorum settle without
# blocking vol-0's I/O.
echo ">> assert no suspended:quorum on either diskful node"
for n in "$N1" "$N2"; do
    status=$(on_node "$n" drbdsetup status "$RD" 2>&1 || true)
    if echo "$status" | grep -q "suspended:quorum"; then
        echo "FAIL: $n reports suspended:quorum after vd c (Bug B.4 surface)"
        echo "      drbdsetup status:"
        echo "$status" | sed 's/^/        /'
        exit 1
    fi
done

# vol-0 MUST NOT be `blocked:upper`. Bug B.4 surface: the
# satellite's failed per-RD create-md against vol-0's attached
# minor leaves the resource in `suspended:quorum` because vol-1
# cannot achieve quorum, which back-pressures vol-0 with
# `blocked:upper`.
echo ">> assert vol-0 is not blocked:upper on either diskful node"
for n in "$N1" "$N2"; do
    status=$(on_node "$n" drbdsetup status "$RD" 2>&1 || true)
    vol0_line=$(echo "$status" | awk '/volume:0/{print; exit}')
    if echo "$vol0_line" | grep -q "blocked:upper"; then
        echo "FAIL: $n vol-0 is blocked:upper (Bug B.4 surface — vol-1 quorum block)"
        echo "      drbdsetup status:"
        echo "$status" | sed 's/^/        /'
        exit 1
    fi
done

# Last guard: vol-1 must NOT be Diskless on a node whose Spec
# was diskful. The pre-fix surface is "Unintentional Diskless"
# for vol-1 because the per-RD create-md failed and the kernel
# brought vol-1 up with no metadata.
echo ">> assert vol-1 is not Diskless on either diskful node"
for n in "$N1" "$N2"; do
    status=$(on_node "$n" drbdsetup status "$RD" 2>&1 || true)
    vol1_line=$(echo "$status" | awk '/volume:1/{print; exit}')
    if echo "$vol1_line" | grep -q "disk:Diskless"; then
        echo "FAIL: $n vol-1 is disk:Diskless (Bug B.4 surface — per-RD create-md EBUSY-loop)"
        echo "      drbdsetup status:"
        echo "$status" | sed 's/^/        /'
        exit 1
    fi
done

echo ">> PASS: late-add vol-1 via vd c reached UpToDate, vol-0 unblocked"
