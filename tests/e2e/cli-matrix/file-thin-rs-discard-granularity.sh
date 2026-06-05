#!/usr/bin/env bash
#
# usage: file-thin-rs-discard-granularity.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case Q3.
#
# Reproduction: a 512 MiB FILE_THIN resource, only ~320 MiB of it
# written, gains a 3rd replica. The new replica's resync MUST transfer
# only the written bytes (~320 MiB), NOT the whole 512 MiB device —
# matching upstream LINSTOR 1.33.2 on the same loop backing.
#
# Pre-fix blockstor rendered NO `disk { }` block for FILE_THIN because
# the satellite coupled rs-discard-granularity to the
# discard-zeroes-if-aligned provider gate (which correctly excludes
# FILE_THIN). Without rs-discard-granularity DRBD cannot UNMAP the
# all-zero unwritten ranges during resync, so it copies the whole device
# (~2x the bytes; measured 524012 KiB vs upstream's 327680 KiB).
#
# Contract — assert all three legs:
#   1. the rendered .res on the diskful node carries a `disk { }` block
#      with `rs-discard-granularity` and `discard-zeroes-if-aligned no`;
#   2. the new replica converges to UpToDate (clean sync);
#   3. the resync TARGET's `received` byte counter is close to the
#      written bytes, NOT to the full device size (the core win).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-q3
SP=stand

# 512 MiB volume, write 320 MiB, expect resync ≈ written.
VOL_MIB=512
WRITTEN_MIB=320
# Upper bound for "received" on the sync target: the written bytes plus
# generous slack for DRBD bitmap/activity-log overhead and rounding.
# A full-device copy would be ~524288 KiB, which this comfortably
# excludes; the pre-fix bug measured 524012 KiB.
MAX_RECEIVED_KIB=$(( (WRITTEN_MIB + 96) * 1024 ))

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2

echo ">> [Q3] 512M FILE_THIN single diskful replica on $N1"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" "${VOL_MIB}M" >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$SP" >/dev/null

echo ">> wait $N1 UpToDate"
wait_disk_state "$RD" "$N1" UpToDate 120

echo ">> assert rendered .res on $N1 carries the discard disk block"
RES=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res")
if ! grep -q "disk {" <<<"$RES"; then
    echo "FAIL: no disk { } block in rendered .res for FILE_THIN:" >&2
    echo "$RES" >&2
    exit 1
fi
if ! grep -Eq "rs-discard-granularity[[:space:]]+[0-9]+;" <<<"$RES"; then
    echo "FAIL: rendered .res lacks rs-discard-granularity:" >&2
    echo "$RES" >&2
    exit 1
fi
if ! grep -Eq "discard-zeroes-if-aligned[[:space:]]+no;" <<<"$RES"; then
    echo "FAIL: rendered .res lacks discard-zeroes-if-aligned no (FILE_THIN):" >&2
    echo "$RES" >&2
    exit 1
fi
echo "   OK: $(grep -E 'rs-discard-granularity|discard-zeroes-if-aligned' <<<"$RES" | tr -s ' \t' ' ')"

echo ">> write ${WRITTEN_MIB} MiB pattern into the volume on $N1"
# /dev/drbd<minor> is the device; resolve the minor from the .res.
MINOR=$(grep -Eo 'minor[[:space:]]+[0-9]+' <<<"$RES" | head -1 | grep -Eo '[0-9]+')
if [[ -z "$MINOR" ]]; then
    echo "FAIL: could not resolve DRBD minor from .res" >&2
    exit 1
fi
on_node "$N1" dd if=/dev/urandom "of=/dev/drbd${MINOR}" bs=1M count="$WRITTEN_MIB" \
    oflag=direct conv=fsync status=none
echo "   wrote ${WRITTEN_MIB} MiB to /dev/drbd${MINOR}"

echo ">> add 3rd... 2nd diskful replica on $N2 (triggers resync)"
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$SP" >/dev/null

echo ">> wait $N2 UpToDate (clean sync)"
wait_disk_state "$RD" "$N2" UpToDate 300

echo ">> read resync-received byte counter on the sync target ($N2)"
# `drbdsetup status --statistics` reports per-peer-device `received:`
# in bytes (total received over the connection — dominated here by the
# initial resync). Sum across peer devices to be robust.
RECEIVED_BYTES=$(on_node "$N2" drbdsetup status "$RD" --statistics --json 2>/dev/null \
    | jq '[.[].connections[].peer_devices[].received // 0] | add' 2>/dev/null || echo "")
if [[ -z "$RECEIVED_BYTES" || "$RECEIVED_BYTES" == "null" ]]; then
    # Text fallback: parse `received:<bytes>` from drbdsetup status.
    RECEIVED_BYTES=$(on_node "$N2" drbdsetup status "$RD" --statistics 2>/dev/null \
        | grep -oE 'received:[0-9]+' | head -1 | cut -d: -f2)
fi
if [[ -z "$RECEIVED_BYTES" ]]; then
    echo "FAIL: could not read received byte counter on $N2" >&2
    exit 1
fi
RECEIVED_KIB=$(( RECEIVED_BYTES / 1024 ))

echo "   sync target received: ${RECEIVED_KIB} KiB (written ${WRITTEN_MIB} MiB,"
echo "   device ${VOL_MIB} MiB; pre-fix bug copied the whole device)"

if (( RECEIVED_KIB > MAX_RECEIVED_KIB )); then
    echo "FAIL: sync target received ${RECEIVED_KIB} KiB > ${MAX_RECEIVED_KIB} KiB" >&2
    echo "      => DRBD copied the unwritten zero ranges (rs-discard-granularity not honoured)" >&2
    exit 1
fi

echo "PASS: Q3 — FILE_THIN resync transferred only the written bytes"
