#!/usr/bin/env bash
#
# usage: file-thin-rs-discard-granularity.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case Q3 (FILE_THIN disk-block render) +
# its fresh-create convergence REGRESSION guard.
#
# Background: the thin-aware-resync feature renders DRBD's
# `discard-zeroes-if-aligned` + `rs-discard-granularity` into the disk
# block so a partially-written THIN volume resyncs ~only the written
# bytes. That win is gated to the discard-zero-safe BLOCK-device provider
# kinds (LVM_THIN / ZFS / ZFS_THIN).
#
# A FILE_THIN volume is loop-backed: it reports a non-zero lsblk
# DISC-GRAN (4096) but rendering `rs-discard-granularity` into its disk
# block REGRESSED fresh-create convergence. When the elected day0 winner
# force-primaries to run mkfs (the FileSystem/Type path), mkfs issues a
# full-device discard; on the loop backing that discard storm, with
# rs-discard-granularity active, dirtied the bitmap relative to the
# day0-seeded peers and forced a FULL initial SyncTarget that then wedged
# (the e2e respawn-standalone-wedge failure). The identical create
# converges instantly on LVM_THIN (real block device, same disk block) —
# so the option is gated OUT of loop-backed FILE_THIN.
#
# discard-zeroes-if-aligned, by contrast, MUST be `yes` for FILE_THIN:
# loop punch-hole reads back zeros by contract, and the flag is what
# lets the kernel treat the whole device of a brand-new replica
# (metadata la_size 0 → device size) as "assumed zeroed" at attach.
# Rendering an explicit `no` (upstream's value — known-deltas row 76)
# made the kernel mark every fresh FILE_THIN attach fully out-of-sync,
# neutralising the day0 GI skip-initial-sync seed: every fresh create
# full-synced (the r-full P1 512M regression).
#
# Contract — assert all three legs on FILE_THIN (`stand` pool):
#   1. the rendered .res carries `discard-zeroes-if-aligned yes;`
#      (the day0-attach clean-bitmap flag; intentional divergence from
#      upstream's `no`) but NO `rs-discard-granularity` (the
#      regression-causing key);
#   2. a fresh resource whose RD carries FileSystem/Type=ext4 (drives the
#      winner force-primary + mkfs path) converges WITHOUT a full initial
#      resync — i.e. the day0 skip holds (peers reach UpToDate, the
#      sync-target `received` byte counter stays ~0, not the device size);
#   3. a 2nd diskful replica added to the *fresh* RD also converges via
#      the day0 skip (no full resync of the empty device).

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

VOL_MIB=512
# The day0 skip means the freshly-added empty replica should transfer
# essentially nothing. Allow generous slack for DRBD bitmap/AL metadata
# overhead, but FAR below the full device (~524288 KiB) — a full resync
# (the regression) would blow past this.
MAX_RECEIVED_KIB=$(( 64 * 1024 ))

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2

echo ">> [Q3] 512M FILE_THIN, FileSystem/Type=ext4 (drives force-primary mkfs), diskful on $N1"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" resource-definition set-property "$RD" FileSystem/Type ext4 >/dev/null
"${LCTL[@]}" volume-definition create "$RD" "${VOL_MIB}M" >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$SP" >/dev/null

echo ">> wait $N1 UpToDate"
wait_disk_state "$RD" "$N1" UpToDate 120

echo ">> assert rendered .res on $N1: discard-zeroes yes, NO rs-discard-granularity"
RES=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res")
if ! grep -Eq "discard-zeroes-if-aligned[[:space:]]+yes;" <<<"$RES"; then
    echo "FAIL: rendered .res lacks discard-zeroes-if-aligned yes (FILE_THIN):" >&2
    echo "      => fresh attaches mark the whole device out-of-sync; day0 skip broken (r-full P1)" >&2
    echo "$RES" >&2
    exit 1
fi
if grep -Eq "rs-discard-granularity" <<<"$RES"; then
    echo "FAIL: rendered .res carries rs-discard-granularity for loop-backed FILE_THIN" >&2
    echo "      => fresh-create + mkfs day0-skip regression (respawn-standalone-wedge)" >&2
    echo "$RES" >&2
    exit 1
fi
echo "   OK: $(grep -E 'discard-zeroes-if-aligned' <<<"$RES" | tr -s ' \t' ' '); no rs-discard-granularity"

echo ">> add 2nd diskful replica on $N2 (must day0-skip, NOT full-resync the empty device)"
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$SP" >/dev/null

echo ">> wait $N2 UpToDate (clean, no full resync)"
wait_disk_state "$RD" "$N2" UpToDate 300

echo ">> read resync-received byte counter on $N2 (day0 skip => ~0)"
RECEIVED_BYTES=$(on_node "$N2" drbdsetup status "$RD" --statistics --json 2>/dev/null \
    | jq '[.[].connections[].peer_devices[].received // 0] | add' 2>/dev/null || echo "")
if [[ -z "$RECEIVED_BYTES" || "$RECEIVED_BYTES" == "null" ]]; then
    RECEIVED_BYTES=$(on_node "$N2" drbdsetup status "$RD" --statistics 2>/dev/null \
        | grep -oE 'received:[0-9]+' | head -1 | cut -d: -f2)
fi
if [[ -z "$RECEIVED_BYTES" ]]; then
    echo "FAIL: could not read received byte counter on $N2" >&2
    exit 1
fi
RECEIVED_KIB=$(( RECEIVED_BYTES / 1024 ))

echo "   sync target received: ${RECEIVED_KIB} KiB (device ${VOL_MIB} MiB;"
echo "   a full resync — the regression — would be ~$(( VOL_MIB * 1024 )) KiB)"

if (( RECEIVED_KIB > MAX_RECEIVED_KIB )); then
    echo "FAIL: $N2 received ${RECEIVED_KIB} KiB > ${MAX_RECEIVED_KIB} KiB" >&2
    echo "      => the empty replica FULL-resynced (day0 skip broke)" >&2
    exit 1
fi

echo "PASS: Q3 — FILE_THIN renders no rs-discard-granularity and the fresh-create"
echo "      + mkfs day0 skip holds (no full resync of the empty device)"
