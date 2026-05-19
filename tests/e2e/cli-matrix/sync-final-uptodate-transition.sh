#!/usr/bin/env bash
#
# usage: sync-final-uptodate-transition.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 329.
#
# Reproduction: add a 3rd diskful replica to an existing 2-replica
# RD. The new replica goes UpToDate(0%) → UpToDate(50%) → … →
# UpToDate(100%) and then must drop the (NN%) suffix entirely once
# replication settles into the Established state. Pre-fix the
# annotateSyncProgress decorator kept the percent suffix even after
# OutOfSyncKib reached 0, leaving `linstor r l` State stuck on
# "UpToDate(100%)" forever — confusing operators into thinking the
# sync was perpetually in-progress.
#
# Contract: poll observer-stamped Status until BOTH:
#   - status.volumes[0].diskState == "UpToDate" (bare, no "(NN%)" suffix)
#   - status.connections[node1<->node3].replicationState == "Established"
#
# Cross-check on the satellite pod: `drbdsetup status RD --verbose`
# shows `replication:Established` and `disk:UpToDate` with no
# `(NN%)` progress on the relevant peer-device line.
#
# Timeout: 240s. Initial sync of a fresh 1G volume on a busy QEMU
# stand plus the UpToDate-decoration race can easily take 90-180s,
# so the budget includes a safety margin.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-329

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

echo ">> [Bug 329] 2-replica RD on $N1+$N2 (1 GiB so the sync window is observable)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool stand >/dev/null

echo ">> wait for the initial pair UpToDate"
RD="$RD" wait_uptodate "$RD" "$N1" "$N2"

echo ">> add 3rd diskful replica on $N3 — Bug 329 trigger"
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool stand >/dev/null

echo ">> wait up to 240s for $N3 sync to fully drain — bare 'UpToDate' AND replication 'Established'"
if ! wait_sync_done "$RD" "$N3" "$N1" 240; then
    echo "FAIL (Bug 329 regression): $N3 never reached (bare UpToDate, Established) within 240s" >&2

    # Diagnostic dump: show the observer's last view + the satellite's
    # raw drbdsetup status. The pre-fix bug shows up as
    # `diskState: "UpToDate(100%)"` on the CRD plus a
    # `replication:Established` line in drbdsetup status — the wire
    # didn't shed the percent suffix.
    kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
        -o json 2>/dev/null | jq '{flags: .spec.flags, status: .status}' >&2 || true
    on_node "$N3" drbdsetup status --verbose "$RD" 2>&1 >&2 || true
    exit 1
fi

# Belt-and-suspenders kernel probe — confirm the bare-UpToDate
# observer claim matches what `drbdsetup status` reports. If the
# observer is buggy and stamps bare UpToDate while the kernel
# still says (50%), this test catches it.
echo ">> kernel probe on $N3"
ker=$(on_node "$N3" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
if grep -qE 'disk:UpToDate\([0-9]+%\)' <<<"$ker"; then
    echo "FAIL (Bug 329 deep): observer says bare UpToDate but kernel still reports UpToDate(NN%) on $N3" >&2
    echo "$ker" >&2
    exit 1
fi
if ! grep -qE 'replication:Established' <<<"$ker"; then
    echo "FAIL (Bug 329 deep): kernel did not report 'replication:Established' for $N3<->$N1" >&2
    echo "$ker" >&2
    exit 1
fi

echo ">> sync-final-uptodate-transition OK (Bug 329 pinned: bare UpToDate + Established on $N3)"
