#!/usr/bin/env bash
#
# usage: r-td-diskless.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 330.
#
# Reproduction: on a 2-replica diskful RD, issue
# `linstor r td --diskless <node> <rd>`. The CLI maps this to PUT
# /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless,
# which must:
#
#   1. Flip Spec.Flags to include DISKLESS (CRD-layer mutation).
#   2. Drive the satellite reconciler to tear down the LV, then
#      detach the drbd backing device, leaving the slot as
#      `disk:Diskless` on the DRBD layer.
#   3. Surface the change on Resource.Status within 30s.
#
# Pre-fix the diskless-toggle path was a no-op on diskful Resources
# (it only worked on already-Diskless ones being promoted to
# diskful) — the operator's command appeared to succeed but the
# replica stayed diskful forever.
#
# Contract: assert all three legs (CRD flag, observer DiskState,
# kernel disk: state).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-330

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2

echo ">> [Bug 330] 2-replica diskful RD on $N1+$N2"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand >/dev/null

echo ">> wait for both diskful UpToDate"
RD="$RD" wait_uptodate "$RD" "$N1" "$N2"

echo ">> linstor r td --diskless $N2 $RD (Bug 330 trigger)"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource toggle-disk --diskless "$N2" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 330): r td --diskless exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 30s for $N2 to converge to Diskless (CRD flag + observer DiskState)"
if ! wait_status_diskless "$RD" "$N2" 30; then
    echo "FAIL (Bug 330 regression): $N2 never converged to Diskless within 30s" >&2
    kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o json 2>/dev/null | jq '{flags: .spec.flags, status: .status}' >&2 || true
    exit 1
fi

# Kernel-leg confirmation: drbdsetup status on $N2 must report
# `disk:Diskless` for the volume (or the RD is gone from drbd
# entirely, which also counts as Diskless from the operator's POV).
echo ">> kernel probe on $N2 (disk:Diskless or RD absent from drbd)"
ker=$(on_node "$N2" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
if [[ -n "$ker" ]] && ! grep -qE 'disk:Diskless' <<<"$ker"; then
    echo "FAIL (Bug 330 deep): observer says Diskless on $N2 but kernel reports otherwise" >&2
    echo "$ker" >&2
    exit 1
fi

# Sibling check: $N1 must still be UpToDate (the toggle on $N2 must
# NOT have demoted $N1 as a side-effect — that would mean the
# toggle path is mistakenly mutating the sibling instead of the
# target replica).
n1_disk=$(status_disk_state "$RD" "$N1" 0)
if [[ "$n1_disk" != "UpToDate" ]]; then
    echo "FAIL (Bug 330 sibling regression): $N1 disk_state=$n1_disk after toggle on $N2 (want UpToDate)" >&2
    exit 1
fi

echo ">> r-td-diskless OK (Bug 330 pinned: diskful→diskless toggle flips Spec.Flags + Status + kernel)"
