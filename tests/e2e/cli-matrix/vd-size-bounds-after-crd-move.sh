#!/usr/bin/env bash
#
# usage: vd-size-bounds-after-crd-move.sh WORK_DIR
#
# L6 cli-matrix cell — the volume-size bound survived moving off the CRD.
#
# The bound used to live on the CRD as well, as
# `+kubebuilder:validation:Minimum` on spec.volumeDefinitions[].sizeKib. It
# came off because spec.volumeDefinitions carries no list-map key: the list is
# atomic, Kubernetes correlates it as a whole for ratcheting, and any update
# touching it re-validates every element. One grandfathered sub-floor volume
# would therefore reject every later write to that definition — the
# controller's own included, since patchRDVolumeMinors rewrites the list to
# stamp drbdMinor.
#
# That trade gives up the `kubectl apply` backstop. This cell exists to pin
# what is left: the writers must still refuse. It drives the same path an
# operator does — CLI to REST to store — and checks both ends of the range
# plus one size inside it, so a refusal that grew into a blanket denial fails
# here too.
#
# If this goes red, the bound has been lost rather than moved, and any volume
# below DRBD's floor loops on `drbdadm create-md` instead of failing.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

# No replica is placed here: the bound is enforced when the volume
# definition is written, so declaring one is enough to exercise it.
RD="cc-vd-bounds-$$"

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> create the definition"
"${LCTL[@]}" resource-definition create "$RD"

echo ">> a size below DRBD's floor must be refused"
if "${LCTL[@]}" volume-definition create "$RD" 1024KiB >/dev/null 2>&1; then
    echo "FAIL: 1024 KiB accepted; it is below the 4 MiB floor and the volume"
    echo "      would loop on create-md instead of failing"
    exit 1
fi

echo ">> a size past DRBD's ceiling must be refused"
if "${LCTL[@]}" volume-definition create "$RD" 2P >/dev/null 2>&1; then
    echo "FAIL: 2 PiB accepted; it is past DRBD 9's per-device ceiling"
    exit 1
fi

echo ">> a size inside the range must still be accepted"
"${LCTL[@]}" volume-definition create "$RD" 100M

# The ceiling moved up with the rule: it used to be 16 TiB, which is below
# what DRBD 9 and upstream LINSTOR handle, and linstormigrate copies sizes
# verbatim — so a cluster holding a larger volume failed part-way through its
# own migration. A definition of that size is not placed here, only declared,
# which is enough to prove the writer accepts it.
echo ">> a large but legal size must be accepted (the old 16 TiB ceiling was too low)"
"${LCTL[@]}" volume-definition create "$RD" 20T --vlmnr 1

echo "PASS vd-size-bounds-after-crd-move"
