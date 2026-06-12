#!/usr/bin/env bash
#
# usage: encryption-passphrase-luks-rd.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 023 (fix: encryption create-passphrase
# unlocks LUKS provisioning).
#
# Audit gap: `linstor encryption create-passphrase` stored the cluster
# master passphrase in the blockstor-cluster-passphrase Secret, but
# nothing downstream read it:
#   - the LUKS RD-create gate only consulted the legacy
#     DrbdOptions/EncryptPassphrase controller property, so the
#     upstream-standard flow (create-passphrase → rd c -l
#     drbd,luks,storage) was rejected with "LUKS layer requires
#     DrbdOptions/EncryptPassphrase to be set first" — and the hint
#     told operators to store a PLAINTEXT passphrase in a controller
#     prop;
#   - the satellite lifted the LUKS key onto the LuksPassphrase wire
#     prop only from controller-scope props, so a Secret-only cluster
#     looped on "LUKS in layer stack but Props.LuksPassphrase empty"
#     at apply time.
#
# Post-fix contract (pinned here): the Secret set by `encryption
# create-passphrase` is the PRIMARY, upstream-parity key source — the
# whole LUKS lifecycle must work WITHOUT the legacy controller prop
# ever being set. The sibling cells (luks-rd-create-encrypted.sh,
# luks-clone-encrypted.sh, replay/luks-encrypted-rd.yaml) still set
# the legacy prop and keep covering the deprecated path.
#
# Flow + assertions:
#   1. cleanup_encryption_state → known-clean baseline (no Secret, no
#      legacy prop).
#   2. linstor encryption create-passphrase --passphrase <pw> → exit 0.
#   3. legacy prop ABSENT on `controller list-properties` (and stays
#      absent through the whole cell — provisioning must not depend on
#      anything writing it behind our back).
#   4. rd c -l drbd,luks,storage → exit 0 (pre-fix: rejected).
#   5. vd c + r c --auto-place=2 → both diskful replicas UpToDate.
#   6. kernel-level proof on EACH replica: backing LV/zvol carries a
#      real LUKS header AND the cluster passphrase opens it
#      (cryptsetup --test-passphrase) — the Secret value travelled the
#      satellite channel to luksFormat, not just past the REST gate.
#
# Unit pins: pkg/rest/luks_gate_bug023_test.go,
# pkg/satellite/controllers/luks_passphrase_internal_test.go. This
# cell is the stand-side companion: real python-linstor → apiserver →
# satellite → cryptsetup.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-023-pp-luks
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-023-secret-pp!'

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

# assert_legacy_prop_absent <phase> — the Bug 023 core invariant: the
# deprecated DrbdOptions/EncryptPassphrase controller property must
# never appear during the Secret-only flow. Checked via the same
# machine-readable list-properties surface the python CLI renders.
assert_legacy_prop_absent() {
    local phase=$1
    local present
    present=$("${LCTL[@]}" --machine-readable controller list-properties 2>/dev/null \
        | jq -r '[.. | objects | select(.key == "DrbdOptions/EncryptPassphrase")] | length' \
        2>/dev/null || echo 0)
    if [[ "$present" != "0" ]]; then
        echo "FAIL (Bug 023): legacy DrbdOptions/EncryptPassphrase controller prop present ($phase)" >&2
        echo "  the Secret-only flow must not set or require it" >&2
        exit 1
    fi
}

echo ">> [Bug 023] pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes) — encrypted-RD autoplace fixture unavailable"
    exit 0
fi

# Known-clean baseline: no passphrase Secret, no legacy controller
# prop. Without this the create-passphrase below answers "already
# set" and the cell would silently test the wrong (modify) path.
cleanup_encryption_state

echo ">> [Bug 023] linstor encryption create-passphrase (Secret-only flow)"
err_file=$(mktemp)
if ! "${LCTL[@]}" encryption create-passphrase --passphrase "$PASSPHRASE" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 023): create-passphrase exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 023] legacy DrbdOptions/EncryptPassphrase prop is ABSENT"
assert_legacy_prop_absent "after create-passphrase"

echo ">> [Bug 023] linstor rd c $RD -l drbd,luks,storage (no legacy prop set)"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource-definition create "$RD" \
        --layer-list drbd,luks,storage 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 023): rd create rejected (exit $rc) — Secret-backed passphrase not accepted by the LUKS gate?" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 023] linstor vd c $RD 128M"
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null

echo ">> [Bug 023] linstor r c $RD --auto-place=2 -s $POOL"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 023): encrypted auto-place=2 exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 023] wait for 2 diskful Resource CRDs to land"
# auto-place=2 may add a DISKLESS TIE_BREAKER witness on top of the 2
# diskful replicas — count diskful only (same convention as the other
# autoplace cells) so the luksDump checks never target a backing-less
# witness.
deadline=$(( $(date +%s) + 60 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(linstor_diskful_nodes "$RD")
    if (( ${#placed_nodes[@]} == 2 )); then
        break
    fi
    sleep 2
done
if (( ${#placed_nodes[@]} != 2 )); then
    echo "FAIL (Bug 023): autoplace did not stage 2 diskful Resource CRDs within 60s (got ${#placed_nodes[@]})" >&2
    echo "   all replicas: $(linstor_replica_count "$RD"), tiebreaker: $(linstor_tiebreaker_node "$RD")" >&2
    exit 1
fi
echo "   placed (diskful) on: ${placed_nodes[*]}"

N1="${placed_nodes[0]}"
N2="${placed_nodes[1]}"

echo ">> [Bug 023] wait both replicas UpToDate (Secret-fed luksFormat ran)"
# Pre-fix failure mode for a gate-only patch: rd-create passes but the
# satellite loops on "LUKS in layer stack but Props.LuksPassphrase
# empty" and the replicas never converge. UpToDate within the bound is
# the proof the Secret reached the satellite channel.
wait_uptodate "$RD" "$N1" "$N2"

echo ">> [Bug 023] legacy prop STILL absent after provisioning"
assert_legacy_prop_absent "after provisioning"

echo ">> [Bug 023] LUKS header present + Secret passphrase opens it on EACH replica"
for node in "$N1" "$N2"; do
    backing=$(luks_backing_device "$RD" "$node" 0)
    if [[ -z "$backing" ]]; then
        echo "FAIL (Bug 023): could not resolve backing device for $RD on $node" >&2
        exit 1
    fi
    echo "   $node: backing=$backing"
    if ! wait_luks_header_present "$node" "$backing" 60; then
        echo "FAIL (Bug 023): no LUKS header on $node:$backing" >&2
        exit 1
    fi
    if ! assert_luks_passphrase_opens "$node" "$backing" "$PASSPHRASE"; then
        echo "FAIL (Bug 023): Secret-backed passphrase does not unlock $node:$backing" >&2
        exit 1
    fi
done

echo ">> encryption-passphrase-luks-rd OK (Bug 023: Secret-only passphrase provisions LUKS end-to-end, no legacy prop)"
