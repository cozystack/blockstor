#!/usr/bin/env bash
#
# usage: rg-spawn-size-parse.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 391 (spawn-size unit).
#
# Upstream LINSTOR contract: `linstor rg spawn-resources <rg> <rd> 32M`
# creates a 32 MiB volume — i.e. a VolumeDefinition of 32768 KiB. The
# size token is parsed by the python client into KiB
# (`parse_volume_size_to_kib("32M") == 32768`) and shipped on the wire
# as the `volume_sizes` array, which the upstream REST API documents as
# "sizes (in kib)".
#
# The bug: blockstor's spawn handler treated `volume_sizes` as BYTES
# and divided every entry by 1024, so `32M` (wire value 32768) landed
# as a 32 KiB VolumeDefinition. 32 KiB is below DRBD's ~4 MiB
# per-device floor, so the satellite reconciler then hot-looped on
# `drbdadm create-md`.
#
# Reproduction:
#
#   $ linstor resource-group create rg --place-count 1 -s stand
#   $ linstor resource-group spawn-resources rg rd 32M
#   $ linstor volume-definition list -r rd      # size MUST be 32 MiB
#
# This cell pins the size half end-to-end against the real CLI: after a
# `32M` spawn, the VolumeDefinition size_kib MUST be exactly 32768, and
# the spawned replica MUST reach UpToDate (which a 32 KiB VD never
# could — create-md would loop). A regression that re-introduces the
# /1024 divide lands a 32 KiB VD and this cell goes red on both the
# size assertion and the UpToDate wait.
#
# Unit pin: pkg/rest/bug_391_spawn_size_kib_test.go.
# L7 replay: tests/operator-harness/replay/rg-spawn-size-parse.yaml.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

RG=cli-matrix-bug391-rg
RD=cli-matrix-bug391-rd
POOL=${POOL:-stand}

# 32M == 32 MiB == 32768 KiB. This is the exact value the python client
# puts on the wire for the operator's `32M` argument.
WANT_KIB=32768

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug391] rg create --place-count 1 + spawn-resources $RD 32M"
"${LCTL[@]}" resource-group create "$RG" --place-count 1 --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M >/dev/null

echo ">> [Bug391] VolumeDefinition size MUST be ${WANT_KIB} KiB (32 MiB), NOT 32 KiB"
if ! wait_vd_size "$RD" 0 "$WANT_KIB" 60; then
    got=$(linstor_vd_size_kib "$RD" 0)
    echo "FAIL (Bug391): VD size_kib=${got}, want ${WANT_KIB} (32M must NOT be divided to 32 KiB)" >&2
    "${LCTL[@]}" volume-definition list --resource-definitions "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> VD size = ${WANT_KIB} KiB — OK"

echo ">> [Bug391] the spawned replica MUST reach UpToDate (a 32 KiB VD never could)"
if ! wait_replica_count "$RD" 1 90; then
    echo "FAIL (Bug391): RD did not reach 1 replica" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

deadline=$(( $(date +%s) + 240 ))
all_up=false
while (( $(date +%s) < deadline )); do
    up=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .volumes[]? | select(.provider_kind != "DISKLESS") | .state.disk_state]
                 | map(select(. == "UpToDate")) | length' 2>/dev/null || echo 0)
    if (( up >= 1 )); then
        all_up=true
        break
    fi
    sleep 3
done

if [[ "$all_up" != "true" ]]; then
    echo "FAIL (Bug391): replica never reached UpToDate (32 KiB VD would loop on create-md)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> rg-spawn-size-parse OK (Bug391 pinned: 32M spawns a 32768 KiB VD, replica UpToDate)"
