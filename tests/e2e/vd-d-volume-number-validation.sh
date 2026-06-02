#!/usr/bin/env bash
#
# usage: vd-d-volume-number-validation.sh WORK_DIR
#
# Bug 365 (P2, hunt-caught 2026-06-02) — `vd d <rd> -1`,
# `vd d <rd> 65536`, `vd d <rd> 99999` all silently returned 200
# + "volume definition already absent" pre-fix. This e2e pins the
# post-fix contract: the same out-of-range inputs must surface a
# 400 envelope from the REST wire boundary, not the idempotent
# warn-mask the store-level miss produces.
#
# Mirrors tests/e2e/vd-volume-number-validation.sh (Bug 363 on the
# create path) — the contract is now symmetric across the entire
# VD CRUD surface.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

if ! command -v linstor >/dev/null 2>&1; then
    echo "FAIL: linstor CLI not in PATH (apt install linstor-client)" >&2
    exit 1
fi

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/vd-d-vn-pf.log 2>&1 &
PF_PID=$!

cleanup() {
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

# Seed a healthy RD so the gate fires on the volume_number alone,
# not on a missing parent.
RD=bug365-rd
"${LCTL[@]}" rd c "$RD"

for vn in -1 65536 99999; do
    echo ">> linstor vd d $RD $vn (Bug 365: must 4xx, NOT 'already absent')"
    # The CLI may format `-1` as a positional arg; pass via --
    # linstor-client (python) writes ERROR descriptions to stdout,
    # not stderr — capture both streams.
    if "${LCTL[@]}" vd d "$RD" -- "$vn" >/tmp/vd-d-err.txt 2>&1; then
        # If exit==0, the gate failed and the CLI happily told us
        # the bogus VlmNr was "absent" — the pre-Bug-365 mask.
        echo "FAIL: vd d $RD $vn returned exit 0 (pre-Bug-365 mask)"
        cat /tmp/vd-d-err.txt
        exit 1
    fi

    # The error body should mention "invalid" or the [0, 65535]
    # range — the writeVDNumberRejection envelope. Don't be too
    # strict on phrasing; pinning the exact CLI output would break
    # on golinstor changes. Strip ANSI colour escapes so grep -E
    # sees the plain text.
    plain=$(sed 's/\x1b\[[0-9;]*[A-Za-z]//g' /tmp/vd-d-err.txt)
    if ! echo "$plain" | grep -qE "invalid|65535|out of|bounds|range"; then
        echo "FAIL: vd d $RD $vn rejected but the message doesn't name the [0, 65535] range:"
        cat /tmp/vd-d-err.txt
        exit 1
    fi
    echo "   $vn correctly rejected"
done

# Cleanup
"${LCTL[@]}" rd d "$RD"

echo ">> PASS: vd d wire boundary rejects out-of-range volume_number"
