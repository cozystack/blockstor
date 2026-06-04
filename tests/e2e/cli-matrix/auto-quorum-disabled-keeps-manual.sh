#!/usr/bin/env bash
#
# usage: auto-quorum-disabled-keeps-manual.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case B1/B4 (auto-quorum × manual quorum).
#
# Upstream LINSTOR (UG9 §"Auto-quorum policies", ~4233-4279):
#   - DrbdOptions/auto-quorum (kebab-case key) controls the
#     auto-quorum reconciler. Setting it to `disabled` hands quorum
#     policy control to the operator.
#   - IMPORTANT: while auto-quorum is NOT disabled the automatism
#     OVERRIDES any manually configured quorum property.
#
# blockstor pre-fix read the camelCase `DrbdOptions/AutoQuorum` key,
# which no production path ever wrote, so a real CLI
# `set-property DrbdOptions/auto-quorum disabled` was silently ignored:
# the reconciler kept re-stamping quorum=majority on the 2-diskful+TB
# shape every pass, clobbering the operator's manual `quorum off`.
#
# This cell drives the real python-linstor CLI:
#   1. auto-place 2 → 2 diskful + auto-TieBreaker, auto-quorum default
#      majority drives quorum=majority.
#   2. B4 check: with auto-quorum still active (default), a manual
#      `quorum off` is OVERRIDDEN back to majority by the automatism.
#   3. B1 check: `set-property DrbdOptions/auto-quorum disabled` then
#      `quorum off` — now the manual value STICKS across reconciles,
#      and the TieBreaker stays (gated on a separate prop).
#
# Unit pin: internal/controller/ensure_tiebreaker_test.go
# (TestIsAutoQuorumDisabled / TestEnsureTiebreakerHonoursAutoQuorumDisabled).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-b1
AUTOQ_KEY="DrbdOptions/auto-quorum"
QUORUM_KEY="DrbdOptions/Resource/quorum"

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# rd_prop_value RD KEY — echo the value of KEY from the RD's
# machine-readable list-properties, empty string if absent.
rd_prop_value() {
    "${LCTL[@]}" -m resource-definition list-properties "$1" 2>/dev/null \
        | KEY="$2" python3 -c "import json,sys,os
key=os.environ['KEY']
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(0)
while isinstance(d, list) and d and isinstance(d[0], list):
    d=d[0]
for it in d if isinstance(d, list) else []:
    if isinstance(it, dict) and it.get('key')==key:
        print(it.get('value','')); sys.exit(0)
"
}

# wait_rd_prop RD KEY EXPECTED TIMEOUT — poll until KEY==EXPECTED.
wait_rd_prop() {
    local rd=$1 key=$2 want=$3 to=$4 deadline got
    deadline=$(( $(date +%s) + to ))
    while (( $(date +%s) < deadline )); do
        got=$(rd_prop_value "$rd" "$key")
        [[ "$got" == "$want" ]] && return 0
        sleep 2
    done
    echo "FAIL: $key never reached '$want' within ${to}s (last='$got')" >&2
    "${LCTL[@]}" resource-definition list-properties "$rd" 2>&1 | tail -20 >&2
    return 1
}

# assert_rd_prop_holds RD KEY EXPECTED SECONDS — verify KEY stays at
# EXPECTED for the whole window (used to prove the automatism does NOT
# revert the operator's value once disabled).
assert_rd_prop_holds() {
    local rd=$1 key=$2 want=$3 secs=$4 deadline got
    deadline=$(( $(date +%s) + secs ))
    while (( $(date +%s) < deadline )); do
        got=$(rd_prop_value "$rd" "$key")
        if [[ "$got" != "$want" ]]; then
            echo "FAIL: $key drifted to '$got' (want '$want' to hold for ${secs}s)" >&2
            return 1
        fi
        sleep 3
    done
    return 0
}

echo ">> [B1/B4] shape-2r-tb: 2-replica RD + auto-tiebreaker"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool=stand "$RD" >/dev/null

echo ">> wait for quorum=majority (auto-quorum default seed)"
wait_rd_prop "$RD" "$QUORUM_KEY" "majority" 180

echo ">> [B4] auto-quorum ACTIVE: manual quorum=off must be OVERRIDDEN back to majority"
"${LCTL[@]}" resource-definition set-property "$RD" "$QUORUM_KEY" off >/dev/null
# The automatism re-asserts majority on the next reconcile pass.
if ! wait_rd_prop "$RD" "$QUORUM_KEY" "majority" 45; then
    echo "FAIL (B4): active auto-quorum did not override manual quorum=off" >&2
    exit 1
fi
echo "   B4 OK: automatism wins while auto-quorum is active"

echo ">> [B1] set auto-quorum=disabled (canonical kebab key), then manual quorum=off"
"${LCTL[@]}" resource-definition set-property "$RD" "$AUTOQ_KEY" disabled >/dev/null
"${LCTL[@]}" resource-definition set-property "$RD" "$QUORUM_KEY" off >/dev/null

echo ">> manual quorum=off must STICK (no revert) for >= 30s of reconciles"
if ! assert_rd_prop_holds "$RD" "$QUORUM_KEY" "off" 30; then
    echo "FAIL (B1): auto-quorum=disabled did not take effect — quorum reverted" >&2
    exit 1
fi
echo "   B1 OK: disabled auto-quorum leaves the operator's manual value alone"

echo ">> TieBreaker must still be present (gated on a separate prop, not auto-quorum)"
deadline=$(( $(date +%s) + 30 ))
tb_present=false
while (( $(date +%s) < deadline )); do
    if "${LCTL[@]}" resource list --resources "$RD" 2>/dev/null | grep -q 'TieBreaker'; then
        tb_present=true
        break
    fi
    sleep 2
done
if [[ "$tb_present" != "true" ]]; then
    echo "FAIL (B1): TieBreaker disappeared after disabling auto-quorum" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> auto-quorum-disabled-keeps-manual OK (B1/B4 pinned)"
