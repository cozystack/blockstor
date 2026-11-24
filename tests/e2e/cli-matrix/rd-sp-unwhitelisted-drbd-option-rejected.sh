#!/usr/bin/env bash
#
# usage: rd-sp-unwhitelisted-drbd-option-rejected.sh WORK_DIR
#
# L6 cli-matrix cell: `rd sp` of a DrbdOptions key LINSTOR's whitelist
# does not list (the typo DrbdOptions/AutoSnapshot/Keep) must be refused
# with FAIL_INVLD_PROP and store nothing, as LINSTOR does.
#
# Before the check the key was stored, every satellite rendered it into
# the options{} block of the .res, drbdadm refused the candidate file and
# each replica reported ApplyFailed until the key was deleted.
#
# Sequence:
#   1. rd c <rd>
#   2. rd sp <rd> DrbdOptions/AutoSnapshot/Keep 2      MUST be rejected
#   3. rd sp <rd> DrbdOptions/Resource/quorum-minimum-redundancy bogus
#                                                     MUST be rejected
#   4. the RD's props carry neither key
#   5. rd sp <rd> DrbdOptions/Resource/on-no-quorum suspend-io  accepted

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

RD=cli-whitelist-rd

cleanup() {
    delete_rd "$RD" 2>/dev/null || true
    linstor_cli_teardown
}
trap cleanup EXIT

# rd_prop RD KEY: the value of KEY from the RD's machine-readable
# list-properties, empty when absent.
rd_prop() {
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

expect_refused() {
    local key=$1 value=$2 out rc
    out=$(mktemp)
    set +e
    "${LCTL[@]}" resource-definition set-property "$RD" "$key" "$value" >"$out" 2>&1
    rc=$?
    set -e
    if (( rc == 0 )); then
        echo "FAIL: rd sp $RD $key $value was ACCEPTED" >&2
        cat "$out" >&2; rm -f "$out"; exit 1
    fi
    if ! grep -qF "$key" "$out"; then
        echo "FAIL: the refusal of $key does not name the key; got:" >&2
        cat "$out" >&2; rm -f "$out"; exit 1
    fi
    rm -f "$out"
    if [[ -n "$(rd_prop "$RD" "$key")" ]]; then
        echo "FAIL: the refused $key was stored on $RD" >&2
        exit 1
    fi
    echo ">>   rd sp $key $value rejected, nothing stored OK"
}

echo ">> rd c $RD"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }

expect_refused DrbdOptions/AutoSnapshot/Keep 2
expect_refused DrbdOptions/Resource/quorum-minimum-redundancy bogus

echo ">> rd sp $RD DrbdOptions/Resource/on-no-quorum suspend-io"
_out=$("${LCTL[@]}" resource-definition set-property "$RD" DrbdOptions/Resource/on-no-quorum suspend-io 2>&1) \
    || { echo "FAIL: a whitelisted option was refused: $_out" >&2; exit 1; }
if [[ "$(rd_prop "$RD" DrbdOptions/Resource/on-no-quorum)" != "suspend-io" ]]; then
    echo "FAIL: DrbdOptions/Resource/on-no-quorum did not land on $RD" >&2
    exit 1
fi

echo ">> rd-sp-unwhitelisted-drbd-option-rejected OK"
