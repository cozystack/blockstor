#!/usr/bin/env bash
#
# usage: rd-c-layer-list-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case D7b (linstor-administration.adoc
# ~1819-1833): `--layer-list` negative cases.
#
# Upstream LINSTOR validates the layer stack at RD-create time and
# rejects malformed inputs. blockstor mirrors the OUTCOME (rejection)
# at the REST wire boundary; the error-envelope wire shape diverges
# (documented in docs/cli-parity-known-deltas.md row D7b).
#
# Three cases:
#   1. invalid order `luks,drbd,storage`  — LUKS must sit below DRBD,
#      not above it (DRBD must replicate ciphertext). CLI sends it;
#      controller rejects.
#   2. duplicate layer `drbd,drbd,storage` — CLI sends it; controller
#      rejects, naming the duplicate.
#   3. unknown layer `bogus,storage` — the python-linstor CLI rejects
#      this CLIENT-side (it never reaches the controller), so we assert
#      the server-side allowlist via a direct REST POST: a hand-rolled
#      caller that skips the client check MUST still be refused.
#
# Contract: every case exits non-zero (CLI) / returns 400 (REST) and
# leaves NO ResourceDefinition CRD behind.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

RD_ORDER=cli-d7b-order
RD_DUP=cli-d7b-dup
RD_UNK=cli-d7b-unknown

cleanup() {
    # Best-effort delete in case any rejected create unexpectedly persisted.
    delete_rd "$RD_ORDER" 2>/dev/null || true
    delete_rd "$RD_DUP" 2>/dev/null || true
    delete_rd "$RD_UNK" 2>/dev/null || true
    linstor_cli_teardown
}
trap cleanup EXIT

# assert_rd_absent: the rejected create must not leak an RD CRD.
assert_rd_absent() {
    local rd=$1
    if kubectl get "resourcedefinitions.blockstor.cozystack.io/${rd}" >/dev/null 2>&1; then
        echo "FAIL (D7b): rejected create leaked RD CRD ${rd}" >&2
        return 1
    fi
    return 0
}

# ---- case 1: invalid order luks,drbd,storage (CLI path) -------------------
echo ">> [D7b] rd c $RD_ORDER --layer-list luks,drbd,storage (must be rejected)"
out=$(mktemp)
set +e
"${LCTL[@]}" resource-definition create "$RD_ORDER" --layer-list luks,drbd,storage >"$out" 2>&1
rc=$?
set -e
if (( rc == 0 )); then
    echo "FAIL (D7b): luks,drbd,storage accepted; must be rejected" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
if ! grep -qiE 'invalid layer order|layer' "$out"; then
    echo "FAIL (D7b): order-rejection envelope must mention the layer order; got:" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
rm -f "$out"
assert_rd_absent "$RD_ORDER"
echo ">>   case 1 OK (invalid order rejected)"

# ---- case 2: duplicate layer drbd,drbd,storage (CLI path) -----------------
echo ">> [D7b] rd c $RD_DUP --layer-list drbd,drbd,storage (must be rejected)"
out=$(mktemp)
set +e
"${LCTL[@]}" resource-definition create "$RD_DUP" --layer-list drbd,drbd,storage >"$out" 2>&1
rc=$?
set -e
if (( rc == 0 )); then
    echo "FAIL (D7b): drbd,drbd,storage accepted; must be rejected" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
if ! grep -qiE 'more than once|invalid layer order|layer' "$out"; then
    echo "FAIL (D7b): duplicate-rejection envelope must mention the layer fault; got:" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
rm -f "$out"
assert_rd_absent "$RD_DUP"
echo ">>   case 2 OK (duplicate layer rejected)"

# ---- case 3: unknown layer (server-side allowlist via raw REST) -----------
# The python-linstor CLI rejects "bogus" client-side, so we POST the raw
# JSON directly to confirm the controller's own allowlist holds.
echo ">> [D7b] raw REST POST rd create $RD_UNK with unknown layer 'bogus' (must be 400)"
code=$(curl -s -m 5 -o /tmp/d7b-unk.json -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"resource_definition\":{\"name\":\"${RD_UNK}\",\"layer_data\":[{\"type\":\"bogus\"},{\"type\":\"storage\"}]}}" \
    "http://localhost:${LCTL_PORT}/v1/resource-definitions")
if [[ "$code" != "400" ]]; then
    echo "FAIL (D7b): unknown-layer POST returned $code, want 400" >&2
    cat /tmp/d7b-unk.json >&2; exit 1
fi
if ! grep -qi 'unsupported layer' /tmp/d7b-unk.json; then
    echo "FAIL (D7b): unknown-layer envelope must mention 'unsupported layer'; got:" >&2
    cat /tmp/d7b-unk.json >&2; exit 1
fi
rm -f /tmp/d7b-unk.json
assert_rd_absent "$RD_UNK"
echo ">>   case 3 OK (unknown layer refused server-side)"

echo ">> rd-c-layer-list-rejected OK (D7b pinned: invalid-order / duplicate / unknown-layer all refused, no RD leaked)"
