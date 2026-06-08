#!/usr/bin/env bash
#
# resend-409-tolerance-selftest.sh — hermetic unit test for the
# idempotent-409-after-success tolerance added for the python-linstor
# blind-resend e2e flake (fix/e2e-duplicate-post-tolerance).
#
# Background: the bundled `linstor` CLI uses python-linstor with
# keep_alive=True. On a dropped read of a POST response,
# linstorapi._rest_request_base UNCONDITIONALLY reconnects and re-sends the
# SAME request for any HTTP method. Through a flaky `kubectl port-forward`
# this resends a `resource create` whose first attempt already landed
# server-side; the controller answers 409 and the CLI surfaces exit 10 with
# stderr "... already exists" / "already diskful". The harness must tolerate
# that specific signature on a step that expected success, while still
# failing on any genuine error.
#
# This test needs NO cluster: it exercises the pure decision logic of
# `lctl_idempotent` (tests/e2e/lib.sh) and the `run_step` resend branch
# predicate (tests/operator-harness/lib.sh) with a stub CLI. Run it from
# `go test`-adjacent CI as a plain shell test:
#
#   tests/operator-harness/resend-409-tolerance-selftest.sh
#
# Exits 0 on all-pass, non-zero on the first failing case.

set -euo pipefail

FAILS=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; FAILS=$((FAILS + 1)); }

# ---- 1. lctl_idempotent (tests/e2e/lib.sh) decision logic --------------
#
# We re-declare the function here verbatim rather than sourcing
# tests/e2e/lib.sh, whose top-level `kubectl get nodes` would fail without
# a cluster. The body MUST stay in sync with lib.sh; the string-signature
# regex is the load-bearing part and is asserted below.
_LCTL_RESEND_409_RE='(object already exists|already diskful|already exists|already registered|already has a resource)'

lctl_idempotent() {
    local out rc
    set +e
    out=$("${LCTL[@]}" "$@" 2>&1)
    rc=$?
    set -e
    if (( rc == 0 )); then
        [[ -n "$out" ]] && printf '%s\n' "$out"
        return 0
    fi
    if printf '%s' "$out" | grep -Eqi "$_LCTL_RESEND_409_RE"; then
        echo "  note: tolerated idempotent-409-after-success (python-linstor resend): linstor $* -> '$out'" >&2
        return 0
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
}

# Guard: the regex declared above must be byte-identical to the one shipped
# in tests/e2e/lib.sh, or this self-test would assert a stale contract.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LIB_RE=$(grep -m1 '^_LCTL_RESEND_409_RE=' "$SCRIPT_DIR/../e2e/lib.sh" | cut -d= -f2-)
if [[ "$LIB_RE" == "'$_LCTL_RESEND_409_RE'" ]]; then
    pass "regex in selftest matches tests/e2e/lib.sh"
else
    fail "regex drifted from tests/e2e/lib.sh (lib='$LIB_RE')"
fi

# Stub CLI driven by env: STUB_EXIT / STUB_OUT.
stub_cli() {
    [[ -n "${STUB_OUT:-}" ]] && printf '%s\n' "$STUB_OUT"
    return "${STUB_EXIT:-0}"
}
LCTL=(stub_cli)

# Case A: success passes through.
STUB_EXIT=0 STUB_OUT="created" ; if out=$(lctl_idempotent resource create n rd 2>/dev/null); then
    [[ "$out" == "created" ]] && pass "A: exit 0 passes through with stdout" \
        || fail "A: stdout not relayed (got '$out')"
else
    fail "A: exit 0 should return success"
fi

# Case B: resend-409 "already diskful: object already exists" is tolerated.
STUB_EXIT=10 STUB_OUT='resource "rd" on node "n" already diskful: object already exists'
if lctl_idempotent resource create n rd >/dev/null 2>&1; then
    pass "B: 'already diskful: object already exists' tolerated (exit 0)"
else
    fail "B: resend-409 signature should be tolerated"
fi

# Case C: bare "object already exists" tolerated.
STUB_EXIT=10 STUB_OUT='object already exists'
if lctl_idempotent resource create n rd >/dev/null 2>&1; then
    pass "C: bare 'object already exists' tolerated"
else
    fail "C: bare already-exists should be tolerated"
fi

# Case D: a genuine non-resend error is NOT swallowed.
STUB_EXIT=10 STUB_OUT='storage pool "zfs" not found on node "n"'
if lctl_idempotent resource create n rd >/dev/null 2>&1; then
    fail "D: a real error (pool not found) must NOT be tolerated"
else
    pass "D: real error propagated (non-zero exit)"
fi

# Case E: connection error is NOT swallowed.
STUB_EXIT=20 STUB_OUT='Unable to connect to controller'
if lctl_idempotent resource create n rd >/dev/null 2>&1; then
    fail "E: connection error must NOT be tolerated"
else
    pass "E: connection error propagated"
fi

# ---- 2. run_step predicate (_expect_includes_zero) ---------------------
_expect_includes_zero() {
    local c
    for c in "$@"; do
        [[ "$c" == "0" ]] && return 0
    done
    return 1
}

if _expect_includes_zero 0; then pass "F: expect=[0] includes zero"; else fail "F"; fi
if _expect_includes_zero 0 10; then pass "G: expect=[0,10] includes zero"; else fail "G"; fi
if _expect_includes_zero 10; then fail "H: expect=[10] must NOT include zero"; else pass "H: expect=[10] excludes zero"; fi
if _expect_includes_zero 9 20; then fail "I: expect=[9,20] must NOT include zero"; else pass "I: non-zero-only excludes zero"; fi

# The run_step branch additionally gates on the same stderr signature; the
# regex is asserted byte-identical to the operator-harness lib.sh copy.
HARNESS_RE=$(grep -m1 -oE "\(object already exists\|already diskful\|already exists\|already registered\|already has a resource\)" "$SCRIPT_DIR/lib.sh" || true)
if [[ -n "$HARNESS_RE" ]]; then
    pass "J: run_step resend-409 signature present in operator-harness lib.sh"
else
    fail "J: run_step resend-409 signature missing from operator-harness lib.sh"
fi

echo
if (( FAILS == 0 )); then
    echo "PASS: resend-409 tolerance self-test (all cases)"
    exit 0
fi
echo "FAIL: $FAILS case(s) failed" >&2
exit 1
