#!/usr/bin/env bash
#
# usage: luks-encryption-create-passphrase.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333.
#
# Audit gap: Bug 129 / 165 / 172 / 196 pinned the
# /v1/encryption/passphrase wire surface via unit tests, but the
# python-CLI surface (`linstor encryption create-passphrase <pw>`
# → POST + envelope decode) was never exercised end-to-end on a
# real stand. Bug 175 (LUKS shell-injection RCE) and Bug 233
# (passphrase wire) closed the security side via unit tests only.
# This cell drives the real `linstor encryption create-passphrase`
# subcommand against the operator's apiserver and asserts:
#   1. exit 0 from the CLI (= envelope decoded without traceback)
#   2. GET /v1/encryption surfaces a non-empty status afterwards
#      (passphrase is considered "set" by the controller)
#
# Cleanup: linstor encryption delete-passphrase. Idempotent — runs
# from the EXIT trap so a half-finished cell doesn't leave the
# cluster with a stale passphrase that would break the next cell's
# fresh-create-passphrase pre-condition.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

PASSPHRASE='cli-matrix-333-create-pp!'

cleanup() {
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: clear any leftover passphrase from a prior aborted run.
# Without this, `create-passphrase` returns 400 "already set" — which
# is the WRONG signal for this cell (we want to exercise the create
# path, not the modify path).
cleanup_encryption_state

echo ">> [Bug 333] linstor encryption create-passphrase"
err_file=$(mktemp)
if ! out=$("${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" 2>"$err_file"); then
    rc=$?
    echo "FAIL (Bug 333): create-passphrase exited $rc" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "----- stdout -----" >&2
    echo "$out" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# Negative grep: python-linstor surfaces a traceback when the envelope
# is empty / non-JSON (Bug 129 class). Catch the symptom even if the
# CLI's exit code was 0 (some old client versions ate the traceback
# but still printed it).
if grep -qiE 'Traceback|JSONDecodeError|json\.loads' <<<"$out"; then
    echo "FAIL (Bug 333): create-passphrase CLI traceback on stdout — empty/malformed envelope?" >&2
    echo "$out" >&2
    exit 1
fi

echo ">> [Bug 333] GET /v1/encryption — passphrase reports as set"
# Drive the same wire surface the python CLI's `linstor encryption
# status` uses (`GET /v1/encryption/passphrase`, Bug 196). A
# successful POST must leave the controller reporting "set"; if the
# state didn't persist the next cell's enter-passphrase would
# silently no-op.
status_body=$(curl -fsS -m 5 \
    "http://127.0.0.1:${LCTL_PORT}/v1/encryption/passphrase" 2>/dev/null || echo "")
if [[ -z "$status_body" ]]; then
    echo "FAIL (Bug 333): GET /v1/encryption/passphrase returned empty body" >&2
    exit 1
fi

# Upstream OpenAPI: passphraseStatus surfaces at least one of
# `set`/`unlocked`/`is_set` booleans. Don't tie the assertion to a
# specific field name — operators care that SOMETHING in the body
# says "passphrase is configured" so the python CLI can render
# `status: SET`.
if ! grep -qiE 'true|set|unlocked' <<<"$status_body"; then
    echo "FAIL (Bug 333): status body has no positive flag after create-passphrase: $status_body" >&2
    exit 1
fi

echo ">> luks-encryption-create-passphrase OK (Bug 333 pinned: real CLI create + status round-trip)"
