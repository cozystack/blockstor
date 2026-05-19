#!/usr/bin/env bash
#
# usage: luks-encryption-enter-passphrase.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 333 (enter-passphrase branch).
#
# Audit gap: Bug 165 pinned the dual-key wire surface
# (`new_passphrase` vs `passphrase`) for PATCH /v1/encryption/passphrase
# via a unit test, but no e2e exercised the full
#   create → controller restart → enter
# cycle. The controller's in-process `passphraseUnlocked` cache must
# be re-populated by the first `linstor encryption enter-passphrase`
# call after a pod restart; if that flow regresses, every encrypted
# RD on the cluster becomes inaccessible until the operator pages.
#
# Scenario:
#   1. Create-passphrase on a clean cluster
#   2. kubectl rollout restart deployment/blockstor-apiserver
#      → forces the controller process to drop its
#        passphraseUnlocked flag; the Secret on disk still holds
#        the hash, but the process must re-derive on next request.
#   3. linstor encryption enter-passphrase <pw>
#      → must succeed without "encryption locked" / "wrong passphrase"
#   4. GET /v1/encryption/passphrase status reports unlocked again
#
# Cleanup: delete-passphrase via cleanup_encryption_state.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

PASSPHRASE='cli-matrix-333-enter-pp!'

cleanup() {
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: drop any stale passphrase so create succeeds cleanly.
cleanup_encryption_state

echo ">> [Bug 333] step 1: create-passphrase (baseline)"
if ! "${LCTL[@]}" encryption create-passphrase "$PASSPHRASE" >/dev/null 2>&1; then
    echo "FAIL: pre-flight create-passphrase failed" >&2
    exit 1
fi

echo ">> [Bug 333] step 2: rollout restart blockstor-apiserver"
# After Phase 11.x the REST surface lives in cmd/apiserver, NOT
# cmd/controller (see MEMORY: blockstor_apiserver_split). The
# port-forward we opened in linstor_cli_setup targets the apiserver
# service, so restarting that Deployment is the right blast radius
# for "drop in-process passphraseUnlocked cache". Fall back to the
# controller Deployment for pre-Phase-11 stands.
if kubectl -n "$NS" get deploy/blockstor-apiserver >/dev/null 2>&1; then
    kubectl -n "$NS" rollout restart deploy/blockstor-apiserver
    kubectl -n "$NS" rollout status deploy/blockstor-apiserver --timeout=120s
elif kubectl -n "$NS" get deploy/blockstor-controller >/dev/null 2>&1; then
    kubectl -n "$NS" rollout restart deploy/blockstor-controller
    kubectl -n "$NS" rollout status deploy/blockstor-controller --timeout=120s
else
    echo "SKIP: neither blockstor-apiserver nor blockstor-controller Deployment present"
    exit 0
fi

# Our port-forward is now broken — re-establish it against the fresh
# pod. linstor_cli_teardown + linstor_cli_setup is the cleanest path
# (preserves the LCTL[] array shape for the rest of the cell).
linstor_cli_teardown
linstor_cli_setup

echo ">> [Bug 333] step 3: linstor encryption enter-passphrase"
err_file=$(mktemp)
if ! out=$("${LCTL[@]}" encryption enter-passphrase "$PASSPHRASE" 2>"$err_file"); then
    rc=$?
    echo "FAIL (Bug 333): enter-passphrase after restart exited $rc" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "----- stdout -----" >&2
    echo "$out" >&2
    rm -f "$err_file"
    exit 1
fi

# Negative greps: "encryption locked" or "Wrong passphrase" are the
# upstream error strings we explicitly do NOT want to see. A buggy
# enter path can return 200 OK but still log these (Bug 110 class
# regression where the envelope claimed success while the process
# state stayed locked).
if grep -qiE 'encryption locked|Wrong passphrase|passphrase mismatch' "$err_file" "$out" 2>/dev/null; then
    echo "FAIL (Bug 333): enter-passphrase output contains 'locked' / 'wrong' marker:" >&2
    cat "$err_file" >&2
    echo "$out" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 333] step 4: GET /v1/encryption/passphrase reports unlocked"
status_body=$(curl -fsS -m 5 \
    "http://127.0.0.1:${LCTL_PORT}/v1/encryption/passphrase" 2>/dev/null || echo "")
if [[ -z "$status_body" ]]; then
    echo "FAIL (Bug 333): status GET returned empty body after enter-passphrase" >&2
    exit 1
fi
if ! grep -qiE 'true|unlocked|set' <<<"$status_body"; then
    echo "FAIL (Bug 333): status body has no positive flag after enter-passphrase: $status_body" >&2
    exit 1
fi

echo ">> luks-encryption-enter-passphrase OK (Bug 333 pinned: create → restart → enter cycle)"
