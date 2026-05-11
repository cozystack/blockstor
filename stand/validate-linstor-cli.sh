#!/usr/bin/env bash
#
# usage: validate-linstor-cli.sh <stand-name>
#
# Exercises the upstream `linstor` CLI surface against blockstor's
# REST endpoint to confirm parity for read+write commands the user
# explicitly listed in 2026-05-11 session:
#
#   linstor rg/v/rd/vd/r/v/ps                  (list family)
#   linstor rg create --place-count N           (LaxInt32 fix)
#   linstor rg query-size-info                  (probe via spawn-resources path)
#   linstor rg query-max-volume-size
#   linstor rg spawn-resources
#   linstor rg adjust
#
# Skipped on hosts without the `linstor` CLI binary.

set -uo pipefail

NAME=${1:?stand name required (e.g. e2e1)}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR="$REPO_ROOT/.work/$NAME"
export KUBECONFIG="$WORK_DIR/kubeconfig"

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH"
    exit 0
fi

# Reuse the worker-enum helper the e2e tests already use so the
# filter assertions below can reference $WORKER_1 etc. Sources
# WORKER_1/2/3 + require_workers + on_node.
# shellcheck source=../tests/e2e/lib.sh
source "$REPO_ROOT/tests/e2e/lib.sh"

require_workers 3

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/validate-linstor-cli-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; \
      linstor --controllers "http://localhost:$PF_PORT" --machine-readable resource-definition delete cli-validate-rd 2>/dev/null || true; \
      linstor --controllers "http://localhost:$PF_PORT" --machine-readable resource-group delete cli-validate-rg 2>/dev/null || true' EXIT

for _ in $(seq 1 10); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

run() {
    local label=$1 ; shift
    echo
    echo "▶ $label"
    if "$@"; then
        echo "  OK"
    else
        echo "  FAIL"
        return 1
    fi
}

RG=cli-validate-rg
RD=cli-validate-rd

# Idempotent pre-cleanup — a previous run that died before its trap
# fired leaves the RG/RD around and every create-style call below
# fails with "object already exists".
"${LCTL[@]}" --machine-readable resource-definition delete "$RD" >/dev/null 2>&1 || true
sleep 1
"${LCTL[@]}" --machine-readable resource-group delete "$RG" >/dev/null 2>&1 || true

run "rg list" "${LCTL[@]}" rg list
run "sp list" "${LCTL[@]}" sp list
run "node list" "${LCTL[@]}" node list

# 1. Create RG with place-count=2 — the LaxInt32 fix path.
run "rg create $RG --place-count 2" \
    "${LCTL[@]}" resource-group create "$RG" --place-count 2 --storage-pool stand
run "vg create $RG 1G" \
    "${LCTL[@]}" volume-group create "$RG"

# 2. RG query family
run "rg query-size-info $RG" \
    "${LCTL[@]}" resource-group query-size-info "$RG"
run "rg query-max-volume-size $RG" \
    "${LCTL[@]}" resource-group query-max-volume-size "$RG"

# 3. Spawn an RD from the RG.
run "rg spawn-resources $RG $RD 32M" \
    "${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M
run "rg adjust $RG" \
    "${LCTL[@]}" resource-group adjust "$RG"

# 4. Inspect lists post-spawn.
run "rd list" "${LCTL[@]}" resource-definition list
run "vd list -r $RD" "${LCTL[@]}" volume-definition list -r "$RD"
run "r list" "${LCTL[@]}" resource list
run "v list" "${LCTL[@]}" volume list

# Filter combinatorics — Python CLI sends `?nodes=…&resources=…` as
# repeat-key query params; pin server-side compatibility.
run "r list -r $RD" "${LCTL[@]}" resource list -r "$RD"
run "r list -n $WORKER_1" "${LCTL[@]}" resource list -n "$WORKER_1"
run "r list -r $RD -n $WORKER_2" "${LCTL[@]}" resource list -r "$RD" -n "$WORKER_2"
run "r list --faulty" "${LCTL[@]}" resource list --faulty
run "v list -r $RD" "${LCTL[@]}" volume list -r "$RD"
run "v list -n $WORKER_1" "${LCTL[@]}" volume list -n "$WORKER_1"
run "sp list -n $WORKER_1" "${LCTL[@]}" storage-pool list -n "$WORKER_1"

echo
echo ">> ALL OK"
