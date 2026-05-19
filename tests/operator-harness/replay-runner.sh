#!/usr/bin/env bash
#
# replay-runner.sh — execute a single operator-workflow YAML against a live
# stand. The YAML describes a real operator sequence as a list of CLI
# invocations + status assertions; the runner walks each step, polls until
# the assertion holds (or times out), and emits PASS/FAIL.
#
# Usage:
#
#   tests/operator-harness/replay-runner.sh <stand-name> <workflow.yaml>
#
# <stand-name>  — informational tag; pulled into the final report. Pass the
#                 name of the cluster you are running against (e.g. "dev-kvaps").
#
# <workflow.yaml> — path to a YAML file under tests/operator-harness/replay/.
#                   Schema (see replay/*.yaml for live examples):
#
#     name: pvc-lifecycle
#     description: |
#       End-to-end PVC lifecycle: rd c → vd c → r c --auto-place=2 → IO →
#       snapshot → r d → rd d. Verifies NoOrphans at teardown.
#     prerequisites:
#       min_nodes: 2
#       storage_pool: stand
#     steps:
#       - name: create-rd
#         cmd: ["resource-definition", "create", "{{rd}}"]
#         expect_exit: 0
#       - name: create-vd
#         cmd: ["volume-definition", "create", "{{rd}}", "32M"]
#         expect_exit: 0
#       - name: auto-place
#         cmd: ["resource", "create", "--auto-place", "2", "--storage-pool", "{{sp}}", "{{rd}}"]
#         expect_exit: 0
#         await:
#           kind: replica_count
#           rd: "{{rd}}"
#           min: 2
#           timeout_s: 60
#     teardown:
#       - cmd: ["resource-definition", "delete", "{{rd}}"]
#     invariants:
#       - no_orphans
#
# Assertion kinds supported under "await":
#
#   - replica_count        wait until N replicas of rd exist with disk≠Diskless
#   - disk_state           wait until rd@node reports disk_state == expected
#   - all_uptodate         wait until every replica reports UpToDate
#   - replica_diskless     wait until rd@node has disk_state == Diskless
#   - no_tiebreaker        assert NO TieBreaker is auto-spawned
#   - sync_clean           wait until UpToDate without "(NN%)" suffix
#   - resource_absent      wait until r d takes effect on a node
#   - rd_absent            wait until rd is gone everywhere
#
# Invariants (post-teardown):
#
#   - no_orphans           no leftover Resource CRDs, no kernel slots, no
#                           LVM/ZFS volumes under the test prefix
#
# Variables interpolated into YAML strings:
#
#   {{rd}}            workflow.vars.rd (default "replay-<name>-<rand4>")
#   {{sp}}            workflow.vars.sp (default "stand")
#   {{node1}} … {{node3}}  resolved from kubectl-discovered worker list
#
# Exit codes:
#
#   0 — every step + assertion passed, all invariants hold
#   1 — at least one step failed; details on stderr
#   2 — usage / config error
#
# Implementation notes (read before extending):
#
# - YAML parsing uses python3 + PyYAML — installed on every blockstor stand
#   by the bring-up script. We deliberately avoid yq here so the runner has
#   one fewer external dep.
# - linstor CLI is invoked with --controllers $BS_URL (port-forwarded by
#   the caller; replay-runner does NOT manage port-forwards).
# - All commands are MUST_PASS unless expect_exit overrides; failures
#   abort the workflow (NOT just the step) so a partial cluster doesn't
#   poison subsequent workflows.
# - The bulk of the executor (run_step, await_assertion, yaml_*) lives in
#   lib.sh so operator-fuzz.sh can share the same code path — both
#   scripts read/execute the same step JSON.

set -euo pipefail

STAND_NAME=${1:?usage: replay-runner.sh <stand-name> <workflow.yaml>}
WORKFLOW=${2:?usage: replay-runner.sh <stand-name> <workflow.yaml>}

if [[ ! -f "$WORKFLOW" ]]; then
    echo "FATAL: workflow file not found: $WORKFLOW" >&2
    exit 2
fi

: "${BS_URL:?BS_URL required (e.g. http://127.0.0.1:3370). Caller manages port-forward.}"

if ! command -v linstor >/dev/null 2>&1; then
    echo "FATAL: linstor CLI not on PATH" >&2
    exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "FATAL: python3 required for YAML parsing" >&2
    exit 2
fi

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./lib.sh
source "$HARNESS_DIR/lib.sh"

# Discover worker nodes for {{node1..3}} substitution. The runner is happy
# with 2 or 3 nodes; workflows that need more declare it via prerequisites
# and the runner skips with a clear message.
mapfile -t WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | sort
)
NODE1="${WORKERS[0]:-}"
NODE2="${WORKERS[1]:-}"
NODE3="${WORKERS[2]:-}"

# ----------------------------------------------------------------------
# variable substitution
# ----------------------------------------------------------------------

NAME=$(yaml_get "$WORKFLOW" "name")
NAME=${NAME:-$(basename "$WORKFLOW" .yaml)}
RAND=$(tr -dc 'a-z0-9' </dev/urandom | head -c 4 || true)
DEFAULT_RD="replay-${NAME}-${RAND}"
RD=$(yaml_get "$WORKFLOW" "vars.rd")
RD=${RD:-$DEFAULT_RD}
SP=$(yaml_get "$WORKFLOW" "vars.sp")
SP=${SP:-stand}

# ----------------------------------------------------------------------
# invariants
# ----------------------------------------------------------------------

invariant_no_orphans() {
    assert_no_orphans "$RD"
}

# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

echo "=== replay: $NAME on stand=$STAND_NAME (rd=$RD sp=$SP) ==="
echo "    workers: $NODE1 $NODE2 $NODE3"

MIN_NODES=$(yaml_get "$WORKFLOW" "prerequisites.min_nodes")
MIN_NODES=${MIN_NODES:-2}
if [[ "${#WORKERS[@]}" -lt "$MIN_NODES" ]]; then
    echo "SKIP: workflow needs $MIN_NODES workers, stand has ${#WORKERS[@]}"
    exit 0
fi

# steps
FAILED=0
while IFS= read -r step; do
    [[ -z "$step" ]] && continue
    if ! run_step "$step"; then
        FAILED=1
        break
    fi
done < <(yaml_steps "$WORKFLOW")

# teardown — always runs, but its failures don't override step failures
echo "--- teardown ---"
while IFS= read -r step; do
    [[ -z "$step" ]] && continue
    run_step "$step" || echo "  teardown step failed (continuing)" >&2
done < <(yaml_teardown "$WORKFLOW")

# invariants — only checked if steps passed
if [[ "$FAILED" == "0" ]]; then
    while IFS= read -r inv; do
        case "$inv" in
            no_orphans)
                invariant_no_orphans || FAILED=1
                ;;
            "" ) : ;;
            *)
                echo "  WARN: unknown invariant '$inv'" >&2
                ;;
        esac
    done < <(yaml_invariants "$WORKFLOW")
fi

if [[ "$FAILED" == "0" ]]; then
    echo "PASS: $NAME"
    exit 0
fi
echo "FAIL: $NAME" >&2
exit 1
