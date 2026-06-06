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
#       # Optional fixture-presence SKIP gates (exit 0 if the fixture is
#       # absent -- a missing fixture is "not exercisable here", not a fault):
#       storage_pool_min_nodes: { name: lvm-thick, min: 2 }
#       device_on_any_node: /dev/loop9
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
#   - replica_count_max    NEGATIVE assert: rd replica count never exceeds
#                           `max`. Pair with `hold_s` to prove no background
#                           reconcile materialises extra replicas (U222:
#                           RG-reassignment is non-retroactive)
#   - active_diskful_count wait until ≥ min ACTIVE diskful replicas exist
#                           (excludes DISKLESS/TIE_BREAKER and INACTIVE;
#                            Bug 393 — INACTIVE must not satisfy place_count)
#   - disk_state           wait until rd@node reports disk_state == expected
#   - all_uptodate         wait until every replica reports UpToDate
#   - replica_diskless     wait until rd@node has disk_state == Diskless
#   - no_tiebreaker        assert NO TieBreaker is auto-spawned
#   - tiebreaker_present   assert a TieBreaker witness EXISTS for rd
#                           (Bug 386: re-created after `n rst`)
#   - prop_value           assert a property on a list-properties
#                           surface: {obj:rd|rg|node|controller, name,
#                           key, expected}. obj defaults to rd; node/
#                           controller added for corner-case I1.
#                           expected omitted/"" ⇒ key must be ABSENT
#                           (empty-value=delete; corner-case B1/B4/B5/I1)
#   - sync_clean           wait until UpToDate without "(NN%)" suffix
#   - resource_absent      wait until r d takes effect on a node
#   - rd_absent            wait until rd is gone everywhere
#   - vd_size_kib          VolumeDefinition.size_kib equals expected_kib
#   - pvc_capacity         PVC.Status.Capacity.storage matches expected
#   - pod_md5_invariant    md5sum of file inside pod matches expected baseline
#   - volumes_settled      every Resource of rd carries EXACTLY the
#                           expected volume-number set in spec.volumes +
#                           status.volumes AND metadata.resourceVersion
#                           is stable over settle_s (Bug 399 no-flap)
#   - drbd_option          `drbdsetup show <rd>` on <node>'s satellite
#                           pod reports <key> == <expected> — the live
#                           kernel view of the rendered DRBD option, so
#                           it captures the full Controller→RG→RD→Resource
#                           inheritance + "closer wins" precedence
#                           (corner-case C1/C2)
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
#   {{rg}}            workflow.vars.rg (default "rg-<name>-<rand4>")
#   {{device}}        workflow.vars.device (no default; physical-storage
#                     workflows like ps-cdp-zfs)
#   {{node1}} … {{node4}}  resolved from kubectl-discovered worker list
#                           ({{node4}} needs min_nodes: 4)
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
# NODE4 is the gap-fill target for redundancy-refill workflows (Bug 393):
# an RD pinned to NODE1..3 that loses one active replica must refill onto
# a node OUTSIDE that set. Workflows needing it declare min_nodes: 4.
NODE4="${WORKERS[3]:-}"

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
# {{rg}} resolves from vars.rg (resource-group workflows). No synthetic
# default — a workflow that references {{rg}} without declaring vars.rg
# would otherwise substitute an empty string and create an unnamed group.
RG=$(yaml_get "$WORKFLOW" "vars.rg")
RG=${RG:-rg-${NAME}-${RAND}}
# {{device}} resolves from vars.device (physical-storage workflows). No
# synthetic default -- a workflow referencing {{device}} without declaring
# vars.device would otherwise substitute an empty string into the CLI.
DEVICE=$(yaml_get "$WORKFLOW" "vars.device")

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

# Fixture-presence SKIP gates. These let a workflow that needs a stand
# fixture the current stand may not have (a thick-LVM storage pool, a
# sacrificial loop device) SKIP cleanly with exit 0 instead of FAILing on
# a missing fixture -- a missing fixture is "not exercisable here", not a
# product bug. Both gates are OPT-IN: a workflow without the prerequisite
# key is unaffected.

# prerequisites.storage_pool_min_nodes: { name: <pool>, min: <N> }
# SKIP unless the named LINSTOR storage pool is registered on >= min nodes.
SP_REQ_NAME=$(yaml_get "$WORKFLOW" "prerequisites.storage_pool_min_nodes.name")
if [[ -n "$SP_REQ_NAME" ]]; then
    SP_REQ_MIN=$(yaml_get "$WORKFLOW" "prerequisites.storage_pool_min_nodes.min")
    SP_REQ_MIN=${SP_REQ_MIN:-1}
    SP_HAVE=$(fixture_sp_node_count "$SP_REQ_NAME")
    if [[ "${SP_HAVE:-0}" -lt "$SP_REQ_MIN" ]]; then
        echo "SKIP: workflow needs storage pool '$SP_REQ_NAME' on >= $SP_REQ_MIN node(s), stand has it on ${SP_HAVE:-0} -- fixture absent"
        exit 0
    fi
fi

# prerequisites.device_on_any_node: <device-path>
# SKIP unless that block device exists on at least one worker satellite.
DEV_REQ=$(yaml_get "$WORKFLOW" "prerequisites.device_on_any_node")
if [[ -n "$DEV_REQ" ]]; then
    if ! fixture_device_on_any_worker "$DEV_REQ"; then
        echo "SKIP: workflow needs block device '$DEV_REQ' on a worker, none present -- fixture absent"
        exit 0
    fi
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
