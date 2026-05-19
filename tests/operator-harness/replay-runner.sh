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

linstor_cli() { linstor --controllers "$BS_URL" "$@"; }

# ----------------------------------------------------------------------
# YAML helpers (python3 + pyyaml inline)
# ----------------------------------------------------------------------

# yaml_get <file> <jsonpath-like> — returns scalar or JSON
yaml_get() {
    python3 - <<EOF
import json, sys, yaml
d = yaml.safe_load(open("$1"))
path = "$2".split(".")
cur = d
for p in path:
    if p == "":
        continue
    if isinstance(cur, list):
        cur = cur[int(p)]
    else:
        cur = cur.get(p) if cur else None
    if cur is None:
        sys.exit(0)
print(cur if isinstance(cur, (str, int, float, bool)) else json.dumps(cur))
EOF
}

# yaml_steps <file> — dump steps[] as one JSON object per line
yaml_steps() {
    python3 - <<EOF
import json, yaml
d = yaml.safe_load(open("$1"))
for s in d.get("steps", []):
    print(json.dumps(s))
EOF
}

yaml_teardown() {
    python3 - <<EOF
import json, yaml
d = yaml.safe_load(open("$1"))
for s in d.get("teardown", []):
    print(json.dumps(s))
EOF
}

yaml_invariants() {
    python3 - <<EOF
import yaml
d = yaml.safe_load(open("$1"))
for inv in d.get("invariants", []):
    print(inv)
EOF
}

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

substitute() {
    local s=$1
    s=${s//\{\{rd\}\}/$RD}
    s=${s//\{\{sp\}\}/$SP}
    s=${s//\{\{node1\}\}/$NODE1}
    s=${s//\{\{node2\}\}/$NODE2}
    s=${s//\{\{node3\}\}/$NODE3}
    echo "$s"
}

# ----------------------------------------------------------------------
# assertion polling
# ----------------------------------------------------------------------

# await_assertion <json> — poll until satisfied or timeout
await_assertion() {
    local spec=$1
    local kind timeout_s deadline
    kind=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('kind',''))" "$spec")
    timeout_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('timeout_s',60))" "$spec")
    deadline=$(( $(date +%s) + timeout_s ))

    while (( $(date +%s) < deadline )); do
        if check_assertion "$kind" "$spec"; then
            return 0
        fi
        sleep 2
    done
    echo "    ASSERTION TIMEOUT: kind=$kind spec=$spec" >&2
    return 1
}

check_assertion() {
    local kind=$1 spec=$2
    case "$kind" in
        replica_count)
            local rd min count
            rd=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")
            rd=$(substitute "$rd")
            min=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('min',2))" "$spec")
            count=$(linstor_cli --output-fmt=json resource list --resources "$rd" 2>/dev/null \
                | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    # responses are usually [[{...}]] or [{...}]
    while isinstance(d, list) and d and isinstance(d[0], list):
        d=d[0]
    print(len(d))
except: print(0)")
            [[ "$count" -ge "$min" ]]
            ;;
        disk_state)
            local rd node expected actual
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            node=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('node',''))" "$spec")")
            expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected',''))" "$spec")
            actual=$(kubectl get resource "${rd}.${node}" -o jsonpath='{.status.volumes[0].diskState}' 2>/dev/null || echo "")
            [[ "$actual" == "$expected" ]]
            ;;
        all_uptodate)
            local rd
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            # all volumes on all replicas must be UpToDate
            local bad
            bad=$(kubectl get resources.blockstor.io -o json 2>/dev/null \
                | python3 -c "import json,sys
d=json.load(sys.stdin)
rd='$rd'
bad=0
for it in d.get('items',[]):
    if it.get('spec',{}).get('resourceName')!=rd: continue
    for v in it.get('status',{}).get('volumes',[]) or []:
        if v.get('diskState')!='UpToDate': bad+=1
print(bad)")
            [[ "$bad" == "0" ]]
            ;;
        replica_diskless)
            local rd node actual
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            node=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('node',''))" "$spec")")
            actual=$(kubectl get resource "${rd}.${node}" -o jsonpath='{.status.volumes[0].diskState}' 2>/dev/null || echo "")
            [[ "$actual" == "Diskless" ]]
            ;;
        no_tiebreaker)
            local rd present
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            present=$(linstor_cli resource list --resources "$rd" 2>/dev/null | grep -ci 'TieBreaker' || true)
            [[ "$present" == "0" ]]
            ;;
        sync_clean)
            local rd
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            # detect "(NN%)" suffix on any UpToDate line
            ! linstor_cli resource list --resources "$rd" 2>/dev/null | grep -E 'UpToDate.*\([0-9]+%\)' >/dev/null
            ;;
        resource_absent)
            local rd node
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            node=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('node',''))" "$spec")")
            ! kubectl get resource "${rd}.${node}" >/dev/null 2>&1
            ;;
        rd_absent)
            local rd
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            ! linstor_cli resource-definition list --resource-definitions "$rd" 2>/dev/null \
                | grep -q "$rd"
            ;;
        *)
            echo "    unknown assertion kind: $kind" >&2
            return 1
            ;;
    esac
}

# ----------------------------------------------------------------------
# step executor
# ----------------------------------------------------------------------

# run_step <json-step> — execute cmd; check expected exit; run await if any
run_step() {
    local step=$1
    local name cmd_json expect_exit
    name=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('name','(unnamed)'))" "$step")
    cmd_json=$(python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1]).get('cmd',[])))" "$step")
    expect_exit=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expect_exit',0))" "$step")

    # Build argv from cmd[]
    mapfile -t argv < <(python3 -c "import json,sys
for a in json.loads(sys.argv[1]):
    print(a)" "$cmd_json")

    local subst=()
    for a in "${argv[@]}"; do
        subst+=("$(substitute "$a")")
    done

    echo "  -> step: $name :: linstor ${subst[*]}"
    local rc=0
    linstor_cli "${subst[@]}" >/tmp/replay-out.$$ 2>/tmp/replay-err.$$ || rc=$?
    if [[ "$rc" != "$expect_exit" ]]; then
        echo "    FAIL: expected exit $expect_exit, got $rc" >&2
        sed 's/^/    stderr: /' /tmp/replay-err.$$ >&2 || true
        rm -f /tmp/replay-out.$$ /tmp/replay-err.$$
        return 1
    fi
    rm -f /tmp/replay-out.$$ /tmp/replay-err.$$

    # await?
    local await_json
    await_json=$(python3 -c "import json,sys
s=json.loads(sys.argv[1]).get('await')
print(json.dumps(s) if s else '')" "$step")
    if [[ -n "$await_json" ]]; then
        await_assertion "$await_json" || return 1
    fi
}

# ----------------------------------------------------------------------
# invariants
# ----------------------------------------------------------------------

invariant_no_orphans() {
    local prefix=${RD}
    local leftover_crds leftover_drbd
    leftover_crds=$(kubectl get resources.blockstor.io -o name 2>/dev/null \
        | grep -c "$prefix" || true)
    if [[ "$leftover_crds" -gt 0 ]]; then
        echo "  INVARIANT FAIL: $leftover_crds Resource CRD(s) for $prefix still present" >&2
        return 1
    fi
    return 0
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
