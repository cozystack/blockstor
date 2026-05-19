#!/usr/bin/env bash
#
# operator-fuzz.sh — property-based fuzzer for operator CLI workflows.
#
# Generates a randomized but FULLY REPRODUCIBLE sequence of `linstor`
# CLI verbs against a live stand. After each verb the loop settles
# (Status no longer mutating across two ticks) and runs the
# NoOrphans invariant at teardown. Any divergence emits a
# replay-runner.sh-compatible YAML the user can drop into
# tests/operator-harness/replay/ as a permanent regression test.
#
# CLI / env:
#
#   SEED=<int>        seed; same SEED → same verb sequence  (default: $RANDOM)
#   STEPS=<int>       number of steps to drive              (default: 50)
#   STAND=<name>      informational stand tag               (default: dev-stand)
#   WORK_DIR=<path>   findings + per-step trace land here   (default: .work/fuzz-<ts>)
#   BS_URL=<url>      linstor controller URL                (required)
#   FUZZ_PREFIX=<s>   RD name prefix; teardown filter       (default: fuzz)
#   FUZZ_SP=<s>       storage pool name on all nodes        (default: stand)
#   NO_SHRINK=1       skip the shrinker after a failure     (default: shrink)
#   SHRINK_LIMIT=<n>  stop after N steps (used by shrinker) (default: STEPS)
#   SHRINK_OUTPUT=<p> emit finding YAML to this path        (used by shrinker)
#
# Exit codes:
#
#   0   STEPS completed cleanly, NoOrphans held at teardown
#   1   a step or invariant failed; finding YAML written
#   2   usage / config error
#
# Implementation notes:
#
# - All shared logic (run_step, settle, NoOrphans, prng) lives in
#   lib.sh so replay-runner.sh and the fuzzer execute identical
#   step semantics.
# - Verb files in fuzz/verb-*.sh export verb_<name>_{name,precondition,
#   generate}. The main loop iterates verbs deterministically until it
#   finds one whose precondition passes for the current cluster state.
# - The PRNG (lib.sh::prng) hashes (SEED, step, verb_index) so reruns
#   with the same SEED produce the same sequence — required for
#   shrinking and for findings to be repeatable.
# - On failure we emit BOTH the full trace (every step that ran) AND
#   the shrunk trace (smallest failing prefix). User picks whichever
#   is more diagnostic for the bug.

set -euo pipefail

# ----------------------------------------------------------------------
# bootstrap
# ----------------------------------------------------------------------

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Internal entrypoint for shrinker recursion. Strip the flag before
# the normal-mode argv handling.
SHRINK_CHILD=0
if [[ "${1:-}" == "--shrink-child" ]]; then
    SHRINK_CHILD=1
    shift
fi

: "${BS_URL:?BS_URL required (e.g. http://127.0.0.1:3370). Caller manages port-forward.}"

if ! command -v linstor >/dev/null 2>&1; then
    echo "FATAL: linstor CLI not on PATH" >&2
    exit 2
fi
if ! command -v kubectl >/dev/null 2>&1; then
    echo "FATAL: kubectl not on PATH" >&2
    exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "FATAL: python3 required" >&2
    exit 2
fi

SEED=${SEED:-$RANDOM}
STEPS=${STEPS:-50}
STAND=${STAND:-dev-stand}
WORK_DIR=${WORK_DIR:-.work/fuzz-$(date +%s)}
FUZZ_PREFIX=${FUZZ_PREFIX:-fuzz}
FUZZ_SP=${FUZZ_SP:-stand}
SHRINK_LIMIT=${SHRINK_LIMIT:-$STEPS}

mkdir -p "$WORK_DIR"

# shellcheck source=./lib.sh
source "$HARNESS_DIR/lib.sh"
# shellcheck source=./fuzz/state.sh
source "$HARNESS_DIR/fuzz/state.sh"
# shellcheck source=./fuzz/shrink.sh
source "$HARNESS_DIR/fuzz/shrink.sh"

# Source every verb file. The set is FIXED at source time so verb
# indexing is stable across runs (a verb_index of 3 always means the
# same verb).
VERBS=()
for vf in "$HARNESS_DIR"/fuzz/verb-*.sh; do
    # shellcheck source=/dev/null
    source "$vf"
    VERBS+=( "$(basename "$vf" .sh | sed 's/^verb-//' | tr - _)" )
done

# Discover workers — same logic as replay-runner.sh.
mapfile -t WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | sort
)
NODE1="${WORKERS[0]:-}"
NODE2="${WORKERS[1]:-}"
NODE3="${WORKERS[2]:-}"
export FUZZ_NODES="${WORKERS[*]}"
export FUZZ_SP FUZZ_PREFIX

# RD/SP globals used by run_step / substitute. RD is left blank since
# the fuzzer addresses RDs by their generated names directly (no
# {{rd}} substitution in synthesized steps).
RD=""
SP=$FUZZ_SP

# ----------------------------------------------------------------------
# step recording / finding YAML emission
# ----------------------------------------------------------------------

TRACE_FILE="$WORK_DIR/trace.jsonl"
: >"$TRACE_FILE"

record_step() {
    local step_index=$1 verb_name=$2
    shift 2
    local cmd_json
    cmd_json=$(python3 -c "import json,sys; print(json.dumps(sys.argv[1:]))" "$@")
    printf '{"step":%d,"verb":"%s","cmd":%s}\n' \
        "$step_index" "$verb_name" "$cmd_json" >>"$TRACE_FILE"
}

# emit_finding_yaml <reason> [out-path]
#
# Writes a YAML in the exact schema of tests/operator-harness/replay/*.yaml.
# The user can `cp` it into replay/ to make the bug a permanent
# regression test.
emit_finding_yaml() {
    local reason=$1
    local out=${2:-"$WORK_DIR/finding-${SEED}-${LAST_STEP_INDEX:-0}.yaml"}
    python3 - "$TRACE_FILE" "$SEED" "${LAST_STEP_INDEX:-0}" "$STAND" "$reason" "$out" <<'EOF'
import json, sys
trace_path, seed, step, stand, reason, out = sys.argv[1:]
steps = []
with open(trace_path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        steps.append(json.loads(line))

with open(out, 'w') as o:
    o.write(f"# Auto-generated by operator-fuzz.sh on stand={stand}\n")
    o.write(f"# seed={seed} fail_step={step} reason={reason}\n")
    o.write(f"name: fuzz-finding-{seed}-{step}\n")
    o.write("description: |\n")
    o.write(f"  Auto-generated by operator-fuzz.sh seed={seed} step={step}\n")
    o.write(f"  Stand: {stand}\n")
    o.write(f"  Failure: {reason}\n")
    o.write("\n")
    o.write("prerequisites:\n")
    o.write("  min_nodes: 3\n")
    o.write("  storage_pool: stand\n")
    o.write("\n")
    o.write("vars:\n")
    o.write("  sp: stand\n")
    o.write("\n")
    o.write("steps:\n")
    for i, s in enumerate(steps):
        cmd = s['cmd']
        cmd_repr = "[" + ", ".join(json.dumps(c) for c in cmd) + "]"
        o.write(f"  - name: step-{i:03d}-{s['verb']}\n")
        o.write(f"    cmd: {cmd_repr}\n")
        o.write(f"    expect_exit: 0\n")
    o.write("\n")
    o.write("teardown:\n")
    # Best-effort: drop any RD whose name starts with the fuzz prefix.
    rds_seen = set()
    for s in steps:
        cmd = s['cmd']
        if len(cmd) >= 3 and cmd[0] in ('resource-definition','rd') and cmd[1] in ('create','c'):
            rds_seen.add(cmd[2])
    for rd in sorted(rds_seen):
        o.write(f"  - cmd: [\"resource-definition\", \"delete\", {json.dumps(rd)}]\n")
        o.write(f"    expect_exit: 0\n")
    o.write("\n")
    o.write("invariants:\n")
    o.write("  - no_orphans\n")
print(f"finding written: {out}", file=sys.stderr)
EOF
}

# ----------------------------------------------------------------------
# generator: pick a verb whose precondition holds, then synthesize cmd
# ----------------------------------------------------------------------

# pick_verb <step_index>
#
# Iterates VERBS[] in deterministic order keyed off (SEED, step).
# Skips verbs whose precondition fails. Echoes the verb name; returns
# non-zero if NO verb is applicable (signals "cluster too constrained
# to fuzz further").
pick_verb() {
    local step_index=$1
    local n=${#VERBS[@]}
    # Deterministic shuffle order: start at offset = prng(SEED, step, "shuf") % n
    local off
    off=$(prng_pick "$SEED" "$step_index" "shuf" "$n")

    local i v idx
    for ((i=0; i<n; i++)); do
        idx=$(( (off + i) % n ))
        v=${VERBS[$idx]}
        if "verb_${v}_precondition" >/dev/null 2>&1; then
            echo "$v"
            return 0
        fi
    done
    return 1
}

# generate_step <verb> <step_index>
#
# Calls the verb's generate function with a fresh prng pull and prints
# the resulting argv (one element per line).
generate_step() {
    local verb=$1 step_index=$2
    local prng_val
    prng_val=$(prng "$SEED" "$step_index" "$verb")
    "verb_${verb}_generate" "$prng_val"
}

# ----------------------------------------------------------------------
# teardown: best-effort `rd delete` for every fuzz-owned RD
# ----------------------------------------------------------------------

cleanup_all_rds() {
    refresh_cluster_state
    local rds
    rds=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    print(json.loads(line)['name'])
")
    local rd
    for rd in $rds; do
        echo "  teardown: rd d $rd"
        run_linstor_cmd "resource-definition" "delete" "$rd" || true
    done
    # Give the operator a window to drain.
    sleep 5
}

# ----------------------------------------------------------------------
# main loop
# ----------------------------------------------------------------------

echo "=== operator-fuzz: SEED=$SEED STEPS=$STEPS (limit=$SHRINK_LIMIT) stand=$STAND ==="
echo "    workers: ${WORKERS[*]:-<none>}"
echo "    work_dir: $WORK_DIR"
echo "    verbs: ${VERBS[*]}"

if (( ${#WORKERS[@]} < 2 )); then
    echo "FATAL: need ≥2 worker nodes, found ${#WORKERS[@]}" >&2
    exit 2
fi

LAST_STEP_INDEX=0
FAILED=0
FAIL_REASON=""

# Loop bound is min(STEPS, SHRINK_LIMIT). The shrinker sets
# SHRINK_LIMIT < STEPS to retry a prefix.
LIMIT=$(( STEPS < SHRINK_LIMIT ? STEPS : SHRINK_LIMIT ))

for ((i=0; i<LIMIT; i++)); do
    LAST_STEP_INDEX=$i
    refresh_cluster_state

    verb=$(pick_verb "$i") || {
        echo "  no applicable verb at step $i; ending run early."
        break
    }

    mapfile -t argv < <(generate_step "$verb" "$i") || {
        echo "  verb $verb generator returned no candidate; skipping step."
        continue
    }
    if (( ${#argv[@]} == 0 )); then
        continue
    fi

    record_step "$i" "$verb" "${argv[@]}"

    echo "  step $i: verb=$verb :: linstor ${argv[*]}"
    run_linstor_cmd "${argv[@]}"
    if (( LAST_EXIT != 0 )); then
        echo "  STEP FAILED: exit=$LAST_EXIT" >&2
        printf '  stderr: %s\n' "$LAST_STDERR" >&2
        FAILED=1
        FAIL_REASON="cmd-exit-$LAST_EXIT: ${LAST_STDERR//$'\n'/ }"
        break
    fi

    # Settle on whichever RD the step targeted, if any. argv tend to
    # include the RD name (fuzz-prefixed) as one of the positional
    # arguments for r/rd/snap operations.
    step_rd=""
    for a in "${argv[@]}"; do
        if [[ "$a" == ${FUZZ_PREFIX}-* ]]; then
            step_rd=$a
        fi
    done
    if [[ -n "$step_rd" ]]; then
        if ! wait_settle "$step_rd" "${SETTLE_TIMEOUT_S:-30}"; then
            FAILED=1
            FAIL_REASON="settle-timeout-on-${step_rd}"
            break
        fi
    fi
done

if (( FAILED )); then
    out="$WORK_DIR/finding-${SEED}-${LAST_STEP_INDEX}.yaml"
    if [[ -n "${SHRINK_OUTPUT:-}" ]]; then
        out=$SHRINK_OUTPUT
    fi
    emit_finding_yaml "$FAIL_REASON" "$out"
    echo "FAIL: $FAIL_REASON" >&2
    echo "finding: $out" >&2

    if [[ "${NO_SHRINK:-0}" != "1" && "$SHRINK_CHILD" == "0" ]]; then
        run_shrinker "$SEED" "$LAST_STEP_INDEX" || true
    fi

    # Always best-effort cleanup so a failing run doesn't poison the stand.
    cleanup_all_rds || true
    exit 1
fi

# Cleanup + NoOrphans.
cleanup_all_rds
if ! assert_no_orphans "$FUZZ_PREFIX"; then
    LAST_STEP_INDEX=$STEPS
    emit_finding_yaml "no-orphans-violation" "$WORK_DIR/finding-${SEED}-orphans.yaml"
    echo "FAIL: no-orphans-violation" >&2
    exit 1
fi

echo "PASS: SEED=$SEED STEPS=$LIMIT (no orphans)"
exit 0
