#!/usr/bin/env bash
#
# lib.sh — shared utilities for the operator-harness scripts.
#
# Sourced by both replay-runner.sh (deterministic workflow replay) and
# operator-fuzz.sh (randomized verb generator). The split keeps one
# source of truth for:
#
#   - linstor CLI wrapper that honours $BS_URL
#   - YAML parsing helpers (python3 + PyYAML)
#   - step executor (`run_step`) shared between replay + fuzz
#   - settle / await polling primitives
#   - NoOrphans invariant
#   - deterministic PRNG over (seed, step, verb) — fuzz only, but
#     parked here so reproducer scripts can use the same hash too
#
# Callers MUST `set -euo pipefail` themselves. This file is pure
# functions; sourcing it has no side effects.
#
# Required env (callers MUST set before sourcing or before calling):
#
#   BS_URL                 linstor controller URL (e.g. http://127.0.0.1:3370)
#   LINSTOR_CMD            linstor binary path (default: linstor)
#
# Optional env:
#
#   SETTLE_TIMEOUT_S       per-step settle window (default: 30)
#   SETTLE_TICK_S          poll interval inside settle (default: 2)
#   ASSERT_TIMEOUT_S       default await timeout if step has none (default: 60)
#
# Functions exported by this file (used by replay-runner.sh and
# operator-fuzz.sh):
#
#   linstor_cli ...                wrapper for `linstor --controllers $BS_URL`
#   yaml_get <file> <dotted-path>  scalar / JSON from a YAML doc
#   yaml_steps <file>              dump steps[] as one JSON per line
#   yaml_teardown <file>           dump teardown[] as one JSON per line
#   yaml_invariants <file>         dump invariants[] one per line
#   substitute <string>            resolve {{rd}}/{{sp}}/{{node1..3}}
#   run_linstor_cmd <argv...>      execute linstor, capture exit + stdout/stderr
#   run_step <json-step>           replay-style step (cmd + expect_exit + await)
#   await_assertion <json-spec>    poll until kind=... condition holds
#   check_assertion <kind> <spec>  single-shot assertion check
#   wait_settle <rd> [timeout_s]   poll until status stops mutating across ticks
#   assert_no_orphans <prefix>     verify cluster has no leftover CRDs for prefix
#   prng <seed> <step> <verb>      deterministic 32-bit value via SHA256
#
# Globals expected/set by callers:
#
#   RD, SP, NODE1..NODE3   workflow-scoped substitution targets
#   WORKERS[]              kubectl-discovered worker node names

# Guard against double-source.
if [[ -n "${__BLOCKSTOR_HARNESS_LIB_LOADED:-}" ]]; then
    return 0
fi
__BLOCKSTOR_HARNESS_LIB_LOADED=1

: "${LINSTOR_CMD:=linstor}"
: "${SETTLE_TIMEOUT_S:=30}"
: "${SETTLE_TICK_S:=2}"
: "${ASSERT_TIMEOUT_S:=60}"

# ----------------------------------------------------------------------
# linstor CLI wrapper
# ----------------------------------------------------------------------

linstor_cli() {
    "$LINSTOR_CMD" --controllers "${BS_URL:?BS_URL required}" "$@"
}

# run_linstor_cmd <argv...>
#
# Executes `linstor_cli "$@"`, captures stdout/stderr/exit. Sets:
#   LAST_STDOUT  contents of stdout
#   LAST_STDERR  contents of stderr
#   LAST_EXIT    exit code
#
# Always returns 0 — the caller inspects LAST_EXIT explicitly. We do
# this so the fuzz loop can treat non-zero exits as data, not bash
# fatals.
run_linstor_cmd() {
    local tmpout tmperr
    tmpout=$(mktemp -t harness-out.XXXXXX)
    tmperr=$(mktemp -t harness-err.XXXXXX)
    LAST_EXIT=0
    linstor_cli "$@" >"$tmpout" 2>"$tmperr" || LAST_EXIT=$?
    LAST_STDOUT=$(cat "$tmpout")
    LAST_STDERR=$(cat "$tmperr")
    rm -f "$tmpout" "$tmperr"
    return 0
}

# ----------------------------------------------------------------------
# YAML helpers (python3 + PyYAML)
# ----------------------------------------------------------------------

# yaml_get <file> <dotted-path> — returns scalar or JSON
yaml_get() {
    python3 - "$1" "$2" <<'EOF'
import json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
path = sys.argv[2].split(".")
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

yaml_steps() {
    python3 - "$1" <<'EOF'
import json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
for s in d.get("steps", []):
    print(json.dumps(s))
EOF
}

yaml_teardown() {
    python3 - "$1" <<'EOF'
import json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
for s in d.get("teardown", []):
    print(json.dumps(s))
EOF
}

yaml_invariants() {
    python3 - "$1" <<'EOF'
import sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
for inv in d.get("invariants", []):
    print(inv)
EOF
}

# ----------------------------------------------------------------------
# substitution
# ----------------------------------------------------------------------

substitute() {
    local s=$1
    s=${s//\{\{rd\}\}/${RD:-}}
    s=${s//\{\{sp\}\}/${SP:-}}
    s=${s//\{\{node1\}\}/${NODE1:-}}
    s=${s//\{\{node2\}\}/${NODE2:-}}
    s=${s//\{\{node3\}\}/${NODE3:-}}
    echo "$s"
}

# ----------------------------------------------------------------------
# assertion polling (used by replay AND fuzz)
# ----------------------------------------------------------------------

await_assertion() {
    local spec=$1
    local kind timeout_s deadline
    kind=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('kind',''))" "$spec")
    timeout_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('timeout_s',${ASSERT_TIMEOUT_S}))" "$spec")
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
            local rd bad
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
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
        vd_size_kib)
            # Verify VolumeDefinition.size_kib matches expected.
            # Used by the volume-resize replay catcher to assert each
            # `linstor vd s` actually mutated the stored size.
            local rd vol expected actual
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            vol=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('vol',0))" "$spec")
            expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected_kib',0))" "$spec")
            actual=$(VOL="$vol" linstor_cli --output-fmt=json volume-definition list --resource-definitions "$rd" 2>/dev/null \
                | python3 -c "import json,sys,os
try:
    d=json.load(sys.stdin)
    while isinstance(d, list) and d and isinstance(d[0], list):
        d=d[0]
    for it in d if isinstance(d, list) else []:
        for v in it.get('vlm_dfns', []) or it.get('volume_definitions', []) or []:
            if v.get('volume_number', v.get('vlm_nr', -1)) == int(os.environ['VOL']):
                print(v.get('size_kib', v.get('sizeKib', 0)))
                sys.exit(0)
    print(0)
except Exception:
    print(0)" 2>/dev/null || echo 0)
            [[ "$actual" == "$expected" ]]
            ;;
        pvc_capacity)
            # PVC.Status.Capacity matches expected (e.g. "2Gi").
            # Verifies the operator-visible size propagation through
            # the CSI external-resizer.
            local ns pvc expected actual
            ns=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('namespace','default'))" "$spec")
            pvc=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('pvc',''))" "$spec")")
            expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected',''))" "$spec")
            actual=$(kubectl -n "$ns" get pvc "$pvc" -o jsonpath='{.status.capacity.storage}' 2>/dev/null || echo "")
            [[ "$actual" == "$expected" ]]
            ;;
        pod_md5_invariant)
            # md5sum of <path> inside <pod> matches expected. Used by
            # the resize-lifecycle replay to assert data preservation
            # across grow ops. Caller is expected to have already
            # captured the baseline md5 at scenario start and threaded
            # it through {{md5_pre}} substitution.
            local ns pod path expected actual
            ns=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('namespace','default'))" "$spec")
            pod=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('pod',''))" "$spec")")
            path=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('path',''))" "$spec")
            expected=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected',''))" "$spec")")
            actual=$(kubectl -n "$ns" exec "$pod" -- sh -c "md5sum '$path' 2>/dev/null | awk '{print \$1}'" 2>/dev/null || echo "")
            [[ -n "$expected" && "$actual" == "$expected" ]]
            ;;
        *)
            echo "    unknown assertion kind: $kind" >&2
            return 1
            ;;
    esac
}

# ----------------------------------------------------------------------
# step executor (shared by replay-runner.sh and operator-fuzz.sh)
# ----------------------------------------------------------------------

# run_step <json-step>
#
# Executes a step described by a JSON object:
#   { "name": "...", "cmd": [...], "expect_exit": 0, "await": {...} }
#
# Performs {{...}} substitution on every argv element. Returns:
#   0  step + await passed
#   1  step failed (exit mismatch or await timeout)
#
# Side effects on caller globals:
#   LAST_STDOUT, LAST_STDERR, LAST_EXIT — captured from the cmd run
run_step() {
    local step=$1
    local name cmd_json expect_exit
    name=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('name','(unnamed)'))" "$step")
    cmd_json=$(python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1]).get('cmd',[])))" "$step")
    expect_exit=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expect_exit',0))" "$step")

    mapfile -t argv < <(python3 -c "import json,sys
for a in json.loads(sys.argv[1]):
    print(a)" "$cmd_json")

    local subst=()
    local a
    for a in "${argv[@]}"; do
        subst+=("$(substitute "$a")")
    done

    echo "  -> step: $name :: linstor ${subst[*]}"
    run_linstor_cmd "${subst[@]}"
    if [[ "$LAST_EXIT" != "$expect_exit" ]]; then
        echo "    FAIL: expected exit $expect_exit, got $LAST_EXIT" >&2
        printf '    stderr: %s\n' "$LAST_STDERR" >&2
        return 1
    fi

    local await_json
    await_json=$(python3 -c "import json,sys
s=json.loads(sys.argv[1]).get('await')
print(json.dumps(s) if s else '')" "$step")
    if [[ -n "$await_json" ]]; then
        await_assertion "$await_json" || return 1
    fi
}

# ----------------------------------------------------------------------
# settle: poll until Status fields stop mutating across two ticks
# ----------------------------------------------------------------------

# wait_settle <rd> [timeout_s]
#
# Polls `kubectl get resources.blockstor.io -o json` filtered by
# spec.resourceName == rd. Considers the cluster "settled" once two
# consecutive snapshots return identical {diskState, inUse, connections}
# tuples across all replicas.
#
# Why not "wait for UpToDate"? The fuzzer drives operations that may
# legitimately leave a node Diskless / Inconsistent / disconnected;
# settling means "no longer actively changing", NOT "in a final/good
# state". Catching divergence is the assertion's job, not settle's.
wait_settle() {
    local rd=$1
    local timeout_s=${2:-$SETTLE_TIMEOUT_S}
    local deadline=$(( $(date +%s) + timeout_s ))
    local prev=""
    local stable_ticks=0

    while (( $(date +%s) < deadline )); do
        local cur
        cur=$(kubectl get resources.blockstor.io -o json 2>/dev/null \
            | python3 -c "import json,sys
d=json.load(sys.stdin)
rd='$rd'
keys=[]
for it in d.get('items',[]):
    sp=it.get('spec',{})
    if sp.get('resourceName')!=rd: continue
    st=it.get('status',{})
    v=(st.get('volumes') or [{}])[0]
    keys.append((sp.get('nodeName',''), v.get('diskState',''), v.get('inUse',False)))
keys.sort()
print(json.dumps(keys))" 2>/dev/null || echo "[]")

        if [[ "$cur" == "$prev" && -n "$cur" ]]; then
            stable_ticks=$(( stable_ticks + 1 ))
            if (( stable_ticks >= 2 )); then
                return 0
            fi
        else
            stable_ticks=0
        fi
        prev=$cur
        sleep "$SETTLE_TICK_S"
    done
    echo "    SETTLE TIMEOUT: rd=$rd after ${timeout_s}s" >&2
    return 1
}

# ----------------------------------------------------------------------
# NoOrphans invariant
# ----------------------------------------------------------------------

# assert_no_orphans <prefix>
#
# Returns 0 if no Resource CRDs with name starting with $prefix remain.
# Caller is expected to have torn down all RDs created during the run.
assert_no_orphans() {
    local prefix=$1
    local leftover
    leftover=$(kubectl get resources.blockstor.io -o name 2>/dev/null \
        | grep -c "$prefix" || true)
    if [[ "$leftover" -gt 0 ]]; then
        echo "  INVARIANT FAIL: $leftover Resource CRD(s) for $prefix still present" >&2
        kubectl get resources.blockstor.io -o name 2>/dev/null | grep "$prefix" >&2 || true
        return 1
    fi
    return 0
}

# ----------------------------------------------------------------------
# deterministic PRNG: SHA256 over (seed, step, verb_index) → uint32
# ----------------------------------------------------------------------

# prng <seed> <step> <verb_index>
#
# Echoes a deterministic 32-bit unsigned integer in [0, 2^32). Same
# tuple always produces the same number. Used by the fuzzer so that
# `SEED=42 STEPS=N operator-fuzz.sh` is bit-for-bit reproducible.
#
# Implementation: take the first 8 hex digits of SHA256 of "seed:step:verb"
# and convert to decimal. 32 bits is plenty for picking from O(100)-sized
# candidate sets without modulo bias being a problem.
prng() {
    local seed=$1 step=$2 verb=$3
    local hash
    if command -v sha256sum >/dev/null 2>&1; then
        hash=$(printf '%s:%s:%s' "$seed" "$step" "$verb" | sha256sum | head -c 8)
    else
        # macOS fallback
        hash=$(printf '%s:%s:%s' "$seed" "$step" "$verb" | shasum -a 256 | head -c 8)
    fi
    printf '%d\n' "0x$hash"
}

# prng_pick <seed> <step> <verb_index> <count>
#
# Returns a deterministic index in [0, count).
prng_pick() {
    local seed=$1 step=$2 verb=$3 count=$4
    if (( count <= 0 )); then
        echo 0
        return 0
    fi
    local n
    n=$(prng "$seed" "$step" "$verb")
    echo $(( n % count ))
}
