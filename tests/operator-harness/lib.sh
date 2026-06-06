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

# `node evict` is a controller-side action that the bundled linstor-client
# 1.27.1 does not expose as a subcommand (it only ships evacuate/lost/
# restore). The REST controller, however, implements it as
# `PUT /v1/nodes/<node>/evict`. So a replay step can stay operator-faithful
# (`cmd: ["node", "evict", "<node>"]`) and we transparently drive it through
# the REST endpoint here. The same call is mirrored by the cli-matrix
# n-evict catcher's expectation. Recognise the long form and the `n e` /
# `node e` short forms.
linstor_cli() {
    if [[ "$1" == "node" || "$1" == "n" ]] \
        && [[ "$2" == "evict" || "$2" == "e" ]] \
        && [[ -n "${3:-}" ]]; then
        local node=$3
        local base=${BS_URL:?BS_URL required}
        # curl returns 0 on a 200; map any non-2xx to a non-zero exit so the
        # step's expect_exit contract still holds.
        curl -fsS -m 10 -X PUT "${base%/}/v1/nodes/${node}/evict" >/dev/null
        return $?
    fi
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
    s=${s//\{\{rg\}\}/${RG:-}}
    # {{device}} resolves from vars.device (physical-storage workflows like
    # ps-cdp-zfs). Without this branch the placeholder passed through verbatim
    # and the device-pool create was handed the literal string "{{device}}" --
    # same class of bug as the earlier {{rg}} pass-through.
    s=${s//\{\{device\}\}/${DEVICE:-}}
    s=${s//\{\{node1\}\}/${NODE1:-}}
    s=${s//\{\{node2\}\}/${NODE2:-}}
    s=${s//\{\{node3\}\}/${NODE3:-}}
    s=${s//\{\{node4\}\}/${NODE4:-}}
    echo "$s"
}

# ----------------------------------------------------------------------
# fixture probes (used by replay-runner.sh prerequisites)
# ----------------------------------------------------------------------

# fixture_sp_node_count <pool-name>
#
# Echoes the number of distinct nodes on which the named LINSTOR storage
# pool is registered (provider_kind != null). Used by the
# prerequisites.storage_pool_min_nodes SKIP gate so a workflow that needs
# a pool the stand does not have (e.g. a thick LVM pool) SKIPs cleanly
# instead of FAILing. Uses the machine-readable `-m` surface so the parse
# is column-agnostic.
fixture_sp_node_count() {
    local pool=$1
    linstor_cli -m storage-pool list --storage-pools "$pool" 2>/dev/null         | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
while isinstance(d, list) and d and isinstance(d[0], list):
    d=d[0]
nodes=set()
for it in d if isinstance(d, list) else []:
    if not isinstance(it, dict): continue
    if it.get('provider_kind') is None: continue
    n=it.get('node_name')
    if n: nodes.add(n)
print(len(nodes))" 2>/dev/null || echo 0
}

# fixture_device_on_any_worker <device-path>
#
# Returns 0 if <device-path> is a present block device on at least one
# worker's satellite pod, 1 otherwise. Used by the
# prerequisites.device_on_any_node SKIP gate so the ps-cdp-zfs workflow
# (which needs a sacrificial loop device that only some stands provision)
# SKIPs cleanly instead of FAILing on stands without it. Probes the
# satellite pod via kubectl exec; if no satellite pod is reachable we
# conservatively report absent (the caller SKIPs).
fixture_device_on_any_worker() {
    local dev=$1
    local ns=${SATELLITE_NS:-blockstor-system}
    local pods
    mapfile -t pods < <(kubectl -n "$ns" get pods -l app=blockstor-satellite         --field-selector status.phase=Running         -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n')
    local p
    for p in "${pods[@]}"; do
        [[ -z "$p" ]] && continue
        if kubectl -n "$ns" exec "$p" -- test -b "$dev" >/dev/null 2>&1; then
            return 0
        fi
    done
    return 1
}

# ----------------------------------------------------------------------
# assertion polling (used by replay AND fuzz)
# ----------------------------------------------------------------------

await_assertion() {
    local spec=$1
    local kind timeout_s deadline hold_s held_since now
    kind=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('kind',''))" "$spec")
    timeout_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('timeout_s',${ASSERT_TIMEOUT_S}))" "$spec")
    # Optional hold_s: the assertion must not only become true once but
    # STAY true for hold_s consecutive seconds. Catches value flapping
    # (e.g. an operator-set property that a reconciler keeps reverting:
    # a plain first-match await can sample the lucky instant between
    # the write and the revert and false-PASS — seen on corner-B2).
    # Any failed sample resets the hold window. The overall deadline
    # spans timeout_s + hold_s so a late first success still gets a
    # full hold window.
    hold_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('hold_s',0))" "$spec")
    deadline=$(( $(date +%s) + timeout_s + hold_s ))
    held_since=""

    while (( $(date +%s) < deadline )); do
        if check_assertion "$kind" "$spec"; then
            if (( hold_s == 0 )); then
                return 0
            fi
            now=$(date +%s)
            [[ -n "$held_since" ]] || held_since=$now
            if (( now - held_since >= hold_s )); then
                return 0
            fi
        else
            held_since=""
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
            count=$(linstor_cli -m resource list --resources "$rd" 2>/dev/null \
                | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    while isinstance(d, list) and d and isinstance(d[0], list):
        d=d[0]
    print(len(d))
except: print(0)")
            [[ "$count" -ge "$min" ]]
            ;;
        replica_count_max)
            # Upstream-issue U222 (non-retroactive placement): assert the
            # replica count of rd NEVER EXCEEDS `max`. This is a NEGATIVE
            # assertion — pair it with `hold_s: <N>` so the await holds the
            # "count <= max" condition for N consecutive seconds, proving no
            # background reconcile materialised extra replicas (e.g. after an
            # RD is reassigned to a higher-place-count RG, which must NOT
            # auto-deploy). Mirrors replica_count's enumeration exactly, only
            # the comparison flips to `-le`.
            local rd max count
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            max=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('max',0))" "$spec")
            count=$(linstor_cli -m resource list --resources "$rd" 2>/dev/null \
                | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    while isinstance(d, list) and d and isinstance(d[0], list):
        d=d[0]
    print(len(d))
except: print(0)")
            [[ "$count" -le "$max" ]]
            ;;
        active_diskful_count)
            # Bug 393: count replicas of rd that are ACTIVE diskful —
            # i.e. NOT DISKLESS / TIE_BREAKER (no backing disk) and NOT
            # INACTIVE (`drbdadm down`, non-voting, non-serving). This is
            # exactly what place_count must be measured against: an
            # INACTIVE diskful replica does NOT count toward satisfied
            # redundancy, so the placer must gap-fill to reach `min`.
            local rd min count
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            min=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('min',2))" "$spec")
            count=$(kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
                | python3 -c "import json,sys
d=json.load(sys.stdin)
rd='$rd'
n=0
for it in d.get('items',[]):
    if it.get('spec',{}).get('resourceDefinitionName')!=rd: continue
    flags=it.get('spec',{}).get('flags',[]) or []
    if 'DISKLESS' in flags or 'TIE_BREAKER' in flags: continue
    if 'INACTIVE' in flags: continue
    n+=1
print(n)")
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
            bad=$(kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
                | python3 -c "import json,sys
d=json.load(sys.stdin)
rd='$rd'
bad=0
seen=0
for it in d.get('items',[]):
    if it.get('spec',{}).get('resourceDefinitionName')!=rd: continue
    seen+=1
    for v in it.get('status',{}).get('volumes',[]) or []:
        # A diskless / tiebreaker replica reports diskState 'Diskless' and is
        # never 'UpToDate' by design — accept it. Only a DISKFUL replica that
        # has not reached UpToDate (Inconsistent / Outdated / SyncTarget / …)
        # counts as 'bad', i.e. not-yet-converged.
        if v.get('diskState') not in ('UpToDate','Diskless'): bad+=1
# No matching replica at all means the rd is absent / not yet observed — that
# is NOT 'all uptodate', so report it as bad so the waiter keeps polling.
if seen==0: bad+=1
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
            # Match the LINSTOR State token `TieBreaker` case-SENSITIVELY:
            # the replay RD names themselves contain the lowercase substring
            # "tiebreaker" (replay-*-tiebreaker-*), so a case-insensitive
            # grep -ci false-matches the resource name on EVERY data row and
            # the count can never reach 0 — the assertion would wrongly time
            # out even when the cluster has no witness. The State column
            # always renders the witness as `TieBreaker` (capital T/B).
            present=$(linstor_cli resource list --resources "$rd" 2>/dev/null | grep -c 'TieBreaker' || true)
            [[ "$present" == "0" ]]
            ;;
        tiebreaker_present)
            # Bug 386: assert a TieBreaker witness EXISTS for the rd.
            # The inverse of no_tiebreaker — used by the node-restore
            # catcher to confirm the witness is RE-created after the
            # drained node is brought back with `n rst`.
            local rd present
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            # Case-SENSITIVE on purpose — see the no_tiebreaker note above: a
            # case-insensitive grep would match the lowercase "tiebreaker"
            # substring in the RD name and report a witness present on every
            # row, masking a genuinely missing TieBreaker.
            present=$(linstor_cli resource list --resources "$rd" 2>/dev/null | grep -c 'TieBreaker' || true)
            [[ "$present" -ge 1 ]]
            ;;
        prop_value)
            # Corner-case B (B1/B4/B5) + I1: assert a property on an
            # object's list-properties surface. spec fields:
            #   obj:      "rd" (default), "rg", "node", or "controller"
            #             — which object kind. (corner-case I1 added the
            #             node/controller cases for the non-RD
            #             empty-value=delete pins.)
            #   name:     object name ({{rd}} / {{rg}} / {{node}}
            #             substituted). Ignored for "controller".
            #   key:      property key (e.g. DrbdOptions/Resource/quorum)
            #   expected: desired value. If OMITTED or "", the key must
            #             be ABSENT (empty-value=delete / B5). Otherwise
            #             the key must be present with exactly this value.
            # Uses the machine-readable `-m` properties listing so the
            # parse is column-agnostic (the python human table aligns
            # differently across client versions).
            local p_kind p_name p_key p_expected p_obj p_actual p_present
            p_kind=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('obj','rd'))" "$spec")
            p_name=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('name',''))" "$spec")")
            p_key=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('key',''))" "$spec")
            p_expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected',''))" "$spec")
            case "$p_kind" in
                rg)
                    p_obj=$(linstor_cli -m resource-group list-properties "$p_name" 2>/dev/null || echo "")
                    ;;
                node)
                    p_obj=$(linstor_cli -m node list-properties "$p_name" 2>/dev/null || echo "")
                    ;;
                controller)
                    p_obj=$(linstor_cli -m controller list-properties 2>/dev/null || echo "")
                    ;;
                *)
                    p_obj=$(linstor_cli -m resource-definition list-properties "$p_name" 2>/dev/null || echo "")
                    ;;
            esac
            # The -m list-properties payload is a list of {key,value}
            # entries (possibly double-nested by golinstor). Resolve the
            # value for p_key, reporting present/absent + the value.
            read -r p_present p_actual < <(printf '%s' "$p_obj" | KEY="$p_key" python3 -c "import json,sys,os
key=os.environ['KEY']
try:
    d=json.load(sys.stdin)
except Exception:
    print('0 '); sys.exit(0)
while isinstance(d, list) and d and isinstance(d[0], list):
    d=d[0]
for it in d if isinstance(d, list) else []:
    if not isinstance(it, dict): continue
    if it.get('key')==key:
        print('1 '+str(it.get('value',''))); sys.exit(0)
print('0 ')")
            if [[ -z "$p_expected" ]]; then
                # Absence assertion (B5).
                [[ "$p_present" == "0" ]]
            else
                [[ "$p_present" == "1" && "$p_actual" == "$p_expected" ]]
            fi
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
            # The target volume number is passed as argv[1] to the parser.
            # It must NOT ride a `VOL=... cmd | python3` env prefix: in a
            # pipeline the prefix binds to the LEFT command (linstor_cli),
            # never the right (python3), so os.environ['VOL'] would KeyError
            # and the parser always fell through to print(0) — vd_size_kib
            # could never pass and the resize lifecycle was permanently red.
            actual=$(linstor_cli -m volume-definition list --resource-definitions "$rd" 2>/dev/null \
                | python3 -c "import json,sys
try:
    target=int(sys.argv[1])
    d=json.load(sys.stdin)
    while isinstance(d, list) and d and isinstance(d[0], list):
        d=d[0]
    for it in d if isinstance(d, list) else []:
        for v in it.get('vlm_dfns', []) or it.get('volume_definitions', []) or []:
            if v.get('volume_number', v.get('vlm_nr', -1)) == target:
                print(v.get('size_kib', v.get('sizeKib', 0)))
                sys.exit(0)
    print(0)
except Exception:
    print(0)" "$vol" 2>/dev/null || echo 0)
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
        volumes_settled)
            # Bug 399: after `vd d`, every Resource of rd must converge
            # to EXACTLY the expected volume-number set in BOTH
            # spec.volumes and status.volumes, AND stop flapping. The
            # bug left a stale spec.volumes[removed] + phantom
            # status.volumes[removed]=Diskless that the observer kept
            # re-emitting — the diskless/tiebreaker replica's
            # status.volumes oscillated between [0] and [0,1] ~1/tick.
            #
            # spec: `expected` is a comma-separated volume-number set
            # (e.g. "0"). We assert two things in one pass:
            #   (a) no Resource carries a volume NOT in `expected` in
            #       either spec.volumes or status.volumes (the prune /
            #       status-GC half), and every diskful replica carries
            #       all expected volumes in spec.volumes;
            #   (b) the per-replica status.volumes SET is byte-stable
            #       across N consecutive polls spanning `settle_s` (the
            #       no-flap half) — a still-flapping replica changes its
            #       volume set between polls.
            #
            # NOTE: the no-flap half deliberately compares the VOLUME SET,
            # NOT metadata.resourceVersion. On the stand's k3s/kine
            # backend a single-object (or list) GET reports the GLOBAL
            # store revision, so resourceVersion climbs on every unrelated
            # write even when THIS resource has zero churn — an rv-equality
            # check can never pass there and is a false negative. The
            # volume-set comparison is kine-safe and is exactly what
            # catches the real phantom (validation: the volume-set half
            # flagged `VOLSET_BAD flap399.dev-worker-3 status=[0,1]`).
            local rd expected settle_s polls
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected','0'))" "$spec")
            settle_s=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('settle_s',6))" "$spec")
            # N consecutive polls (>=2). The window settle_s is split
            # evenly across the gaps so total wall time stays ~settle_s.
            polls=$(python3 -c "import json,sys; print(max(2,int(json.loads(sys.argv[1]).get('polls',3))))" "$spec")

            local prev="" cur gap i
            gap=$(python3 -c "import sys; print(max(1, int(float(sys.argv[1])/(int(sys.argv[2])-1))))" "$settle_s" "$polls")
            for (( i=0; i<polls; i++ )); do
                cur=$(volumes_settled_snapshot "$rd" "$expected") || return 1
                # volume sets must be in-set and the rd must be observed.
                [[ "$cur" == VOLSET_BAD* ]] && return 1
                [[ -z "$cur" ]] && return 1
                # Identical volume-set map across consecutive polls => no
                # flap. Any change between polls is a live oscillation.
                if [[ -n "$prev" && "$cur" != "$prev" ]]; then
                    return 1
                fi
                prev="$cur"
                (( i < polls-1 )) && sleep "$gap"
            done

            return 0
            ;;
        drbd_option)
            # Corner-case C1/C2: assert the rendered DRBD config on a
            # node carries <key> == <expected> for rd. Reads the live
            # kernel config via `drbdsetup show <rd>` on the satellite
            # pod scheduled on <node>, so it reflects the full
            # Controller→RG→RD→Resource inheritance + "closer wins"
            # precedence — not just what the CRD stored. Used to pin the
            # effectiveprops controller-tier precedence fix end-to-end.
            #
            # spec fields:
            #   rd        resource-definition name (substituted)
            #   node      node whose satellite pod to probe (substituted)
            #   key       drbd option token as drbdsetup prints it
            #             (e.g. "max-buffers")
            #   expected  expected value (string match)
            #   namespace satellite namespace (default blockstor-system)
            #   show_defaults  optional bool: pass --show-defaults so an
            #             option whose configured value EQUALS the
            #             compiled-in default (which plain `drbdsetup
            #             show` omits) is still printed and assertable
            #             (e.g. FILE_THIN's discard-zeroes-if-aligned
            #             yes). Leave unset for absence assertions
            #             (expected "") — with --show-defaults nothing
            #             is ever absent.
            local rd node key expected ns pod actual showdef showflag
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            node=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('node',''))" "$spec")")
            key=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('key',''))" "$spec")
            expected=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('expected',''))" "$spec")
            ns=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('namespace','blockstor-system'))" "$spec")
            showdef=$(python3 -c "import json,sys; print(str(json.loads(sys.argv[1]).get('show_defaults',False)).lower())" "$spec")
            showflag=""
            if [[ "$showdef" == "true" ]]; then
                showflag="--show-defaults"
            fi
            pod=$(kubectl -n "$ns" get pods -l app=blockstor-satellite \
                --field-selector "spec.nodeName=${node},status.phase=Running" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -z "$pod" ]]; then
                return 1
            fi
            # `drbdsetup show` prints numeric options bare (`max-buffers
            # 36864;`) but STRING options quoted (`verify-alg "crc32c";`).
            # Strip both the trailing `;` AND surrounding double-quotes so
            # a string-valued option (verify-alg, cram-hmac-alg, …) matches
            # its unquoted `expected`. Without the quote-strip the await
            # compared `"crc32c"` against `crc32c` and always timed out
            # (U302: verify-alg DOES render verbatim into net{}, confirmed
            # via drbdsetup on the stand — the miss was the parser, not BS).
            # shellcheck disable=SC2086  # $showflag is deliberately word-split (empty or one flag)
            actual=$(kubectl -n "$ns" exec "$pod" -- drbdsetup show $showflag "$rd" 2>/dev/null \
                | awk -v k="$key" '$1==k { gsub(/[;"]/,""); print $2; exit }')
            [[ "$actual" == "$expected" ]]
            ;;
        quorum)
            # Upstream-issue U341 (P1, "Lost quorum when migrating a
            # resource to another node"): assert the live DRBD quorum
            # on <node>'s satellite, read straight from the kernel via
            # `drbdsetup status <rd> --json`. Pair with `hold_s: <N>`
            # to prove quorum is HELD for N consecutive seconds across
            # a migration window — a single transient `quorum:false`
            # sample (the U341 symptom: the migrate vacated the
            # quorum-providing peer before the new diskful was
            # UpToDate) fails the assertion.
            #
            # spec fields:
            #   rd        resource-definition name (substituted)
            #   node      node whose satellite pod to probe; this is
            #             the SURVIVING replica we assert keeps quorum
            #   expected  "true" (default) | "false"
            #   namespace satellite namespace (default blockstor-system)
            #
            # The probe reads the device-level `quorum` flag DRBD
            # stamps per-volume. A node whose resource isn't up yet
            # (no JSON / parse error) reports not-quorate, so a
            # standalone `quorum: true` await also doubles as a
            # "resource is up and quorate on this node" gate.
            local rd node expected ns pod actual
            rd=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('rd',''))" "$spec")")
            node=$(substitute "$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('node',''))" "$spec")")
            expected=$(python3 -c "import json,sys; print(str(json.loads(sys.argv[1]).get('expected','true')).lower())" "$spec")
            ns=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('namespace','blockstor-system'))" "$spec")
            pod=$(kubectl -n "$ns" get pods -l app=blockstor-satellite \
                --field-selector "spec.nodeName=${node},status.phase=Running" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -z "$pod" ]]; then
                return 1
            fi
            # `drbdsetup status --json` prints an array of resources,
            # each with a `devices` array carrying a per-volume
            # `quorum` boolean. Quorate iff EVERY local device is
            # quorate (a multi-volume resource must hold quorum on all
            # of them). Any parse failure / missing resource ⇒ not
            # quorate (returns "false"), so the await keeps polling
            # rather than false-PASSing on a transient read error.
            actual=$(kubectl -n "$ns" exec "$pod" -- drbdsetup status "$rd" --json 2>/dev/null \
                | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    devs=[]
    for r in d:
        devs += r.get('devices',[])
    if not devs:
        print('false')
    else:
        print('true' if all(bool(v.get('quorum',False)) for v in devs) else 'false')
except Exception:
    print('false')")
            [[ "$actual" == "$expected" ]]
            ;;
        *)
            echo "    unknown assertion kind: $kind" >&2
            return 1
            ;;
    esac
}

# volumes_settled_snapshot <rd> <expected-csv>
#
# Helper for the volumes_settled assertion (Bug 399). Emits a stable,
# sorted "<name>=<sorted-status-volume-set>" line per Resource of rd, but
# ONLY if every Resource's spec.volumes and status.volumes volume-number
# sets are consistent with the expected set:
#   - no volume present that is NOT in `expected` (stale / phantom);
#   - status.volumes may be a SUBSET (a freshly-observed replica may not
#     have stamped every volume yet, and diskless/tiebreaker rows carry
#     fewer) but must never carry an out-of-set volume.
# Prints "VOLSET_BAD <detail>" (and returns 0) when a volume-set
# violation is found, so the caller treats it as not-yet-settled.
#
# The emitted value is the per-replica status.volumes volume-number SET
# (NOT metadata.resourceVersion). The caller's no-flap half compares
# consecutive snapshots for byte-equality: a still-flapping diskless
# replica oscillates its status volume set ([0] <-> [0,1]) between polls,
# so the set changes and the comparison fails. resourceVersion is NOT
# usable here because the stand's kine backend reports the global store
# revision per GET, which climbs on every unrelated write.
volumes_settled_snapshot() {
    local rd=$1 expected=$2
    kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
        | EXPECTED="$expected" RD="$rd" python3 -c "import json,sys,os
d=json.load(sys.stdin)
rd=os.environ['RD']
expected=set(int(x) for x in os.environ['EXPECTED'].split(',') if x.strip()!='')
rows=[]
seen=0
for it in d.get('items',[]):
    sp=it.get('spec',{})
    if sp.get('resourceDefinitionName')!=rd: continue
    seen+=1
    name=it.get('metadata',{}).get('name','')
    spec_nums=set(v.get('volumeNumber') for v in (sp.get('volumes') or []))
    st_nums=set(v.get('volumeNumber') for v in ((it.get('status',{}) or {}).get('volumes') or []))
    # Any volume outside the expected set is a stale spec entry or a
    # phantom status entry -> not settled.
    if (spec_nums - expected) or (st_nums - expected):
        print('VOLSET_BAD %s spec=%s status=%s expected=%s' % (name, sorted(x for x in spec_nums if x is not None), sorted(x for x in st_nums if x is not None), sorted(expected)))
        sys.exit(0)
    # Emit the per-replica STATUS volume set. The no-flap half compares
    # consecutive snapshots: a flapping replica's status set changes.
    st_sorted=sorted(x for x in st_nums if x is not None)
    rows.append('%s=%s' % (name, st_sorted))
if seen==0:
    # rd not observed yet -> not settled (empty output).
    sys.exit(0)
rows.sort()
print('\n'.join(rows))"
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
    # expect_exit may be a scalar (default 0) OR a list of acceptable codes.
    # A list is needed for idempotent steps whose exit depends on shared
    # pre-existing controller state (e.g. `encryption create-passphrase`
    # returns 0 on a fresh controller and 10 when a passphrase already
    # exists on the shared stand). Emit the accepted codes one per line.
    mapfile -t expect_exits < <(python3 -c "import json,sys
e=json.loads(sys.argv[1]).get('expect_exit',0)
for v in (e if isinstance(e,list) else [e]):
    print(v)" "$step")

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
    local ok=0 code
    for code in "${expect_exits[@]}"; do
        [[ "$LAST_EXIT" == "$code" ]] && { ok=1; break; }
    done
    if [[ "$ok" != "1" ]]; then
        echo "    FAIL: expected exit ${expect_exits[*]}, got $LAST_EXIT" >&2
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
# Polls `kubectl get resources.blockstor.cozystack.io -o json` filtered by
# spec.resourceDefinitionName == rd. Considers the cluster "settled" once two
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
        cur=$(kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
            | python3 -c "import json,sys
d=json.load(sys.stdin)
rd='$rd'
keys=[]
for it in d.get('items',[]):
    sp=it.get('spec',{})
    if sp.get('resourceDefinitionName')!=rd: continue
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
#
# Teardown (`rd delete`) removes the Resource CRDs asynchronously: the
# apiserver returns from the delete call before the satellite has finished
# `drbdadm down` + finalizer removal, so a single snapshot taken right after
# teardown races the GC and reports phantom orphans. Poll for up to
# NO_ORPHANS_SETTLE_S (default 30s), passing the instant the count reaches 0;
# only a count that is still non-zero after the window is a real orphan.
assert_no_orphans() {
    local prefix=$1
    local settle_s=${NO_ORPHANS_SETTLE_S:-30}
    local deadline=$(( $(date +%s) + settle_s ))
    local leftover
    while :; do
        leftover=$(kubectl get resources.blockstor.cozystack.io -o name 2>/dev/null \
            | grep -c "$prefix" || true)
        [[ "$leftover" -eq 0 ]] && return 0
        (( $(date +%s) >= deadline )) && break
        sleep 2
    done
    echo "  INVARIANT FAIL: $leftover Resource CRD(s) for $prefix still present after ${settle_s}s" >&2
    kubectl get resources.blockstor.cozystack.io -o name 2>/dev/null | grep "$prefix" >&2 || true
    return 1
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
