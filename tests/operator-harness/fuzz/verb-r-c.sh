#!/usr/bin/env bash
#
# verb-r-c.sh — `resource create <node> <rd> --storage-pool <sp>`.
#
# Precondition: at least one fuzz-owned RD with a VD and at least one
# node that does NOT already hold a replica of that RD AND has a
# matching storage pool. We deliberately stick to node-pinned creates
# (not --auto-place) so the fuzz step is a single, reproducible action.

verb_r_c_name() { echo "r c"; }

verb_r_c_precondition() {
    # Need at least one RD with VD and a free (rd,node) slot.
    state_fuzz_rds | python3 -c "
import json,sys,os
sps_env = os.environ.get('FUZZ_SP','stand')
nodes = os.environ.get('FUZZ_NODES','').split()
ok = False
for line in sys.stdin:
    r = json.loads(line)
    if not r['has_vd']: continue
    occupied = {x['node'] for x in r['replicas']}
    free = [n for n in nodes if n not in occupied]
    if free:
        ok = True; break
sys.exit(0 if ok else 1)
"
}

verb_r_c_generate() {
    local prng=$1
    local sp=${FUZZ_SP:-stand}
    # Build candidate list: (rd, node) pairs with rd has VD and node free.
    local cands
    cands=$(state_fuzz_rds | FUZZ_NODES="$FUZZ_NODES" python3 -c "
import json,sys,os
nodes = os.environ['FUZZ_NODES'].split()
for line in sys.stdin:
    r = json.loads(line)
    if not r['has_vd']: continue
    occupied = {x['node'] for x in r['replicas']}
    for n in nodes:
        if n not in occupied:
            print(f\"{r['name']} {n}\")
")
    local n_cand
    n_cand=$(echo "$cands" | grep -c . || true)
    if (( n_cand == 0 )); then
        return 1
    fi
    local idx=$(( prng % n_cand ))
    local pair
    pair=$(echo "$cands" | sed -n "$((idx+1))p")
    local rd node
    rd=${pair% *}
    node=${pair#* }
    printf '%s\n' "resource" "create" "$node" "$rd" "--storage-pool" "$sp"
}
