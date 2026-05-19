#!/usr/bin/env bash
#
# verb-r-d.sh — `resource delete <node> <rd>`.
#
# Precondition: pick an RD with ≥2 diskful replicas; we must leave
# the RD with at least one diskful replica afterwards so the volume
# still has a single source of truth. (Going to zero replicas is the
# rd-d verb's job; here we just exercise replica shrinkage.)

verb_r_d_name() { echo "r d"; }

verb_r_d_precondition() {
    state_fuzz_rds | python3 -c "
import json,sys
ok = False
for line in sys.stdin:
    r = json.loads(line)
    diskful = [x for x in r['replicas'] if x['diskful']]
    if len(diskful) >= 2:
        ok = True; break
sys.exit(0 if ok else 1)
"
}

verb_r_d_generate() {
    local prng=$1
    local cands
    cands=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    r = json.loads(line)
    diskful = [x for x in r['replicas'] if x['diskful']]
    if len(diskful) < 2: continue
    # Drop any replica EXCEPT the last diskful one. To keep it simple,
    # only consider non-first diskful replicas as deletion candidates.
    for rep in diskful[1:]:
        print(f\"{r['name']} {rep['node']}\")
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
    printf '%s\n' "resource" "delete" "$node" "$rd"
}
