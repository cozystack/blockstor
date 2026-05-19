#!/usr/bin/env bash
#
# verb-r-td-diskless.sh — `resource toggle-disk --diskless <node> <rd>`.
#
# Precondition: at least one RD has ≥2 diskful replicas; we flip one
# of them (not the first) to diskless. Same "preserve a diskful peer"
# invariant as verb-r-d.

verb_r_td_diskless_name() { echo "r td --diskless"; }

verb_r_td_diskless_precondition() {
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

verb_r_td_diskless_generate() {
    local prng=$1
    local cands
    cands=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    r = json.loads(line)
    diskful = [x for x in r['replicas'] if x['diskful']]
    if len(diskful) < 2: continue
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
    printf '%s\n' "resource" "toggle-disk" "--diskless" "$node" "$rd"
}
