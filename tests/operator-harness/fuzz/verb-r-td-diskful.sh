#!/usr/bin/env bash
#
# verb-r-td-diskful.sh — `resource toggle-disk --storage-pool <sp> <node> <rd>`.
#
# Inverse of verb-r-td-diskless. Precondition: an RD has at least one
# diskless replica we can re-disk. We need a valid SP on that node.

verb_r_td_diskful_name() { echo "r td --diskful"; }

verb_r_td_diskful_precondition() {
    state_fuzz_rds | python3 -c "
import json,sys
ok = False
for line in sys.stdin:
    r = json.loads(line)
    for x in r['replicas']:
        if not x['diskful']:
            ok = True; break
    if ok: break
sys.exit(0 if ok else 1)
"
}

verb_r_td_diskful_generate() {
    local prng=$1
    local sp=${FUZZ_SP:-stand}
    local cands
    cands=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    r = json.loads(line)
    for rep in r['replicas']:
        if not rep['diskful']:
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
    printf '%s\n' "resource" "toggle-disk" "--storage-pool" "$sp" "$node" "$rd"
}
