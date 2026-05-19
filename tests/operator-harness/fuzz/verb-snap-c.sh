#!/usr/bin/env bash
#
# verb-snap-c.sh — `snapshot create <rd> <snap-name>`.
#
# Precondition: at least one fuzz-owned RD with a VD AND at least
# one UpToDate replica (otherwise snapshot would fail by design).

verb_snap_c_name() { echo "snap c"; }

verb_snap_c_precondition() {
    state_fuzz_rds | python3 -c "
import json,sys
ok = False
for line in sys.stdin:
    r = json.loads(line)
    if not r['has_vd']: continue
    if any(x.get('diskstate')=='UpToDate' for x in r['replicas']):
        ok = True; break
sys.exit(0 if ok else 1)
"
}

verb_snap_c_generate() {
    local prng=$1
    local cands
    cands=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    r = json.loads(line)
    if not r['has_vd']: continue
    if any(x.get('diskstate')=='UpToDate' for x in r['replicas']):
        print(r['name'])
")
    local n_cand
    n_cand=$(echo "$cands" | grep -c . || true)
    if (( n_cand == 0 )); then
        return 1
    fi
    local idx=$(( prng % n_cand ))
    local rd
    rd=$(echo "$cands" | sed -n "$((idx+1))p")
    local snap_suffix
    snap_suffix=$(printf '%04x' $(( prng & 0xffff )))
    printf '%s\n' "snapshot" "create" "$rd" "${rd}-snap-${snap_suffix}"
}
