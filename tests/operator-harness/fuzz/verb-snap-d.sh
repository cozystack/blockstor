#!/usr/bin/env bash
#
# verb-snap-d.sh — `snapshot delete <rd> <snap-name>`.
#
# Precondition: at least one snapshot exists owned by a fuzz RD.

verb_snap_d_name() { echo "snap d"; }

verb_snap_d_precondition() {
    local prefix=${FUZZ_PREFIX:-fuzz}
    state_snaps | python3 -c "
import json,sys,os
prefix=os.environ.get('FUZZ_PREFIX','fuzz')
ok=False
for line in sys.stdin:
    s=json.loads(line)
    if s['rd'].startswith(prefix):
        ok=True; break
sys.exit(0 if ok else 1)
"
}

verb_snap_d_generate() {
    local prng=$1
    local cands
    cands=$(state_snaps | FUZZ_PREFIX="${FUZZ_PREFIX:-fuzz}" python3 -c "
import json,sys,os
prefix=os.environ['FUZZ_PREFIX']
for line in sys.stdin:
    s=json.loads(line)
    if s['rd'].startswith(prefix):
        print(f\"{s['rd']} {s['name']}\")
")
    local n_cand
    n_cand=$(echo "$cands" | grep -c . || true)
    if (( n_cand == 0 )); then
        return 1
    fi
    local idx=$(( prng % n_cand ))
    local pair
    pair=$(echo "$cands" | sed -n "$((idx+1))p")
    local rd snap
    rd=${pair% *}
    snap=${pair#* }
    printf '%s\n' "snapshot" "delete" "$rd" "$snap"
}
