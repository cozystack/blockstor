#!/usr/bin/env bash
#
# verb-vd-c.sh — `volume-definition create <rd> <size>`.
#
# Precondition: at least one fuzz-owned RD with NO VolumeDefinition
# yet. The fuzzer doesn't (yet) extend VDs, so each RD gets at most
# one. Sizes chosen from a small fixed pool — large enough for DRBD
# meta overhead but small enough to keep teardown fast.

verb_vd_c_name() { echo "vd c"; }

verb_vd_c_precondition() {
    local count
    count=$(state_fuzz_rds | python3 -c "
import json,sys
n=0
for line in sys.stdin:
    r=json.loads(line)
    if not r['has_vd']: n+=1
print(n)")
    (( count > 0 ))
}

verb_vd_c_generate() {
    local prng=$1
    # Pick a deterministic RD without a VD.
    local candidates rd_name
    candidates=$(state_fuzz_rds | python3 -c "
import json,sys
for line in sys.stdin:
    r=json.loads(line)
    if not r['has_vd']: print(r['name'])")
    local n_cand
    n_cand=$(echo "$candidates" | grep -c . || true)
    if (( n_cand == 0 )); then
        return 1
    fi
    local idx=$(( prng % n_cand ))
    rd_name=$(echo "$candidates" | sed -n "$((idx+1))p")

    # Size pool: 8M..128M, deterministic per prng.
    local sizes=(8M 16M 32M 64M 128M)
    local size_idx=$(( (prng / 7) % ${#sizes[@]} ))
    local size=${sizes[$size_idx]}

    printf '%s\n' "volume-definition" "create" "$rd_name" "$size"
}
