#!/usr/bin/env bash
#
# verb-rg-modify.sh — `resource-group modify <rg> --place-count <N>`.
#
# Toggles place_count between [1, 2, 3] for an existing resource
# group. We only ever modify the default RG ("DfltRscGrp") plus any
# fuzz-prefixed RG — modifying user-defined RGs would surprise the
# operator.

verb_rg_modify_name() { echo "rg modify"; }

verb_rg_modify_precondition() {
    # DfltRscGrp always exists, so this verb is always applicable.
    # But we still gate on at least one RG showing up in state.
    printf '%s' "$CLUSTER_STATE_JSON" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read() or '{}')
ok = any(g['name'] == 'DfltRscGrp' for g in d.get('rgs', []))
sys.exit(0 if ok else 1)
"
}

verb_rg_modify_generate() {
    local prng=$1
    local counts=(1 2 3)
    local count_idx=$(( prng % ${#counts[@]} ))
    local pc=${counts[$count_idx]}
    printf '%s\n' "resource-group" "modify" "DfltRscGrp" "--place-count" "$pc"
}
