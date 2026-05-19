#!/usr/bin/env bash
#
# verb-rd-c.sh — `resource-definition create <unique-name>`.
#
# Precondition: always applicable (no cluster state required). We cap
# total fuzz-owned RDs at $FUZZ_MAX_RDS (default 8) so the cluster
# doesn't grow unbounded across a long run.

verb_rd_c_name() { echo "rd c"; }

verb_rd_c_precondition() {
    local max=${FUZZ_MAX_RDS:-8}
    local current
    current=$(state_fuzz_rds | wc -l | tr -d ' ')
    (( current < max ))
}

# verb_rd_c_generate <prng>
#
# Emits a new RD name keyed off the prng value so reruns with the
# same seed/step pick the same name.
verb_rd_c_generate() {
    local prng=$1
    local suffix
    suffix=$(printf '%08x' "$prng" | head -c 6)
    local name="${FUZZ_PREFIX:-fuzz}-${suffix}"
    printf '%s\n' "resource-definition" "create" "$name"
}
