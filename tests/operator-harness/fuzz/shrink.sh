#!/usr/bin/env bash
#
# shrink.sh — binary-search shrinker for a failing fuzz prefix.
#
# Sourced (NOT executed) by operator-fuzz.sh. Exposes:
#
#   run_shrinker <seed> <failing-step-N>
#
# Algorithm:
#
#   1. We know a full run with SEED produced a failure at step N.
#   2. Replay the run with SEED but STOP after `mid = N/2` steps.
#      If mid fails       → upper bound moves to mid; try mid/2.
#      If mid passes      → lower bound moves to mid; try (mid+N)/2.
#   3. Continue until lo+1 == hi. `hi` is the smallest failing prefix.
#   4. Emit $WORK_DIR/shrunk-finding-<seed>.yaml — the minimal
#      replay/*.yaml that reproduces the bug.
#
# The shrinker invokes operator-fuzz.sh recursively via the
# SHRINK_LIMIT env var so we don't fork an external process — same
# script, same code path, just stops earlier. This keeps the seed
# wiring tight: every shrink iteration is bit-for-bit a prefix of the
# original run.
#
# NOTE: a "passing" mid-prefix means the failure was state-dependent
# (e.g. needed step K to land for step K+5 to misbehave). The shrinker
# preserves ALL steps up to the failure boundary; it doesn't try to
# remove individual steps in the middle (that's delta-debugging,
# future work).

run_shrinker() {
    local seed=$1
    local failing_step=$2
    local lo=0 hi=$failing_step

    echo "=== shrinker: starting binary search for SEED=$seed (failure at step=$failing_step) ==="

    while (( hi - lo > 1 )); do
        local mid=$(( (lo + hi) / 2 ))
        echo "  shrink try: lo=$lo mid=$mid hi=$hi"

        local rc=0
        ( SHRINK_LIMIT=$mid \
          WORK_DIR="$WORK_DIR/shrink-$mid" \
          SEED=$seed STEPS=$mid \
          NO_SHRINK=1 \
          bash "$HARNESS_DIR/operator-fuzz.sh" --shrink-child ) || rc=$?

        if (( rc == 0 )); then
            # Passed — failure needs more steps.
            lo=$mid
        else
            # Failed at or before mid.
            hi=$mid
        fi
    done

    echo "=== shrunk: smallest failing prefix is $hi step(s) ==="

    # Re-run the smallest failing prefix once more, this time emitting
    # the finding YAML in shrunk form. The SHRINK_OUTPUT path is
    # anchored on the ORIGINAL WORK_DIR so the user finds it next to
    # the full-run finding.
    local shrunk_out="$WORK_DIR/shrunk-finding-${seed}.yaml"
    SHRINK_LIMIT=$hi \
    WORK_DIR="$WORK_DIR/shrunk" \
    SEED=$seed STEPS=$hi \
    NO_SHRINK=1 \
    SHRINK_OUTPUT="$shrunk_out" \
    bash "$HARNESS_DIR/operator-fuzz.sh" --shrink-child || true

    echo "shrunk finding YAML: $shrunk_out"
}
