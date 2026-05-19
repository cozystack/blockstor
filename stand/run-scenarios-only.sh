#!/usr/bin/env bash
# Run scenarios on an already-provisioned stand.
#
# Canonical source of truth for the stand-side runner. Lives in
# /tmp/run-scenarios-only.sh on the OCI dev stand (129.213.29.101) and
# is invoked by /tmp/run<N>-dispatch.sh for each parallel e2eN lane.
# The dispatcher itself stays out-of-band because it carries the
# per-run scenario matrix (SCEN[e2eN]=...), which changes every Run.
# This harness, by contrast, is stable across runs — keep it in repo
# so improvements (like the StoragePool guard below) are not lost
# when the operator regenerates the dispatcher.
#
# Usage:
#   bash /tmp/run-scenarios-only.sh <stand-name> <scenario1> [<scenario2> ...]
#
# Assumes:
#   - cluster up (talos+qemu)
#   - blockstor controller+apiserver+satellites Running
#   - StoragePool CRDs present (auto-provisioned below if missing)
set -uo pipefail
NAME=${1:?NAME required}
shift
SCENARIOS=("$@")
cd ~/blockstor
RESULTS=/tmp/e2e-$NAME.results
: > "$RESULTS"
echo "stand: $NAME (already provisioned)" >> "$RESULTS"
echo "scenarios: ${SCENARIOS[*]}" >> "$RESULTS"
echo "start: $(date -Iseconds)" >> "$RESULTS"

KUBECONFIG=.work/$NAME/kubeconfig
export KUBECONFIG

nodes_ready=$(kubectl --request-timeout=3s get nodes 2>/dev/null | grep -c " Ready ")
bs_running=$(kubectl --request-timeout=3s -n blockstor-system get pods 2>/dev/null | grep -c "Running")
echo "ready-check: nodes=$nodes_ready blockstor_system_running=$bs_running" >> "$RESULTS"
if [ "$nodes_ready" -lt 3 ] || [ "$bs_running" -lt 3 ]; then
    echo "FATAL: stand not ready (nodes=$nodes_ready bs=$bs_running)" >> "$RESULTS"
    echo "all-scenarios-done: $(date -Iseconds)" >> "$RESULTS"
    exit 1
fi

# StoragePool guard: when a stand has been re-provisioned (`make up
# NAME=<n>`), its StoragePool CRDs are wiped along with the cluster.
# install-blockstor.sh by design only restores CRDs+Node CRs+controller
# +satellite, NOT pools — that's `make pools` (stand/install-pools.sh).
# Without this guard, every scenario on a freshly re-provisioned stand
# fails with `unknown storage pool "stand"` (observed Run 38 e2e2: 9/10).
# `make pools` is idempotent (install-pools.sh skips existing pools), so
# this is a safe no-op when pools are already in place.
pool_count=$(kubectl --request-timeout=3s get storagepools --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo "pool-check: storagepools=$pool_count" >> "$RESULTS"
if [ "$pool_count" -lt 1 ]; then
    echo ">> no StoragePool CRDs found, provisioning via 'make pools NAME=$NAME TYPE=both'" >> "$RESULTS"
    if ! make pools NAME=$NAME TYPE=both >> "$RESULTS" 2>&1; then
        echo "FATAL: make pools failed; scenarios would fail with 'unknown storage pool'" >> "$RESULTS"
        echo "all-scenarios-done: $(date -Iseconds)" >> "$RESULTS"
        exit 1
    fi
    pool_count=$(kubectl --request-timeout=3s get storagepools --no-headers 2>/dev/null | wc -l | tr -d ' ')
    echo "pool-check (post-provision): storagepools=$pool_count" >> "$RESULTS"
fi

for sc in "${SCENARIOS[@]}"; do
    # L6 cli-matrix cells are referenced as `cli-matrix/<cell>` so
    # SCENARIO=<that> resolves to ./tests/e2e/cli-matrix/<cell>.sh
    # via stand/Makefile's `./tests/e2e/$${SCENARIO}.sh`. The slash
    # would break the per-cell log path `/tmp/e2e-$NAME-$sc.log`
    # (parent dir doesn't exist), so sanitize for the log name only.
    sc_log=${sc//\//__}
    echo "=== START $(date -Iseconds) $sc ===" >> "$RESULTS"
    if timeout 600 make e2e NAME=$NAME SCENARIO=$sc > /tmp/e2e-$NAME-$sc_log.log 2>&1; then
        echo "PASS $sc" >> "$RESULTS"
    else
        rc=$?
        if [ $rc -eq 124 ]; then
            echo "TIMEOUT $sc" >> "$RESULTS"
        else
            echo "FAIL $sc (exit $rc)" >> "$RESULTS"
        fi
    fi
    echo "=== END $(date -Iseconds) $sc ===" >> "$RESULTS"
done

echo "all-scenarios-done: $(date -Iseconds)" >> "$RESULTS"
