#!/usr/bin/env bash
# Run scenarios on an already-provisioned stand.
#
# Canonical source of truth for the stand-side runner. Lives in
# /tmp/run-scenarios-only.sh on the OCI dev stand (<dev-stand-host>) and
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
# Dev stand runs this from /tmp with the repo at ~/blockstor; CI (ci-e2e.sh)
# exports BS_REPO to its $GITHUB_WORKSPACE checkout. Honour the override.
cd "${BS_REPO:-$HOME/blockstor}"
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

# reset_cluster_state (tests/e2e/lib.sh) is the shared inter-scenario
# cleanup. We source lib.sh inside a subshell per call so its
# `set -euo pipefail` doesn't leak into this dispatcher — the loop
# below deliberately runs without `set -e` so one scenario's FAIL
# doesn't abort the whole batch.
LIB="$PWD/tests/e2e/lib.sh"
reset_between_scenarios() {
    # KUBECONFIG is already exported above; lib.sh reads it + $NS.
    ( set +e; source "$LIB"; reset_cluster_state 120 ) >> "$RESULTS" 2>&1 || true
}

# Scenarios ALLOWED to emit the SKIP sentinel (tests/e2e/lib.sh skip()).
# These are environment-gated cases that cannot run on the Talos+QEMU CI
# substrate and are deliberately observational:
#   - backing-device-fail / storage-error-injection: need writable loop
#     autoclear + dm-control sysfs; the Talos kernel marks them read-only.
#   - quorum-loss-recovery: xfail on the DRBD 9.2.14-class kernel (kernel
#     bug); recovery-bitmap-drop: xfail once the kernel carries the
#     upstream bitmap-drop fix (>= 9.2.17).
#   - resize-pvc: needs a blockstor-side CSI StorageClass, which is not
#     shipped yet; it can only exercise piraeus's SC, so it stays an
#     explicit xfail until the blockstor CSI provisioner lands.
#   - rwx-ganesha: coexistence-only — was wired against piraeus's bundled
#     Java linstor-controller (pool=pool, RWX publish through linstor-csi
#     NFS-Ganesha). The e2e-piraeus CI job now installs piraeus in EXTERNAL
#     mode against blockstor's apiserver, so the upstream controller is
#     absent and the SC cannot resolve. Tracked as a follow-up port.
# EVERY other scenario that skips is a regression (a mandatory test
# silently opting out) and is recorded as FAIL so the lane goes red.
SKIP_ALLOWLIST="backing-device-fail storage-error-injection quorum-loss-recovery recovery-bitmap-drop resize-pvc rwx-ganesha"

scenario_count=${#SCENARIOS[@]}
scenario_idx=0
for sc in "${SCENARIOS[@]}"; do
    scenario_idx=$((scenario_idx + 1))
    # L6 cli-matrix cells are referenced as `cli-matrix/<cell>` so
    # SCENARIO=<that> resolves to ./tests/e2e/cli-matrix/<cell>.sh
    # via stand/Makefile's `./tests/e2e/$${SCENARIO}.sh`. The slash
    # would break the per-cell log path `/tmp/e2e-$NAME-$sc.log`
    # (parent dir doesn't exist), so sanitize for the log name only.
    sc_log=${sc//\//__}
    logf=/tmp/e2e-$NAME-$sc_log.log
    echo "=== START $(date -Iseconds) $sc ===" >> "$RESULTS"
    if timeout 600 make e2e NAME=$NAME SCENARIO=$sc > "$logf" 2>&1; then
        # exit 0 from make — but a scenario may have emitted the SKIP
        # sentinel (tests/e2e/lib.sh skip()) and exited 0 so make reports
        # success. Reclassify: allowlisted env-gated scenario → SKIP;
        # anything else opting out → FAIL (a mandatory test must not
        # silently vanish into a green PASS).
        if grep -q '^__E2E_SKIP__:' "$logf"; then
            reason=$(grep -m1 '^__E2E_SKIP__:' "$logf" | sed 's/^__E2E_SKIP__: *//')
            case " $SKIP_ALLOWLIST " in
                *" $sc "*) echo "SKIP $sc ($reason)" >> "$RESULTS" ;;
                *)         echo "FAIL $sc (unexpected skip, not allowlisted: $reason)" >> "$RESULTS" ;;
            esac
        else
            echo "PASS $sc" >> "$RESULTS"
        fi
    else
        rc=$?
        if [ $rc -eq 124 ]; then
            echo "TIMEOUT $sc" >> "$RESULTS"
        else
            echo "FAIL $sc (exit $rc)" >> "$RESULTS"
        fi
    fi
    echo "=== END $(date -Iseconds) $sc ===" >> "$RESULTS"

    # Inter-scenario cleanup: reset the cluster to a clean slate so the
    # next scenario doesn't inherit orphan kernel slots / stale .res
    # files / stuck finalizers left behind by this one (Run 54 cascades).
    # Skip after the final scenario — nothing follows, so it's wasted
    # work and only delays the results file.
    if (( scenario_idx < scenario_count )); then
        echo "=== CLEANUP $(date -Iseconds) after $sc ===" >> "$RESULTS"
        reset_between_scenarios
        echo "=== CLEANUP-DONE $(date -Iseconds) after $sc ===" >> "$RESULTS"
    fi
done

echo "all-scenarios-done: $(date -Iseconds)" >> "$RESULTS"
