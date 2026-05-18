#!/usr/bin/env bash
#
# usage: recovery-port-collision.sh WORK_DIR
#
# Bug 306 — batch autoplace port-collision guard.
#
# Goal: validate that the controller's per-RD DRBD-TCP-port
# allocator is collision-free under PARALLEL RD creation. The
# realistic production hazard isn't an admin patching Status (that's
# operator error — leave it). It's CI pipelines, GitOps batch
# apply, and mass-import flows that POST N resource-definitions +
# autoplace requests in parallel against the controller REST API.
#
# Pre-Bug-306, two RDs reconciling at the same time both observed a
# stale (cached) cluster-wide taken-set, both picked the lowest
# free port (7000), and both Status().Update succeeded because
# Kubernetes optimistic concurrency is per-object: two RDs writing
# to their OWN statuses don't conflict. Result: N RDs got the SAME
# DRBD port, the satellite-side .res files collided, neither
# resource connected, `drbdadm adjust` printed "tcp port <N> is
# also used" forever.
#
# Fix: a process-wide `clusterAllocMu` held across {APIReader-list
# taken → pick free → Status().Update} so cross-RD allocation is
# strictly serial. APIReader bypasses the informer cache so the
# second allocator observes the first one's committed write.
#
# Setup:
#   - N=10 RDs created in parallel via REST POST against the
#     blockstor-controller apiserver.
#   - Each RD gets a 2-replica autoplace on workers 1+2.
#
# Steps:
#   1. POST N resource-definitions concurrently (N curl &; wait).
#   2. POST N volume-definitions concurrently.
#   3. POST N resources (autoplace) concurrently.
#   4. Wait for every replica's Status.DRBDPort to be stamped.
#   5. Collect all ports and assert pairwise uniqueness across RDs.
#   6. (Sanity) Each RD's two replicas share the same port (per-RD
#      invariant, Bug 268).
#
# Regression guards:
#   - At least N distinct ports allocated.
#   - Every port is inside the configured DRBD port range.
#   - All replicas of one RD share the same port.
#
# This test was rewritten from the admin-status-patch shape to the
# batch-autoplace shape — admin patching Status.DRBDPort to force a
# collision is operator error and the controller is not obligated
# to undo it. The real production hazard is parallel autoplace.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

N1=$WORKER_1
N2=$WORKER_2
RD_PREFIX=batch-port-bug306
NUM_RDS=10

# Random ephemeral port for the controller port-forward — parallel
# iters on the same host would collide on a fixed port.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/recovery-port-collision-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: kubectl get resources -o wide ----"
    kubectl get resources.blockstor.io.blockstor.io -o wide 2>/dev/null || true
    echo "---- dump: kubectl get resourcedefinitions -o wide ----"
    kubectl get resourcedefinitions.blockstor.io.blockstor.io -o wide 2>/dev/null || true
    echo "---- dump: controller log tail ----"
    kubectl -n "$NS" logs -l app=blockstor-controller --tail=80 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    for i in $(seq 1 "$NUM_RDS"); do
        delete_rd "${RD_PREFIX}-${i}" 2>/dev/null || true
    done
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to bind before issuing requests.
for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

API="http://127.0.0.1:${PF_PORT}"

# Pre-clean any leftover RDs from a previous run.
for i in $(seq 1 "$NUM_RDS"); do
    delete_rd "${RD_PREFIX}-${i}" 2>/dev/null || true
done

# Phase 1: POST N resource-definitions in parallel. Background each
# curl and wait — that's the realistic shape of a CI pipeline doing
# `helm install` over a list of PVCs.
#
# Bug 308: `wait` with no argument waits for EVERY backgrounded job
# in the current shell — including the `kubectl port-forward` started
# above (PF_PID), which never exits. The whole test then hung in
# Phase 1 until the per-scenario timeout fired (600s). Always wait
# on the explicit PID set of the curl fan-out, never on PF_PID.
echo ">> POST $NUM_RDS resource-definitions in parallel"
pids=()
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    curl -sf -X POST -m 10 \
        -H 'Content-Type: application/json' \
        -d "{\"resource_definition\":{\"name\":\"${rd}\"}}" \
        "${API}/v1/resource-definitions" \
        >/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

# Phase 2: POST N volume-definitions in parallel.
echo ">> POST $NUM_RDS volume-definitions in parallel"
pids=()
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    curl -sf -X POST -m 10 \
        -H 'Content-Type: application/json' \
        -d '{"volume_definition":{"size_kib":65536}}' \
        "${API}/v1/resource-definitions/${rd}/volume-definitions" \
        >/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

# Phase 3: POST N autoplace requests in parallel. This is the
# allocator's worst case — N RDs all reaching the controller
# reconcile at the same instant.
echo ">> POST $NUM_RDS autoplace requests in parallel"
pids=()
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    curl -sf -X POST -m 10 \
        -H 'Content-Type: application/json' \
        -d '{"select_filter":{"place_count":2}}' \
        "${API}/v1/resource-definitions/${rd}/autoplace" \
        >/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

# Phase 4: wait until every replica has Status.DRBDPort stamped.
echo ">> wait up to 60s for every replica to receive a port"
deadline=$(( $(date +%s) + 60 ))
stamped_all=false
while (( $(date +%s) < deadline )); do
    missing=0
    for i in $(seq 1 "$NUM_RDS"); do
        rd="${RD_PREFIX}-${i}"
        for node in "$N1" "$N2"; do
            port=$(kubectl get resources.blockstor.io.blockstor.io "${rd}.${node}" \
                -o jsonpath='{.status.drbdPort}' 2>/dev/null || true)
            if [[ -z "$port" ]]; then
                missing=$((missing + 1))
            fi
        done
    done
    if (( missing == 0 )); then
        stamped_all=true
        break
    fi
    sleep 2
done
if [[ "$stamped_all" != "true" ]]; then
    echo "FAIL: $missing replicas never received a DRBDPort"
    exit 1
fi

# Phase 5: collect ports and check uniqueness across RDs + per-RD
# consistency across replicas.
echo ">> collect allocated ports"
declare -A rd_port_n1=()
declare -A rd_port_n2=()
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    p1=$(kubectl get resources.blockstor.io.blockstor.io "${rd}.${N1}" \
        -o jsonpath='{.status.drbdPort}' 2>/dev/null || true)
    p2=$(kubectl get resources.blockstor.io.blockstor.io "${rd}.${N2}" \
        -o jsonpath='{.status.drbdPort}' 2>/dev/null || true)
    rd_port_n1[$rd]=$p1
    rd_port_n2[$rd]=$p2
    echo "   $rd: $N1=$p1  $N2=$p2"
done

# Per-RD invariant: replicas of one RD share one port (Bug 268).
echo ">> per-RD invariant: every RD's replicas share one port"
fail_per_rd=0
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    if [[ "${rd_port_n1[$rd]}" != "${rd_port_n2[$rd]}" ]]; then
        echo "FAIL: $rd port diverges across peers: $N1=${rd_port_n1[$rd]} vs $N2=${rd_port_n2[$rd]}"
        fail_per_rd=$((fail_per_rd + 1))
    fi
done
if (( fail_per_rd > 0 )); then
    exit 1
fi

# Cross-RD uniqueness: every RD must have its own port (Bug 306).
echo ">> cross-RD uniqueness: every RD must have its own port (Bug 306)"
declare -A seen=()
fail_cross=0
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    port="${rd_port_n1[$rd]}"
    if [[ -n "${seen[$port]:-}" ]]; then
        echo "FAIL: port collision (Bug 306) — RDs '${seen[$port]}' and '$rd' both got port $port"
        echo "      under parallel batch autoplace. Two satellite-side .res files now"
        echo "      reference the same port; neither resource will connect."
        fail_cross=$((fail_cross + 1))
    fi
    seen[$port]=$rd
done
if (( fail_cross > 0 )); then
    exit 1
fi

# Sanity: every port inside the default 7000-7999 range.
echo ">> sanity: every port inside default 7000-7999 range"
for i in $(seq 1 "$NUM_RDS"); do
    rd="${RD_PREFIX}-${i}"
    port="${rd_port_n1[$rd]}"
    if (( port < 7000 || port > 7999 )); then
        echo "FAIL: $rd port $port outside default 7000-7999 range"
        exit 1
    fi
done

unique_count=${#seen[@]}
echo ">> RECOVERY-PORT-COLLISION OK ($unique_count unique ports for $NUM_RDS RDs," \
    "all in 7000-7999, no cross-RD collisions under parallel batch autoplace)"
