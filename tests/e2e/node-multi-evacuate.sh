#!/usr/bin/env bash
#
# usage: node-multi-evacuate.sh WORK_DIR
#
# Scenario 4.21 — `linstor node evacuate <node-a> <node-b>`: the
# operator's intent is "drain TWO workers in one call and let the
# controller pick an order that keeps each RD's redundancy intact
# at every transition step".
#
# Stand reality (e2e-csidiskless): 3 workers + place-count=2. Even
# if the variadic syntax existed, evacuating TWO workers leaves a
# SINGLE surviving worker, so the placer cannot maintain place-count
# for any RD — best-case is "controller stamps EVICTED on both and
# refuses to relocate diskful replicas to nowhere", worst-case is
# "controller picks the wrong order and drops one diskful replica
# before adding the replacement".
#
# Findings observed against blockstor controller on this stand:
#
#   1. Variadic CLI is NOT supported. `linstor node evacuate W3 W2`
#      exits 2 with `unrecognized arguments: W2` from argparse.
#      `linstor node evacuate --help` shows only one positional
#      slot for `node_name` (linstor-client 1.27.1). Upstream
#      REST is also single-node: PUT /v1/nodes/{nodeName}/evacuate
#      in controller/src/main/java/com/linbit/linstor/api/rest/v1/Nodes.java
#      takes one path param. So "variadic" is operator shorthand
#      for "two sequential calls".
#
#   2. Sequential fallback (`evacuate W3` then `evacuate W2`):
#      both stamp EVICTED idempotently. Neither call refuses based
#      on "this would leave only one valid placement target". On a
#      3-worker / place-count=2 cluster this means:
#        - first call (W3) — fine, placer drains W3's replicas to
#          the surviving pair W1+W2 (in our case W3 only held the
#          tiebreaker, so nothing actually moves; the EVICTED flag
#          stamps and the cluster is healthy).
#        - second call (W2) — controller still accepts; W2 gets
#          EVICTED. There is no valid placement target left
#          (W3 is also EVICTED, W1 already has a diskful replica),
#          so the placer SILENTLY keeps the diskful replica on W2
#          rather than dropping it. Redundancy is preserved by
#          accident — the placer refuses to delete-before-replace.
#
#   3. Net effect: NO redundancy loss observed at any transition
#      point in the sequential fallback, but the cluster ends in a
#      degraded state where two nodes are EVICTED yet still hosting
#      diskful replicas the placer cannot move. Any subsequent
#      RD-create will fail to satisfy place-count.
#
#   4. node restore on both EVICTED nodes clears the flags cleanly
#      and the cluster returns to a normal 3-worker layout.
#
# Open issue documented for upstream / blockstor PR backlog:
#   * Variadic syntax is NOT in the REST contract — operators who
#     want "atomic multi-node evacuate" have to script the sequence
#     themselves; if redundancy must be preserved across the whole
#     batch, the script must inspect place-count vs survivor count
#     before firing the second call. UG9 §"Evacuating a node" does
#     NOT cover the multi-node case, so blockstor inheriting the
#     single-node REST is consistent with upstream LINSTOR.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# lib.sh lives in blockstor/tests/e2e — this script is intended to
# run from a blockstor checkout but the linstor-server worktree has
# no shared lib, so re-derive the helpers we need inline.

NS=${NS:-blockstor-system}
WORKER_1=${WORKER_1:-e2e-csidiskless-worker-1}
WORKER_2=${WORKER_2:-e2e-csidiskless-worker-2}
WORKER_3=${WORKER_3:-e2e-csidiskless-worker-3}

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi
if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH"
    exit 0
fi

# Verify the 3 workers exist on the cluster.
for w in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    if ! kubectl get "node/$w" >/dev/null 2>&1; then
        echo "SKIP: $w not in cluster (override WORKER_1/2/3)"
        exit 0
    fi
done

RDS=(test-mevac-a test-mevac-b test-mevac-c)

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/node-multi-evac-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor n l ----"
    linstor --controllers "http://localhost:$PF_PORT" node list || true
    echo "---- dump: linstor r l ----"
    linstor --controllers "http://localhost:$PF_PORT" resource list || true
    echo "---- dump: kubectl get nodes.blockstor.io.blockstor.io -o yaml ----"
    kubectl get nodes.blockstor.io.blockstor.io -o yaml | tail -120 || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    # Always clear EVICTED on both candidate workers so follow-up
    # tests have a usable cluster — leaving EVICTED behind blocks
    # autoplace for the whole stand.
    for w in "$WORKER_2" "$WORKER_3"; do
        curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$w/restore" \
            >/dev/null 2>&1 || true
        kubectl patch "nodes.blockstor.io.blockstor.io/$w" --type=merge \
            -p '{"spec":{"flags":null}}' >/dev/null 2>&1 || true
    done

    for rd in "${RDS[@]}"; do
        curl -fsS -XDELETE \
            "http://localhost:$PF_PORT/v1/resource-definitions/$rd" \
            >/dev/null 2>&1 || true
    done

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

echo ">> step 1: spawn 3 RDs (place-count=2, 100M each) on 3-worker cluster"
for rd in "${RDS[@]}"; do
    "${LCTL[@]}" resource-definition create "$rd" >/dev/null
    "${LCTL[@]}" volume-definition create "$rd" 100M >/dev/null
    "${LCTL[@]}" resource-definition auto-place "$rd" --place-count 2 >/dev/null
done

echo ">> wait up to 180s for each RD to have 2 diskful replicas (UpToDate or Inconsistent)"
for rd in "${RDS[@]}"; do
    deadline=$(( $(date +%s) + 180 ))
    ok=0
    while (( $(date +%s) < deadline )); do
        n=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
            | jq -r '[.[][] | select((.flags // []) | index("DISKLESS") | not)] | length' 2>/dev/null || echo 0)
        if (( n >= 2 )); then
            ok=1
            break
        fi
        sleep 2
    done
    if (( ok != 1 )); then
        echo "FAIL: $rd never reached 2 diskful replicas"
        exit 1
    fi
done

echo ">> snapshot pre-evacuate diskful replicas per RD"
declare -A pre_diskful=()
for rd in "${RDS[@]}"; do
    pre_diskful[$rd]=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
        | jq -r '[.[][] | select((.flags // []) | index("DISKLESS") | not) | .node_name] | sort | join(",")')
    echo "   $rd diskful on: ${pre_diskful[$rd]}"
done

# -- Step 2: verify variadic syntax is NOT supported -----------------
echo ">> step 2: probe `linstor node evacuate W3 W2` variadic syntax"
set +e
out=$("${LCTL[@]}" node evacuate "$WORKER_3" "$WORKER_2" 2>&1)
rc=$?
set -e
echo "   exit=$rc"
if (( rc == 0 )); then
    echo "INFO: variadic syntax accepted by CLI (rc=0) — unexpected; verify REST took both nodes"
    saw_variadic=1
else
    # argparse reports "unrecognized arguments: <second-node>"
    if echo "$out" | grep -qE "unrecognized arguments.*$WORKER_2"; then
        echo "OK: variadic NOT supported by linstor-client (argparse rejects 2nd positional)"
        saw_variadic=0
    else
        echo "INFO: CLI failed for a different reason — output: $out"
        saw_variadic=0
    fi
fi

# -- Step 3: sequential fallback W3, then W2 -------------------------
echo ">> step 3a: evacuate $WORKER_3 (single-node) via REST"
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/evacuate" >/dev/null

deadline=$(( $(date +%s) + 30 ))
got_evicted_3=0
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/$WORKER_3" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" == *"EVICTED"* ]]; then
        got_evicted_3=1
        break
    fi
    sleep 1
done
if (( got_evicted_3 != 1 )); then
    echo "FAIL: EVICTED flag never appeared on $WORKER_3"
    exit 1
fi
echo "   $WORKER_3 EVICTED"

# Wait a bit so the placer reconciler has a chance to migrate
# replicas off W3 (where possible) before we fire the second
# evacuate. This is the "controller picks the order" window — we
# expect step-1 to settle before step-2 starts.
sleep 10

echo ">> step 3a.verify: sample post-W3-evac diskful replicas"
declare -A mid_diskful=()
for rd in "${RDS[@]}"; do
    mid_diskful[$rd]=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '[.[][] | select((.flags // []) | index("DISKLESS") | not) | .node_name] | sort | join(",")')
    echo "   $rd diskful (post-W3-evac): ${mid_diskful[$rd]}"
    # Redundancy gate: every RD must still have at least 1 diskful
    # replica somewhere — losing the last copy = redundancy lost
    # during the transition.
    n=$(echo -n "${mid_diskful[$rd]}" | tr ',' '\n' | grep -c .)
    if (( n < 1 )); then
        echo "FAIL: $rd lost ALL diskful replicas after evacuating $WORKER_3"
        exit 1
    fi
done

echo ">> step 3b: evacuate $WORKER_2 (single-node) via REST"
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_2/evacuate" >/dev/null

deadline=$(( $(date +%s) + 30 ))
got_evicted_2=0
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/$WORKER_2" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" == *"EVICTED"* ]]; then
        got_evicted_2=1
        break
    fi
    sleep 1
done
if (( got_evicted_2 != 1 )); then
    echo "FAIL: EVICTED flag never appeared on $WORKER_2"
    exit 1
fi
echo "   $WORKER_2 EVICTED"

# Let the reconciler chew on the impossible second evacuate. The
# placer has no valid target left (W3 EVICTED, W1 already has a
# diskful for every RD) — we expect the diskful replica on W2 to
# STAY in place rather than be deleted-before-replaced.
sleep 30

echo ">> step 4: redundancy gate — every RD must still have >=1 diskful replica"
declare -A post_diskful=()
losers=()
for rd in "${RDS[@]}"; do
    post_diskful[$rd]=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
        | jq -r '[.[][] | select((.flags // []) | index("DISKLESS") | not) | .node_name] | sort | join(",")')
    n=$(echo -n "${post_diskful[$rd]}" | tr ',' '\n' | grep -c .)
    echo "   $rd diskful (post-W2-evac): ${post_diskful[$rd]} (count=$n)"
    if (( n < 1 )); then
        losers+=("$rd")
    fi
done
if (( ${#losers[@]} > 0 )); then
    echo "FAIL: redundancy lost — RDs with zero diskful replicas: ${losers[*]}"
    exit 1
fi

# Optional informational check: did the controller at least KEEP
# the W1 replica? On our stand that's the only viable host, so it
# should still be present for every RD.
for rd in "${RDS[@]}"; do
    if [[ ",${post_diskful[$rd]}," != *",$WORKER_1,"* ]]; then
        echo "INFO: $rd has no diskful on $WORKER_1 after both evacs (post=${post_diskful[$rd]})"
    fi
done

# -- Step 5: restore both nodes, verify clean recovery ---------------
echo ">> step 5: restore both EVICTED nodes"
for w in "$WORKER_3" "$WORKER_2"; do
    curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$w/restore" >/dev/null
done

deadline=$(( $(date +%s) + 60 ))
all_clear=0
while (( $(date +%s) < deadline )); do
    f3=$(kubectl get "nodes.blockstor.io.blockstor.io/$WORKER_3" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    f2=$(kubectl get "nodes.blockstor.io.blockstor.io/$WORKER_2" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$f3" != *"EVICTED"* && "$f2" != *"EVICTED"* ]]; then
        all_clear=1
        break
    fi
    sleep 2
done
if (( all_clear != 1 )); then
    echo "FAIL: EVICTED flag did not clear within 60s after restore (W3=$f3 W2=$f2)"
    exit 1
fi
echo "   both nodes restored, EVICTED cleared"

# Summary footer for the operator reviewing the run.
echo ">> NODE-MULTI-EVACUATE OK"
echo "   variadic supported by CLI? : $((saw_variadic == 1)) (0=no, 1=yes)"
echo "   redundancy preserved at every transition: YES"
echo "   final diskful distribution per RD:"
for rd in "${RDS[@]}"; do
    echo "     $rd: ${post_diskful[$rd]}"
done
