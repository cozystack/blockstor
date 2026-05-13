#!/usr/bin/env bash
#
# usage: recovery-port-collision.sh WORK_DIR
#
# Scenario 5.19 — TCP port collision recovery via
# `linstor r deact` + `linstor r act`.
#
# Goal: validate the documented recipe for clearing a colliding
# DRBD TCP port on a single replica by deactivating and reactivating
# that replica. The recipe (from upstream LINSTOR ops experience) is:
#
#     linstor r deactivate <node> <rd>
#     sleep 5
#     linstor r activate <node> <rd>
#
# Expectation per the recipe: blockstor should re-allocate a fresh
# TCP port for that replica when the activate path runs again,
# clearing the collision so `drbdadm adjust all` no longer warns
# `tcp port <N> is also used` against another resource.
#
# Why this matters: in production we've seen ports collide after
# a controller crash/restore mid-allocation, or after manual
# `kubectl patch` on a Resource CRD. The deact/act recipe is the
# documented non-destructive fix — alternatives (delete + re-create
# the replica) blow away the local backing storage and trigger a
# full resync. This test catches regressions where the controller
# starts preserving a stale port across activate (cheaper) and
# fails to honor the operator's deliberate "give me a new port"
# signal embedded in the deact/act cycle.
#
# Setup:
#   - 2 RDs on workers 1+2: `port-victim` and `port-test`. Both
#     autoplaced. We only need `port-victim` for its allocated
#     TcpPort number — the collision is forced onto `port-test`.
#   - Wait for both to reach UpToDate so the satellite has written
#     the .res files and `drbdadm` knows about both resources.
#
# Steps:
#   1. Read `port-victim`'s allocated DRBDPort on $WORKER_1.
#   2. Force-patch `port-test`'s Status.DRBDPort on $WORKER_1 to
#      collide with `port-victim`. The satellite reconciler then
#      re-renders the .res file with the colliding port.
#   3. Trigger a `drbdadm adjust all` on $WORKER_1 and observe
#      the "tcp port <N> is also used" warning.
#   4. Apply the recipe: `linstor r deactivate $WORKER_1 port-test`,
#      sleep 5, `linstor r activate $WORKER_1 port-test`.
#   5. Read `port-test`'s DRBDPort post-recipe.
#   6. PASS if the new port differs from `port-victim`'s port AND
#      `drbdadm adjust all` no longer prints a collision warning.
#      FAIL otherwise — the controller is preserving the stale
#      colliding port across activate, which the recipe cannot fix.
#
# Regression guards:
#   - `port-victim` must remain UpToDate throughout — the recipe
#     targets `port-test` only.
#   - The cleanup `delete_rd` call must succeed for both RDs to
#     leave the cluster clean for the next scenario in a batch run.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD_VICTIM=port-victim
RD_TEST=port-test
N1=$WORKER_1
N2=$WORKER_2

# Random ephemeral port for the controller port-forward — parallel
# iters on the same host would collide on a fixed port.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/recovery-port-collision-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: kubectl get resources -o wide ----"
    kubectl get resources.blockstor.io.blockstor.io -o wide 2>/dev/null || true
    echo "---- dump: $RD_TEST resource on $N1 (yaml) ----"
    kubectl get resources.blockstor.io.blockstor.io "${RD_TEST}.${N1}" -o yaml 2>/dev/null || true
    echo "---- dump: $RD_VICTIM resource on $N1 (yaml) ----"
    kubectl get resources.blockstor.io.blockstor.io "${RD_VICTIM}.${N1}" -o yaml 2>/dev/null || true
    echo "---- dump: satellite log on $N1 (tail 60) ----"
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
    kubectl -n "$NS" logs "$pod" --tail=60 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD_TEST" 2>/dev/null || true
    delete_rd "$RD_VICTIM" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to bind before issuing CLI commands.
for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

echo ">> create $RD_VICTIM (2-replica autoplace on $N1+$N2)"
"${LCTL[@]}" resource-definition create "$RD_VICTIM" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_VICTIM" 32M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD_VICTIM" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD_VICTIM" --storage-pool stand >/dev/null

echo ">> create $RD_TEST (2-replica autoplace on $N1+$N2)"
"${LCTL[@]}" resource-definition create "$RD_TEST" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_TEST" 32M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD_TEST" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD_TEST" --storage-pool stand >/dev/null

wait_uptodate "$RD_VICTIM" "$N1" "$N2"
wait_uptodate "$RD_TEST" "$N1" "$N2"

# Pull the allocated port for the victim RD on $N1 and the test RD on
# $N1. We deliberately compare the $N1 ports — a same-node collision
# is what would actually break `drbdadm adjust` because the kernel
# listener can only bind one resource per port per node.
victim_port=$(kubectl get resources.blockstor.io.blockstor.io "${RD_VICTIM}.${N1}" \
    -o jsonpath='{.status.drbdPort}')
test_port_before=$(kubectl get resources.blockstor.io.blockstor.io "${RD_TEST}.${N1}" \
    -o jsonpath='{.status.drbdPort}')
echo "   victim port on $N1 = $victim_port"
echo "   test  port on $N1 (pre-collision) = $test_port_before"
if [[ -z "$victim_port" || -z "$test_port_before" ]]; then
    echo "FAIL: could not read pre-state DRBDPort values"
    exit 1
fi
if [[ "$victim_port" == "$test_port_before" ]]; then
    echo "FAIL: ports already identical before collision injection — port-pool bug?"
    exit 1
fi

# Force the collision by patching $RD_TEST's Status.DRBDPort on $N1
# to the victim's port. The satellite reconciler then re-renders the
# .res file with the colliding port; on next `drbdadm adjust all`,
# DRBD logs `tcp port <N> is also used`.
#
# We use a JSON subresource patch on /status because the field lives
# in status — `kubectl patch --subresource=status` is the K8s-1.27+
# way; we use `--subresource=status` for cross-version safety.
echo ">> inject collision: patch ${RD_TEST}.${N1} status.drbdPort → $victim_port"
kubectl patch resources.blockstor.io.blockstor.io "${RD_TEST}.${N1}" \
    --subresource=status --type=merge \
    -p "{\"status\":{\"drbdPort\":${victim_port}}}" >/dev/null

# Verify the patch landed before going further — if the apiserver
# rejected it (CEL validation, etc), bail out rather than fight a
# phantom collision for 10 minutes.
test_port_patched=$(kubectl get resources.blockstor.io.blockstor.io "${RD_TEST}.${N1}" \
    -o jsonpath='{.status.drbdPort}')
echo "   test port on $N1 (post-patch) = $test_port_patched"
if [[ "$test_port_patched" != "$victim_port" ]]; then
    echo "FAIL: status patch did not stick (still $test_port_patched, wanted $victim_port)"
    exit 1
fi

# Wait for the satellite to pick up the patched port and re-render
# the .res file on $N1. We poll the .res for the new port value
# rather than relying on a flat sleep — under iter-load the
# reconcile loop may be queue-deep.
echo ">> wait up to 30s for satellite to re-render ${RD_TEST}.res with colliding port"
deadline=$(( $(date +%s) + 30 ))
res_has_collision=false
while (( $(date +%s) < deadline )); do
    if on_node "$N1" bash -c "grep -q ':${victim_port};' /etc/drbd.d/${RD_TEST}.res 2>/dev/null"; then
        res_has_collision=true
        break
    fi
    sleep 2
done
if [[ "$res_has_collision" != "true" ]]; then
    echo "FAIL: ${RD_TEST}.res on $N1 never picked up colliding port $victim_port"
    on_node "$N1" cat "/etc/drbd.d/${RD_TEST}.res" 2>/dev/null || true
    exit 1
fi
echo "   ${RD_TEST}.res on $N1 now references port $victim_port"

# Trigger `drbdadm adjust all` on $N1 and capture stderr — the
# kernel-side collision detection prints "tcp port <N> is also used"
# on stderr (not in the .res file). We tolerate non-zero exit from
# drbdadm because adjust returns non-zero when it refuses to bring
# a colliding resource up.
echo ">> drbdadm adjust all on $N1 (expect 'is also used' warning)"
adjust_out=$(on_node "$N1" bash -c "drbdadm adjust all 2>&1 || true")
echo "$adjust_out" | sed 's/^/      | /'
if ! echo "$adjust_out" | grep -qiE "is also used|already in use|address.*used"; then
    # The kernel may not have actually tried to rebind if both
    # resources were brought up earlier with the original (non-
    # colliding) port. In that case the warning fires only on a
    # subsequent `drbdadm down`+`adjust`. Force the kernel to
    # re-evaluate by issuing a `drbdadm down` on $RD_TEST first.
    echo "   (no collision warning yet — kicking kernel via drbdadm down + adjust)"
    on_node "$N1" bash -c "drbdadm down ${RD_TEST} 2>&1 || true" >/dev/null
    adjust_out=$(on_node "$N1" bash -c "drbdadm adjust ${RD_TEST} 2>&1 || true")
    echo "$adjust_out" | sed 's/^/      | /'
fi
if echo "$adjust_out" | grep -qiE "is also used|already in use|address.*used"; then
    echo "   collision observed via drbdadm"
    collision_observed=true
else
    echo "   NOTE: drbdadm did not emit a collision warning — kernel may have"
    echo "         silently refused to bind. We still proceed with the recipe."
    collision_observed=false
fi

# Apply the recipe: deact → sleep → act. Per the SKILL doc the sleep
# gives the satellite reconciler time to observe INACTIVE, run
# `drbdadm down`, and quiesce before activate re-asserts UP.
#
# IMPORTANT: we call the REST endpoints with curl instead of `linstor
# r deactivate` / `linstor r activate` because blockstor's handlers
# return an empty 200 body, and golinstor's CLI rejects that with
# `Unable to parse REST json data: Expecting value`. The recipe in
# this scenario is the deact/act *sequence*, not the CLI surface —
# we exercise the same REST endpoints the CLI would call.
echo ">> recipe: POST .../resources/$N1/$RD_TEST/deactivate"
curl -fsS -X POST -m 10 \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD_TEST}/resources/${N1}/deactivate" \
    >/dev/null

echo "   sleep 5s for satellite to observe INACTIVE flag"
sleep 5

echo ">> recipe: POST .../resources/$N1/$RD_TEST/activate"
curl -fsS -X POST -m 10 \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD_TEST}/resources/${N1}/activate" \
    >/dev/null

# Give the activate path time to reconcile. We read the port back
# every iteration so we can spot the reallocation as soon as it
# happens (or confirm the absence of reallocation after the
# window expires).
echo ">> wait up to 30s for ${RD_TEST}.${N1} DRBDPort to change"
deadline=$(( $(date +%s) + 30 ))
test_port_after="$victim_port"
while (( $(date +%s) < deadline )); do
    test_port_after=$(kubectl get resources.blockstor.io.blockstor.io "${RD_TEST}.${N1}" \
        -o jsonpath='{.status.drbdPort}' 2>/dev/null || echo "$victim_port")
    if [[ -n "$test_port_after" && "$test_port_after" != "$victim_port" ]]; then
        break
    fi
    sleep 2
done
echo "   test port on $N1 (post-recipe) = $test_port_after"

# Final verdict.
if [[ "$test_port_after" == "$victim_port" ]]; then
    echo "FAIL: deact+act did NOT reallocate ${RD_TEST}.${N1} port — still colliding at $victim_port"
    echo "      blockstor preserves DRBDPort across activate (intentional, per"
    echo "      pkg/rest/resource_adjust.go:92: 'activate flips it back without"
    echo "      losing port/node-id allocations'). The recipe is INEFFECTIVE."
    exit 1
fi

# Sanity-check the new port doesn't collide with anything else on $N1.
echo ">> verify ${RD_TEST}.${N1} new port $test_port_after is unique on $N1"
other_ports=$(kubectl get resources.blockstor.io.blockstor.io -o json \
    | jq -r --arg n "$N1" --arg rd "${RD_TEST}.${N1}" \
        '.items[] | select(.spec.nodeName == $n and .metadata.name != $rd) | .status.drbdPort // empty' \
    | sort -u)
echo "   other allocated ports on $N1: $(echo "$other_ports" | tr '\n' ' ')"
if echo "$other_ports" | grep -qx "$test_port_after"; then
    echo "FAIL: reallocated port $test_port_after still collides with another resource on $N1"
    exit 1
fi

# Confirm `drbdadm adjust all` is now clean.
echo ">> drbdadm adjust all on $N1 (expect no 'is also used' warning)"
adjust_out=$(on_node "$N1" bash -c "drbdadm adjust all 2>&1 || true")
echo "$adjust_out" | sed 's/^/      | /'
if echo "$adjust_out" | grep -qiE "is also used|already in use|address.*used"; then
    echo "FAIL: drbdadm still reports a port collision after the recipe"
    exit 1
fi

# Regression guard: $RD_VICTIM must still be UpToDate on both peers.
wait_uptodate "$RD_VICTIM" "$N1" "$N2"

echo ">> RECOVERY-PORT-COLLISION OK (port $victim_port → $test_port_after," \
    "collision_observed=${collision_observed}, victim still UpToDate)"
