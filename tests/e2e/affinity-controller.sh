#!/usr/bin/env bash
#
# usage: affinity-controller.sh WORK_DIR
#
# Scenario 11.W04 (wave2-11-kubernetes.md) — contract test for the
# piraeus `linstor-affinity-controller` that runs on top of
# blockstor. The affinity controller polls blockstor's REST surface
# and rewrites each PV's `spec.nodeAffinity` to match the set of
# nodes that currently hold a DISKFUL+UpToDate replica. Without it
# the PV's nodeAffinity is set ONCE at provisioning and never
# updated, so a Pod that gets rescheduled after a replica move
# stays Pending with "no nodes match".
#
# Operator-side coverage (helm install + Pod-reschedules-onto-new-node)
# is owned by the piraeus chart's own conformance tests. This script
# is the BLOCKSTOR-side contract: against a live blockstor controller,
# verify that the data the affinity controller depends on is actually
# present and freshly updated on the wire.
#
# Contract pinned here:
#
#   (1) GET /v1/view/resources?resources=<RD> returns one entry per
#       replica (no collapsing of multi-node RDs).
#
#   (2) Each entry carries `node_name` populated — the affinity
#       controller cannot map a replica back to a K8s node without
#       it.
#
#   (3) Each diskful entry's `volumes[0].state.disk_state` reaches
#       "UpToDate" within `STATUS_TIMEOUT` seconds of the resource
#       coming up. This is the "Status freshness" half of the
#       contract — the affinity controller polls; if DiskState
#       lags too far behind reality the PV's nodeAffinity is
#       perpetually wrong.
#
#   (4) DISKLESS / TIE_BREAKER flags round-trip on the wire — the
#       controller MUST exclude witnesses from nodeAffinity, and
#       does so by `flags`, not by guessing.
#
#   (5) The eligible-node set derived from (3)+(4) matches the
#       actual set of nodes the operator placed diskful replicas on.
#
# This is NOT an e2e test of the affinity controller itself (that
# requires the piraeus controller to be running). It IS the
# regression guard for blockstor's wire shape; if any of (1)-(5)
# break, the affinity controller silently misbehaves in production.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH (apt install jq)"
    exit 0
fi

RD=e2e-affinity-controller

# Tunables. Both extend the wait_uptodate ceiling: DRBD's reported
# state can lag blockstor's CRD by a beat or two on a busy QEMU
# stand, and `STATUS_TIMEOUT` is the budget for that extra hop
# (REST view to converge with DRBD).
STATUS_TIMEOUT=${STATUS_TIMEOUT:-30}

# 2 diskful + 1 explicit DISKLESS witness — mirrors the
# 3-replica-cluster shape the affinity controller cares about:
# WORKER_1+WORKER_2 hold real data, WORKER_3 is a witness that
# MUST be excluded from PV nodeAffinity.
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/affinity-controller-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- view ----"
    curl -sf -m5 "http://localhost:$PF_PORT/v1/view/resources?resources=$RD" 2>/dev/null | jq . || true
    echo "---- Resource CRDs ----"
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' || true
    echo "---- controller logs ----"
    kubectl -n "$NS" logs deploy/blockstor-controller --tail=60 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the in-cluster controller to answer on the forwarded port.
for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

echo ">> apply 2 diskful ($N1+$N2) + 1 DISKLESS witness ($N3)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  flags: ["DISKLESS"]
EOF

# Wait for DRBD to actually settle on the diskful pair. wait_uptodate
# polls drbdsetup; the affinity-controller assertions below then
# verify the REST view is in step with that ground truth.
wait_uptodate "$RD" "$N1" "$N2"

# ---- Contract (1) + (2): one entry per replica with node_name set --

echo ">> assert /v1/view/resources lists one entry per replica with node_name set"
view=$(curl -fsS -m5 "http://localhost:$PF_PORT/v1/view/resources?resources=$RD")

count=$(echo "$view" | jq 'length')
if [[ "$count" != "3" ]]; then
    echo "FAIL: view length=$count, want 3 (one entry per replica: $N1+$N2+$N3)"
    echo "$view" | jq .
    exit 1
fi

missing_node=$(echo "$view" | jq '[.[] | select(.node_name == null or .node_name == "")] | length')
if [[ "$missing_node" != "0" ]]; then
    echo "FAIL: $missing_node replicas have empty node_name — affinity controller cannot map them to a K8s node"
    echo "$view" | jq .
    exit 1
fi

# ---- Contract (3): DiskState freshness ----

echo ">> assert DiskState=UpToDate surfaces on $N1 + $N2 within ${STATUS_TIMEOUT}s"
deadline=$(( $(date +%s) + STATUS_TIMEOUT ))
while (( $(date +%s) < deadline )); do
    view=$(curl -fsS -m5 "http://localhost:$PF_PORT/v1/view/resources?resources=$RD" 2>/dev/null || echo "[]")

    n1_state=$(echo "$view" | jq -r --arg n "$N1" '.[] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')
    n2_state=$(echo "$view" | jq -r --arg n "$N2" '.[] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')

    # Treat the sync-progress-annotated "UpToDate(NN%)" emitted by
    # the REST view's annotateSyncProgress decorator as equivalent
    # to plain "UpToDate" — same eligibility for affinity.
    if [[ "$n1_state" == UpToDate* && "$n2_state" == UpToDate* ]]; then
        echo ">> DiskState converged: $N1=$n1_state, $N2=$n2_state"
        break
    fi
    sleep 2
done

if [[ "$n1_state" != UpToDate* || "$n2_state" != UpToDate* ]]; then
    echo "FAIL: DiskState never reached UpToDate within ${STATUS_TIMEOUT}s"
    echo "      $N1=$n1_state  $N2=$n2_state"
    echo "$view" | jq .
    exit 1
fi

# ---- Contract (4): DISKLESS/TIE_BREAKER flags survive on the wire --

echo ">> assert DISKLESS flag round-trips on $N3 and does NOT leak onto $N1/$N2"
n3_diskless=$(echo "$view" | jq --arg n "$N3" '[.[] | select(.node_name == $n) | .flags[]?] | any(. == "DISKLESS")')
n1_diskless=$(echo "$view" | jq --arg n "$N1" '[.[] | select(.node_name == $n) | .flags[]?] | any(. == "DISKLESS")')
n2_diskless=$(echo "$view" | jq --arg n "$N2" '[.[] | select(.node_name == $n) | .flags[]?] | any(. == "DISKLESS")')

if [[ "$n3_diskless" != "true" ]]; then
    echo "FAIL: DISKLESS missing from $N3 flags — affinity controller cannot exclude witness"
    echo "$view" | jq --arg n "$N3" '.[] | select(.node_name == $n) | .flags'
    exit 1
fi

if [[ "$n1_diskless" == "true" || "$n2_diskless" == "true" ]]; then
    echo "FAIL: DISKLESS leaked onto diskful replica ($N1=$n1_diskless, $N2=$n2_diskless)"
    echo "$view" | jq .
    exit 1
fi

# ---- Contract (5): derived eligible-node set ----
#
# The affinity controller's effective predicate: "no DISKLESS flag
# AND every volume.state.disk_state matches ^UpToDate". Encode that
# here and assert the resulting set is exactly {N1, N2}.
echo ">> assert derived eligible-node set == {$N1, $N2}"
eligible=$(echo "$view" | jq -r '
    [
      .[]
      | select((.flags // []) | index("DISKLESS") | not)
      | select(
          ((.volumes // []) | length) > 0 and
          ((.volumes // []) | all(.state.disk_state // "" | test("^UpToDate")))
        )
      | .node_name
    ] | sort | join(",")
')
want=$(printf '%s\n%s\n' "$N1" "$N2" | sort | paste -sd, -)
if [[ "$eligible" != "$want" ]]; then
    echo "FAIL: eligible set=$eligible, want $want"
    echo "$view" | jq .
    exit 1
fi

echo ">> AFFINITY-CONTROLLER CONTRACT OK"
echo "   - 3 entries, node_name populated"
echo "   - DiskState=UpToDate on $N1+$N2 within ${STATUS_TIMEOUT}s"
echo "   - DISKLESS flag round-trips on $N3, no leakage onto diskful"
echo "   - eligible-node set = $eligible"
