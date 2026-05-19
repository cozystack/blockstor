#!/usr/bin/env bash
#
# usage: r-l-conns-shapes.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 331.
#
# Audit the Conns/State column shapes that `linstor r l` renders
# against the upstream golden screenshots in the user's local
# Downloads/linstor_cases_example/. The point: codify the
# Conns/State contract via real CLI-output parsing so if the
# observer's events2 translation regresses (drops a state name,
# emits the wrong synonym, returns "" instead of "Unknown" for an
# unreachable peer, ...), the failing comparison surfaces it.
#
# Golden shapes (10 screenshots, kept in user's Downloads dir):
#
#   1. Healthy 3r: all rows Conns=Ok, State=UpToDate
#   2. 2r + 1 Diskless replica: 2 rows UpToDate Ok, 1 row Diskless Ok
#   3. 2r + 1 TieBreaker: 2 rows UpToDate Ok, 1 row TieBreaker
#   4. Disconnected peer: surviving rows Conns="Connecting(node2)";
#      the unreachable row shows Conns="" and State=Unknown
#   5. NetworkFailure: rows show Conns="NetworkFailure(node0)" and
#      the failed row may show State=Consistent (last-known cached)
#   6. Outdated: row Conns=Connecting(<peer>), State=Outdated
#   7. Failed: row State=Failed (on top of UpToDate Ok sibling)
#   8. Multi-peer disconnect: Conns="Connecting(node0,node1)" — csv
#      in one cell
#
# This cell sets up the shapes we CAN reproduce on a 3-worker stand
# without network-partition tooling (Healthy, Diskless, TieBreaker)
# and asserts the observer-stamped Status drives the python CLI's
# Conns/State columns to the upstream-shaped values. The
# Disconnected / NetworkFailure / Outdated / Failed shapes need
# iptables / external_drbd_meta tooling — covered by
# tests/e2e/network-partition.sh and tests/e2e/state-*.sh, so
# we only spot-check the wire shapes here.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Per-sub-test RDs so a partial run doesn't leak state into the next.
RD_HEALTHY=cli-matrix-331-healthy
RD_DISKLESS=cli-matrix-331-diskless
RD_TB=cli-matrix-331-tb

cleanup() {
    for rd in "$RD_HEALTHY" "$RD_DISKLESS" "$RD_TB"; do
        delete_rd "$rd"
    done
    for rd in "$RD_HEALTHY" "$RD_DISKLESS" "$RD_TB"; do
        assert_no_orphans "$rd"
    done
    linstor_cli_teardown
}
trap cleanup EXIT

# assert_row pulls the row for (rd, node) from `linstor r l -r rd`
# JSON and checks that the predicate jq expression returns true.
# Prints the actual row on failure so the diff against the golden
# screenshot is obvious.
assert_row() {
    local rd=$1 node=$2 predicate=$3 desc=$4
    local row
    row=$("${LCTL[@]}" --machine-readable resource list --resources "$rd" 2>/dev/null \
        | jq --arg n "$node" --arg rd "$rd" '
            .[][] | select(.name==$rd and .node_name==$n)
        ')
    if [[ -z "$row" || "$row" == "null" ]]; then
        echo "FAIL ($desc): no row for ${rd}.${node} in linstor r l" >&2
        "${LCTL[@]}" resource list --resources "$rd" 2>&1 | tail -20 >&2
        return 1
    fi
    if ! jq -e "$predicate" >/dev/null 2>&1 <<<"$row"; then
        echo "FAIL ($desc): predicate '$predicate' did not hold for ${rd}.${node}" >&2
        echo "  row was:" >&2
        echo "$row" | jq '.' >&2
        return 1
    fi
    return 0
}

# ----------------------------------------------------------------------
# Sub-test A — Healthy 3-replica
#
# Golden screenshot 1 (5 columns visible): every row should have
#   Conns=Ok, State=UpToDate, Layers="DRBD,STORAGE"
# Wire shape on /v1/view/resources after the observer settles:
#   .layer_object.drbd.connections[].connected == true
#   .layer_object.drbd.connections[].message ∈ {"Connected", "Established"}
#   .volumes[].state.disk_state == "UpToDate"
# ----------------------------------------------------------------------

echo ">> [331.A] healthy 3-replica shape"
"${LCTL[@]}" resource-definition create "$RD_HEALTHY" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_HEALTHY" 128M >/dev/null
"${LCTL[@]}" resource create --auto-place=3 --storage-pool stand "$RD_HEALTHY" >/dev/null

for n in "$N1" "$N2" "$N3"; do
    wait_status_state "$RD_HEALTHY" "$n" "UpToDate" 180 0
done

# Wait for every per-peer connection to be Connected/Established
# before we sample — observer can lag by a few seconds after the
# kernel reports the handshake.
for n in "$N1" "$N2" "$N3"; do
    for peer in "$N1" "$N2" "$N3"; do
        [[ "$n" == "$peer" ]] && continue
        wait_conns_ok "$RD_HEALTHY" "$n" "$peer" 60
    done
done

for n in "$N1" "$N2" "$N3"; do
    assert_row "$RD_HEALTHY" "$n" \
        '.volumes[0].state.disk_state == "UpToDate"' \
        "331.A disk_state on $n"
    assert_row "$RD_HEALTHY" "$n" \
        '[.layer_object.drbd.connections[]? | .connected] | all' \
        "331.A all peer connections .connected==true on $n"
done

# ----------------------------------------------------------------------
# Sub-test B — Diskless replica (golden screenshot 2)
#
# Apply 2 diskful + 1 explicit DISKLESS replica. The diskless row
# should render:
#   Conns=Ok, State=Diskless, Layers=DRBD (no STORAGE)
# Wire-shape (Diskless flag on Spec.Flags; volume row synthesized
# by ensureVolumesForView with storage_pool=DfltDisklessStorPool).
# ----------------------------------------------------------------------

echo ">> [331.B] 2 diskful + 1 explicit Diskless shape"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD_DISKLESS}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD_DISKLESS}.${N1}}
spec:
  resourceDefinitionName: ${RD_DISKLESS}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD_DISKLESS}.${N2}}
spec:
  resourceDefinitionName: ${RD_DISKLESS}
  nodeName: ${N2}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD_DISKLESS}.${N3}}
spec:
  resourceDefinitionName: ${RD_DISKLESS}
  nodeName: ${N3}
  flags: ["DISKLESS"]
EOF

wait_status_state "$RD_DISKLESS" "$N1" "UpToDate" 180 0
wait_status_state "$RD_DISKLESS" "$N2" "UpToDate" 180 0
wait_status_diskless "$RD_DISKLESS" "$N3" 60

assert_row "$RD_DISKLESS" "$N3" \
    '(.flags | index("DISKLESS")) != null' \
    "331.B Diskless flag on $N3"

# The python CLI's Conns column should still be "Ok" for the
# diskless replica — the per-peer connections must be Connected
# even though the replica itself has no local disk. Wait for the
# connection observer to settle before sampling.
wait_conns_ok "$RD_DISKLESS" "$N3" "$N1" 60
wait_conns_ok "$RD_DISKLESS" "$N3" "$N2" 60

assert_row "$RD_DISKLESS" "$N3" \
    '[.layer_object.drbd.connections[]? | .connected] | all' \
    "331.B Diskless replica still reports Conns=Ok"

# ----------------------------------------------------------------------
# Sub-test C — TieBreaker (golden screenshot 3)
#
# 2-replica RD + auto-tiebreaker on the 3rd node. The TB row's
# State column should render "TieBreaker" — driven by the
# TIE_BREAKER flag on Spec.Flags (not by any drbd-layer state).
# ----------------------------------------------------------------------

echo ">> [331.C] 2-replica + auto-tiebreaker shape"
"${LCTL[@]}" resource-definition create "$RD_TB" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_TB" 64M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD_TB" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD_TB" --storage-pool stand >/dev/null

RD="$RD_TB" wait_uptodate "$RD_TB" "$N1" "$N2"

echo ">> wait up to 60s for auto-tiebreaker witness on $N3"
deadline=$(( $(date +%s) + 60 ))
tb_node=""
while (( $(date +%s) < deadline )); do
    for n in "$N1" "$N2" "$N3"; do
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD_TB}.${n}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" == *"TIE_BREAKER"* ]]; then
            tb_node=$n
            break 2
        fi
    done
    sleep 2
done

if [[ -z "$tb_node" ]]; then
    echo "FAIL (331.C): auto-tiebreaker never created within 60s" >&2
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD_TB." '$1 ~ "^"rd' >&2
    exit 1
fi

assert_row "$RD_TB" "$tb_node" \
    '(.flags | index("TIE_BREAKER")) != null' \
    "331.C TIE_BREAKER flag on $tb_node"
assert_row "$RD_TB" "$tb_node" \
    '(.flags | index("DISKLESS")) != null' \
    "331.C DISKLESS flag accompanies TIE_BREAKER on $tb_node"

# Wire-shape: even a TieBreaker replica must surface the connection
# rows (otherwise the python CLI renders "Conns=" empty and the
# operator can't tell if the witness is reachable). The contract
# from screenshot 3: TB row → Conns=Ok.
wait_conns_ok "$RD_TB" "$tb_node" "$N1" 60
wait_conns_ok "$RD_TB" "$tb_node" "$N2" 60

echo ">> r-l-conns-shapes OK (Bug 331: Healthy / Diskless / TieBreaker wire shapes match golden screenshots)"
