#!/usr/bin/env bash
#
# usage: u216-add-peer-mesh.sh WORK_DIR
#
# L6 cli-matrix cell — U216 (P1).
#
# Upstream LINSTOR user report: autoplace-EXTENDING an existing resource
# produced a StandAlone replica. The EXISTING replicas' .res was not
# regenerated to include the newly-added peer, so DRBD on the surviving
# siblings had no `on <newnode>` block / no connection path to the fresh
# replica → the new replica's handshake found no peer and wedged
# StandAlone.
#
# blockstor regenerates the DesiredResource for EVERY replica each
# reconcile from the live RD's full replica set (pinned at L1 by
# pkg/dispatcher TestU216AddPeerRegeneratesExistingReplicaMesh). This
# cell is the kernel-truth half: after adding a 3rd replica to a
# 2-replica resource,
#
#   A. every node's rendered .res lists ALL three `on <node>` blocks
#      (the existing replicas' .res WAS regenerated with the new peer);
#   B. the new replica reaches UpToDate and its connections are
#      Established — it is NEVER StandAlone;
#   C. drbdsetup status on an EXISTING replica lists the new node as a
#      connection (the surviving sibling dials the new peer).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-u216
POOL=${POOL:-stand}

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [U216] rd c + vd c (32M) + r c on $N1 + $N2 (-s $POOL)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 32M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for 2-replica UpToDate ($N1 + $N2)"
wait_uptodate "$RD" "$N1" "$N2"

# THE EXTENSION: add the 3rd diskful replica.
echo ">> [U216] r c $N3 $RD -s $POOL (extend to 3 replicas)"
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool="$POOL" >/dev/null

# Assertion B: new replica reaches UpToDate (never StandAlone).
echo ">> [U216] new replica $N3 reaches UpToDate"
wait_uptodate "$RD" "$N1" "$N3"
wait_uptodate "$RD" "$N2" "$N3"

# Assertion A: every node's .res must list all three on-blocks — the
# EXISTING replicas' .res WAS regenerated to include the new peer. Read
# each node's .res via its satellite pod and require all three blocks.
echo ">> [U216] every node's .res lists all three on-blocks (mesh regenerated)"
res_on_block() {
    # res_on_block <view-node> <listed-node> — does the .res on
    # <view-node> contain an `on <listed-node> { ... }` block?
    local view=$1 listed=$2 sat res
    sat=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o jsonpath="{.items[?(@.spec.nodeName==\"$view\")].metadata.name}")
    [[ -z "$sat" ]] && return 1
    res=$(kubectl -n "$NS" exec "$sat" -- cat "/etc/drbd.d/${RD}.res" 2>/dev/null || echo "")
    grep -qE "^[[:space:]]*on[[:space:]]+\"?${listed}\"?[[:space:]]*\\{" <<<"$res"
}

for view in "$N1" "$N2" "$N3"; do
    for listed in "$N1" "$N2" "$N3"; do
        if ! res_on_block "$view" "$listed"; then
            echo "FAIL (U216): .res on $view is MISSING 'on $listed' block — the mesh" >&2
            echo "  was not regenerated when the peer was added (upstream → StandAlone)." >&2
            sat=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
                -o jsonpath="{.items[?(@.spec.nodeName==\"$view\")].metadata.name}")
            kubectl -n "$NS" exec "$sat" -- cat "/etc/drbd.d/${RD}.res" 2>&1 | sed 's/^/    /' >&2 || true
            exit 1
        fi
    done
done
echo "   OK: all three nodes carry on-blocks for all three peers"

# Assertion B (kernel): no replica's connections are StandAlone.
echo ">> [U216] no replica is StandAlone on any node"
for node in "$N1" "$N2" "$N3"; do
    st=$(on_node "$node" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
    if grep -qiE 'connection:StandAlone|StandAlone' <<<"$st"; then
        echo "FAIL (U216): $node reports a StandAlone connection for $RD" >&2
        echo "$st" | sed 's/^/    /' >&2
        exit 1
    fi
done

# Assertion C: an EXISTING replica ($N1) must list the new node ($N3) as
# a connection (the surviving sibling dials the new peer — proving the
# .res regen reached the kernel via drbdadm adjust).
echo ">> [U216] existing replica $N1 lists new peer $N3 as a connection"
n1_status=$(on_node "$N1" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
if ! grep -qE "(^|[[:space:]])${N3}([[:space:]]|$)" <<<"$n1_status"; then
    echo "FAIL (U216): drbdsetup status on $N1 does NOT list the new peer $N3" >&2
    echo "$n1_status" | sed 's/^/    /' >&2
    exit 1
fi

echo ">> u216-add-peer-mesh OK (existing replicas' .res regenerated with new peer, full mesh, no StandAlone)"
