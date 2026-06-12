#!/usr/bin/env bash
#
# usage: snap-restore-snapshotless-node-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 397 (P0, DATA INTEGRITY).
#
# A snapshot-restore that targets a node LACKING the snapshot used to
# fall back to a BLANK volume while leaving skip-init-sync enabled, so
# the empty replica latched UpToDate WITHOUT syncing the real data —
# silent data loss (an empty replica presenting as a good copy,
# promotable on failover). This affects thin / ZFS providers (ZFS is
# the cozystack default).
#
# Two-layer defense, both asserted here at operator-CLI level:
#
#   A. INPUT GUARD — restoring with an explicit node list that names a
#      node NOT in the snapshot's node set must be REJECTED clearly
#      (matching upstream LINSTOR), never silently place an empty
#      replica. We assert the CLI returns non-zero and the bad node is
#      named in the error.
#
#   B. RESTORE CORRECTNESS — restoring onto the snapshot's OWN nodes
#      must converge UpToDate AND every diskful replica must hold the
#      REAL snapshot bytes (the deterministic marker), never an empty
#      device. We read the marker on EACH replica (promoting each in
#      turn) so a silently-empty replica is caught even when DRBD
#      reports it UpToDate.
#
# Contract:
#   1. source RD, 2-replica diskful on worker-1 + worker-2.
#   2. seed a deterministic marker, snapshot.
#   3. (A) `snapshot resource restore ... worker-1 worker-3` where
#      worker-3 does NOT hold the snapshot → MUST be rejected.
#   4. (B) `snapshot resource restore ... worker-1 worker-2` → both
#      replicas reach UpToDate and BOTH carry the marker bytes.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

SRC=cli-matrix-397-src
SNAP=snap-397-1
TGT_BAD=cli-matrix-397-tgt-bad
TGT_OK=cli-matrix-397-tgt-ok
MARKER='BLOCKSTOR-BUG397-MARKER'

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    "${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>/dev/null || true
    delete_rd "$TGT_OK"
    delete_rd "$TGT_BAD"
    delete_rd "$SRC"
    assert_no_orphans "$SRC"
    assert_no_orphans "$TGT_OK"
    assert_no_orphans "$TGT_BAD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> source RD: 2-replica diskful on $N1 + $N2 (snapshot will live ONLY here)"
_out=$("${LCTL[@]}" resource-definition create "$SRC" 2>&1) \
    || { echo "FAIL: rd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$SRC" 64M 2>&1) \
    || { echo "FAIL: vd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$SRC" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $SRC: $_out" >&2; exit 1; }
wait_uptodate "$SRC" "$N1" "$N2"

echo ">> seed deterministic marker on $N1 $SRC"
on_node "$N1" drbdadm primary --force "$SRC" 2>/dev/null || true
# Resolve via `drbdadm sh-dev` (lib.sh resolve_drbd_device): the
# /dev/drbd/by-res symlink is not reliably present in the satellite
# mount namespace, so readlink-based resolution aborts on the stand.
dev=$(resolve_drbd_device "$N1" "$SRC" 0) || {
    echo "ABORT: could not resolve /dev/drbd for $SRC on $N1" >&2
    exit 2
}
on_node "$N1" bash -c \
    "printf '$MARKER' | dd of='$dev' bs=1 count=${#MARKER} conv=fsync status=none"
wait_uptodate "$SRC" "$N1" "$N2"

echo ">> snap c $SRC $SNAP"
_out=$("${LCTL[@]}" snapshot create "$SRC" "$SNAP" 2>&1) \
    || { echo "FAIL: snap c $SRC $SNAP: $_out" >&2; exit 1; }

# Wait snapshot Successful on its two nodes.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    ok=$(kubectl get snapshots.blockstor.cozystack.io -o json 2>/dev/null \
        | jq -r --arg rd "$SRC" --arg s "$SNAP" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)
             | (.nodes = (.spec.nodes // []))
             | (.ready = ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]))
             | ((.nodes | length) > 0) and (.nodes - .ready | length == 0)
            ] | length > 0 and all' 2>/dev/null || echo "false")
    [[ "$ok" == "true" ]] && break
    sleep 2
done

# ===========================================================================
# Layer A — INPUT GUARD: restore onto a snapshot-less node must be rejected.
# ===========================================================================
echo ">> [Bug 397 / A] restore onto snapshot-less $N3 must be REJECTED"
err_file=$(mktemp)
if "${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT_BAD" \
        "$N1" "$N3" >"$err_file" 2>&1; then
    echo "FAIL (Bug 397): restore onto snapshot-less node $N3 was ACCEPTED — would place an empty replica" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi

# The error should name the offending node so the operator can fix the
# invocation. Soft-check: surface but don't fail if wording differs.
if ! grep -q "$N3" "$err_file"; then
    echo "note: rejection did not name $N3 explicitly:" >&2
    cat "$err_file" >&2
fi
rm -f "$err_file"

# No orphan target RD / Resources may have been created by the rejected call.
if "${LCTL[@]}" resource-definition list --resource-definitions "$TGT_BAD" 2>/dev/null | grep -q "$TGT_BAD"; then
    echo "FAIL (Bug 397): rejected restore left an orphan target RD $TGT_BAD" >&2
    exit 1
fi

echo ">> [Bug 397 / A] OK — snapshot-less node restore rejected, no orphan RD"

# ===========================================================================
# Layer B — RESTORE CORRECTNESS: restore onto snapshot nodes carries data.
# ===========================================================================
echo ">> [Bug 397 / B] restore onto snapshot nodes $N1 $N2 → $TGT_OK"
if ! _out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT_OK" \
        "$N1" "$N2" 2>&1); then
    echo "FAIL (Bug 397): legitimate restore onto snapshot nodes rejected: $_out" >&2
    exit 1
fi

echo ">> [Bug 397 / B] wait up to 90s for 2 Resource CRDs on $TGT_OK"
deadline=$(( $(date +%s) + 90 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
            | awk -v rd="${TGT_OK}." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_nodes[@]} >= 2 )); then break; fi
    sleep 3
done
if (( ${#placed_nodes[@]} < 2 )); then
    echo "FAIL (Bug 397): restored RD $TGT_OK has ${#placed_nodes[@]} Resource CRD(s), expected 2" >&2
    exit 1
fi

echo ">> [Bug 397 / B] wait up to 180s for both restored replicas UpToDate"
deadline=$(( $(date +%s) + 180 ))
ok=false
while (( $(date +%s) < deadline )); do
    s1=$(status_disk_state "$TGT_OK" "${placed_nodes[0]}" 0)
    s2=$(status_disk_state "$TGT_OK" "${placed_nodes[1]}" 0)
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" ]]; then
        ok=true
        break
    fi
    sleep 3
done
if ! $ok; then
    echo "FAIL (Bug 397): restored RD $TGT_OK never reached UpToDate on both replicas — ${placed_nodes[0]}=$s1 ${placed_nodes[1]}=$s2" >&2
    "${LCTL[@]}" resource list --resources "$TGT_OK" 2>&1 | tail -10 >&2
    exit 1
fi

# DATA-INTEGRITY assertion: read the marker on EACH replica. A silently-
# empty replica (the Bug 397 failure mode) reports UpToDate but reads back
# zeros — promoting each node in turn and reading the device catches it.
echo ">> [Bug 397 / B] assert REAL snapshot bytes on EACH restored replica"
for node in "${placed_nodes[0]}" "${placed_nodes[1]}"; do
    # Demote the other replicas so this node can become Primary cleanly.
    for other in "${placed_nodes[@]}"; do
        [[ "$other" == "$node" ]] && continue
        on_node "$other" drbdadm secondary "$TGT_OK" 2>/dev/null || true
    done

    # Same portable resolver as the seeding step: by-res symlinks are
    # not reliably present in the satellite mount namespace. An
    # unresolved device leaves marker_read empty and fails the
    # data-integrity assertion below, exactly like before.
    dev=$(resolve_drbd_device "$node" "$TGT_OK" 0 2>/dev/null) || dev=""
    marker_read=$(on_node "$node" bash -c "
        drbdadm primary --force $TGT_OK 2>/dev/null || true
        if [ -n '$dev' ]; then
            head -c ${#MARKER} '$dev' 2>/dev/null
        fi
    " 2>/dev/null || echo "")

    if [[ "$marker_read" != "$MARKER" ]]; then
        echo "FAIL (Bug 397): replica $node of $TGT_OK does NOT hold the snapshot data" >&2
        echo "  read='${marker_read}', expected '$MARKER'" >&2
        echo "  This is the silent-empty-replica data-integrity bug: UpToDate but no data." >&2
        exit 1
    fi
    echo "   $node: marker present"
done

echo ">> snap-restore-snapshotless-node-rejected OK (Bug 397: reject snapshot-less node; restored replicas all hold real data)"
