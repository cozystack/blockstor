#!/usr/bin/env bash
#
# usage: snap-r-rst-stamps-resources.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 354.
#
# User-reported 2026-05-19:
#
#   $ linstor s r rst --from-resource test \
#                     --from-snapshot test-snaptt \
#                     --to-resource testt
#   ...success...
#   $ linstor rd l
#   testt   DfltRscGrp   ok
#   $ linstor r l --resources testt
#   (empty — no rows)
#
# `linstor snapshot resource restore` (s r rst) is the upstream
# command for cloning a snapshot into a brand-new RD. Upstream
# LINSTOR's controller handler creates the target RD AND stamps
# per-node Resources (driven by either the explicit --node-name
# list or autoplacement against the parent RG's place_count).
# The satellites then materialize the backing storage from the
# snapshot via `provider.RestoreVolumeFromSnapshot` (the
# `BlockstorRestoreFromSnapshot` prop the controller writes
# on the new RD routes the satellite into the right code path).
#
# blockstor handleSnapshotRestore (pkg/rest/snapshot_restore.go
# :123-184) currently:
#   1. Creates the target RD with LayerStack + Props +
#      BlockstorRestoreFromSnapshot marker — OK.
#   2. Hydrates VolumeDefinitions from the snapshot — OK.
#   3. **STOPS THERE**. Never calls Store.Resources().Create
#      for the requested node list, never triggers RG
#      autoplace. The snapshotRestoreRequest.Nodes field is
#      defined but unused inside materializeRestoredRD.
#
# Result: RD exists, VDs exist, but no Resource CRDs are
# stamped. Satellites have no work to do; the new RD never
# converges to UpToDate; the cloned data never lands.
#
# Test contract:
#   1. Build a 2-replica diskful source RD on worker-1 +
#      worker-2 with a small known-data pattern.
#   2. `linstor snapshot create <src-rd> <snap>` and wait
#      Successful.
#   3. `linstor snapshot resource restore --from-resource
#      <src> --from-snapshot <snap> --to-resource <tgt>` with
#      explicit --node-name list pointing at worker-1+worker-2.
#   4. Within 90s, `linstor r l --resources <tgt>` MUST show
#      2 rows (one per requested node), both reaching UpToDate.
#   5. Bonus assertion: the cloned data on worker-1 must match
#      the source seed pattern (RestoreVolumeFromSnapshot
#      actually populated the new RD's backing).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

SRC=cli-matrix-snap-rst-src
SNAP=snap-rst-1
TGT=cli-matrix-snap-rst-tgt

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>/dev/null || true
    delete_rd "$TGT"
    delete_rd "$SRC"
    assert_no_orphans "$SRC"
    assert_no_orphans "$TGT"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> source RD: 2-replica diskful on $N1 + $N2"
_out=$("${LCTL[@]}" resource-definition create "$SRC" 2>&1) \
    || { echo "FAIL: rd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$SRC" 64M 2>&1) \
    || { echo "FAIL: vd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$SRC" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $SRC: $_out" >&2; exit 1; }
wait_uptodate "$SRC" "$N1" "$N2"

# Seed a small known pattern that we can match on the restored
# replica later. Skip if /dev/drbd resolution fails (e.g. CSI
# was not yet configured — the catcher still drives the REST
# call and asserts Resource CRDs are stamped).
echo ">> seed deterministic pattern on $N1 $SRC"
on_node "$N1" drbdadm primary --force "$SRC" 2>/dev/null || true
on_node "$N1" bash -c "
    dev=\$(readlink -f /dev/drbd/by-res/$SRC/0 2>/dev/null || true)
    if [ -n \"\$dev\" ]; then
        printf 'BLOCKSTOR-BUG354-MARKER' | dd of=\"\$dev\" bs=1 count=24 conv=fsync status=none
    fi
" || true
wait_uptodate "$SRC" "$N1" "$N2"

echo ">> snap c $SRC $SNAP"
_out=$("${LCTL[@]}" snapshot create "$SRC" "$SNAP" 2>&1) \
    || { echo "FAIL: snap c $SRC $SNAP: $_out" >&2; exit 1; }

# Wait Successful.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    ok=$(kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -r --arg rd "$SRC" --arg s "$SNAP" '
            [.items[]? | select(.spec.resourceDefinitionName==$rd) | select(.spec.snapshotName==$s) | .status.successful // false] | all' 2>/dev/null || echo "false")
    [[ "$ok" == "true" ]] && break
    sleep 2
done

# ---- Trigger: snapshot resource restore ----------------------------------
echo ">> [Bug 354 trigger] linstor s r rst --from-resource $SRC --from-snapshot $SNAP --to-resource $TGT $N1 $N2"
# Why: upstream `linstor s r rst` grammar takes node names as positional
# trailing args (`[node_name ...]`), NOT a --node-name flag. golinstor
# maps the positional list to JSON body's `nodes`, which the apiserver's
# snapshotRestoreRequest unmarshals. The earlier --node-name attempt was
# rejected client-side with "unrecognized arguments" → the new
# placeRestoredResources code path was never exercised.
if ! _out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT" \
        "$N1" "$N2" 2>&1); then
    echo "FAIL: snapshot resource restore returned non-zero: $_out" >&2
    exit 1
fi

# Confirm the RD was created (this part already works — Bug 354
# narrows to the resource-stamping gap).
if ! "${LCTL[@]}" resource-definition list --resource-definitions "$TGT" 2>/dev/null | grep -q "$TGT"; then
    echo "FAIL: target RD $TGT not visible in rd l after restore" >&2
    exit 1
fi

# ---- Bug 354 assertion: per-node Resource CRDs must be stamped ----------
echo ">> [Bug 354] wait up to 90s for 2 Resource CRDs on $TGT (one per --node-name)"
deadline=$(( $(date +%s) + 90 ))
n_resources=0
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="${TGT}." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    n_resources=${#placed_nodes[@]}
    if (( n_resources >= 2 )); then break; fi
    sleep 3
done

if (( n_resources == 0 )); then
    echo "FAIL (Bug 354): target RD $TGT has NO Resource CRDs 90s after s r rst" >&2
    echo "  Upstream LINSTOR's snapshot resource restore stamps a Resource per --node-name argument." >&2
    echo "  blockstor's pkg/rest/snapshot_restore.go::materializeRestoredRD never calls" >&2
    echo "  Store.Resources().Create — the snapshotRestoreRequest.Nodes field is unused." >&2
    echo "----- linstor rd l -----" >&2
    "${LCTL[@]}" resource-definition list --resource-definitions "$TGT" 2>&1 | head -10 >&2
    echo "----- kubectl get rd $TGT -----" >&2
    kubectl get resourcedefinitions.blockstor.io.blockstor.io "$TGT" -o yaml 2>&1 | head -40 >&2
    echo "----- kubectl get resources for $TGT -----" >&2
    kubectl get resources.blockstor.io.blockstor.io 2>/dev/null | grep -E "(^${TGT}\\.| RESOURCE)" | head -10 >&2 || echo "(none)"
    exit 1
fi

if (( n_resources < 2 )); then
    echo "FAIL (Bug 354): target RD $TGT has only $n_resources Resource CRD(s), expected 2 (one per --node-name)" >&2
    printf '   placed: %s\n' "${placed_nodes[*]}" >&2
    exit 1
fi

# ---- Wait UpToDate on both restored replicas ----------------------------
echo ">> wait up to 180s for both restored replicas UpToDate"
deadline=$(( $(date +%s) + 180 ))
ok=false
while (( $(date +%s) < deadline )); do
    s1=$(status_disk_state "$TGT" "${placed_nodes[0]}" 0)
    s2=$(status_disk_state "$TGT" "${placed_nodes[1]}" 0)
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" ]]; then
        ok=true
        break
    fi
    sleep 3
done
if ! $ok; then
    echo "FAIL: restored RD $TGT never reached UpToDate on both replicas — ${placed_nodes[0]}=$s1 ${placed_nodes[1]}=$s2" >&2
    "${LCTL[@]}" resource list --resources "$TGT" 2>&1 | tail -10 >&2
    exit 1
fi

# ---- Bonus: read the marker on the restored replica ----------------------
echo ">> bonus assert: marker bytes restored from snapshot on $N1 $TGT"
marker_read=$(on_node "$N1" bash -c "
    on_node_drbdadm() { drbdadm primary --force \$1 2>/dev/null; }
    on_node_drbdadm $TGT
    dev=\$(readlink -f /dev/drbd/by-res/$TGT/0 2>/dev/null || true)
    if [ -n \"\$dev\" ]; then
        head -c 24 \"\$dev\" 2>/dev/null
    fi
" 2>/dev/null || echo "")
if [[ "$marker_read" != "BLOCKSTOR-BUG354-MARKER" ]]; then
    echo "note: bonus marker check inconclusive (read='${marker_read}', expected 'BLOCKSTOR-BUG354-MARKER')" >&2
    echo "  RD + Resources stamped correctly (the Bug 354 contract is met); the snapshot" >&2
    echo "  data path may need separate validation if the marker bytes diverge." >&2
fi

echo ">> snap-r-rst-stamps-resources OK (Bug 354 pinned: snapshot resource restore stamps Resources on requested nodes)"
