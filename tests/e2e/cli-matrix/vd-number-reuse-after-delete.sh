#!/usr/bin/env bash
#
# usage: vd-number-reuse-after-delete.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case A2b/A3/A5 (volume-number allocation
# + single DRBD consistency group).
#
# Upstream LINSTOR assigns the SMALLEST FREE volume number on a plain
# `vd c` (no --vlmnr) and the SMALLEST FREE /dev/drbd<N> minor — it does
# NOT append max+1. Two operator-observable sequences, verified
# identical against the upstream Java oracle (piraeus-server v1.33.2):
#
#   A2b — reuse after delete: rd with vols 0+1 on 2 diskful replicas,
#         `vd d 0`, then a plain `vd c` re-takes the freed 0 (NOT 2).
#         The re-added volume's backing LV is allocated, create-md fires
#         per-volume, the kernel slot picks it up, and (replica, vol-0)
#         settles UpToDate.  This exercises the Bug-399 prune path (the
#         deleted vol-0 must leave Resource.spec.volumes / status.volumes
#         on every replica) AND the Bug-384 late-add seed path (the
#         re-added vol-0 must be re-projected + brought UpToDate) in one
#         shot — a juicy interaction.
#
#   A3  — explicit-then-plain: `vd c --vlmnr 5` first, then a plain
#         `vd c` lands at the smallest free 0 (NOT 6). The explicit-create
#         must not poison the auto-assign allocator into max+1 mode.
#
# L1 pins: pkg/rest/vd_number_reuse_corner_a_test.go
#   (TestCornerA2bVolumeNumberReusedAfterDelete /
#    TestCornerA3ExplicitThenPlainGetsSmallestFree).
# L7 replay: tests/operator-harness/replay/vd-number-reuse-after-delete.yaml.
# This L6 cell is the kernel-truth half — only the real stand can observe
# the re-added vol-0 reaching UpToDate and the rendered DRBD minor.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-vd-reuse
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

# vd_numbers <rd> — sorted CSV of the VolumeDefinition numbers under rd.
vd_numbers() {
    local rd=$1
    "${LCTL[@]}" --machine-readable volume-definition list --resource-definitions "$rd" 2>/dev/null \
        | jq -r '
            [.[]? | .[]?
                | (.vlm_dfns // .volume_definitions // [])[]
                | (.volume_number // .vlm_nr // -1)
            ] | sort | join(",")' 2>/dev/null || echo ""
}

##############################################################################
# A2b — reuse after delete
##############################################################################
echo ">> [A2b] rd c + vd c (vol-0) + vd c (vol-1)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null   # auto-assign → 0
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null   # auto-assign → 1

got=$(vd_numbers "$RD")
if [[ "$got" != "0,1" ]]; then
    echo "FAIL [A2b]: after two plain vd c, expected VlmNrs 0,1 — got '$got'" >&2
    exit 1
fi

echo ">> [A2b] r c on 2 nodes + wait both volumes UpToDate"
mapfile -t nodes < <(
    jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | .[]' <<<"$sp_json" 2>/dev/null
)
n1=${nodes[0]}; n2=${nodes[1]}
"${LCTL[@]}" resource create "$n1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$n2" "$RD" --storage-pool="$POOL" >/dev/null
wait_uptodate "$RD" "$n1" "$n2" 0
wait_uptodate "$RD" "$n1" "$n2" 1

echo ">> [A2b] vd d 0 (delete the lowest number, freeing it)"
"${LCTL[@]}" volume-definition delete "$RD" 0 >/dev/null

# After delete, only vol-1 remains.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    [[ "$(vd_numbers "$RD")" == "1" ]] && break
    sleep 2
done
got=$(vd_numbers "$RD")
if [[ "$got" != "1" ]]; then
    echo "FAIL [A2b]: after vd d 0, expected VlmNrs '1' — got '$got'" >&2
    exit 1
fi

echo ">> [A2b] plain vd c — MUST reuse the freed 0 (not append 2)"
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null

deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    [[ "$(vd_numbers "$RD")" == "0,1" ]] && break
    sleep 2
done
got=$(vd_numbers "$RD")
if [[ "$got" != "0,1" ]]; then
    echo "FAIL [A2b]: plain vd c after vd d 0 must re-take 0 (smallest free); got VlmNrs '$got'" >&2
    echo "   upstream LINSTOR reuses the lowest free number, never max+1." >&2
    exit 1
fi

echo ">> [A2b] kernel-truth: re-added vol-0 reaches UpToDate on both replicas"
# This is the Bug-399-prune + Bug-384-late-add interaction: the deleted
# vol-0 had to leave every replica's projected volume set, and the
# re-added vol-0 has to be seeded + create-md'd + brought UpToDate.
wait_uptodate "$RD" "$n1" "$n2" 0
wait_uptodate "$RD" "$n1" "$n2" 1

# Defensive: the re-added vol-0 must NOT be stuck Diskless on either
# DISKFUL replica (the late-add seed path must allocate its backing LV).
# Use the k8s-native LOCAL disk-state reader, NOT a `drbdsetup status`
# grep: an auto-spawned TieBreaker (diskless witness on a third node)
# legitimately shows `peer-disk:Diskless` in drbdsetup output and would
# false-positive a naive grep. status_disk_state reads THIS replica's
# own kernel disk-state off Resource.Status.volumes[].diskState.
for dn in "$n1" "$n2"; do
    ds=$(status_disk_state "$RD" "$dn" 0)
    if [[ "$ds" == "Diskless" ]]; then
        echo "FAIL [A2b]: re-added vol-0 stuck Diskless on diskful node $dn (late-add seed regression)" >&2
        on_node "$dn" drbdsetup status "$RD" >&2 2>/dev/null || true
        exit 1
    fi
done

echo ">> [A2b] OK — vd d 0 freed the number, plain vd c re-took 0, converged UpToDate"

##############################################################################
# A3 — explicit --vlmnr 5, then plain vd c → smallest free 0 (not 6)
##############################################################################
RD2=cli-matrix-vd-explicit
echo ">> [A3] rd c + vd c --vlmnr 5 + plain vd c"
"${LCTL[@]}" resource-definition create "$RD2" >/dev/null
"${LCTL[@]}" volume-definition create --vlmnr 5 "$RD2" 64M >/dev/null   # explicit → 5
"${LCTL[@]}" volume-definition create "$RD2" 64M >/dev/null            # plain → 0

got=$(vd_numbers "$RD2")
if [[ "$got" != "0,5" ]]; then
    echo "FAIL [A3]: vd c --vlmnr 5 then plain vd c must yield VlmNrs 0,5 (smallest free), not 5,6 — got '$got'" >&2
    delete_rd "$RD2" || true
    exit 1
fi
echo ">> [A3] OK — plain vd c after explicit --vlmnr 5 landed at smallest free 0"

delete_rd "$RD2"
assert_no_orphans "$RD2"

##############################################################################
# A5 — multiple VDs land in ONE .res as multiple volume{} blocks (a single
# DRBD consistency group), NOT one resource per volume.
#
# Upstream LINSTOR renders one DRBD `resource <rd> { }` per RD with N
# nested `volume <N> { }` blocks inside each node's `on { }` stanza — all
# volumes of an RD share one DRBD connection / consistency group.
# blockstor's renderresfile must do the same. We assert it against the
# rendered .res on a diskful node (the artifact handed to `drbdadm
# adjust`). Convergence is gated by the k8s-native wait_uptodate
# (status_disk_state + kernel ground truth), NOT the `r l -m` .vlms wire
# field whose per-volume disk_state lags on a freshly-placed RD.
##############################################################################
RD3=cli-matrix-vd-onegroup
echo ">> [A5] rd c + 3x vd c (vols 0,1,2) on 2 diskful replicas"
"${LCTL[@]}" resource-definition create "$RD3" >/dev/null
"${LCTL[@]}" volume-definition create "$RD3" 64M >/dev/null   # 0
"${LCTL[@]}" volume-definition create "$RD3" 64M >/dev/null   # 1
"${LCTL[@]}" volume-definition create "$RD3" 64M >/dev/null   # 2

got=$(vd_numbers "$RD3")
if [[ "$got" != "0,1,2" ]]; then
    echo "FAIL [A5]: expected VlmNrs 0,1,2 — got '$got'" >&2
    delete_rd "$RD3" || true
    exit 1
fi

"${LCTL[@]}" resource create "$n1" "$RD3" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$n2" "$RD3" --storage-pool="$POOL" >/dev/null
for v in 0 1 2; do
    wait_uptodate "$RD3" "$n1" "$n2" "$v"
done

echo ">> [A5] rendered .res: 3 VDs => 3 distinct volume{} blocks in ONE resource{}"
# Resolve the satellite --state-dir from the running pod's args so the
# assertion works whether the deployment uses the binary default
# (/var/lib/blockstor-satellite) or the stand's override (/etc/drbd.d).
RES_DIR=${RES_DIR:-$(
    kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o jsonpath='{.items[0].spec.containers[*].args}' 2>/dev/null \
        | tr ' ,[]"' '\n' | sed -n 's/^--state-dir=//p' | head -1
)}
RES_DIR=${RES_DIR:-/etc/drbd.d}
res_file="$RES_DIR/$RD3.res"
echo ">> [A5] reading rendered res file: $res_file (state-dir=$RES_DIR)"
if res_out=$(on_node "$n1" cat "$res_file" 2>&1); then
    echo "$res_out"
    # The .res emits one `volume <N> { }` stanza per (node, volume), so
    # for a 3-volume RD on 2 diskful nodes the raw count is 6. Reduce to
    # the DISTINCT volume numbers — must be exactly 0,1,2.
    distinct_vols=$(grep -oE 'volume[[:space:]]+[0-9]+[[:space:]]*\{' <<<"$res_out" \
        | grep -oE '[0-9]+' | sort -un | tr '\n' ',' | sed 's/,$//')
    # Exactly ONE top-level `resource <rd> {` stanza — not one per volume.
    res_blocks=$(grep -cE '^resource[[:space:]]+"?'"$RD3"'"?[[:space:]]*\{' <<<"$res_out" || true)

    if [[ "$distinct_vols" != "0,1,2" ]]; then
        echo "FAIL [A5]: expected distinct volume blocks 0,1,2 in $res_file, got '$distinct_vols'" >&2
        echo "   All VDs of an RD must share ONE DRBD resource / consistency group." >&2
        delete_rd "$RD3" || true
        exit 1
    fi
    if (( res_blocks != 1 )); then
        echo "FAIL [A5]: expected exactly 1 'resource $RD3 {' stanza in $res_file, got $res_blocks" >&2
        echo "   Multiple volumes must NOT be split across separate DRBD resources." >&2
        delete_rd "$RD3" || true
        exit 1
    fi
    echo ">> [A5] OK — $RD3 renders 1 resource{} with volume blocks $distinct_vols, one consistency group"
else
    echo "SKIP-PARTIAL [A5]: cat $res_file on $n1 failed; VlmNr-set + UpToDate pins still asserted"
fi

delete_rd "$RD3"
assert_no_orphans "$RD3"

echo ">> vd-number-reuse-after-delete OK (A2b reuse-after-delete + A3 explicit-then-plain + A5 single-consistency-group pinned)"
