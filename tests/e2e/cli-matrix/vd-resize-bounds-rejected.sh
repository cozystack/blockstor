#!/usr/bin/env bash
#
# usage: vd-resize-bounds-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — VD resize size-bounds rejection (adversarial
# round 4, 2026-07-03).
#
# The CREATE path gates size_kib into [4 MiB, 1 PiB] (Bug 155) so the
# satellite never hot-loops on `drbdadm create-md` for a size DRBD
# cannot address. `linstor vd set-size` (RESIZE) must enforce the SAME
# floor/ceiling — otherwise an operator/CSI resize persists an
# unmaterializable spec and the satellite hot-loops forever (the Bug
# 155 failure mode, reached through the resize verb). Does not self-heal.
#
# RUN-DEFERRED (2026-07-03): authored as the CLI-level paper trail per
# the blockstor CLAUDE.md CLI-bug-fix protocol. The fix itself is proven
# in the SAME PR at the L1 (handler) + integration (real apiserver via
# envtest) tiers — a REST-layer input REJECTION is fully provable there
# (the L6/L7 tiers validate DRBD-state convergence, which is N/A for a
# refused request). This cell needs the live dev-kvaps stand, which is
# unavailable this session; a stand-pending task tracks running it once
# the oracle is up.
#
# Steps:
#   1. rd c + vd c 1G + r c --auto-place=2 -s <pool>; wait UpToDate.
#   2. over-ceiling GROW via CLI: `vd s <rd> 0 1025T` (1 PiB + 1 TiB)
#      MUST exit non-zero, error names the size/maximum, size still 1G.
#   3. below-floor FORCE-shrink via raw REST PUT (force=true, 3072 KiB
#      = 3 MiB < 4 MiB floor) MUST return 400 + FAIL_INVLD_VLM_SIZE,
#      size still 1G. The floor gate only fires on a shrink, and the
#      python-linstor `vd set-size` exposes no force flag, so — like
#      rd-c-layer-list-rejected.sh's server-side case — it is exercised
#      via a raw REST PUT with force=true.
#   4. in-bounds GROW via CLI: `vd s <rd> 0 2G` MUST succeed (exit 0,
#      size becomes 2G) — the gate must not regress legitimate resizes.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-resize-bounds-rejected
POOL=${POOL:-lvm-thin}
SIZE_1G_KIB=1048576
SIZE_2G_KIB=2097152

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

echo ">> rd c + vd c 1G + r c --auto-place=2 -s $POOL"
_rc_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_rc_out" >&2; exit 1; }
_rc_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_rc_out" >&2; exit 1; }
_rc_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
    || { echo "FAIL: r c --auto-place=2 -s $POOL $RD: $_rc_out" >&2; exit 1; }

# Wait for both diskful replicas before any resize so we assert the
# steady-state gate, not a mid-sync race. Diskful = Spec.Flags carries
# NEITHER "DISKLESS" NOR "TIE_BREAKER" (see vd-shrink-rejected.sh).
deadline=$(( $(date +%s) + 90 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
            | jq -r --arg rd "$RD" '
                .items[]?
                | select(.spec.resourceDefinitionName==$rd)
                | select(((.spec.flags // [])
                    | map(select(.=="DISKLESS" or .=="TIE_BREAKER"))
                    | length) == 0)
                | .spec.nodeName'
    )
    if (( ${#placed_nodes[@]} >= 2 )); then break; fi
    sleep 2
done
if (( ${#placed_nodes[@]} < 2 )); then
    echo "FAIL: autoplace did not stage 2 diskful replicas (got ${#placed_nodes[@]})" >&2
    exit 1
fi
wait_uptodate "$RD" "${placed_nodes[0]}" "${placed_nodes[1]}"

echo ">> over-ceiling grow vd s $RD 0 1025T (1 PiB + 1 TiB — MUST exit non-zero)"
err_file=$(mktemp)
if "${LCTL[@]}" volume-definition set-size "$RD" 0 1025T >"$err_file" 2>&1; then
    echo "FAIL: over-ceiling grow (1 PiB + 1 TiB) unexpectedly succeeded" >&2
    echo "   size_kib > 1 PiB is past DRBD 9's per-device limit — REST must reject." >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
if ! grep -qiE 'above maximum|maximum|ceiling|invalid volume definition size' "$err_file"; then
    echo "FAIL: over-ceiling rejected but error text is unhelpful:" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

cur_kib=$(linstor_vd_size_kib "$RD" 0)
if (( cur_kib != SIZE_1G_KIB )); then
    echo "FAIL: post-reject SizeKib=$cur_kib != $SIZE_1G_KIB (over-ceiling reject mutated state)" >&2
    exit 1
fi

echo ">> below-floor force-shrink via raw REST PUT (3072 KiB, force=true — MUST be 400)"
floor_json=$(mktemp)
code=$(curl -s -m 5 -o "$floor_json" -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    -d '{"size_kib":3072,"force":true}' \
    "http://localhost:${LCTL_PORT}/v1/resource-definitions/${RD}/volume-definitions/0")
if [[ "$code" != "400" ]]; then
    echo "FAIL: below-floor force-shrink returned HTTP $code, want 400" >&2
    cat "$floor_json" >&2
    rm -f "$floor_json"
    exit 1
fi
if ! grep -qiE 'below minimum|invalid volume definition size' "$floor_json"; then
    echo "FAIL: 400 but envelope is not a size rejection:" >&2
    cat "$floor_json" >&2
    rm -f "$floor_json"
    exit 1
fi
rm -f "$floor_json"

cur_kib=$(linstor_vd_size_kib "$RD" 0)
if (( cur_kib != SIZE_1G_KIB )); then
    echo "FAIL: post-reject SizeKib=$cur_kib != $SIZE_1G_KIB (below-floor reject mutated state)" >&2
    exit 1
fi

echo ">> in-bounds grow vd s $RD 0 2G (MUST succeed — no over-broad regression)"
_out=$("${LCTL[@]}" volume-definition set-size "$RD" 0 2G 2>&1) \
    || { echo "FAIL: in-bounds grow 1G->2G rejected: $_out" >&2; exit 1; }
wait_vd_size "$RD" 0 "$SIZE_2G_KIB"

echo ">> vd-resize-bounds-rejected OK (out-of-bounds refused, in-bounds grow landed, no partial writes)"
