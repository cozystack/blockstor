#!/usr/bin/env bash
#
# usage: vd-d-prune-resource-volumes-no-flap.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 399 (the `vd d` remove-side mirror of the
# Bug 384 / Bug 332 late-add).
#
# Reproduction from the dev stand:
#
#   $ linstor rd c test
#   $ linstor vd c test 1G            # vol-0
#   $ linstor vd c test 1G            # vol-1
#   $ linstor r c test --auto-place=2 -s <pool>
#   # wait until both volumes UpToDate
#   $ linstor vd d test 1             # drop vol-1
#
#   RD.spec.volumeDefinitions correctly drops to [vol-0] and the
#   kernel removes volume:1 — but the per-node Resource CRD's
#   spec.volumes STILL carries {volumeNumber:1}, because the
#   RD→Resource projection was ADD-ONLY. The leftover spec entry kept
#   the controller "knowing" about vol-1, so the phantom
#   status.volumes[1]=Diskless entry was never GC'd and the Resource
#   STATUS oscillated forever (resourceVersion churned ~1/s — an
#   apiserver PATCH storm).
#
# This cell also covers the DISKLESS / TieBreaker facet: a diskless
# replica has no backing disk to tear down, so drbd-9 never emits a
# `destroy device` frame for its removed volume — without a destroy
# signal the observer's volCache re-emitted a phantom status.volumes[1]
# every tick and the diskless replica's status.volumes oscillated between
# [0] and [0,1] ~1/tick. We hand-create a diskless replica on a 3rd node
# so this facet is exercised deterministically.
#
# Expected after the fix:
#   - every Resource's spec.volumes settles to exactly [vol-0];
#   - status.volumes settles to exactly [vol-0] (no phantom v1:Diskless),
#     including the diskless tiebreaker;
#   - the per-replica status.volumes SET STOPS changing (the flap is
#     gone). NB: we assert on the volume SET, not metadata.resourceVersion
#     — on the stand's k3s/kine backend a GET reports the global store
#     revision, which climbs on unrelated writes, so an rv-equality flap
#     check is a false negative there.
#
# Unit pins (the BS↔kernel halves this stand cell complements):
#   internal/controller/bug_399_vd_d_prune_resource_volumes_test.go
#       — controller prunes Resource.spec.volumes once converged.
#   pkg/satellite/controllers/bug_399_volume_evict_test.go
#       — observer evicts the removed volume from volCache on
#         `destroy device` (diskful) AND converges the diskless replica's
#         cached set to the RD volume-number annotation, so it stops
#         re-emitting the phantom status entry.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-399
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: 2 healthy SATELLITE nodes carrying the target pool.
echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on 2 nodes (got $ok_nodes) — Bug 399 fixture not available"
    exit 0
fi

echo ">> [Bug 399] rd c + vd c (vol-0) + vd c (vol-1)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 399] r c --auto-place=2 -s $POOL"
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

# Hand-create a DISKLESS replica on a free node so the diskless /
# tiebreaker flap facet is exercised deterministically (independent of
# auto-tiebreaker config). A diskless replica gets NO `destroy device`
# frame when vol-1 is removed, so it is the harder half of Bug 399.
echo ">> wait for the 2 diskful replicas to land, then place a diskless replica on a 3rd node"
deadline=$(( $(date +%s) + 60 ))
diskless_node=""
while (( $(date +%s) < deadline )); do
    mapfile -t diskful < <(linstor_diskful_nodes "$RD")
    if (( ${#diskful[@]} >= 2 )); then
        diskless_node=$(linstor_pick_free_node "$RD" "${diskful[@]}")
        [[ -n "$diskless_node" ]] && break
    fi
    sleep 2
done

if [[ -z "$diskless_node" ]]; then
    echo "SKIP: could not find a free 3rd node for the diskless replica — diskless facet not available"
else
    echo ">> [Bug 399] r c $diskless_node $RD --diskless (tiebreaker witness)"
    "${LCTL[@]}" resource create "$diskless_node" "$RD" --diskless >/dev/null
fi

echo ">> wait up to 120s for vol-0 + vol-1 to reach UpToDate on both diskful replicas"
deadline=$(( $(date +%s) + 120 ))
both_up=false
while (( $(date +%s) < deadline )); do
    # 2 diskful replicas × 2 volumes = 4 disk_state strings, all UpToDate.
    states=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | select((.flags//[]) | (map(. == "DISKLESS" or . == "TIE_BREAKER") | any | not)) | .vlms[]? | .state.disk_state // "Unknown"] | join(",")' \
        2>/dev/null || echo "")
    count_uptodate=$(awk -F, '{ for (i=1;i<=NF;i++) if ($i=="UpToDate") n++ } END { print n+0 }' <<<"$states")
    if (( count_uptodate == 4 )); then
        both_up=true
        break
    fi
    sleep 3
done

if [[ "$both_up" != "true" ]]; then
    echo "FAIL: vol-0 + vol-1 did not reach UpToDate on both replicas within 120s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi

# THE BUG: remove vol-1 from the 2-volume RD.
echo ">> [Bug 399] vd d test 1 (drop vol-1)"
"${LCTL[@]}" volume-definition delete "$RD" 1 >/dev/null

# RD.spec.volumeDefinitions must drop to exactly [vol-0] first — the
# satellite/controller projection chain keys off this.
echo ">> wait up to 60s for RD.spec.volumeDefinitions to settle to [vol-0]"
deadline=$(( $(date +%s) + 60 ))
rd_settled=false
while (( $(date +%s) < deadline )); do
    vd_nrs=$(kubectl get resourcedefinitions.blockstor.cozystack.io "$RD" \
        -o jsonpath='{.spec.volumeDefinitions[*].volumeNumber}' 2>/dev/null || echo "")
    if [[ "$vd_nrs" == "0" ]]; then
        rd_settled=true
        break
    fi
    sleep 2
done

if [[ "$rd_settled" != "true" ]]; then
    echo "FAIL: RD.spec.volumeDefinitions did not settle to [vol-0] within 60s (got: ${vd_nrs:-<empty>})" >&2
    exit 1
fi

# Now the load-bearing assertion: every Resource CRD's spec.volumes and
# status.volumes must converge to exactly [vol-0]. A Bug-399-bitten
# controller leaves spec.volumes=[0,1] (stale) and status flaps.
echo ">> wait up to 90s for every Resource spec.volumes + status.volumes to converge to [vol-0]"
deadline=$(( $(date +%s) + 90 ))
converged=false
while (( $(date +%s) < deadline )); do
    bad=0
    while read -r name; do
        [[ -z "$name" ]] && continue
        spec_nrs=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.spec.volumes[*].volumeNumber}' 2>/dev/null || echo "ERR")
        status_nrs=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.status.volumes[*].volumeNumber}' 2>/dev/null || echo "ERR")
        # status.volumes may legitimately be empty on a brand-new
        # diskless/tiebreaker row, but must NEVER contain volume 1.
        if [[ "$spec_nrs" == *"1"* ]] || [[ "$status_nrs" == *"1"* ]]; then
            bad=1
        fi
    done < <(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
                | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}')
    if (( bad == 0 )); then
        converged=true
        break
    fi
    sleep 3
done

if [[ "$converged" != "true" ]]; then
    echo "FAIL (Bug 399): a Resource still carries volume 1 in spec.volumes or status.volumes 90s after vd d" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            echo "----- $name -----" >&2
            kubectl get "resources.blockstor.cozystack.io/${name}" \
                -o jsonpath='spec.volumes={.spec.volumes[*].volumeNumber} status.volumes={.status.volumes[*].volumeNumber}{"\n"}' >&2 || true
        done
    exit 1
fi

# THE FLAP ASSERTION: with the volume set converged, every Resource's
# status.volumes SET must STOP changing. A Bug-399 flap (notably the
# diskless tiebreaker) oscillates status.volumes between [0] and [0,1]
# ~1/tick, so the set changes between consecutive polls.
#
# We assert on the volume SET, NOT metadata.resourceVersion: on the
# stand's k3s/kine backend a GET reports the global store revision, which
# climbs on every unrelated write even when THIS resource has zero churn
# — an rv-equality check is a false negative there. The volume-set
# comparison is kine-safe and is exactly what catches the real phantom.
echo ">> [Bug 399] flap check: per-replica status.volumes SET must be stable over a 12s window (5 polls)"
status_volset_snapshot() {
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' \
        | sort \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local st
            st=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
                -o jsonpath='{.status.volumes[*].volumeNumber}' 2>/dev/null \
                | tr ' ' '\n' | sort -n | tr '\n' ',')
            echo "${name}=${st}"
        done
}

prev_snap=""
flapped=0
for (( poll=0; poll<5; poll++ )); do
    cur_snap=$(status_volset_snapshot)
    if [[ -n "$prev_snap" && "$cur_snap" != "$prev_snap" ]]; then
        echo "FLAP: status.volumes set changed between polls" >&2
        diff <(printf '%s\n' "$prev_snap") <(printf '%s\n' "$cur_snap") >&2 || true
        flapped=1
        break
    fi
    prev_snap="$cur_snap"
    (( poll < 4 )) && sleep 3
done

if (( flapped )); then
    echo "FAIL (Bug 399): Resource status is still flapping after vd d — status.volumes set churn detected" >&2
    exit 1
fi

echo ">> vd-d-prune-resource-volumes-no-flap OK (Bug 399 pinned: vd d on $RD pruned spec.volumes + status.volumes to [vol-0], no flap)"
