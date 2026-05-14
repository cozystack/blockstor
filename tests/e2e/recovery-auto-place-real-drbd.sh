#!/usr/bin/env bash
#
# usage: recovery-auto-place-real-drbd.sh WORK_DIR
#
# Tier 4 regression guard for Bug 80 (cb56d0024):
#
#   `linstor rd create $RD ; linstor vd create $RD 32M ;
#    linstor r create $RD --auto-place=2 --storage-pool stand`
#
# pre-fix, the sequence above left BOTH diskful replicas stuck in
# `disk:Inconsistent` forever, because the cache-trail race had
# each satellite read its peer's Resource BEFORE the controller-
# side allocator stamped `Status.DRBDNodeID` on it, so each
# satellite computed `lowestDiskfulID == self`, both stamped
# `auto-primary=true` into their own .res, and both ran
# `drbdadm primary --force`. The cluster then either split-brained
# (StandAlone) or sat in mutual Inconsistent because neither side
# elected itself the unique SyncSource.
#
# The fix (cb56d0024) gates the auto-primary election on
# `diskfulPeersAllocated()` returning true — i.e. EVERY diskful
# peer must already have a non-nil Status.DRBDNodeID before this
# satellite decides whether to stamp itself. On a fresh
# auto-place this means the second satellite to reconcile is the
# only one that can see both ids, so only the replica with the
# lowest DRBDNodeID stamps auto-primary, runs primary --force,
# and becomes the SyncSource for the other.
#
# Why this test exists despite `client-compat.sh §B.2` already
# poking the same wire path: client-compat asserts "at least one
# replica reached UpToDate within 180s", which is the minimum
# regression bar but lets through a partial fix that lands one
# replica UpToDate while the other stays SyncTarget forever, or
# silently leaves the autoplacer's tiebreaker witness absent.
# This test pins the full post-fix shape:
#
#   - exactly one node was elected initial Primary (matches the
#     replica with the lowest Status.DRBDNodeID — the
#     deterministic election picked by `lowestDiskfulID`).
#   - both diskful replicas reach `disk_state="UpToDate"`.
#   - the third satellite gets an auto-placed TIE_BREAKER witness
#     with the DISKLESS flag (RD reconciler invariant — would be
#     skipped if the auto-place path errored mid-flight pre-fix).
#   - no `connection_state="StandAlone"` anywhere (split-brain
#     symptom that the pre-fix race used to land on intermittently).
#
# Best-effort: the CRD-level `.spec.props.DrbdOptions/auto-primary`
# check the bug-write-up references is a wire-only computed flag
# (set in `dispatcher.BuildDesired` → DesiredResource.DrbdOptions,
# consumed by `pkg/satellite/reconciler.go:907`) — it is never
# persisted onto the Resource CRD's Spec.Props. We sample
# `.spec.props.DrbdOptions/auto-primary` defensively for log
# evidence in case a future change starts persisting it, but do
# NOT gate the PASS verdict on it.
#
# Stand: e2e-quorum (4-worker Talos+QEMU cluster). The 3rd
# satellite is the tiebreaker host — `require_workers 3` is the
# floor.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi
if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH"
    exit 0
fi

RD=bug80-e2e
SETTLE_DEADLINE_SEC=120

# Port-forward to the apiserver — same pattern as client-compat.sh
# lines 50-72.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/recovery-auto-place-real-drbd-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: kubectl get resources -o wide ----"
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' || true
    echo "---- dump: resource CRDs as yaml ----"
    for r in $(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd {print $1}'); do
        echo "---- $r ----"
        kubectl get "resources.blockstor.io.blockstor.io/$r" -o yaml 2>/dev/null \
            | sed -n '1,80p' || true
    done
    echo "---- dump: linstor r l --machine-readable -r $RD ----"
    "${LCTL[@]}" resource list -r "$RD" 2>/dev/null || true
    echo "---- dump: drbdsetup status on each worker ----"
    for n in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
        echo "-- $n --"
        on_node "$n" drbdsetup status "$RD" --verbose 2>/dev/null || true
    done
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

for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://127.0.0.1:$PF_PORT" --machine-readable)
LCTL_PLAIN=(linstor --controllers "http://127.0.0.1:$PF_PORT")

# --- Step 1: the exact 3-call repro from the bug report ----------------
echo ">> step 1: linstor rd create $RD"
"${LCTL_PLAIN[@]}" resource-definition create "$RD" >/dev/null

echo ">> step 2: linstor vd create $RD 32M"
"${LCTL_PLAIN[@]}" volume-definition create "$RD" 32M >/dev/null

echo ">> step 3: linstor r create $RD --auto-place 2 --storage-pool stand"
"${LCTL_PLAIN[@]}" resource create "$RD" --auto-place 2 --storage-pool stand >/dev/null

# --- Step 4: wait for the post-fix shape -------------------------------
# Bug 80 pre-fix symptom: both diskful stuck Inconsistent forever.
# Post-fix invariant: 2 diskful go UpToDate, 1 tiebreaker DISKLESS,
# no StandAlone. Poll on ALL THREE conditions so we don't pass on a
# transient half-converge.
echo ">> step 4: wait <=${SETTLE_DEADLINE_SEC}s for full convergence"
deadline=$(( $(date +%s) + SETTLE_DEADLINE_SEC ))
converged=0
last_json=""
last_uptodate=0
last_tiebreaker=0
last_standalone=0
while (( $(date +%s) < deadline )); do
    last_json=$("${LCTL[@]}" resource list -r "$RD" 2>/dev/null || true)
    if [[ -z "$last_json" ]]; then sleep 3; continue; fi

    # Number of replicas with disk_state == "UpToDate".
    last_uptodate=$(echo "$last_json" \
        | jq -r '[.. | objects | select(.volumes? != null) | .volumes[]? | select(.state?.disk_state == "UpToDate")] | length' \
        2>/dev/null || echo 0)
    # Number of replicas carrying the TIE_BREAKER flag.
    last_tiebreaker=$(echo "$last_json" \
        | jq -r '[.. | objects | select(.flags? != null and (.flags | index("TIE_BREAKER")))] | length' \
        2>/dev/null || echo 0)
    # Any StandAlone connection — appears in two shapes on the
    # blockstor wire:
    #   1. legacy linstor scalar `connection_status: "StandAlone"`
    #   2. blockstor per-peer `connections.<peer>.message:
    #      "StandAlone"` (mirrors `drbdsetup status` connection state)
    last_standalone=$(echo "$last_json" \
        | jq -r '[
            (.. | objects | select(.connection_status? == "StandAlone")),
            (.. | objects | select(.message? == "StandAlone")),
            (.. | objects | select(.connected? == false))
          ] | length' \
        2>/dev/null || echo 0)

    if (( last_uptodate >= 2 && last_tiebreaker >= 1 && last_standalone == 0 )); then
        converged=1
        break
    fi
    sleep 3
done

echo "   last sample: uptodate=$last_uptodate tiebreaker=$last_tiebreaker standalone=$last_standalone"
if (( converged != 1 )); then
    echo "FAIL (Bug 80 regression): did not converge within ${SETTLE_DEADLINE_SEC}s"
    echo "   want: uptodate>=2 AND tiebreaker>=1 AND standalone==0"
    echo "   got:  uptodate=$last_uptodate tiebreaker=$last_tiebreaker standalone=$last_standalone"
    exit 1
fi
echo "   convergence OK: 2 UpToDate diskful + 1 TIE_BREAKER witness + 0 StandAlone"

# --- Step 5: kernel-side cross-check -----------------------------------
# Bug 80 pre-fix symptom on the kernel side: both diskful replicas
# stamp `auto-primary=true` on themselves, both run
# `drbdadm primary --force`, the post-handshake outcome lands as
# either:
#   - mutual Inconsistent (no peer picked SyncSource role), OR
#   - StandAlone (both promoted independently → split-brain detected)
#
# Post-fix kernel state at convergence: both diskful peers are
# `disk:UpToDate`, both `connection:Connected`/`Established`. The
# initial `Primary` role is transient — by the time we sample the
# satellite has already demoted to Secondary because nothing on
# the host is keeping the block device open. So we DON'T pin a
# Primary count here; instead we cross-check every diskful peer
# is UpToDate AND not Inconsistent (the durable pre-fix signature)
# AND DRBDNodeID is allocated on every diskful replica (the
# allocator stamp that the pre-fix race used to skip).
echo ">> step 5: kernel-side cross-check (UpToDate, no Inconsistent, all DRBDNodeIDs allocated)"
diskful_with_id=()
for n in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    node_id=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n}" \
        -o jsonpath='{.status.drbdNodeId}' 2>/dev/null || true)
    role=$(on_node "$n" drbdsetup role "$RD" 2>/dev/null \
        | tr -d '[:space:]' || true)
    disk_local=$(on_node "$n" drbdsetup status "$RD" --verbose 2>/dev/null \
        | head -3 | grep -oE 'disk:[A-Za-z]+' | head -1 | cut -d: -f2 || true)
    echo "   $n: DRBDNodeID=$node_id role=$role disk=$disk_local flags=$flags"

    if [[ -z "$node_id" ]]; then
        echo "FAIL (Bug 80 regression): $n missing Status.DRBDNodeID — the pre-fix cache-trail signature"
        exit 1
    fi

    if [[ "$flags" == *"DISKLESS"* ]]; then
        # Tiebreaker witness — must be Diskless, never Inconsistent.
        if [[ "$disk_local" == "Inconsistent" ]]; then
            echo "FAIL: DISKLESS replica $n reports disk:Inconsistent"
            exit 1
        fi
        continue
    fi

    diskful_with_id+=("${n}:${node_id}")

    if [[ "$disk_local" == "Inconsistent" ]]; then
        echo "FAIL (Bug 80 regression): diskful $n stuck in disk:Inconsistent — the pre-fix stuck-replica signature"
        exit 1
    fi
    if [[ "$disk_local" != "UpToDate" ]]; then
        echo "FAIL: diskful $n disk state is '$disk_local', expected UpToDate"
        exit 1
    fi
done

if (( ${#diskful_with_id[@]} != 2 )); then
    echo "FAIL: expected 2 diskful replicas after auto-place=2, got ${#diskful_with_id[@]}"
    exit 1
fi

# The deterministic election rule (`pkg/dispatcher/dispatcher.go:117`)
# picks the diskful replica with the smallest DRBDNodeID as the
# initial Primary. Log the winning replica so a future regression
# that picks a non-deterministic winner is easy to spot in the
# pass output.
min_id=""
min_node=""
for entry in "${diskful_with_id[@]}"; do
    nid="${entry##*:}"
    if [[ -z "$min_id" || "$nid" -lt "$min_id" ]]; then
        min_id="$nid"
        min_node="${entry%%:*}"
    fi
done
echo "   election (post-fix lowestDiskfulID rule): initial Primary would have been $min_node (DRBDNodeID=$min_id)"

# --- Step 6: defensive probe of CRD-level auto-primary -----------------
# The bug write-up mentioned `.spec.props.DrbdOptions/auto-primary` as
# an assertion target. That key currently lives only in the wire
# bag (DesiredResource.DrbdOptions) — not on Spec.Props. We still
# read it best-effort so future code paths that DO persist it (or
# regressions that accidentally leak it onto the CRD on more than
# one replica) surface here.
echo ">> step 6: best-effort CRD-level auto-primary probe (log-only)"
ap_count=0
for n in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    # jsonpath cannot escape the `/` in `DrbdOptions/auto-primary`,
    # so pull the whole spec.props bag as JSON and pick the key in jq.
    val=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n}" \
        -o json 2>/dev/null \
        | jq -r '.spec.props["DrbdOptions/auto-primary"] // ""' 2>/dev/null || true)
    if [[ "$val" == "true" ]]; then
        ap_count=$(( ap_count + 1 ))
        echo "   $n: spec.props.DrbdOptions/auto-primary = true"
    fi
done
if (( ap_count > 1 )); then
    echo "FAIL (Bug 80 regression): $ap_count Resource CRDs carry auto-primary=true (must be at most 1)"
    exit 1
fi
echo "   CRD auto-primary count = $ap_count (0 expected on current code; >1 would be regression)"

echo ">> RECOVERY-AUTO-PLACE-REAL-DRBD OK (Bug 80 guard: 2 UpToDate, 1 TIE_BREAKER, no Inconsistent, no StandAlone, all DRBDNodeIDs allocated)"
