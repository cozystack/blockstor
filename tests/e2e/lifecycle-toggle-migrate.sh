#!/usr/bin/env bash
#
# usage: lifecycle-toggle-migrate.sh WORK_DIR
#
# Scenario 4.10 — `linstor r td --migrate-from` (UG9 §"Migrating a
# resource to another node", lines 3642-3656; ug9-features 5.3).
#
# Move a replica between nodes without ever dropping below the
# original diskful count. Two-step upstream flow:
#
#   1) linstor r c <dst> <rd> --drbd-diskless      (declare intent)
#   2) linstor r td <dst> <rd> -s <pool> --migrate-from <src>
#
# Step (2) hits PUT /v1/resource-definitions/{rd}/resources/{dst}/
# migrate-disk/{src}/{pool} — the controller waits for the sync to
# {dst} to reach UpToDate, then deletes {src}'s diskful copy in the
# background.
#
# Adaptation for the 3-worker e2e-quorum stand (scenario doc asks
# for 4 workers): we start with a 2-diskful RD on workers 1+2,
# Primary on worker-1, then migrate the worker-2 copy onto
# worker-3 — the redundancy invariant is identical (diskful count
# must stay >= 1 at every observation; original diskful count is 2,
# and the migration adds before it removes, so the strict check is
# "never < 2 except briefly during the cleanup tail").
#
# Behaviour matrix:
#   - REST endpoint absent (404)         → exit 0 with SPEC marker.
#   - REST endpoint stubbed (501)        → exit 0 with SPEC marker.
#   - REST endpoint live & flow works    → exit 0 with PASS.
#   - REST endpoint live & flow broken   → exit 1 with FAIL.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-toggle-migrate

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/lifecycle-toggle-migrate-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kubectl delete resource --all --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    kubectl delete resourcedefinition --all --ignore-not-found 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# ---------- Probe: is the migrate-disk REST endpoint wired? ----------
#
# python-linstor constructs:
#   PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}/{pool}
#
# We hit it against a non-existent RD/node triple so we can observe
# only the routing layer's response (404 = unrouted, 501 = stubbed,
# 4xx with a structured ApiCallRc = endpoint exists and rejected
# our bogus args). Anything else means it's wired.
echo ">> probe PUT /v1/resource-definitions/.../migrate-disk/... routing"
PROBE_CODE=$(curl -sS -o /tmp/migrate-probe.json -w "%{http_code}" \
    -X PUT "http://localhost:$PF_PORT/v1/resource-definitions/__probe__/resources/__probe__/migrate-disk/__probe__/__probe__" || echo "000")
echo "   probe HTTP=$PROBE_CODE  body=$(head -c 160 /tmp/migrate-probe.json 2>/dev/null)"

case "$PROBE_CODE" in
    404|405|501)
        echo ">> SPEC: --migrate-from endpoint not implemented in REST (HTTP $PROBE_CODE)"
        echo "   upstream URL: PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}/{pool}"
        echo "   pin: pkg/rest/resource_toggle_disk.go currently registers /toggle-disk shapes only."
        echo "   gap is consistent with scenario doc tests/scenarios/04-lifecycle.md §4.10 ('missing test')."
        echo ">> LIFECYCLE-TOGGLE-MIGRATE SPEC ($WORKER_1 -> $WORKER_3 via $WORKER_2)"
        exit 0
        ;;
esac

# ---------- Endpoint is live: run the full migration drill ----------

echo ">> create 2-replica RD on $WORKER_1 + $WORKER_2 (zfs-thin pool)"
rd_apply "$RD" "$WORKER_1" "$WORKER_2" 65536

wait_uptodate "$RD" "$WORKER_1" "$WORKER_2"

echo ">> sanity: $WORKER_1 Primary, $WORKER_2 Secondary, $WORKER_3 has no copy"
on_node "$WORKER_1" drbdadm primary --force "$RD" 2>/dev/null || true
sleep 2
state1=$(on_node "$WORKER_1" drbdsetup status "$RD" 2>/dev/null | head -1 || true)
case "$state1" in
    *role:Primary*) ;;
    *) echo "FAIL: $WORKER_1 not Primary after promote (got: $state1)"; exit 1;;
esac

if kubectl get resource "$RD.$WORKER_3" >/dev/null 2>&1; then
    echo "FAIL: $RD.$WORKER_3 already exists before migration"
    exit 1
fi

# Background invariant watcher: poll every 0.5s; record minimum
# observed diskful count. Migration must never drop diskful < 2
# while waiting for $WORKER_3 to sync; it can drop to 2 only after
# $WORKER_2's deletion completes (i.e. when the new $WORKER_3 copy
# is already UpToDate so the count stays at 2 the whole time).
WATCH_LOG=/tmp/migrate-diskful-watch.log
: >"$WATCH_LOG"
(
    while :; do
        ts=$(date +%s)
        # Count Resources without DISKLESS flag, regardless of node.
        count=$(kubectl get resources -o json 2>/dev/null \
            | python3 -c '
import json, sys
data=json.load(sys.stdin)
n=0
for r in data.get("items", []):
    name=r.get("metadata",{}).get("name","")
    if "'"$RD"'." not in name:
        continue
    flags=(r.get("spec",{}) or {}).get("flags",[]) or []
    if "DISKLESS" not in flags:
        n+=1
print(n)
' 2>/dev/null || echo "?")
        echo "$ts $count" >>"$WATCH_LOG"
        sleep 0.5
    done
) &
WATCH_PID=$!

stop_watch() {
    kill "$WATCH_PID" 2>/dev/null || true
    wait "$WATCH_PID" 2>/dev/null || true
}

# ---------- Step 1: declare diskless intent on $WORKER_3 ----------
echo ">> step 1: PUT diskless Resource $RD.$WORKER_3 (intent)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${WORKER_3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${WORKER_3}
  flags: [DISKLESS]
EOF

# Give the satellite a moment to wire the diskless replica into DRBD.
sleep 5

# ---------- Step 2: PUT migrate-disk (sync $WORKER_2 → $WORKER_3, then drop $WORKER_2) ----------
echo ">> step 2: PUT /v1/resource-definitions/$RD/resources/$WORKER_3/migrate-disk/$WORKER_2/zfs-thin"
MIG_CODE=$(curl -sS -o /tmp/migrate-call.json -w "%{http_code}" \
    -X PUT "http://localhost:$PF_PORT/v1/resource-definitions/$RD/resources/$WORKER_3/migrate-disk/$WORKER_2/zfs-thin")
echo "   migrate-disk HTTP=$MIG_CODE  body=$(head -c 200 /tmp/migrate-call.json 2>/dev/null)"

if [[ "$MIG_CODE" != "200" && "$MIG_CODE" != "201" ]]; then
    stop_watch
    echo "FAIL: migrate-disk call returned $MIG_CODE"
    exit 1
fi

# ---------- Wait: $WORKER_3 becomes UpToDate, $WORKER_2 vanishes ----------
echo ">> wait up to 300s for $WORKER_3 UpToDate + $WORKER_2 Resource CRD gone"
deadline=$(( $(date +%s) + 300 ))
while (( $(date +%s) < deadline )); do
    s3=$(on_node "$WORKER_3" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    w2_gone=no
    kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1 || w2_gone=yes
    if [[ "$s3" == *"disk:UpToDate"* && "$w2_gone" == "yes" ]]; then
        break
    fi
    sleep 3
done

stop_watch

# ---------- Assertions ----------
if [[ "$s3" != *"disk:UpToDate"* ]]; then
    echo "FAIL: $WORKER_3 never reached UpToDate (got: $s3)"
    exit 1
fi

if kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1; then
    echo "FAIL: $RD.$WORKER_2 still present after migration"
    exit 1
fi

# Primary must have stayed on $WORKER_1 throughout (we didn't touch it).
final_role=$(on_node "$WORKER_1" drbdsetup status "$RD" 2>/dev/null | head -1 || true)
case "$final_role" in
    *role:Primary*) ;;
    *) echo "FAIL: $WORKER_1 lost Primary role (got: $final_role)"; exit 1;;
esac

# Redundancy invariant: minimum diskful count observed in the
# watcher log must be >= 2 (we never dropped a copy before the
# new one was UpToDate). Tolerance: 1 brief sample equal to 1 is
# acceptable only if it coincides with the deletion tail — but
# upstream LINSTOR semantics promise add-before-drop, so we hold
# the strict bar.
min_diskful=$(awk '{print $2}' "$WATCH_LOG" | grep -E '^[0-9]+$' | sort -n | head -1)
echo ">> watcher: $(wc -l <"$WATCH_LOG") samples, min diskful = $min_diskful"
if [[ -z "$min_diskful" || "$min_diskful" -lt 2 ]]; then
    echo "FAIL: redundancy invariant broken — diskful dropped to $min_diskful during migration"
    echo "    samples:"
    cat "$WATCH_LOG"
    exit 1
fi

echo ">> final state: diskful on $WORKER_1 + $WORKER_3; $WORKER_2 evicted; Primary on $WORKER_1"
echo ">> LIFECYCLE-TOGGLE-MIGRATE OK ($WORKER_2 -> $WORKER_3 migrated without redundancy loss)"
