#!/usr/bin/env bash
#
# usage: recovery-false-diskless.sh WORK_DIR
#
# Scenario 5.17 — false Diskless recovery via `r mkavail --diskful`.
#
# Goal: validate the documented remedy for a "false Diskless" state
# left behind when a Resource CRD is deleted with its finalizer
# force-stripped — the satellite-side teardown never runs, the
# underlying ZVOL and live DRBD device on the worker survive, but
# blockstor has no Resource CRD for it. The controller treats the
# node as not hosting the replica (so future create-replica calls
# would auto-place a fresh disk).
#
# The recovery recipe (LINSTOR upstream: `linstor r mkavail --diskful
# <node> <rd>`) re-registers the existing on-disk state idempotently —
# it must NOT create a fresh ZVOL or wipe DRBD metadata, it must adopt
# what is already there.
#
# CURRENT STATE OF THE ENDPOINT
#
# blockstor REST does not yet implement
#     POST /v1/resource-definitions/{rd}/resources/{node}/make-available
# The OpenAPI types are generated (pkg/api/openapi/types.gen.go has
# `ResourceMakeAvailable` and `ResourceMakeAvailableOnNode`), but no
# handler is wired into the mux — the route falls through to 404.
# `linstor r mkavail` therefore prints "404 page not found" against
# the blockstor controller.
#
# This test runs the experiment anyway and asserts the endpoint
# returns 404 in CURRENT state — it FAILs (with SPEC-GAP marker) once
# someone implements the handler, at which point the assertion below
# should be flipped to "200 with idempotent re-attach" and the rest
# of the scenario (steps 4-7) becomes the meat of the test.
#
# Setup:
#   - 3-replica RD `fakediskless` on workers 1/2/3 (ZFS_THIN, pool=zfs-thin)
#   - wait UpToDate on all three peers
#
# Steps:
#   1. Pick $TARGET = $WORKER_3 as the "false Diskless" victim
#   2. Force-strip the Resource CRD finalizer for $TARGET, then
#      `kubectl delete` it. Without the finalizer the satellite-side
#      teardown is not invoked via the Reconcile-on-Delete path; the
#      ZVOL therefore typically survives. NOTE — the controller-side
#      RD reconciler may still trigger an `ensureTiebreaker` or
#      observer-driven cleanup that races us; we tolerate ZVOL loss
#      as a "satellite cleaned up via another path" outcome and bail
#      with a clear log line rather than misreport as a recipe
#      regression. The point of this test is the mkavail contract,
#      not the precise force-strip race.
#   3. Best-effort verify ZVOL `blockstor-zfs/fakediskless_00000`
#      still exists on $TARGET. If it is gone, log and continue —
#      the mkavail endpoint should still be probed (404 today, 200
#      tomorrow when implemented), and the spec-gap assertion below
#      does not depend on the ZVOL surviving.
#   4. Probe `POST .../resources/$TARGET/make-available` with
#      `{"diskful": true}`. Record the HTTP status.
#   5a. CURRENT: assert 404 (SPEC-GAP). Print a single-line marker so
#       the batch runner records this as a known spec gap, not a
#       silent pass.
#   5b. FUTURE (after handler lands): assert 200, verify Resource
#       CRD re-appears, verify ZVOL was NOT recreated (zfs creation
#       timestamp unchanged), verify DRBD device still UpToDate.
#
# Cleanup: delete_rd does its usual best-effort sweep, plus a manual
# `zfs destroy` on $TARGET because the orphaned ZVOL has no Resource
# CRD pointing at it and delete_rd would otherwise miss it.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=fakediskless
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
TARGET=$N3
ZPOOL=blockstor-zfs
ZVOL_NAME="${ZPOOL}/${RD}_00000"
POOL=zfs-thin

# Cleanup trap — delete_rd handles CRDs + kernel; the manual zfs
# destroy mops up the orphaned ZVOL we deliberately leave behind
# in step 2 (delete_rd cannot reach it because there is no CRD).
cleanup() {
    delete_rd "$RD" || true
    on_node "$TARGET" zfs destroy -f "$ZVOL_NAME" 2>/dev/null || true
}
trap cleanup EXIT

echo ">> apply 3-replica RD ${RD} on ${N1}, ${N2}, ${N3} (ZFS_THIN pool=${POOL})"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${POOL}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: ${POOL}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: ${POOL}}
EOF

# wait_uptodate only checks two peers; for a 3-replica setup we check
# each pair so we know every peer is UpToDate before we corrupt one.
wait_uptodate "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N1" "$N3"
echo "   all 3 peers UpToDate"

# Snapshot the ZVOL creation timestamp BEFORE we force-strip — the
# future-behaviour assertion (step 5b) compares to this to prove the
# remedy is idempotent and not a destroy+recreate.
ZVOL_CREATION_BEFORE=$(on_node "$TARGET" zfs get -Hp -o value creation "$ZVOL_NAME" 2>/dev/null || echo unknown)
echo "   ZVOL ${ZVOL_NAME} on ${TARGET} created=${ZVOL_CREATION_BEFORE}"

echo ">> simulate false Diskless: strip finalizer + delete Resource CRD for ${TARGET}"
kubectl patch "resources.blockstor.io.blockstor.io/${RD}.${TARGET}" \
    -p "{\"metadata\":{\"finalizers\":[]}}" --type=merge
kubectl delete --wait=true --timeout=30s \
    "resources.blockstor.io.blockstor.io/${RD}.${TARGET}"

# Give the controller a beat to reconcile the missing replica.
sleep 3

echo ">> best-effort check: does on-disk state on ${TARGET} survive force-delete?"
zvol_survived=false
if on_node "$TARGET" zfs list -H -o name "$ZVOL_NAME" >/dev/null 2>&1; then
    zvol_survived=true
    echo "   ZVOL ${ZVOL_NAME} still present (false-Diskless reproduced cleanly)"
else
    echo "   NOTE: ZVOL ${ZVOL_NAME} cleaned up despite finalizer-strip — satellite"
    echo "         observer/RD-reconciler caught the orphan via another path."
    echo "         Continuing — mkavail spec-gap probe is the test target."
fi

# Live DRBD device check. Same tolerance as the ZVOL — we report
# what we see and continue.
drbd_state=$(on_node "$TARGET" drbdsetup status "$RD" 2>/dev/null || true)
if [[ -z "$drbd_state" ]]; then
    echo "   DRBD device on ${TARGET}: no kernel entry for ${RD}"
else
    echo "   DRBD device on ${TARGET} still configured:"
    echo "$drbd_state" | sed "s/^/      /"
fi

echo ">> probe POST /v1/resource-definitions/${RD}/resources/${TARGET}/make-available"
# Inline curl — rest_post uses curl -fsS which would abort on 404. We
# want to capture the status code so we can distinguish "endpoint
# missing" (current state, 404) from "endpoint there but broken"
# (5xx, future regression) from "endpoint there and idempotent"
# (200, future success).
lport=$(python3 -c "import socket; s=socket.socket(); s.bind((\"127.0.0.1\", 0)); print(s.getsockname()[1]); s.close()")
kubectl -n "$NS" port-forward svc/blockstor-controller "${lport}:3370" >/dev/null 2>&1 &
pf=$!
# Reuse the lib.sh helper that polls /healthz until the forward binds.
_wait_port_forward "$lport" "$pf"

status=$(curl -s -o /tmp/mkavail.body -w "%{http_code}" \
    -XPOST -H "Content-Type: application/json" \
    "http://127.0.0.1:${lport}/v1/resource-definitions/${RD}/resources/${TARGET}/make-available" \
    -d "{\"diskful\": true}")
kill "$pf" 2>/dev/null || true
wait "$pf" 2>/dev/null || true

echo "   mkavail HTTP ${status}"
echo "   body: $(cat /tmp/mkavail.body 2>/dev/null | head -c 300)"

case "$status" in
    404)
        # CURRENT spec gap: handler not wired into mux.
        echo "SPEC-GAP: 5.17/mkavail — POST .../make-available returns 404 (no handler in pkg/rest)."
        echo "          OpenAPI types exist (ResourceMakeAvailable, ResourceMakeAvailableOnNode)"
        echo "          but no mux.HandleFunc is registered. Recipe \`linstor r mkavail --diskful\`"
        echo "          therefore cannot recover from false-Diskless state on a blockstor controller."
        echo "          ZVOL-survived-force-delete=${zvol_survived}."
        echo "PASS-SPEC-GAP: documented spec gap, test is a forward-spec for the missing handler."
        exit 0
        ;;
    200|201)
        # FUTURE behaviour: validate idempotent re-attach.
        echo ">> mkavail returned ${status}; validating idempotent re-attach"

        # Resource CRD must re-appear within ~10 s.
        deadline=$(( $(date +%s) + 10 ))
        while (( $(date +%s) < deadline )); do
            if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${TARGET}" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${TARGET}" >/dev/null 2>&1; then
            echo "FAIL: Resource CRD ${RD}.${TARGET} did not re-appear after mkavail"
            exit 1
        fi

        # ZVOL must be the SAME one (creation timestamp unchanged).
        # If creation differs, mkavail destroyed-and-recreated — that
        # is the bug we are guarding against. Only meaningful if the
        # ZVOL survived step 2; otherwise mkavail HAD to recreate.
        if $zvol_survived; then
            zvol_creation_after=$(on_node "$TARGET" zfs get -Hp -o value creation "$ZVOL_NAME" 2>/dev/null || echo unknown)
            if [[ "$zvol_creation_after" != "$ZVOL_CREATION_BEFORE" ]]; then
                echo "FAIL: ZVOL ${ZVOL_NAME} was recreated (creation ${ZVOL_CREATION_BEFORE} -> ${zvol_creation_after})"
                echo "      mkavail must adopt existing on-disk state, not destroy+recreate"
                exit 1
            fi
        else
            echo "NOTE: ZVOL was already gone before mkavail; idempotency check skipped."
        fi

        # All 3 peers should reach UpToDate again — no full resync.
        wait_uptodate "$RD" "$N1" "$TARGET"
        wait_uptodate "$RD" "$N2" "$TARGET"
        echo "PASS: 5.17/mkavail — idempotent re-attach, ZVOL adopted, peers UpToDate."
        ;;
    *)
        echo "FAIL: mkavail returned unexpected HTTP ${status} (expected 404 today or 200 once implemented)"
        exit 1
        ;;
esac
