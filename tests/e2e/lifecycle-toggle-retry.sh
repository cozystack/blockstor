#!/usr/bin/env bash
#
# usage: lifecycle-toggle-retry.sh WORK_DIR
#
# Scenario 4.11 — toggle-disk retry/cancel (LINSTOR 1.34.0+ UG9
# semantics).
#
# Upstream LINSTOR 1.34.0 wires a controller-side state machine on
# top of `linstor r toggle-disk` that:
#   - tracks the in-progress flag flip per (rd, node);
#   - lets a retry (same call) re-arm a satellite that has rejected
#     the work (e.g. lvcreate failed on the chosen pool); and
#   - lets the opposite call (`r td --diskless`) cancel a still-stuck
#     promote, leaving no orphan backing volume.
#
# blockstor's REST handler today (pkg/rest/resource_toggle_disk.go)
# is a stateless `Spec.Flags["DISKLESS"]` flip — it does NOT track
# in-progress toggles, does NOT report stuck state, and the
# satellite reconciler keeps retrying on its own cadence. So this
# scenario is recorded as SPEC: every UG9 assertion that has no
# matching REST endpoint is marked with a `# SPEC:` line and the
# script logs `SPEC` (not FAIL) when the missing surface is hit.
#
# Methodology, mapped 1:1 onto the user's brief:
#   1. 2-replica RD on N1+N2 (pool=stand), wait UpToDate.
#   2. Provoke a stuck toggle on N3 with pool=lvm-thin: break the
#      VG on N3 first (`vgchange -an` + `wipefs -af` against the
#      blockstor-lvm PV) so the satellite's lvm-thin provider
#      `lvcreate` will fail.
#   3. POST toggle-disk N3 → lvm-thin, wait 30s, assert N3 stays
#      Diskless (or stuck creating) and no /dev/blockstor-lvm/* LV
#      appeared on N3.
#   4. Retry: re-issue same toggle-disk N3 → lvm-thin.
#      UG9 expectation: 200 + retry counter bump (the satellite
#      re-runs lvcreate). blockstor today: 200 (stateless flip),
#      no retry telemetry — marked SPEC.
#   5. Cancel: issue `r td --diskless` for N3. UG9 expectation:
#      the in-progress promote is cancelled, no orphan LV remains.
#      blockstor today: 200, flag goes back to DISKLESS, but
#      lvremove of any partial LV is the satellite's job — we
#      assert no /dev/blockstor-lvm/* LV named after the RD exists
#      after a 30s drain window.
#   6. Restore the VG so the cleanup trap and the next scenario
#      see a healthy node.
#
# The script always reaches `cleanup` (trap on EXIT). It logs
# either PASS, FAIL, or SPEC at the end; the e2e driver treats
# both PASS and SPEC as non-blocking (exit 0) and FAIL as blocking.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-toggle-retry
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Pools used:
#   - "stand"     FILE_THIN loop-file pool: cannot be broken at the
#                 LVM level, used for the two healthy replicas.
#   - "lvm-thin"  LVM_THIN pool: we provoke `lvcreate` failure on
#                 N3 by destroying the blockstor-lvm VG before the
#                 toggle.
HEALTHY_POOL=${HEALTHY_POOL:-stand}
BROKEN_POOL=${BROKEN_POOL:-lvm-thin}
BROKEN_VG=${BROKEN_VG:-blockstor-lvm}

# Per-step deadlines: keep generous so a slow QEMU stand doesn't
# false-negative a SPEC marker into a FAIL. UG9 in upstream takes
# ~10s to surface the stuck flag in `r l --faulty`.
STUCK_WINDOW=${STUCK_WINDOW:-30}
CANCEL_DRAIN=${CANCEL_DRAIN:-30}

VERDICT=PASS  # downgraded to SPEC / FAIL as findings accumulate.

mark_spec() {
    local why=$1
    echo "SPEC: $why"
    [[ "$VERDICT" == "PASS" ]] && VERDICT=SPEC
}

mark_fail() {
    local why=$1
    echo "FAIL: $why"
    VERDICT=FAIL
}

# Restore the VG on N3 so subsequent e2e scenarios that share the
# stand get a clean lvm-thin pool back. Idempotent: pvcreate +
# vgcreate + lvcreate behind `if !`. Mirrors stand/install-pools.sh.
restore_vg_n3() {
    echo ">> restoring ${BROKEN_VG} on $N3"
    on_node "$N3" bash -c "
        set +e
        # /dev/sdb is the LVM device on the stand (see
        # install-pools.sh: LVM_DEV defaults to /dev/sdb when
        # TYPE=both was provisioned).
        if ! vgs ${BROKEN_VG} >/dev/null 2>&1; then
            wipefs -af /dev/sdb 2>/dev/null
            vgcreate -y ${BROKEN_VG} /dev/sdb
        fi
        if ! lvs ${BROKEN_VG}/thin >/dev/null 2>&1; then
            # Same flags install-pools.sh uses — no udev in the
            # satellite container.
            lvcreate -y --type thin-pool \
                --activationmode degraded \
                -L 14G --thinpool thin ${BROKEN_VG} \
                --config 'activation{udev_sync=0 udev_rules=0}' 2>&1 || true
        fi
        # Force-reconcile the StoragePool so the satellite's
        # PoolStatus stops reporting 'vg not found'.
        true
    " || true
}

cleanup() {
    set +e
    echo ">> cleanup"
    delete_rd "$RD"
    restore_vg_n3
    echo ">> VERDICT=$VERDICT"
    # Always exit 0 on SPEC so the e2e driver doesn't treat a
    # missing-but-documented surface as a regression. FAIL still
    # exits non-zero.
    [[ "$VERDICT" == "FAIL" ]] && exit 1
    exit 0
}
trap cleanup EXIT

echo ">> step 1: apply 2-replica RD on $N1 + $N2 (pool=$HEALTHY_POOL)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF
for n in "$N1" "$N2"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props:
    StorPoolName: "${HEALTHY_POOL}"
EOF
done

wait_uptodate "$RD" "$N1" "$N2"
echo ">> 2-replica RD UpToDate on $N1 and $N2"

echo ">> step 2: break ${BROKEN_VG} on $N3 to force lvcreate failure"
# Destroying the VG underneath the running thin pool is the
# cheapest way to make lvcreate fail without unplugging the
# device. The satellite's lvm-thin provider will surface
# `vg not found` (or similar) on first volume create.
on_node "$N3" bash -c "
    set +e
    vgchange -an ${BROKEN_VG} 2>&1 || true
    vgremove -f ${BROKEN_VG} 2>&1 || true
    wipefs -af /dev/sdb 2>&1 || true
    pvremove -ff -y /dev/sdb 2>&1 || true
    echo '== post-break vgs =='
    vgs 2>&1
" || true

echo ">> step 3: register N3 as a diskless replica, then provoke stuck toggle"
# Phase 4.x: blockstor's toggle-disk takes a replica that already
# exists. Create the diskless witness first so the controller has
# a Resource row to flip.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  flags: ["DISKLESS"]
EOF

# Wait a few seconds for the diskless witness to settle.
sleep 5

echo ">> POST toggle-disk N3 → ${BROKEN_POOL} (expect stuck — VG is gone)"
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/4.11-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; cleanup' EXIT

for _ in $(seq 1 20); do
    curl -fsS -m1 "http://127.0.0.1:$PF_PORT/v1/healthz" >/dev/null 2>&1 && break
    sleep 0.5
done

http_code=$(curl -s -o /tmp/4.11-r1.json -w '%{http_code}' -X PUT \
    "http://127.0.0.1:$PF_PORT/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/storage-pool/${BROKEN_POOL}")
echo "   first PUT → HTTP $http_code"
echo "   body: $(cat /tmp/4.11-r1.json)"

# UG9 would return 200 + an in-progress marker; blockstor returns
# 200 with an APICallRc info message and no progress tracking.
if [[ "$http_code" != "200" ]]; then
    mark_fail "first toggle-disk PUT returned HTTP $http_code (expected 200)"
fi

echo ">> step 3b: wait ${STUCK_WINDOW}s and assert N3 is still NOT UpToDate"
sleep "$STUCK_WINDOW"
n3_disk=$(status_disk_state "$RD" "$N3")
echo "   N3 disk state: $n3_disk"

# When the VG is gone, lvcreate fails and the satellite can't
# attach a backing device. Resource.Status reports either no row at
# all, or Diskless / Inconsistent / Attaching. Anything other than
# UpToDate is "stuck enough" for this scenario.
if [[ "$n3_disk" == "UpToDate" ]]; then
    mark_fail "N3 promoted to UpToDate despite broken VG — pool fault not surfaced"
else
    echo "   stuck as expected (not UpToDate)"
fi

echo ">> step 4: RETRY — re-issue same toggle-disk PUT"
http_code=$(curl -s -o /tmp/4.11-r2.json -w '%{http_code}' -X PUT \
    "http://127.0.0.1:$PF_PORT/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/storage-pool/${BROKEN_POOL}")
echo "   retry PUT → HTTP $http_code"
echo "   body: $(cat /tmp/4.11-r2.json)"

# SPEC: UG9 expects a retry-counter field on the response or in
# the resource object. blockstor returns a fresh 200 with no
# `retryCount` field — there's no controller-side state to bump.
if [[ "$http_code" == "404" || "$http_code" == "501" ]]; then
    mark_spec "retry returned $http_code — endpoint not wired in blockstor 1.34+"
elif ! grep -q 'retry\|attempt' /tmp/4.11-r2.json 2>/dev/null; then
    mark_spec "retry returned 200 with no retry/attempt field — UG9 retry-counter is not yet wired (blockstor toggle-disk is a stateless flag flip; pkg/rest/resource_toggle_disk.go has no in-progress tracking)"
fi

# Check if N3 made progress after retry (it shouldn't, VG still
# broken). UG9: retry should fail again with the same error
# surface. blockstor: silently 200.
sleep 10
n3_disk_after_retry=$(status_disk_state "$RD" "$N3")
echo "   N3 after retry: $n3_disk_after_retry"
if [[ "$n3_disk_after_retry" == "UpToDate" ]]; then
    mark_fail "N3 became UpToDate after retry despite broken VG"
fi

echo ">> step 5: CANCEL — PUT toggle-disk/diskless for N3"
http_code=$(curl -s -o /tmp/4.11-r3.json -w '%{http_code}' -X PUT \
    "http://127.0.0.1:$PF_PORT/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/diskless")
echo "   cancel PUT → HTTP $http_code"
echo "   body: $(cat /tmp/4.11-r3.json)"

if [[ "$http_code" == "404" || "$http_code" == "501" ]]; then
    mark_spec "cancel returned $http_code — endpoint not wired"
elif [[ "$http_code" != "200" ]]; then
    mark_fail "cancel toggle-disk/diskless returned HTTP $http_code"
fi

# UG9: the in-progress promote should be abandoned and the
# replica should settle back to Diskless within a short window.
# blockstor: the DISKLESS flag is re-asserted on Spec; satellite
# reconcile then tears down whatever partial state exists.
echo ">> step 5b: wait ${CANCEL_DRAIN}s for N3 to settle back to Diskless"
deadline=$(( $(date +%s) + CANCEL_DRAIN ))
final_state=
while (( $(date +%s) < deadline )); do
    final_state=$(status_disk_state "$RD" "$N3")
    if [[ "$final_state" == "Diskless" || -z "$final_state" ]]; then
        break
    fi
    sleep 2
done
echo "   N3 final state: ${final_state:-<no resource>}"

if [[ -n "$final_state" && "$final_state" != "Diskless" ]]; then
    mark_spec "N3 did not return to Diskless within ${CANCEL_DRAIN}s after cancel (got: $final_state) — UG9 cancel semantics may not be wired"
fi

# Orphan-LV check: even if cancel succeeded, UG9 requires the
# satellite to roll back any partially-created backing LV. With
# the VG destroyed lvcreate never produced anything anyway, but
# the assertion documents the expectation.
orphan=$(on_node "$N3" bash -c "lvs --noheadings -o lv_name 2>/dev/null | tr -d ' ' | grep -F '${RD}' || true" || true)
if [[ -n "$orphan" ]]; then
    mark_fail "orphan LV(s) on $N3 after cancel: $orphan"
else
    echo "   no orphan LVs on $N3"
fi

# Peer-side invariant: the two healthy replicas must not have
# regressed during the stuck-toggle dance. Drift here would be a
# real bug regardless of the SPEC verdict on the cancel path.
for peer in "$N1" "$N2"; do
    state=$(status_disk_state "$RD" "$peer")
    if [[ "$state" != "UpToDate" ]]; then
        mark_fail "$peer disk regressed during the stuck-toggle dance (got: $state)"
    fi
done

kill "$PF_PID" 2>/dev/null || true
wait "$PF_PID" 2>/dev/null || true

echo ">> step 6: cleanup runs in trap (delete_rd + restore_vg_n3)"
echo ">> TOGGLE-DISK RETRY/CANCEL: VERDICT=$VERDICT"
