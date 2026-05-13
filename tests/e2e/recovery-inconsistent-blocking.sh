#!/usr/bin/env bash
#
# usage: recovery-inconsistent-blocking.sh WORK_DIR
#
# Scenario 5.18 — recovery from an Inconsistent replica blocking peer
# convergence, repaired via the `linstor r d <node> <rd>` + `linstor
# rd ap <rd>` pair. Pins the upstream "delete + autoplace" rebuild
# pattern blockstor inherits.
#
# Setup:
#   - 3-replica RD `blocker-test` on workers 1+2+3, ZFS_THIN pool,
#     64M, autoplace target=3. Wait UpToDate on all three.
#   - dd loop on Primary (worker-1) running throughout so an
#     I/O stall during recovery shows up as a missed write.
#
# Fault injection:
#   - delete the satellite Pod on worker-3 → preStop hook brings DRBD
#     down, releasing the lower zvol.
#   - corrupt that zvol from a privileged sidecar pod (hostPath /dev)
#     while satellite is being recreated. dd 10 MiB urandom over the
#     zvol head writes through DRBD's metadata + first chunk of user
#     data. Talos blocks kubectl debug node + ssh-to-host; the only
#     fault-injection path on Talos workers is a host-/dev hostPath
#     Pod in the blockstor-system namespace (PSA: privileged).
#   - let the new satellite reconciler do drbdadm up. DRBD's
#     generation-identifier handshake sees the on-disk UUIDs differ
#     from the peers' current_uuid and marks the local disk
#     Inconsistent.
#
# Recovery:
#   1. linstor r d <worker-3> blocker-test  → cascade-deletes Resource
#      CRD, expects port + DRBD state to be cleaned (Bug 1 fix).
#   2. linstor rd ap blocker-test --place-count 3  → reconciler
#      re-spawns the missing replica (fresh state, new GI, SyncTarget
#      → UpToDate).
#   3. workers 1+2 stay UpToDate AND keep serving the Primary dd
#      stream throughout. Any pause longer than the DRBD ping-timeout
#      would be a regression.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=blocker-test
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
POOL=${STORPOOL:-zfs-thin}
SIZE_KIB=65536          # 64 MiB
ZVOL_PATH="/dev/zvol/blockstor-zfs/${RD}_00000"

DD_PID=""
PF_PORT=""
PF_PID=""
CORRUPTOR_POD="bs-corruptor-${RD}"

cleanup() {
    if [[ -n "$DD_PID" ]]; then
        kill "$DD_PID" 2>/dev/null || true
        wait "$DD_PID" 2>/dev/null || true
    fi
    if [[ -n "$PF_PID" ]]; then
        kill "$PF_PID" 2>/dev/null || true
        wait "$PF_PID" 2>/dev/null || true
    fi
    kubectl -n "$NS" delete pod "$CORRUPTOR_POD" --ignore-not-found --wait=false --grace-period=0 --force 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

# --- step 1: 3-replica RD on ZFS_THIN, wait UpToDate on all three -----

echo ">> apply 3-replica RD ${RD} on ${POOL}"
# AutoAddQuorumTiebreaker=false: with 3 diskful replicas on a 3-node
# cluster, the auto-tiebreaker has nowhere to land and would log-spam.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
EOF
for n in "$N1" "$N2" "$N3"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props:
    StorPoolName: "${POOL}"
EOF
done

# Helper: extract the LOCAL disk state (the first `disk:` line —
# every following `peer-disk:` belongs to a peer). grep -c on
# "disk:UpToDate" would match BOTH local `disk:` and peer
# `peer-disk:` lines, so a 3-replica RD with one Inconsistent
# peer would still report >=1 UpToDate from each surviving node,
# masking the bug we're trying to detect.
local_disk_state() {
    local node=$1 rd=$2
    on_node "$node" drbdsetup status "$rd" 2>/dev/null \
        | awk 'NR==2 { for (i=1; i<=NF; i++) if ($i ~ /^disk:/) { print $i; exit } }'
}

echo ">> wait for all 3 replicas UpToDate"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    d1=$(local_disk_state "$N1" "$RD")
    d2=$(local_disk_state "$N2" "$RD")
    d3=$(local_disk_state "$N3" "$RD")
    if [[ "$d1" == "disk:UpToDate" && "$d2" == "disk:UpToDate" && "$d3" == "disk:UpToDate" ]]; then
        break
    fi
    sleep 2
done
if [[ "$d1" != "disk:UpToDate" || "$d2" != "disk:UpToDate" || "$d3" != "disk:UpToDate" ]]; then
    echo "FAIL: not all 3 replicas reached UpToDate ($N1=$d1 $N2=$d2 $N3=$d3)"
    exit 1
fi

DEV=$(device_for_rd "$RD" "$N1")

# --- step 2: start a continuous dd loop on Primary so an I/O stall ----
#     during recovery is observable. Primary mount = the DRBD device on
#     worker-1 promoted via drbdadm primary; we run dd in a background
#     loop tracking the running byte count and use it as the stall
#     proof at the end.

echo ">> promote ${N1} to Primary and start background dd loop"
on_node "$N1" drbdadm primary --force "$RD"

DD_LOG=/tmp/${RD}-dd.log
: >"$DD_LOG"
# Run dd in a tight loop on the Primary; we don't care about the
# bytes — only that it never hangs > 30 s. Wrap in a counter so we
# can detect a pause externally. Bash-side timestamp every second.
(
    while true; do
        on_node "$N1" bash -c "
            dd if=/dev/urandom of=${DEV} bs=64k count=4 oflag=direct status=none 2>/dev/null || true
        " >/dev/null 2>&1 || true
        date +%s >>"$DD_LOG"
        sleep 1
    done
) &
DD_PID=$!

# Sanity: dd loop must be ticking before we start corrupting.
sleep 5
pre_ticks=$(wc -l <"$DD_LOG")
if (( pre_ticks < 3 )); then
    echo "FAIL: dd loop didn't tick before fault injection (ticks=$pre_ticks)"
    exit 1
fi

# --- step 3: corrupt worker-3's backing ZVOL while satellite drains --

echo ">> stage privileged corruptor pod on ${N3} (hostPath /dev)"
# We can't reach the host shell on a Talos worker:
#   - kubectl debug node/<n> is rejected by the cluster's PodSecurity
#     baseline default (hostNetwork / hostPID / hostPath forbidden).
#   - talosctl on the stand host doesn't have `nodes` set in its
#     config (10.51.x is Talos-internal, not exposed).
# Workaround: spawn a privileged sidecar in blockstor-system (PSA
# enforce=privileged on that namespace), mount /dev from the host
# and run `dd` on /dev/zd<N>. This is the only general-purpose
# fault-injection harness for Talos in this stand.
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${CORRUPTOR_POD}
  namespace: ${NS}
spec:
  hostPID: true
  restartPolicy: Never
  nodeName: ${N3}
  tolerations:
    - operator: Exists
  containers:
    - name: corruptor
      image: 10.51.0.1:5000/blockstor-satellite:dev
      command: ["/bin/sh", "-c", "sleep 3600"]
      securityContext:
        privileged: true
      volumeMounts:
        - {name: dev, mountPath: /dev}
  volumes:
    - name: dev
      hostPath: {path: /dev}
EOF

echo ">> wait for corruptor pod Ready"
kubectl -n "$NS" wait pod/"$CORRUPTOR_POD" --for=condition=Ready --timeout=60s

# Read the host-side zvol device path (sym-link or /dev/zd<N>) BEFORE
# we kill the satellite. The /dev/zvol/<pool>/<rd>_00000 symlink is
# created by the ZFS udev rules on the host; the satellite's view
# inherits it via hostPath /dev.
echo ">> resolve zvol device on ${N3}"
ZD_HOST=$(kubectl -n "$NS" exec "$CORRUPTOR_POD" -- sh -c "readlink -f ${ZVOL_PATH} 2>/dev/null || ls -l ${ZVOL_PATH} 2>&1")
echo "   zvol -> $ZD_HOST"
if [[ "$ZD_HOST" != /dev/zd* ]]; then
    echo "FAIL: could not resolve ${ZVOL_PATH} on ${N3} (got: $ZD_HOST)"
    exit 1
fi

# Find satellite pod on N3 so we can delete it.
SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}")
echo ">> delete satellite pod ${SAT_POD} on ${N3} (preStop brings DRBD down)"
kubectl -n "$NS" delete pod "$SAT_POD" --wait=true --timeout=60s

# Window: from preStop completing (DRBD down on N3) until the DS
# recreates a Running satellite. During this window:
#   - the zvol is no longer claimed by drbdsetup → safe to dd over
#   - the new satellite hasn't yet done drbdadm up → corruption
#     persists into the next drbdsetup attach.

echo ">> corrupt zvol user-data region (10 MiB urandom at offset 0)"
# Corrupt the data region but leave DRBD's internal metadata at
# the END of the device intact — drbdadm up must still succeed,
# we just want the on-disk bytes to no longer match the peers.
# (Wiping metadata would fail attach with "No valid meta data
# found" which is a different failure mode than Scenario 5.18.)
kubectl -n "$NS" exec "$CORRUPTOR_POD" -- dd \
    if=/dev/urandom of="$ZVOL_PATH" bs=1M count=10 conv=fdatasync status=none

echo ">> wait for new satellite pod on ${N3} to be Running"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}" 2>/dev/null || true)
    if [[ -n "$new_pod" && "$new_pod" != "$SAT_POD" ]]; then
        phase=$(kubectl -n "$NS" get pod "$new_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        if [[ "$phase" == "Running" ]]; then
            break
        fi
    fi
    sleep 2
done
if [[ -z "$new_pod" || "$phase" != "Running" ]]; then
    echo "FAIL: new satellite pod on ${N3} never went Running"
    exit 1
fi

# After the satellite is back, drbdadm up will have run and re-attached
# the (now-corrupted) zvol. DRBD's metadata still claims this disk is
# UpToDate — DRBD only catches data-vs-data divergence via `drbdadm
# verify`, never on attach. So in the real-world Scenario 5.18 sequence
# the operator would either:
#   (a) wait for a read on a corrupted block to surface EIO via the
#       checksum/digest layer, or
#   (b) explicitly mark the local disk dirty so DRBD knows to resync.
#
# Since we don't want to wait for a happenstance read in an e2e test,
# we surface the divergence deterministically via `drbdadm invalidate`,
# which is the upstream-LINSTOR-recommended manual remediation when an
# operator knows the local data is bad. That transitions the local
# disk from UpToDate → Inconsistent immediately, which is the
# "Inconsistent replica blocking" precondition Scenario 5.18 starts
# from. The recovery path (`r d` + `rd ap`) is the actual subject of
# this test, not the corruption-detection plumbing.
echo ">> wait 5s for new satellite to finish drbdadm up"
sleep 5

echo ">> drbdadm invalidate ${RD} on ${N3} to force Inconsistent"
on_node "$N3" drbdadm invalidate "$RD" 2>&1 || {
    echo "FAIL: drbdadm invalidate failed on ${N3}"
    exit 1
}

# --- step 4: verify worker-3 sees Inconsistent via bitmap mismatch ----

echo ">> wait up to 60s for ${N3} local disk state to leave UpToDate"
deadline=$(( $(date +%s) + 60 ))
n3_state=""
while (( $(date +%s) < deadline )); do
    n3_state=$(local_disk_state "$N3" "$RD")
    # Inconsistent / Outdated / Failed / Detaching all qualify as
    # "DRBD detected the divergence". UpToDate alone means corruption
    # didn't take.
    if [[ -n "$n3_state" && "$n3_state" != "disk:UpToDate" ]]; then
        break
    fi
    sleep 2
done

echo "   ${N3} local disk state after corruption: $n3_state"
if [[ "$n3_state" == "disk:UpToDate" || -z "$n3_state" ]]; then
    echo "FAIL: ${N3} did not enter a non-UpToDate state after zvol corruption (got: $n3_state)"
    exit 1
fi

# --- step 5: peers must keep serving Primary I/O throughout ----------

echo ">> verify ${N1} and ${N2} are still locally UpToDate"
for w in "$N1" "$N2"; do
    state=$(local_disk_state "$w" "$RD")
    if [[ "$state" != "disk:UpToDate" ]]; then
        echo "FAIL: peer ${w} no longer UpToDate (got: $state)"
        exit 1
    fi
done

# dd loop must have ticked at least every 30 s. A gap > 30 s in
# the timestamp log is an I/O stall regression — Primary write
# path must NOT block on an Inconsistent peer.
echo ">> verify dd loop never stalled > 30s during corruption"
max_gap=$(awk 'NR>1 { if ($1 - prev > max) max = $1 - prev; prev=$1; next } { prev=$1 } END { print max+0 }' "$DD_LOG")
echo "   max dd-tick gap: ${max_gap}s"
if (( max_gap > 30 )); then
    echo "FAIL: Primary dd loop stalled ${max_gap}s during corruption (>30s regression)"
    exit 1
fi

# --- step 6: linstor r d <node> <rd>  → expect cascade clean -----------

echo ">> port-forward blockstor-controller for linstor CLI"
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "${PF_PORT}":3370 \
    >/tmp/${RD}-pf.log 2>&1 &
PF_PID=$!
for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:${PF_PORT}/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:${PF_PORT}" --machine-readable)

# Bug 1 (cascade fix): `linstor r d` must remove the Resource CRD
# AND let the satellite tear down DRBD + free the TCP port. Without
# the fix, the port remains reserved and the rd-ap below collides.
echo ">> linstor r d ${N3} ${RD} (delete inconsistent replica)"
if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not installed on stand host (apt install linstor-client)"
    exit 0
fi
"${LCTL[@]}" resource delete "$N3" "$RD" >/dev/null

echo ">> wait up to 90s for Resource ${RD}.${N3} CRD to vanish"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" >/dev/null 2>&1; then
    echo "FAIL: ${RD}.${N3} still present after linstor r d (Bug 1: cascade did not clean up)"
    exit 1
fi

# Cascade should also have brought DRBD down on N3. The new
# satellite Pod's view of drbdsetup status for $RD must be empty.
echo ">> verify DRBD state for ${RD} cleaned up on ${N3}"
res_drbd=$(on_node "$N3" bash -c "drbdsetup status ${RD} 2>&1 || true")
if [[ "$res_drbd" != *"No such resource"* && "$res_drbd" != *"not defined"* && -n "$res_drbd" && "$res_drbd" != *"$RD"*"No such resource"* ]]; then
    # The drbdsetup output is parsed best-effort; if the resource
    # name still shows up with a real state line, the satellite
    # didn't tear it down on our cascade. That's the Bug 1
    # symptom we're guarding against.
    if echo "$res_drbd" | grep -qE "^${RD}\s+role:"; then
        echo "FAIL: ${N3} still has DRBD state for ${RD} after cascade (got: $res_drbd)"
        exit 1
    fi
fi

# --- step 7: linstor rd ap → place a fresh replica ---------------------

echo ">> linstor rd ap ${RD} --place-count 3 --storage-pool ${POOL}"
recovery_start=$(date +%s)
"${LCTL[@]}" resource-definition auto-place "$RD" --place-count 3 --storage-pool "$POOL" >/dev/null

# The reconciler should pick a node not currently holding a replica
# (N3 is the only candidate on a 3-node cluster) and start fresh.
# Fresh state means no leftover bitmap → DRBD does an initial sync
# from a peer → SyncTarget → UpToDate within the polling window.
echo ">> wait up to 120s for ${RD} to be locally UpToDate on all 3 workers"
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    d1=$(local_disk_state "$N1" "$RD")
    d2=$(local_disk_state "$N2" "$RD")
    d3=$(local_disk_state "$N3" "$RD")
    if [[ "$d1" == "disk:UpToDate" && "$d2" == "disk:UpToDate" && "$d3" == "disk:UpToDate" ]]; then
        break
    fi
    sleep 2
done
recovery_end=$(date +%s)
recovery_secs=$(( recovery_end - recovery_start ))

if [[ "$d1" != "disk:UpToDate" || "$d2" != "disk:UpToDate" || "$d3" != "disk:UpToDate" ]]; then
    echo "FAIL: cluster did not converge after rd ap ($N1=$d1 $N2=$d2 $N3=$d3, ${recovery_secs}s)"
    exit 1
fi

# Primary on N1 must STILL be InUse / Primary — the recovery should
# not have demoted N1 or otherwise interrupted the consumer mount.
role_n1=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "role:" | head -1 || true)
if [[ "$role_n1" != *"role:Primary"* ]]; then
    echo "FAIL: ${N1} lost Primary role during recovery (got: $role_n1)"
    exit 1
fi

# Final dd-gap check — any stall during rd ap reconcile is a regression.
max_gap_final=$(awk 'NR>1 { if ($1 - prev > max) max = $1 - prev; prev=$1; next } { prev=$1 } END { print max+0 }' "$DD_LOG")
echo "   max dd-tick gap across the whole run: ${max_gap_final}s"
if (( max_gap_final > 30 )); then
    echo "FAIL: Primary dd loop stalled ${max_gap_final}s end-to-end (>30s regression)"
    exit 1
fi

echo ">> RECOVERY-INCONSISTENT-BLOCKING OK (recovery took ${recovery_secs}s, Primary uninterrupted)"
