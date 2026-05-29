#!/usr/bin/env bash
#
# usage: csi-pvc-replicated-rwo.sh WORK_DIR
#
# linstor-csi v1.10.1 auto-mkfs contract — "replicated" StorageClass.
#
# Reproduces the production demo SC the user runs on the dev stand:
#
#     parameters:
#       linstor.csi.linbit.com/storagePool: "data"
#       linstor.csi.linbit.com/autoPlace: "3"
#       linstor.csi.linbit.com/layerList: "drbd storage"
#       linstor.csi.linbit.com/allowRemoteVolumeAccess: "true"
#       property.linstor.csi.linbit.com/DrbdOptions/auto-quorum: suspend-io
#       property.linstor.csi.linbit.com/DrbdOptions/Resource/on-no-data-accessible: suspend-io
#       property.linstor.csi.linbit.com/DrbdOptions/Resource/on-suspended-primary-outdated: force-secondary
#       property.linstor.csi.linbit.com/DrbdOptions/Net/rr-conflict: retry-connect
#     volumeBindingMode: Immediate
#
# Steps:
#   1. apply the SC verbatim (with pool name override).
#   2. PVC 128Mi RWO + busybox Pod on WORKER_1; linstor-csi binds
#      Immediate (3 DRBD replicas land regardless of the consumer).
#   3. wait Pod Ready (NodePublishVolume succeeded → fsck saw an
#      already-mkfs'd /dev/drbd<minor>).
#   4. write a marker file, delete the Pod, recreate on WORKER_2
#      (allowRemoteVolumeAccess=true → DRBD diskless attach on a
#      non-diskful peer), wait Ready, read the marker back.
#   5. cleanup is via register_strict_cleanup for the RD; SC/PVC/Pod
#      are deleted in the local EXIT trap.
#
# Same pool-name override as csi-pvc-local.sh: the SC says `data`, the
# stand has `lvm-thin`; sed rewrites only the storagePool key at apply
# time so the SC contract is otherwise verbatim.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

# We need at least one extra worker to validate the cross-node remount
# leg (proves DRBD replication actually delivered the bytes). 2-worker
# stand is enough; 3 lets allowRemoteVolumeAccess prove its real value.
require_workers 2

# Prerequisite: this scenario exercises piraeus's bundled linstor-csi.
# The matrix lanes (E2E_EXCLUDE in .github/workflows/pull-request.yml)
# never install piraeus — only the dedicated `e2e-piraeus` job does. If
# the LinstorCluster CRD is missing here, the rest of the script would
# hard-fail (PVC stays Pending, `kubectl -n piraeus-datastore ...` 404,
# `kubectl exec` into a satellite-pod-without-linstor blows up). Fail
# fast with a clear message instead — placement in the right CI job is
# the fix, not a silent skip.
if ! kubectl get crd linstorclusters.piraeus.io >/dev/null 2>&1; then
    echo "FAIL: LinstorCluster CRD (piraeus.io) absent — this scenario belongs in the e2e-piraeus job (INSTALL_PIRAEUS=1)" >&2
    exit 1
fi

SC=e2e-csi-replicated
PVC=e2e-csi-replicated-pvc
POD=e2e-csi-replicated-pod

# Stand-only pool rename: the demo SC says `data`; the QEMU stand
# registers `lvm-thin`. Keep the SC body identical to the demo
# contract and override the pool name at apply time.
POOL=${POOL:-lvm-thin}

# stand/install-piraeus.sh pre-wires the LinstorCluster at blockstor's
# apiserver (externalController.url + apiTLS.certManager). On a stand
# that was provisioned in coexistence mode (legacy spec:{}), call
# wire_linstor_csi_mtls so linstor-csi can actually reach blockstor;
# the helper short-circuits when the LinstorCluster is already wired.
BLOCKSTOR_URL="https://blockstor-apiserver.blockstor-system.svc:3371"
wire_linstor_csi_mtls "$BLOCKSTOR_URL"

# Port-forward to blockstor-apiserver:3370 (plain HTTP REST) so the
# host's `linstor` CLI can dump `r l` / `v l` on failure. The
# blockstor-apiserver pod itself has no linstor binary, and exec-ing
# into a satellite pod fails the same way ("executable not found");
# port-forward + host CLI is the canonical pattern (also used by
# observability-three-way, recovery-late-vd-real-drbd, etc.).
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/csi-pvc-replicated-rwo-pf.log 2>&1 &
PF_PID=$!

local_cleanup() {
    # Delete Pod synchronously so kubelet finishes NodeUnpublishVolume +
    # ControllerUnpublishVolume BEFORE the PVC delete triggers
    # linstor-csi DeleteVolume. Without this ordering, pv-controller
    # keeps retrying ControllerPublishVolume on the deleted volume
    # into the next scenario's run window (observed on the piraeus
    # interop lane: `volume not present in storage backend` retries
    # every 60s monopolised linstor-csi-node and starved the next
    # csi-pvc-local PVC's NodePublishVolume past its 240s Pod-Ready
    # budget).
    kubectl delete pod "$POD" --ignore-not-found --wait=true --timeout=60s 2>/dev/null || true
    kubectl delete pvc "$PVC" --ignore-not-found --wait=true --timeout=60s 2>/dev/null || true
    kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap local_cleanup EXIT

# Wait for port-forward to come up (best-effort — diag output is
# informational, not load-bearing for the pass/fail verdict).
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

dump_diag() {
    local label=$1
    echo "----- diag ($label): describe pvc $PVC -----" >&2
    kubectl describe pvc "$PVC" 2>&1 | tail -40 >&2 || true
    echo "----- diag ($label): describe pod $POD -----" >&2
    kubectl describe pod "$POD" 2>&1 | tail -40 >&2 || true
    echo "----- diag ($label): linstor-csi-controller log tail -----" >&2
    kubectl -n piraeus-datastore logs deploy/linstor-csi-controller \
        -c linstor-csi --tail=80 2>&1 >&2 || true
    echo "----- diag ($label): linstor-csi-node logs -----" >&2
    kubectl -n piraeus-datastore logs ds/linstor-csi-node \
        -c linstor-csi --tail=80 --prefix=true 2>&1 >&2 || true
    echo "----- diag ($label): linstor r l (port-forward to blockstor apiserver) -----" >&2
    linstor --controllers "http://localhost:$PF_PORT" r l 2>&1 >&2 || true
    echo "----- diag ($label): linstor v l (port-forward to blockstor apiserver) -----" >&2
    linstor --controllers "http://localhost:$PF_PORT" v l 2>&1 >&2 || true
}

# How many DRBD replicas to autoplace. autoPlace=3 from the demo SC
# but stands may have only 2 workers; pick min(autoPlace, workers).
AUTOPLACE=3
if [[ -z "${WORKER_3:-}" ]]; then
    AUTOPLACE=2
fi

echo ">> apply the demo 'replicated' SC verbatim (pool=$POOL override, autoPlace=$AUTOPLACE)"
cat <<EOF | sed -e "s/storagePool: \"data\"/storagePool: \"$POOL\"/" \
                -e "s/autoPlace: \"3\"/autoPlace: \"$AUTOPLACE\"/" \
            | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $SC
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "data"
  linstor.csi.linbit.com/autoPlace: "3"
  linstor.csi.linbit.com/layerList: "drbd storage"
  linstor.csi.linbit.com/allowRemoteVolumeAccess: "true"
  property.linstor.csi.linbit.com/DrbdOptions/auto-quorum: suspend-io
  property.linstor.csi.linbit.com/DrbdOptions/Resource/on-no-data-accessible: suspend-io
  property.linstor.csi.linbit.com/DrbdOptions/Resource/on-suspended-primary-outdated: force-secondary
  property.linstor.csi.linbit.com/DrbdOptions/Net/rr-conflict: retry-connect
volumeBindingMode: Immediate
allowVolumeExpansion: true
EOF

echo ">> PVC 128Mi RWO (Immediate binding → 3 DRBD replicas land regardless of Pod)"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  resources:
    requests: {storage: 128Mi}
EOF

echo ">> wait PVC Bound (180s)"
deadline=$(( $(date +%s) + 180 ))
phase=""
while (( $(date +%s) < deadline )); do
    phase=$(kubectl get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Bound" ]] && break
    sleep 3
done

if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC never Bound (phase=$phase)" >&2
    dump_diag "wait-bound"
    exit 1
fi

PV=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}')
echo "   PVC bound → PV=$PV (= linstor RD name)"

register_strict_cleanup "$PV"

echo ">> busybox Pod on $WORKER_1"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: {name: $POD}
spec:
  nodeName: $WORKER_1
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: w
      image: busybox:1.36
      command: ["sh", "-c", "sleep 600"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $PVC}
EOF

# Pod Ready proves NodePublishVolume completed → the DRBD device
# carried a recognisable filesystem at fsck time. Pre-fix the satellite
# only ran auto-mkfs through runAutoPromote → primary --force → mkfs;
# regressions in the gate cause the same exit-8 fsck failure that
# storage-only had.
echo ">> wait Pod Ready on $WORKER_1 (300s)"
if ! kubectl wait --for=condition=Ready --timeout=300s pod/"$POD"; then
    echo "FAIL: Pod $POD on $WORKER_1 never reached Ready — likely NodePublishVolume fsck exit 8" >&2
    dump_diag "wait-ready-w1"
    exit 1
fi

MARK="repl-$(date +%s)-$$"
echo ">> write marker '$MARK' to /data/marker on $WORKER_1, sync"
if ! kubectl exec "$POD" -- sh -c "echo $MARK > /data/marker && sync"; then
    echo "FAIL: cannot write to /data — auto-mkfs likely produced a broken FS" >&2
    dump_diag "write-marker"
    exit 1
fi

echo ">> delete Pod, recreate on $WORKER_2 (DRBD-replicated → marker should be there)"
kubectl delete pod "$POD" --wait=true --timeout=120s

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: {name: $POD}
spec:
  nodeName: $WORKER_2
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: w
      image: busybox:1.36
      command: ["sh", "-c", "sleep 600"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $PVC}
EOF

if ! kubectl wait --for=condition=Ready --timeout=300s pod/"$POD"; then
    echo "FAIL: Pod $POD on $WORKER_2 never reached Ready" >&2
    dump_diag "wait-ready-w2"
    exit 1
fi

got=$(kubectl exec "$POD" -- cat /data/marker 2>&1 || true)
if [[ "$got" != "$MARK" ]]; then
    echo "FAIL: marker round-trip mismatch on $WORKER_2 — got '$got', want '$MARK'" >&2
    dump_diag "read-marker-w2"
    exit 1
fi

echo ">> CSI-PVC-REPLICATED-RWO OK (PV=$PV, marker '$MARK' survived Pod migration $WORKER_1 → $WORKER_2 via DRBD)"
