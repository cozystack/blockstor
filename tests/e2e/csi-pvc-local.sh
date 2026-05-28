#!/usr/bin/env bash
#
# usage: csi-pvc-local.sh WORK_DIR
#
# linstor-csi v1.10.1 auto-mkfs contract — "local" StorageClass shape.
#
# Reproduces the production demo SC the user runs on the dev stand:
#
#     parameters:
#       linstor.csi.linbit.com/storagePool: "data"
#       linstor.csi.linbit.com/layerList: "storage"
#       linstor.csi.linbit.com/allowRemoteVolumeAccess: "false"
#     volumeBindingMode: WaitForFirstConsumer
#
# Before the satellite-side auto-mkfs lift in this PR, a PVC bound to
# such an SC reached NodePublishVolume with an unformatted LV — the
# linstor-csi v1.10.1 mount path runs `fsck` then plain `mount(2)`
# (pkg/client/linstor.go lines 2287-2293; there is no FormatAndMount
# fallback) so the kubelet's fsck rejected the volume with exit 8
# ("Bad magic number"). The fix routes storage-only RDs through the
# same `runAutoMkfs` path the DRBD-stacked path has owned since the
# Bug 311 follow-up.
#
# Stand pool overlap: the demo SC names pool `data`; the QEMU stand's
# install-pools.sh registers `lvm-thin`. We `sed` the pool name at
# apply time rather than altering the demo SC text — the SC body is
# the load-bearing contract, the pool name is a stand-only override.
#
# Steps:
#   1. apply the SC verbatim (with pool name override).
#   2. PVC 128Mi RWO + busybox pod on WORKER_1; volumeBindingMode is
#      WaitForFirstConsumer, so the SC requires a consumer pod to
#      trigger provisioning — the pod scheduling is what makes the
#      CreateVolume call fire.
#   3. wait Pod Ready (proves NodePublishVolume succeeded, i.e. fsck
#      saw a real filesystem on the LV).
#   4. write a marker file, restart the pod (delete + recreate so the
#      kubelet re-mounts), read the marker back.
#   5. cleanup is handled by `register_strict_cleanup` for the RD
#      linstor-csi created; the SC/PVC/Pod are deleted in the local
#      EXIT trap.
#
# No skip / no xfail. Failure = `exit 1` with diagnostics.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

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

SC=e2e-csi-local
PVC=e2e-csi-local-pvc
POD=e2e-csi-local-pod

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
    >/tmp/csi-pvc-local-pf.log 2>&1 &
PF_PID=$!

# We don't know the linstor-csi-generated RD name (`pvc-<uuid>`) up
# front; capture it after PVC Bound and register_strict_cleanup it.
# Until then, do a best-effort SC/PVC/Pod sweep on EXIT.
local_cleanup() {
    kubectl delete pod "$POD" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete pvc "$PVC" --ignore-not-found --wait=false 2>/dev/null || true
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

echo ">> apply the demo 'local' SC verbatim (pool=$POOL override for stand)"
# The demo SC body — keep edits to the SC contract minimal so a future
# change to the user-facing shape catches a regression here too.
cat <<EOF | sed "s/storagePool: \"data\"/storagePool: \"$POOL\"/" | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $SC
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "data"
  linstor.csi.linbit.com/layerList: "storage"
  linstor.csi.linbit.com/allowRemoteVolumeAccess: "false"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
EOF

echo ">> PVC 128Mi RWO (volumeBindingMode=WaitForFirstConsumer → not provisioned until Pod schedules)"
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

echo ">> busybox Pod on $WORKER_1 (triggers CreateVolume + NodePublishVolume)"
# securityContext fields satisfy the PodSecurity 'restricted' profile
# Talos enforces by default. fsGroup=1000 chowns the mounted ext4
# filesystem so the non-root UID can write the marker; without it the
# `echo > /data/marker` fails with EACCES.
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

# Pod Ready proves NodePublishVolume completed, which proves the
# kubelet's fsck did NOT trip on an unformatted device (the original
# symptom). Generous 240s budget to absorb stand-side delays.
echo ">> wait Pod Ready (240s)"
if ! kubectl wait --for=condition=Ready --timeout=240s pod/"$POD"; then
    echo "FAIL: Pod $POD never reached Ready — likely NodePublishVolume fsck exit 8 (no FS on device)" >&2
    dump_diag "wait-ready"
    exit 1
fi

PV=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}')
echo "   PVC bound → PV=$PV (= linstor RD name)"

# Now that we know the RD name, hand it to strict_cleanup_on_exit so
# the next scenario inherits a clean cluster.
register_strict_cleanup "$PV"

MARK="local-$(date +%s)-$$"
echo ">> write marker '$MARK' to /data/marker, sync"
if ! kubectl exec "$POD" -- sh -c "echo $MARK > /data/marker && sync"; then
    echo "FAIL: cannot write to /data — auto-mkfs likely produced a broken FS" >&2
    dump_diag "write-marker"
    exit 1
fi

echo ">> delete + recreate Pod (proves marker persists across remount)"
kubectl delete pod "$POD" --wait=true --timeout=120s

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

if ! kubectl wait --for=condition=Ready --timeout=240s pod/"$POD"; then
    echo "FAIL: Pod $POD did not become Ready on remount" >&2
    dump_diag "wait-remount"
    exit 1
fi

got=$(kubectl exec "$POD" -- cat /data/marker 2>&1 || true)
if [[ "$got" != "$MARK" ]]; then
    echo "FAIL: marker round-trip mismatch — got '$got', want '$MARK'" >&2
    dump_diag "read-marker"
    exit 1
fi

echo ">> CSI-PVC-LOCAL OK (PV=$PV, marker '$MARK' survived Pod remount, layerList=storage)"
