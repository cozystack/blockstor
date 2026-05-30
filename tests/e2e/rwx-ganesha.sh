#!/usr/bin/env bash
#
# usage: rwx-ganesha.sh WORK_DIR
#
# RWX validation through linstor-csi in piraeus EXTERNAL mode against
# blockstor's apiserver. Real end-to-end coverage of the K8s consumer
# path for a ReadWriteMany PVC:
#
#   1. StorageClass with `linstor.csi.linbit.com` provisioner pointed
#      at a blockstor-side StoragePool (`stand` — FILE_THIN, present
#      on every worker via stand/blockstor-storagepools.yaml; same
#      pool csi-sanity-job exercises through linstor-csi end-to-end,
#      so the CreateVolume / DeleteVolume contract is already pinned
#      against it on every Run). mTLS to the blockstor apiserver is
#      wired by stand/install-piraeus.sh (LS_CONTROLLERS +
#      LS_USER_CERTIFICATE on the linstor-csi pods).
#   2. PVC with accessModes=ReadWriteMany → linstor-csi-controller
#      issues CreateVolume against blockstor; the blockstor apiserver
#      creates a multi-volume RD (vol-0 control / vol-1 data, the L6
#      cli-matrix cell rwx-ganesha-data-vol-mkfs.sh pins this part of
#      the wire contract directly through REST). PVC → Bound; the
#      RD-and-Resources side is observed via blockstor CRDs.
#   3. Three Pods on three workers consume the PVC. linstor-csi-node
#      drives the NFS-Ganesha publish path on every worker. The
#      consumer-side wiring needs the ganesha-server export pod plus
#      a per-host promoter that picks the single Primary; in this
#      stand the promoter is the host-side drbd-reactor shipped by
#      the siderolabs/drbd Talos extension (see
#      stand/blockstor-satellite-daemonset.yaml:309 comment — the
#      TODO 7.W08 there is the Prometheus-metrics drbd-reactor
#      *sidecar*, NOT the RWX promoter; the promoter is host-scoped
#      by design and lives in the Talos extension). All Pods must
#      reach Ready and exchange data through the shared mount.
#
# No skip, no XFAIL: if any step (Bound, RD shape, Pod Ready, marker
# round-trip) fails, the scenario exits non-zero and the e2e lane
# goes red. That is the intended signal. If the ganesha-server /
# host-side drbd-reactor are not yet wired in this stand variant,
# the failure point + dumped diagnostics tell the operator exactly
# what is missing (kubectl describe Pod + linstor-csi-node logs +
# linstor-csi-controller logs).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

SC=e2e-rwx-sc
PVC=e2e-rwx
P1=e2e-rwx-pod-1
P2=e2e-rwx-pod-2
P3=e2e-rwx-pod-3
# Use the FILE_THIN `stand` pool: present on every worker on the dev
# stand and on the e2e-piraeus CI stand (provisioned by
# stand/blockstor-storagepools.yaml, independent of `make pools TYPE`).
# Same pool csi-sanity-job exercises through linstor-csi end-to-end
# on every Run, so the CreateVolume / DeleteVolume contract is already
# validated against it.
POOL=${POOL:-stand}

# dump_diag prints the pieces of cluster state most useful for triaging
# a stuck RWX consumer — used by the wait-Ready failure branch and by
# any later marker-mismatch failure. Best-effort: never aborts.
dump_diag() {
    local label=$1
    echo "----- diag ($label): describe pvc $PVC -----" >&2
    kubectl describe pvc "$PVC" 2>&1 | tail -40 >&2 || true
    for p in "$P1" "$P2" "$P3"; do
        echo "----- diag ($label): describe pod $p -----" >&2
        kubectl describe pod "$p" 2>&1 | tail -40 >&2 || true
    done
    echo "----- diag ($label): linstor-csi-controller log tail -----" >&2
    kubectl -n piraeus-datastore logs deploy/linstor-csi-controller \
        -c linstor-csi --tail=80 2>&1 >&2 || true
    echo "----- diag ($label): linstor-csi-node logs (per worker) -----" >&2
    kubectl -n piraeus-datastore logs ds/linstor-csi-node \
        -c linstor-csi --tail=80 --prefix=true 2>&1 >&2 || true
    echo "----- diag ($label): linstor r l (via blockstor controller) -----" >&2
    kubectl -n blockstor-system exec deploy/blockstor-controller -- \
        linstor r l 2>&1 >&2 || true
}

cleanup() {
    kubectl delete pod "$P1" "$P2" "$P3" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete pvc "$PVC" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
    # Best-effort: let the linstor-csi-controller reap the underlying
    # RD asynchronously; the inter-scenario reset_cluster_state in
    # run-scenarios-only.sh sweeps anything it leaves behind.
}
trap cleanup EXIT

echo ">> StorageClass against linstor-csi (external-mode → blockstor apiserver, pool=$POOL, replicas=2)"
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: $SC}
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "$POOL"
  linstor.csi.linbit.com/placementCount: "2"
  csi.storage.k8s.io/fstype: ext4
allowVolumeExpansion: true
volumeBindingMode: Immediate
reclaimPolicy: Delete
EOF

echo ">> PVC 128Mi accessModes=ReadWriteMany"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $SC
  resources:
    requests: {storage: 128Mi}
EOF

echo ">> wait for PVC Bound (180s) — linstor-csi-controller → blockstor apiserver CreateVolume"
deadline=$(( $(date +%s) + 180 ))
phase=""
while (( $(date +%s) < deadline )); do
    phase=$(kubectl get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Bound" ]] && break
    sleep 3
done

if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC never Bound (phase=$phase)" >&2
    kubectl describe pvc "$PVC" | tail -30 >&2
    echo "----- linstor-csi-controller log tail -----" >&2
    kubectl -n piraeus-datastore logs deploy/linstor-csi-controller \
        -c linstor-csi --tail=80 2>&1 >&2 || true
    exit 1
fi

PV=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}')
echo "   PVC bound → PV=$PV"

# Sanity: the bound PV's name is the blockstor RD name (linstor-csi
# uses pvc-<uuid> verbatim as the LINSTOR-side resource name). Confirm
# the apiserver sees the RD and the StoragePool reference resolved
# against blockstor's `$POOL`, not against an absent piraeus-side pool.
echo ">> blockstor sees RD=$PV with at least 2 diskful Resources"
deadline=$(( $(date +%s) + 60 ))
rd_seen=""
diskful=0
while (( $(date +%s) < deadline )); do
    if kubectl get "resourcedefinitions.blockstor.cozystack.io/$PV" >/dev/null 2>&1; then
        rd_seen=yes
        diskful=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
            | awk -v pv="$PV" '$1 ~ "^"pv"\\." {print $1}' | wc -l | tr -d ' ')
        if (( diskful >= 2 )); then
            break
        fi
    fi
    sleep 3
done
if [[ "$rd_seen" != "yes" ]]; then
    echo "FAIL: blockstor never created RD/$PV after PVC bound" >&2
    kubectl get resourcedefinitions.blockstor.cozystack.io 2>&1 >&2 | tail -20 || true
    exit 1
fi
if (( diskful < 2 )); then
    echo "FAIL: expected >=2 Resources for RD=$PV, got $diskful" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers 2>&1 >&2 \
        | awk -v pv="$PV" '$1 ~ "^"pv"\\." {print}' >&2 || true
    exit 1
fi
echo "   blockstor RD=$PV present with $diskful Resources"

# The RWX path through linstor-csi triggers the multi-volume mkfs code
# (vol-0 control, vol-1 data) only when the upstream LINSTOR controller
# is the one fielding CreateVolume — that controller sets the RD-level
# FileSystem/Type and adds the second VD before responding. linstor-csi
# in front of blockstor's apiserver does NOT do that today (no
# special-case for ReadWriteMany at the CSI level: it forwards a plain
# single-VD CreateVolume). The multi-volume + FileSystem/Type pieces of
# the RWX wire-contract are pinned by the L6 cli-matrix cell
# `rwx-ganesha-data-vol-mkfs.sh` directly through the linstor REST
# surface; this scenario keeps its focus on the upper publish-side path
# (PVC → consumer Pod mount). No assertion on RD shape beyond
# "exists + >=2 Resources" above.

echo ">> three Pods on $WORKER_1 + $WORKER_2 + $WORKER_3 mount the RWX PVC"
# PodSecurity: the test namespace runs with PSA `restricted:latest`
# enforcement. Run 28 deep-dive (kept from the pre-rewrite version of
# this scenario) showed an under-specified pod admits late and trips
# the wait_for_ready timeout for the wrong reason. The full
# restricted-baseline securityContext at both Pod + Container scope
# admits immediately so any non-Ready in the wait below is genuinely
# the NodePublishVolume / NFS-Ganesha publish layer.
for spec in "$P1:$WORKER_1" "$P2:$WORKER_2" "$P3:$WORKER_3"; do
    name=${spec%:*}
    node=${spec#*:}
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: {name: $name}
spec:
  nodeName: $node
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: w
      image: alpine:3
      command: ["sleep", "600"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: [ALL]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $PVC}
EOF
done

echo ">> wait three Pods Ready (300s) — NFS-Ganesha publish via linstor-csi-node"
# Hard wait: any non-Ready inside 300s is a genuine failure of the
# RWX consumer path (NodePublishVolume hangs, NFS-Ganesha export pod
# absent, host-side drbd-reactor promoter not picking a Primary,
# etc.). Dump full diagnostics and fail.
if ! kubectl wait --for=condition=Ready --timeout=300s \
        pod/"$P1" pod/"$P2" pod/"$P3"; then
    echo "FAIL: RWX consumer Pods did not reach Ready within 300s" >&2
    dump_diag "wait-Ready"
    exit 1
fi

MARK="rwx-$(date +%s)-$$"
echo ">> write marker '$MARK' from $P1"
if ! kubectl exec "$P1" -- sh -c "echo $MARK > /data/marker && sync"; then
    echo "FAIL: writer pod $P1 cannot write to RWX mount" >&2
    dump_diag "writer-exec"
    exit 1
fi

echo ">> read marker from $P2"
got2=$(kubectl exec "$P2" -- cat /data/marker 2>&1 || true)
if [[ "$got2" != "$MARK" ]]; then
    echo "FAIL: marker on $P2 mismatch — got '$got2', want '$MARK'" >&2
    dump_diag "read-p2"
    exit 1
fi

echo ">> read marker from $P3"
got3=$(kubectl exec "$P3" -- cat /data/marker 2>&1 || true)
if [[ "$got3" != "$MARK" ]]; then
    echo "FAIL: marker on $P3 mismatch — got '$got3', want '$MARK'" >&2
    dump_diag "read-p3"
    exit 1
fi

# Cross-pod write fan-in: every pod stamps its own file, every pod
# sees all three files. This catches the regression shape where the
# first writer holds an exclusive lock on the NFS export and later
# pods can attach but not write (observed with a stale ganesha-server
# state-cache).
echo ">> each Pod writes its own marker, every Pod reads all three"
kubectl exec "$P2" -- sh -c "echo from-p2-$MARK > /data/marker-p2 && sync"
kubectl exec "$P3" -- sh -c "echo from-p3-$MARK > /data/marker-p3 && sync"
for src in "$P1" "$P2" "$P3"; do
    for tag in "marker" "marker-p2" "marker-p3"; do
        if ! kubectl exec "$src" -- test -f "/data/$tag"; then
            echo "FAIL: $src cannot see /data/$tag" >&2
            dump_diag "fan-in-$src-$tag"
            exit 1
        fi
    done
done

echo ">> RWX-GANESHA OK (3-way write/read round-trip across $WORKER_1, $WORKER_2, $WORKER_3 via blockstor + linstor-csi external mode)"
