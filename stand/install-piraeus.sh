#!/usr/bin/env bash
# usage: install-piraeus.sh WORK_DIR
# Installs piraeus-operator + linstor-csi via the published manifests,
# wired in EXTERNAL mode: linstor-csi talks to blockstor's apiserver as
# the LINSTOR-compatible backend, and piraeus-operator does NOT spawn
# its own in-cluster Java linstor-controller. blockstor must already be
# installed (stand/install-blockstor.sh) so the blockstor-api-ca Secret
# exists for the CA mirror below and the apiserver Service is reachable
# at https://blockstor-apiserver.blockstor-system.svc:3371.
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

PIRAEUS_VERSION=${PIRAEUS_VERSION:-v2.10.0}

# External-mode endpoint. blockstor's apiserver Service exposes ONLY
# the mTLS port (:3371, RequireAndVerifyClientCert) — the plain HTTP
# debug port (:3370) is bound only inside the pod and not in the
# Service spec, so linstor-csi MUST go through :3371 with a client
# cert chained to blockstor-api-ca. The apiTLS knob below handles the
# client-cert side via cert-manager.
BLOCKSTOR_URL=${BLOCKSTOR_URL:-https://blockstor-apiserver.blockstor-system.svc:3371}

echo ">> applying piraeus-operator $PIRAEUS_VERSION"
kubectl apply --server-side \
    -k "https://github.com/piraeusdatastore/piraeus-operator//config/default?ref=$PIRAEUS_VERSION"

echo ">> waiting for piraeus-operator to be ready"
kubectl -n piraeus-datastore wait deploy/piraeus-operator-controller-manager \
    --for=condition=Available --timeout=5m

# Mirror blockstor's apiserver CA into piraeus-datastore so the
# piraeus-operator can issue linstor-csi client certs from the SAME CA
# the blockstor apiserver trusts. The operator-native knob is
# LinstorCluster.spec.apiTLS.certManager: when set, the operator creates
# Certificate resources (linstor-csi-controller-tls / -node-tls) in
# piraeus-datastore from the referenced cert-manager Issuer and wires
# LS_USER_CERTIFICATE / LS_USER_KEY / LS_ROOT_CA into the CSI pods (see
# piraeus-operator v2.10.0 internal/controller/linstorcluster_controller.go
# kustomizeCSIControllerResources + patches/api-tls-csi-controller.yaml).
# A cert-manager Issuer is namespace-scoped, so the CA private key must
# live in piraeus-datastore: copy the blockstor-api-ca Secret here and
# stand up a CA Issuer over it. Must run BEFORE LinstorCluster creation
# below — the operator reconciles apiTLS as soon as the CR appears and
# would block on the missing Issuer otherwise.
echo ">> mirror blockstor-api-ca into piraeus-datastore + create CA Issuer"
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if kubectl -n blockstor-system get secret blockstor-api-ca >/dev/null 2>&1; then
        break
    fi
    sleep 3
done
if ! kubectl -n blockstor-system get secret blockstor-api-ca >/dev/null 2>&1; then
    echo "FATAL: blockstor-api-ca Secret not found in blockstor-system after 120s — install blockstor BEFORE piraeus (external mode needs the apiserver mTLS CA mirrored)" >&2
    exit 1
fi
kubectl -n blockstor-system get secret blockstor-api-ca -o json \
    | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.ownerReferences, .metadata.annotations, .metadata.labels)' \
    | kubectl -n piraeus-datastore apply -f -
kubectl apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: blockstor-api-ca
  namespace: piraeus-datastore
spec:
  ca:
    secretName: blockstor-api-ca
EOF
kubectl -n piraeus-datastore wait --for=condition=Ready issuer/blockstor-api-ca --timeout=60s

echo ">> creating LinstorCluster in EXTERNAL mode -> $BLOCKSTOR_URL"
# spec.externalController.url disables piraeus's bundled in-cluster
# linstor-controller Deployment and re-renders linstor-csi with
# LS_CONTROLLERS pointing at blockstor's apiserver.
# spec.apiTLS.certManager makes the operator issue linstor-csi-{controller,node}-tls
# Secrets from the mirrored blockstor-api-ca Issuer; LS_USER_CERTIFICATE /
# LS_USER_KEY / LS_ROOT_CA are then wired onto the CSI pods so they can
# present a client cert to blockstor's RequireAndVerifyClientCert :3371.
kubectl apply -f - <<EOF
apiVersion: piraeus.io/v1
kind: LinstorCluster
metadata:
  name: linstorcluster
spec:
  externalController:
    url: $BLOCKSTOR_URL
  apiTLS:
    certManager:
      name: blockstor-api-ca
      kind: Issuer
EOF

echo ">> waiting for LinstorCluster Available"
for i in {1..60}; do
    if kubectl get linstorcluster linstorcluster -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null | grep -q True; then
        echo ">> LinstorCluster Available"
        break
    fi
    sleep 5
done

echo ">> applying Talos-specific satellite override"
# Piraeus's default satellite Pod tries to mount paths that don't exist on
# Talos (/run/systemd, /usr/src, etc.) and runs a drbd-module-loader that
# builds DRBD from source — we don't need that since the siderolabs/drbd
# extension already ships the kernel module. Strip the unwanted bits and
# point LVM bookkeeping at Talos's writable /var/etc.
kubectl apply -f - <<'EOF'
apiVersion: piraeus.io/v1
kind: LinstorSatelliteConfiguration
metadata:
  name: talos-loader-override
spec:
  podTemplate:
    spec:
      initContainers:
        - name: drbd-shutdown-guard
          $patch: delete
        - name: drbd-module-loader
          $patch: delete
      volumes:
        - name: run-systemd-system
          $patch: delete
        - name: run-drbd-shutdown-guard
          $patch: delete
        - name: systemd-bus-socket
          $patch: delete
        - name: lib-modules
          $patch: delete
        - name: usr-src
          $patch: delete
        - name: etc-lvm-backup
          hostPath:
            path: /var/etc/lvm/backup
            type: DirectoryOrCreate
        - name: etc-lvm-archive
          hostPath:
            path: /var/etc/lvm/archive
            type: DirectoryOrCreate
EOF

# External mode: blockstor's satellite owns the storage pools (see
# stand/install-pools.sh + stand/blockstor-storagepools.yaml). We do NOT
# create a piraeus-side LinstorSatelliteConfiguration storage pool here —
# in external mode piraeus-operator drives only linstor-csi, the LINSTOR
# state of record (nodes, pools, RDs) lives in blockstor.

echo ">> wait for linstor-csi rollouts (LS_CONTROLLERS + client cert wired by operator)"
kubectl -n piraeus-datastore rollout status deploy/linstor-csi-controller --timeout=180s
kubectl -n piraeus-datastore rollout status ds/linstor-csi-node --timeout=180s

echo ">> piraeus install complete (external mode)"
kubectl get pods -n piraeus-datastore
echo
echo ">> linstor-csi controller env (LS_CONTROLLERS + client cert):"
kubectl -n piraeus-datastore get deploy linstor-csi-controller -o jsonpath='{range .spec.template.spec.containers[?(@.name=="linstor-csi")].env[*]}{.name}={.value}{.valueFrom.secretKeyRef.name}{"\n"}{end}' 2>/dev/null || true
