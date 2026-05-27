#!/usr/bin/env bash
# usage: install-piraeus.sh WORK_DIR
# Installs piraeus-operator + linstor-csi via the published manifests.
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

PIRAEUS_VERSION=${PIRAEUS_VERSION:-v2.10.0}

echo ">> applying piraeus-operator $PIRAEUS_VERSION"
kubectl apply --server-side \
    -k "https://github.com/piraeusdatastore/piraeus-operator//config/default?ref=$PIRAEUS_VERSION"

echo ">> waiting for piraeus-operator to be ready"
kubectl -n piraeus-datastore wait deploy/piraeus-operator-controller-manager \
    --for=condition=Available --timeout=5m

echo ">> creating LinstorCluster"
kubectl apply -f - <<EOF
apiVersion: piraeus.io/v1
kind: LinstorCluster
metadata:
  name: linstorcluster
spec: {}
EOF

# Mirror blockstor's apiserver CA into piraeus-datastore so the
# piraeus-operator can issue linstor-csi client certs from the SAME CA
# the blockstor apiserver trusts. The observability scenarios
# (observability-three-way / -capacity-correlation) repoint piraeus's
# bundled linstor-csi at blockstor's mTLS apiserver
# (LinstorCluster.spec.externalController.url =
# https://blockstor-apiserver...:3371). That endpoint is
# RequireAndVerifyClientCert, so linstor-csi MUST present a client cert
# chained to blockstor-api-ca. The operator-native knob is
# LinstorCluster.spec.apiTLS.certManager: when set, the operator creates
# Certificate resources (linstor-csi-controller-tls / -node-tls) in
# piraeus-datastore from the referenced cert-manager Issuer and wires
# LS_USER_CERTIFICATE / LS_USER_KEY / LS_ROOT_CA into the CSI pods (see
# piraeus-operator v2.10.0 internal/controller/linstorcluster_controller.go
# kustomizeCSIControllerResources + patches/api-tls-csi-controller.yaml).
# A cert-manager Issuer is namespace-scoped, so the CA private key must
# live in piraeus-datastore: copy the blockstor-api-ca Secret here and
# stand up a CA Issuer over it. Idempotent; harmless to the rwx-ganesha
# scenario, which keeps piraeus's own controller and never sets apiTLS.
echo ">> mirror blockstor-api-ca into piraeus-datastore + create CA Issuer"
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if kubectl -n blockstor-system get secret blockstor-api-ca >/dev/null 2>&1; then
        break
    fi
    sleep 3
done
if kubectl -n blockstor-system get secret blockstor-api-ca >/dev/null 2>&1; then
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
    kubectl -n piraeus-datastore wait --for=condition=Ready issuer/blockstor-api-ca --timeout=60s || true
else
    echo ">> WARN: blockstor-api-ca Secret not found in blockstor-system — apiserver mTLS PKI absent; observability scenarios will SKIP" >&2
fi

echo ">> waiting for LinstorCluster ready"
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

echo ">> creating LinstorSatelliteConfiguration with a file-thin storage pool"
# File-thin pool uses an LVM thin volume that piraeus creates from a sparse
# file under /var/lib/piraeus on each satellite. No host-side prep required.
kubectl apply -f - <<EOF
apiVersion: piraeus.io/v1
kind: LinstorSatelliteConfiguration
metadata:
  name: pool
spec:
  storagePools:
    - name: pool
      fileThinPool:
        directory: /var/lib/piraeus/file-thin
EOF

echo ">> waiting for storage pools to register"
for i in {1..60}; do
    READY=$(kubectl get linstornodeconnections -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Available")].status}{"\n"}{end}' 2>/dev/null | grep -c True || true)
    POOLS=$(kubectl get linstorsatellites -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="StoragePools")].status}{"\n"}{end}' 2>/dev/null | grep -c True || true)
    if [[ "$POOLS" -ge 1 ]]; then
        echo ">> storage pools ready on $POOLS satellites"
        break
    fi
    sleep 5
done

echo ">> piraeus install complete"
kubectl get pods -n piraeus-datastore
echo
echo ">> linstorsatellites:"
kubectl get linstorsatellites
echo
echo ">> exec linstor controller and list pools:"
kubectl exec -n piraeus-datastore deploy/linstor-controller -- linstor sp l 2>/dev/null || true
