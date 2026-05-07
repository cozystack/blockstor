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
