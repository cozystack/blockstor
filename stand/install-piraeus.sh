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
kubectl -n piraeus-datastore wait deploy/piraeus-operator --for=condition=Available --timeout=5m

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

echo ">> piraeus install complete"
kubectl get pods -n piraeus-datastore
