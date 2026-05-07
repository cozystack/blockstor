#!/usr/bin/env bash
# usage: install-oracle.sh WORK_DIR
# Spins up an upstream Java LINSTOR controller in the cluster as a reference
# oracle. Useful for contract-diff tests against a Go reimplementation.
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

ORACLE_IMAGE=${ORACLE_IMAGE:-quay.io/piraeusdatastore/piraeus-server:v1.33.2}

echo ">> deploying Java LINSTOR oracle ($ORACLE_IMAGE)"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: linstor-oracle
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: linstor-controller-oracle
  namespace: linstor-oracle
spec:
  replicas: 1
  selector: { matchLabels: { app: linstor-oracle } }
  template:
    metadata: { labels: { app: linstor-oracle } }
    spec:
      containers:
      - name: linstor-controller
        image: $ORACLE_IMAGE
        args: ["startController"]
        ports:
        - { name: rest,   containerPort: 3370 }
        - { name: rest-s, containerPort: 3371 }
        readinessProbe:
          tcpSocket: { port: 3370 }
          initialDelaySeconds: 20
---
apiVersion: v1
kind: Service
metadata:
  name: linstor-oracle
  namespace: linstor-oracle
spec:
  selector: { app: linstor-oracle }
  ports:
  - { name: rest, port: 3370, targetPort: rest }
EOF

kubectl -n linstor-oracle wait deploy/linstor-controller-oracle \
    --for=condition=Available --timeout=5m

echo ">> oracle ready at http://linstor-oracle.linstor-oracle.svc:3370/v1"
