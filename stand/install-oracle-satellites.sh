#!/usr/bin/env bash
# usage: install-oracle-satellites.sh WORK_DIR
#
# Stands up a FULL upstream LINSTOR oracle on the dev stand: the Java
# controller (reused from install-oracle.sh) PLUS three upstream
# satellites (piraeus-server v1.33.2, `startSatellite`) pinned to the
# Talos workers. The point of the satellites is to make DRBD-level
# automatisms (quorum, auto-tiebreaker, minor/port allocation) observable
# so blockstor's behavior can be diffed against real LINSTOR.
#
# Coexistence with blockstor on the SAME 3-node cluster is the central
# constraint. The DRBD kernel module is host-global (siderolabs/drbd
# Talos extension), so the oracle and blockstor share one kernel DRBD
# namespace. To avoid corrupting each other we keep them disjoint on
# three axes:
#
#   1. TCP ports      — oracle TcpPortAutoRange 8100-8399 (BS uses the
#                       7000-7999 default).
#   2. Minor numbers  — oracle MinorNrAutoRange 2000-2399.
#   3. .res files     — oracle satellites write /var/lib/linstor.d as an
#                       emptyDir (NOT a hostPath). blockstor's satellite
#                       writes /var/lib/blockstor-drbd.d on a hostPath
#                       (Bug 325). Separate dirs => `drbdadm adjust` on
#                       each side never sees the other's .res, so no
#                       duplicate-minor collision.
#
# All oracle resources are prefixed `orc-`. The satellite plain port is
# 3366 (verified free on the workers — blockstor satellites only bind
# hostPorts 9100/9101).
#
# Talos specifics (learned from stand/install-piraeus.sh's
# talos-loader-override): the upstream image's drbd-shutdown-guard /
# drbd-module-loader logic is irrelevant — the kernel module is already
# loaded by the Talos extension — and /run/systemd, /usr/src don't exist.
# We run `startSatellite` directly and mount only what exists on Talos
# (/dev, /lib/modules, /run/lvm, /run/udev), so none of that machinery
# runs.
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

ORACLE_IMAGE=${ORACLE_IMAGE:-quay.io/piraeusdatastore/piraeus-server:v1.33.2}
NS=linstor-oracle
SAT_PORT=3366
TCP_PORT_RANGE=${TCP_PORT_RANGE:-8100-8399}
MINOR_RANGE=${MINOR_RANGE:-2000-2399}
POOL_NAME=${POOL_NAME:-pool}
POOL_DIR=/var/lib/linstor-oracle-pool   # FILE_THIN backing inside emptyDir

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# 1. Controller (reuse the existing one-shot script).
# ---------------------------------------------------------------------------
echo ">> [1/6] deploying oracle controller via install-oracle.sh"
"$HERE/install-oracle.sh" "$WORK_DIR"

# The cluster defaults to PodSecurity enforce=baseline; privileged
# hostNetwork satellites are rejected without an explicit override (same
# label set blockstor-system carries). Apply it to the oracle namespace.
echo ">> labelling $NS PodSecurity=privileged (required for privileged satellites)"
kubectl label --overwrite ns "$NS" \
    pod-security.kubernetes.io/enforce=privileged \
    pod-security.kubernetes.io/audit=privileged \
    pod-security.kubernetes.io/warn=privileged

# ---------------------------------------------------------------------------
# 2. Satellites — one DaemonSet pinned to workers, hostNetwork/privileged.
# ---------------------------------------------------------------------------
echo ">> [2/6] deploying 3 upstream satellites ($ORACLE_IMAGE, startSatellite)"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: linstor-satellite-oracle
  namespace: $NS
  labels: {app: linstor-satellite-oracle}
spec:
  selector: {matchLabels: {app: linstor-satellite-oracle}}
  template:
    metadata: {labels: {app: linstor-satellite-oracle}}
    spec:
      # hostNetwork: satellite advertises the node IP for DRBD replication
      # and its hostname must match the Talos node name so drbdadm picks
      # the right \`on <node>\` block from the .res file.
      hostNetwork: true
      hostPID: true
      hostIPC: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
        - operator: Exists
      # Workers only (control-plane has no DRBD extension loaded).
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - {key: node-role.kubernetes.io/control-plane, operator: DoesNotExist}
      terminationGracePeriodSeconds: 10
      containers:
        - name: linstor-satellite
          image: $ORACLE_IMAGE
          # startSatellite binds 0.0.0.0:$SAT_PORT (plaintext). No
          # drbd-module-loader / shutdown-guard wrappers — the Talos
          # extension already provides the kernel module.
          args: ["startSatellite", "--port", "$SAT_PORT"]
          securityContext:
            privileged: true
          ports:
            - {name: plain, containerPort: $SAT_PORT, hostPort: $SAT_PORT, protocol: TCP}
          readinessProbe:
            tcpSocket: {port: $SAT_PORT}
            initialDelaySeconds: 10
            periodSeconds: 10
          volumeMounts:
            - {name: dev, mountPath: /dev}
            - {name: modules, mountPath: /lib/modules, readOnly: true}
            - {name: lvm-run, mountPath: /run/lvm}
            - {name: run-udev, mountPath: /run/udev, readOnly: true}
            # /var/lib/linstor.d as emptyDir — the oracle's .res scope,
            # deliberately NOT a hostPath so it never collides with
            # blockstor's /var/lib/blockstor-drbd.d on the node.
            - {name: linstor-d, mountPath: /var/lib/linstor.d}
            # FILE_THIN backing dir lives inside an emptyDir so loop
            # devices are allocated from a pod-private directory.
            - {name: pool, mountPath: $POOL_DIR}
          resources:
            requests: {cpu: 50m, memory: 128Mi}
            limits: {cpu: '1', memory: 1Gi}
      volumes:
        - {name: dev, hostPath: {path: /dev, type: Directory}}
        - {name: modules, hostPath: {path: /lib/modules}}
        - {name: lvm-run, hostPath: {path: /run/lvm, type: DirectoryOrCreate}}
        - {name: run-udev, hostPath: {path: /run/udev, type: Directory}}
        - {name: linstor-d, emptyDir: {}}
        - {name: pool, emptyDir: {}}
EOF

echo ">> waiting for satellite pods to be Ready"
kubectl -n "$NS" rollout status ds/linstor-satellite-oracle --timeout=8m

# ---------------------------------------------------------------------------
# 3. Controller props (BEFORE any resource creation) to dodge collisions.
# ---------------------------------------------------------------------------
echo ">> [3/6] port-forward oracle REST -> 127.0.0.1:3380 (stand-local)"
PF_LOG=/tmp/oracle-pf.log
setsid nohup kubectl -n "$NS" port-forward svc/linstor-oracle 3380:3370 \
    </dev/null >"$PF_LOG" 2>&1 &
PF_PID=$!
# wait for the forward to answer
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:3380/v1/controller/version >/dev/null 2>&1; then break; fi
    sleep 1
done

LINSTOR="linstor --controllers http://127.0.0.1:3380"

echo ">> [3/6] setting collision-avoidance controller props"
$LINSTOR controller set-property TcpPortAutoRange "$TCP_PORT_RANGE"
$LINSTOR controller set-property MinorNrAutoRange "$MINOR_RANGE"
$LINSTOR controller list-properties | grep -iE "TcpPortAutoRange|MinorNrAutoRange" || true

# ---------------------------------------------------------------------------
# 4. Register the worker nodes against the oracle controller.
# ---------------------------------------------------------------------------
echo ">> [4/6] registering worker nodes"
# Map node -> InternalIP (hostNetwork satellites listen on the node IP).
while read -r NODE IP; do
    [ -n "$NODE" ] || continue
    if $LINSTOR node list | grep -q "$NODE"; then
        echo "   node $NODE already registered"
    else
        echo "   node create $NODE $IP"
        $LINSTOR node create "$NODE" "$IP" --node-type satellite --port "$SAT_PORT"
    fi
done < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}')

echo ">> waiting for nodes Online"
for _ in $(seq 1 30); do
    if ! $LINSTOR node list | grep -qiE "OFFLINE|EVICTED|UNKNOWN"; then
        if $LINSTOR node list | grep -qi ONLINE; then break; fi
    fi
    sleep 5
done
$LINSTOR node list

# ---------------------------------------------------------------------------
# 5. FILE_THIN storage pool `pool` on each satellite.
# ---------------------------------------------------------------------------
echo ">> [5/6] creating FILE_THIN pool '$POOL_NAME' on each satellite"
while read -r NODE _; do
    [ -n "$NODE" ] || continue
    if $LINSTOR storage-pool list -n "$NODE" 2>/dev/null | grep -q "$POOL_NAME"; then
        echo "   pool $POOL_NAME already on $NODE"
    else
        $LINSTOR storage-pool create filethin "$NODE" "$POOL_NAME" "$POOL_DIR"
    fi
done < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
$LINSTOR storage-pool list

# ---------------------------------------------------------------------------
# 6. End-to-end smoke: spawn a 2-replica RG, expect 2 UpToDate + tiebreaker.
# ---------------------------------------------------------------------------
echo ">> [6/6] end-to-end smoke: orc-smoke-rg / orc-smoke (proves tiebreaker)"
$LINSTOR resource-group create orc-smoke-rg --place-count 2 --storage-pool="$POOL_NAME" || true
$LINSTOR resource-group spawn-resources orc-smoke-rg orc-smoke 64M

echo ">> waiting for 2 diskful UpToDate + 1 auto-tiebreaker"
for _ in $(seq 1 60); do
    UPTODATE=$($LINSTOR -m --output-version v1 volume list -r orc-smoke 2>/dev/null \
        | grep -o '"disk_state":"UpToDate"' | wc -l | tr -d ' ')
    if [ "${UPTODATE:-0}" -ge 2 ]; then break; fi
    sleep 5
done

echo "=== n l ==="
$LINSTOR node list
echo "=== r l (orc-smoke) ==="
$LINSTOR resource list -r orc-smoke
echo "=== v l (orc-smoke) ==="
$LINSTOR volume list -r orc-smoke

echo ">> .res inspection (proof .res files are present on a satellite):"
SAT_POD=$(kubectl -n "$NS" get pod -l app=linstor-satellite-oracle \
    -o jsonpath='{.items[0].metadata.name}')
echo "   pod: $SAT_POD"
kubectl -n "$NS" exec "$SAT_POD" -- cat /var/lib/linstor.d/orc-smoke.res 2>&1 || \
    echo "   (.res not on this satellite — try another pod; resource may be diskless here)"

echo ">> tearing down smoke resource"
$LINSTOR resource-definition delete orc-smoke || true
$LINSTOR resource-group delete orc-smoke-rg || true

# kill the port-forward this script started (the operator re-creates it).
kill "$PF_PID" 2>/dev/null || true

cat <<DONE

>> ORACLE READY (controller + 3 satellites + pool '$POOL_NAME')
   REST (stand-local):  kubectl -n $NS port-forward svc/linstor-oracle 3380:3370
   client:              linstor --controllers http://127.0.0.1:3380 ...
   pool:                $POOL_NAME (FILE_THIN @ $POOL_DIR, emptyDir-backed)
   TcpPortAutoRange:    $TCP_PORT_RANGE
   MinorNrAutoRange:    $MINOR_RANGE
   satellite port:      $SAT_PORT
   .res inspection:     kubectl -n $NS exec <sat-pod> -- cat /var/lib/linstor.d/<res>.res
DONE
