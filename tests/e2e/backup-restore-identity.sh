#!/usr/bin/env bash
#
# usage: backup-restore-identity.sh WORK_DIR
#
# Acceptance test for the DRBD-identity-in-Spec refactor (clusterIP
# model). Proves that a plain `kubectl get -o yaml` backup +
# `kubectl apply` restore preserves every resource's DRBD identity
# (per-volume minor, per-node port, per-replica node-id) with NO
# resync / flap, exactly as Service.spec.clusterIP survives a backup.
#
# Steps:
#   1. Create 3 RDs (one multi-volume) with 2-3 diskful replicas +
#      auto-tiebreaker. Write known data, wait UpToDate.
#   2. Record per-volume minors, per-node ports, node-ids, current-uuids.
#   3. Backup all blockstor CRDs to a YAML file.
#   4. Scale blockstor down (controller Deployment + satellite DaemonSet).
#   5. Delete the RD/Resource/VD CRDs (NOT the on-disk DRBD/zvol data).
#   6. kubectl apply the backup.
#   7. Scale blockstor back up.
#   8. ASSERT: same minors/ports/node-ids; no resync (replicas stay
#      UpToDate, current-uuids unchanged, data md5 intact); Status
#      reconverges; nothing wedges.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

RD1=bkprest-single        # 1 volume, 2 replicas + tiebreaker
RD2=bkprest-multivol      # 2 volumes, 2 replicas + tiebreaker
RD3=bkprest-three         # 1 volume, 3 replicas

ALL_RDS=("$RD1" "$RD2" "$RD3")
BKP=/tmp/bkprest-backup.yaml
DATA_BYTES=$((4 * 1024 * 1024))   # 4 MiB of known data per volume

cleanup() {
    for rd in "${ALL_RDS[@]}"; do
        delete_rd "$rd" || true
    done
}
trap cleanup EXIT

# ---- helpers --------------------------------------------------------

# vol_minor RD VOL — the per-volume minor off RD.Spec.VolumeDefinitions.
vol_minor() {
    kubectl get resourcedefinition "$1" -o jsonpath="{.spec.volumeDefinitions[?(@.volumeNumber==$2)].drbdMinor}" 2>/dev/null
}

# res_port RD NODE — the per-node port off Resource.Spec.DRBDPort.
res_port() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.spec.drbdPort}' 2>/dev/null
}

# res_nodeid RD NODE — the per-replica node-id off Resource.Spec.DRBDNodeID.
res_nodeid() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.spec.drbdNodeID}' 2>/dev/null
}

# current_uuid RD NODE — the DRBD-9 current UUID for volume 0 on a node.
# Read via drbdadm show-gi (first token is the current-uuid).
current_uuid() {
    on_node "$2" bash -c "drbdadm show-gi ${1}/0 2>/dev/null | grep -oE 'current[^;]*' | head -1" 2>/dev/null \
        || on_node "$2" bash -c "drbdsetup show-gi ${1} 0 2>/dev/null | head -1"
}

fail() { echo "FAIL: $*" >&2; exit 1; }

# ---- 1. create -------------------------------------------------------

# AutoAddQuorumTiebreaker=false on every RD so the topology is
# deterministic (explicit diskful replicas only — no auto-witness
# turning the would-be-3rd-diskful into a Diskless TIE_BREAKER). The
# identity-preservation contract is identical either way; this just
# makes the assertions deterministic.
echo ">> create RD1 ($RD1): 1 volume, 2 diskful on $N1+$N2"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD1}}
spec:
  props: {DrbdOptions/AutoAddQuorumTiebreaker: "false"}
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF

echo ">> create RD2 ($RD2): 2 volumes, 2 diskful on $N1+$N3"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD2}}
spec:
  props: {DrbdOptions/AutoAddQuorumTiebreaker: "false"}
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
    - {volumeNumber: 1, sizeKib: 65536}
EOF

echo ">> create RD3 ($RD3): 1 volume, 3 diskful on $N1+$N2+$N3"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD3}}
spec:
  props: {DrbdOptions/AutoAddQuorumTiebreaker: "false"}
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF

create_res() {
    local rd=$1 node=$2
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${node}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${node}
  props: {StorPoolName: stand}
EOF
}

create_res "$RD1" "$N1"; create_res "$RD1" "$N2"
create_res "$RD2" "$N1"; create_res "$RD2" "$N3"
create_res "$RD3" "$N1"; create_res "$RD3" "$N2"; create_res "$RD3" "$N3"

echo ">> wait all replicas UpToDate"
wait_uptodate "$RD1" "$N1" "$N2" 0
wait_uptodate "$RD2" "$N1" "$N3" 0
wait_uptodate "$RD2" "$N1" "$N3" 1
wait_uptodate "$RD3" "$N1" "$N2" 0
wait_disk_state "$RD3" "$N3" UpToDate 180 0

echo ">> stage known data on volume devices (primary node)"
declare -A MD5
stage_data() {
    local rd=$1 node=$2 vol=$3
    local minor dev md5
    minor=$(vol_minor "$rd" "$vol")
    dev="/dev/drbd${minor}"
    md5=$(on_node "$node" bash -c "
        drbdadm primary --force ${rd} 2>/dev/null || true
        test -b ${dev} || { echo MISSING; exit 2; }
        dd if=/dev/urandom of=${dev} bs=4096 count=$((DATA_BYTES/4096)) status=none oflag=direct
        dd if=${dev} bs=4096 count=$((DATA_BYTES/4096)) status=none iflag=direct | md5sum | awk '{print \$1}'
    ")
    [[ "$md5" == MISSING ]] && fail "$dev not a block device on $node"
    MD5["${rd}/${vol}"]=$md5
    echo "  ${rd} vol${vol} on ${node} (${dev}) md5=${md5}"
}
stage_data "$RD1" "$N1" 0
stage_data "$RD2" "$N1" 0
stage_data "$RD2" "$N1" 1
stage_data "$RD3" "$N1" 0

# demote back so the cluster is in a clean steady state
for rd in "${ALL_RDS[@]}"; do on_node "$N1" bash -c "drbdadm secondary ${rd} 2>/dev/null || true"; done

# ---- 2. record identities -------------------------------------------

echo ">> record identities BEFORE backup"
declare -A BEFORE
record_identities() {
    BEFORE["minor:${RD1}:0"]=$(vol_minor "$RD1" 0)
    BEFORE["minor:${RD2}:0"]=$(vol_minor "$RD2" 0)
    BEFORE["minor:${RD2}:1"]=$(vol_minor "$RD2" 1)
    BEFORE["minor:${RD3}:0"]=$(vol_minor "$RD3" 0)

    for spec in "${RD1}:${N1}" "${RD1}:${N2}" "${RD2}:${N1}" "${RD2}:${N3}" \
                "${RD3}:${N1}" "${RD3}:${N2}" "${RD3}:${N3}"; do
        local rd=${spec%%:*} node=${spec##*:}
        BEFORE["port:${rd}:${node}"]=$(res_port "$rd" "$node")
        BEFORE["nodeid:${rd}:${node}"]=$(res_nodeid "$rd" "$node")
    done
}
record_identities

for k in $(printf '%s\n' "${!BEFORE[@]}" | sort); do
    [[ -z "${BEFORE[$k]}" ]] && fail "identity $k is empty before backup (allocator did not stamp Spec)"
    echo "  $k = ${BEFORE[$k]}"
done

echo ">> record current-uuids BEFORE backup"
declare -A UUID_BEFORE
UUID_BEFORE["${RD1}:${N1}"]=$(current_uuid "$RD1" "$N1")
UUID_BEFORE["${RD1}:${N2}"]=$(current_uuid "$RD1" "$N2")
UUID_BEFORE["${RD3}:${N1}"]=$(current_uuid "$RD3" "$N1")
for k in "${!UUID_BEFORE[@]}"; do echo "  uuid $k = ${UUID_BEFORE[$k]}"; done

# ---- 3. backup -------------------------------------------------------

echo ">> backup all blockstor CRDs to $BKP"
kubectl get resourcedefinitions,resources,volumedefinitions.blockstor.cozystack.io,storagepools,nodes.blockstor.cozystack.io,resourcegroups \
    -A -o yaml > "$BKP" 2>/dev/null || \
kubectl get resourcedefinitions,resources,storagepools,nodes.blockstor.cozystack.io,resourcegroups \
    -A -o yaml > "$BKP"
echo "  backup size: $(wc -l < "$BKP") lines"

# ---- 4. shut down blockstor -----------------------------------------

echo ">> scale blockstor down (controller + satellites)"
kubectl -n "$NS" scale deploy/blockstor-controller --replicas=0
# DaemonSets can't scale; park it on an unsatisfiable nodeSelector.
kubectl -n "$NS" patch ds/blockstor-satellite --type merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"bkprest/parked":"true"}}}}}'

echo ">> wait for blockstor pods to drain"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    n=$(kubectl -n "$NS" get pods -l 'app in (blockstor-controller,blockstor-satellite)' --no-headers 2>/dev/null | wc -l)
    (( n == 0 )) && break
    sleep 2
done
echo "  blockstor pods remaining: $(kubectl -n "$NS" get pods -l 'app in (blockstor-controller,blockstor-satellite)' --no-headers 2>/dev/null | wc -l)"

# ---- 5. delete the CRD objects (NOT on-disk data) -------------------

echo ">> delete RD/Resource CRD objects (leave zvols + DRBD metadata intact)"
# Strip finalizers so deletion completes with satellites down (the
# satellite finalizer can't run — but we must NOT touch on-disk data).
for rd in "${ALL_RDS[@]}"; do
    for r in $(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null | awk -v p="${rd}." '$1 ~ "^"p {print $1}'); do
        kubectl patch resource "$r" --type merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
        kubectl delete resource "$r" --wait=false 2>/dev/null || true
    done
    kubectl delete resourcedefinition "$rd" --wait=false 2>/dev/null || true
done

# wait until the CRD objects are gone from the apiserver
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    left=0
    for rd in "${ALL_RDS[@]}"; do
        kubectl get resourcedefinition "$rd" >/dev/null 2>&1 && left=$((left+1))
    done
    (( left == 0 )) && break
    sleep 2
done
echo "  RDs still present: $left"
(( left == 0 )) || fail "RD CRD objects did not delete with blockstor down"

# ---- 6. restore ------------------------------------------------------

echo ">> restore from backup"
# Strip server-managed metadata (resourceVersion / uid /
# creationTimestamp / generation / managedFields) and the status
# stanza so `kubectl apply` recreates the objects cleanly. The
# identity lives in spec, which we keep verbatim — that is the whole
# point of the test.
python3 - "$BKP" > /tmp/bkprest-restore.yaml <<'PY'
import sys, yaml
docs = list(yaml.safe_load_all(open(sys.argv[1])))
out = []
for d in docs:
    if not d:
        continue
    items = d.get("items", [d]) if d.get("kind","").endswith("List") else [d]
    for it in items:
        md = it.get("metadata", {})
        for k in ("resourceVersion","uid","creationTimestamp","generation","managedFields","selfLink","ownerReferences"):
            md.pop(k, None)
        ann = md.get("annotations", {})
        ann.pop("kubectl.kubernetes.io/last-applied-configuration", None)
        if not ann:
            md.pop("annotations", None)
        it.pop("status", None)
        out.append(it)
yaml.safe_dump_all(out, sys.stdout, default_flow_style=False)
PY
kubectl apply -f /tmp/bkprest-restore.yaml 2>&1 | tail -12

# ---- 7. start blockstor ---------------------------------------------

echo ">> scale blockstor back up"
kubectl -n "$NS" patch ds/blockstor-satellite --type json \
    -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector/bkprest~1parked"}]' 2>/dev/null \
    || kubectl -n "$NS" patch ds/blockstor-satellite --type merge -p '{"spec":{"template":{"spec":{"nodeSelector":null}}}}'
kubectl -n "$NS" scale deploy/blockstor-controller --replicas=1

echo ">> wait for blockstor to come back"
kubectl -n "$NS" rollout status deploy/blockstor-controller --timeout=120s
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    ready=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o 'jsonpath={range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name} {end}' 2>/dev/null | wc -w)
    (( ready >= 3 )) && break
    sleep 3
done
echo "  satellite ready pods: $ready"

# ---- 8. ASSERT -------------------------------------------------------

echo ">> ASSERT identities preserved (no reallocation)"
record_after() {
    declare -gA AFTER
    AFTER["minor:${RD1}:0"]=$(vol_minor "$RD1" 0)
    AFTER["minor:${RD2}:0"]=$(vol_minor "$RD2" 0)
    AFTER["minor:${RD2}:1"]=$(vol_minor "$RD2" 1)
    AFTER["minor:${RD3}:0"]=$(vol_minor "$RD3" 0)
    for spec in "${RD1}:${N1}" "${RD1}:${N2}" "${RD2}:${N1}" "${RD2}:${N3}" \
                "${RD3}:${N1}" "${RD3}:${N2}" "${RD3}:${N3}"; do
        local rd=${spec%%:*} node=${spec##*:}
        AFTER["port:${rd}:${node}"]=$(res_port "$rd" "$node")
        AFTER["nodeid:${rd}:${node}"]=$(res_nodeid "$rd" "$node")
    done
}
record_after

ident_ok=true
for k in "${!BEFORE[@]}"; do
    if [[ "${BEFORE[$k]}" != "${AFTER[$k]:-}" ]]; then
        echo "  IDENTITY CHANGED: $k  before=${BEFORE[$k]}  after=${AFTER[$k]:-<empty>}" >&2
        ident_ok=false
    fi
done
$ident_ok || fail "DRBD identities were NOT preserved across backup/restore (allocator reallocated)"
echo "  all minors/ports/node-ids identical before/after"

echo ">> ASSERT no resync / flap: every replica UpToDate, no SyncTarget/Inconsistent"
wait_uptodate "$RD1" "$N1" "$N2" 0
wait_uptodate "$RD2" "$N1" "$N3" 0
wait_uptodate "$RD2" "$N1" "$N3" 1
wait_uptodate "$RD3" "$N1" "$N2" 0
wait_disk_state "$RD3" "$N3" UpToDate 180 0

# No replica may be SyncTarget/SyncSource/Inconsistent right now.
for spec in "${RD1}:${N1}:${N2}" "${RD1}:${N2}:${N1}" \
            "${RD2}:${N1}:${N3}" "${RD2}:${N3}:${N1}" \
            "${RD3}:${N1}:${N2}" "${RD3}:${N2}:${N1}" "${RD3}:${N3}:${N1}"; do
    rd=${spec%%:*}; rest=${spec#*:}; node=${rest%%:*}; peer=${rest##*:}
    repl=$(status_replication_state "$rd" "$node" "$peer")
    case "$repl" in
        SyncTarget|SyncSource|PausedSyncT|PausedSyncS|StartingSyncS|StartingSyncT|WFBitMapS|WFBitMapT)
            fail "RESYNC detected: ${rd} ${node}->${peer} replication=$repl (restore triggered a flap)"
            ;;
    esac
done
echo "  no resync in progress on any peer"

echo ">> ASSERT current-uuids unchanged (no new-current-uuid bump)"
uuid_ok=true
for k in "${!UUID_BEFORE[@]}"; do
    rd=${k%%:*}; node=${k##*:}
    after=$(current_uuid "$rd" "$node")
    if [[ -n "${UUID_BEFORE[$k]}" && "${UUID_BEFORE[$k]}" != "$after" ]]; then
        echo "  CURRENT-UUID CHANGED: $k  before=${UUID_BEFORE[$k]}  after=$after" >&2
        uuid_ok=false
    fi
done
$uuid_ok || fail "current-uuid changed across restore — DRBD regenerated GI (would force resync)"
echo "  current-uuids unchanged"

echo ">> ASSERT data md5 intact"
for k in "${!MD5[@]}"; do
    rd=${k%%/*}; vol=${k##*/}
    minor=$(vol_minor "$rd" "$vol")
    dev="/dev/drbd${minor}"
    got=$(on_node "$N1" bash -c "
        drbdadm primary ${rd} 2>/dev/null || true
        test -b ${dev} || { echo MISSING; exit 2; }
        dd if=${dev} bs=4096 count=$((DATA_BYTES/4096)) status=none iflag=direct | md5sum | awk '{print \$1}'
    ")
    on_node "$N1" bash -c "drbdadm secondary ${rd} 2>/dev/null || true"
    [[ "$got" == "${MD5[$k]}" ]] || fail "data md5 mismatch ${rd} vol${vol}: before=${MD5[$k]} after=$got"
    echo "  ${rd} vol${vol} md5 intact (${got})"
done

echo ">> ASSERT Status reconverged (disk-state re-populated by observer)"
# Poll rather than instant-read: the md5 reads above toggled
# primary/secondary, which briefly perturbs the events2 observer's
# diskState before it re-stamps UpToDate. wait_disk_state tolerates
# that transient window (the no-resync section already proved the
# replicas are UpToDate).
for spec in "${RD1}:${N1}" "${RD3}:${N1}"; do
    rd=${spec%%:*}; node=${spec##*:}
    wait_disk_state "$rd" "$node" UpToDate 60 0 \
        || fail "Status did not reconverge: ${rd}.${node} never re-stamped UpToDate"
done
echo "  Status reconverged"

echo "PASS: backup/restore preserved every DRBD identity with no resync/flap"
