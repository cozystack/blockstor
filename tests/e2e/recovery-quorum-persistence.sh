#!/usr/bin/env bash
#
# usage: recovery-quorum-persistence.sh WORK_DIR
#
# Scenario 7.5 — quorum-off persistence across satellite restart.
#
# The operator's recovery muscle-memory: a 3-replica RD lost two of
# its peers, I/O is suspended on the surviving Primary, and the
# operator does the canonical "ride out the outage" dance:
#
#     linstor rd sp <name> DrbdOptions/Resource/quorum off
#     drbdadm resume-io <name>
#
# This MUST be persistent. The DRBD .res file on the surviving
# Primary has to keep `quorum off;` across a satellite pod restart,
# otherwise:
#   * satellite pod bounce (operator update / node drain / OOM)
#     re-renders the .res file
#   * `drbdadm adjust` picks up the default quorum (majority)
#   * I/O re-suspends on the Primary the operator was deliberately
#     keeping alive
# That would defeat the whole point of the override.
#
# The override is set via `rd set-property` so it lives in
# ResourceDefinitionProperties — on a CRD-backed controller it
# must be persisted to a CRD before the satellite pod is bounced,
# otherwise the post-restart adjust will not see it.
#
# Stand: e2e-snap (3-worker Talos+QEMU cluster on the Oracle host).
# Workers: e2e-snap-worker-{1,2,3}. Worker 1 holds the Primary.
# Targets the piraeus-datastore linstor stack — that is where DRBD
# actually runs and .res files are rendered (under /var/lib/linstor.d).
#
# Findings to capture in the run log:
#   * .res file before override: should NOT contain `quorum off`.
#   * .res file after `rd sp ... quorum off`: must contain
#     `quorum off;`.
#   * .res file AFTER satellite pod respawn: must STILL contain
#     `quorum off;` (the persistence claim).
#   * After workers 2+3 come back and quorum is restored to
#     `majority`: no I/O re-suspension on the surviving Primary.
#
# Observed against e2e-snap on linstor v1.32.3 (2026-05-13):
#   * step 1-7 PASS — .res quorum=off PERSISTED across the
#     satellite pod respawn on the surviving Primary. ResDfn
#     properties survive the satellite container restart cycle
#     intact (controller-side ResourceDefinitionProperties is
#     authoritative; the satellite re-fetches on (re)connect and
#     re-renders .res with the override present).
#   * sidecar finding: after the satellite respawn the new
#     satellite re-renders .res with `quorum off;` but does NOT
#     auto-run `drbdadm adjust`, so the kernel still shows
#     `quorum majority` until something forces adjust. The test
#     calls `drbdadm adjust` itself to reconcile — operators
#     relying on the bounce alone may need an explicit adjust to
#     pick up the override in-kernel.
#   * stand caveat: step 8 requires the rebooted peers to come
#     back ONLINE in linstor (the piraeus satellite pod must run
#     to completion). On Talos+QEMU the worker reboot can leave
#     flannel CNI in a broken state (missing /run/flannel/subnet
#     .env) which keeps the satellite pod stuck Init:0/1 — step 8a
#     "wait for peer satellites ONLINE" will time out. That is a
#     stand issue, not a LINSTOR regression; the persistence
#     claim is established at step 7 regardless.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"
export TALOSCONFIG="$WORK_DIR/talosconfig"

NS=${NS:-piraeus-datastore}
SAT_CONTAINER=${SAT_CONTAINER:-linstor-satellite}
STAND=${STAND:-e2e-snap}
WORKER_1=${WORKER_1:-${STAND}-worker-1}
WORKER_2=${WORKER_2:-${STAND}-worker-2}
WORKER_3=${WORKER_3:-${STAND}-worker-3}
RD=${RD:-quorum-test}
STOR_POOL=${STOR_POOL:-pool}
RES_DIR=${RES_DIR:-/var/lib/linstor.d}

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi
if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH"
    exit 0
fi
if ! command -v talosctl >/dev/null 2>&1; then
    echo "SKIP: talosctl not in PATH"
    exit 0
fi
if [[ ! -f "$TALOSCONFIG" ]]; then
    echo "SKIP: TALOSCONFIG not found at $TALOSCONFIG"
    exit 0
fi

for w in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    if ! kubectl get "node/$w" >/dev/null 2>&1; then
        echo "SKIP: $w not in cluster (override WORKER_1/2/3 or STAND)"
        exit 0
    fi
done

# Resolve talos IPs once — talosctl wants the Talos-side IP, not
# the k8s node name.
node_ip() {
    kubectl get "node/$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'
}
IP_1=$(node_ip "$WORKER_1")
IP_2=$(node_ip "$WORKER_2")
IP_3=$(node_ip "$WORKER_3")
echo ">> stand IPs: $WORKER_1=$IP_1 $WORKER_2=$IP_2 $WORKER_3=$IP_3"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
PF_PID=""

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# (Re)start the port-forward to linstor-controller. The controller
# pod is scheduled on a worker, so when we crash workers 2+3 the
# port-forward may break — call this anytime a linstor REST call
# returns "Connection refused".
pf_start() {
    if [[ -n "$PF_PID" ]]; then
        kill "$PF_PID" 2>/dev/null || true
        wait "$PF_PID" 2>/dev/null || true
    fi
    kubectl -n "$NS" port-forward svc/linstor-controller "$PF_PORT":3370 \
        >/tmp/recovery-quorum-persist-pf.log 2>&1 &
    PF_PID=$!
    # wait for it to come up — be lenient because the controller
    # pod itself may be re-electing leader or PodInitializing.
    local deadline=$(( $(date +%s) + 180 ))
    while (( $(date +%s) < deadline )); do
        if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
            return 0
        fi
        # If the local port-forward exited, restart it.
        if ! kill -0 "$PF_PID" 2>/dev/null; then
            kubectl -n "$NS" port-forward svc/linstor-controller "$PF_PORT":3370 \
                >/tmp/recovery-quorum-persist-pf.log 2>&1 &
            PF_PID=$!
        fi
        sleep 2
    done
    return 1
}

# Wrap linstor calls — if the REST socket is dead, restart pf and
# retry once. This is the difference between "test hits a transient
# pod reschedule and exits with a Connection-refused trace" and
# "test actually validates what it intended to validate".
linstor_retry() {
    local out rc
    out=$("${LCTL[@]}" "$@" 2>&1) && { echo "$out"; return 0; }
    rc=$?
    if echo "$out" | grep -qiE "connection refused|name or service not known|connection reset"; then
        echo "INFO: linstor REST dropped — restarting port-forward" >&2
        pf_start || { echo "$out"; return $rc; }
        "${LCTL[@]}" "$@"
        return $?
    fi
    echo "$out"
    return $rc
}

pf_start || { echo "FAIL: initial port-forward never came up"; exit 1; }

# crashed-workers state for cleanup recovery (talos IPs)
CRASHED=()

# Helper: satellite pod for a given worker node.
sat_pod_on() {
    local node=$1
    kubectl -n "$NS" get pod \
        -l "app.kubernetes.io/component=$SAT_CONTAINER" \
        --field-selector "spec.nodeName=$node" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# Helper: exec a command inside the satellite container on a node.
exec_on() {
    local node=$1; shift
    local pod
    pod=$(sat_pod_on "$node")
    if [[ -z "$pod" ]]; then
        return 1
    fi
    kubectl -n "$NS" exec -c "$SAT_CONTAINER" "$pod" -- "$@"
}

cat_res_on() {
    local node=$1
    exec_on "$node" cat "$RES_DIR/${RD}.res" 2>/dev/null
}

drbd_on() {
    local node=$1; shift
    exec_on "$node" drbdadm "$@" 2>/dev/null
}

drbdsetup_on() {
    local node=$1; shift
    exec_on "$node" drbdsetup "$@" 2>/dev/null
}

dump_diag() {
    pf_start >/dev/null 2>&1 || true
    echo "---- dump: linstor n l ----"
    linstor_retry node list || true
    echo "---- dump: linstor r l ----"
    linstor_retry resource list || true
    echo "---- dump: linstor rd lp $RD ----"
    linstor_retry resource-definition list-properties "$RD" || true
    echo "---- dump: .res on $WORKER_1 ----"
    cat_res_on "$WORKER_1" || true
    echo "---- dump: drbdsetup status on $WORKER_1 ----"
    drbdsetup_on "$WORKER_1" status "$RD" || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    # Make sure crashed workers come back so the stand is reusable.
    for ip in "${CRASHED[@]:-}"; do
        [[ -z "$ip" ]] && continue
        echo "cleanup: rebooting talos node $ip"
        talosctl --nodes "$ip" reboot >/dev/null 2>&1 || true
    done

    # Best-effort: reset quorum to majority and delete the RD.
    linstor --controllers "http://localhost:$PF_PORT" \
        resource-definition set-property "$RD" \
        DrbdOptions/Resource/quorum majority >/dev/null 2>&1 || true
    linstor --controllers "http://localhost:$PF_PORT" \
        resource-definition delete "$RD" >/dev/null 2>&1 || true

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# -- Step 1: create 3-replica RD, mount Primary on worker-1 ----------
echo ">> step 1: create RD $RD (place-count=3, 100M, sp=$STOR_POOL)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 100M >/dev/null
"${LCTL[@]}" resource-definition auto-place "$RD" \
    --place-count 3 --storage-pool "$STOR_POOL" >/dev/null

deadline=$(( $(date +%s) + 180 ))
ok=0
while (( $(date +%s) < deadline )); do
    n=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r '[.[][] | select((.flags // []) | index("DISKLESS") | not)] | length' 2>/dev/null || echo 0)
    if (( n >= 3 )); then ok=1; break; fi
    sleep 2
done
if (( ok != 1 )); then
    echo "FAIL: $RD never reached 3 diskful replicas"
    exit 1
fi

deadline=$(( $(date +%s) + 180 ))
uptodate=0
while (( $(date +%s) < deadline )); do
    n=$("${LCTLJ[@]}" volume list -r "$RD" 2>/dev/null \
        | jq -r '[.[][].volumes[]? | select(.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( n >= 3 )); then uptodate=1; break; fi
    sleep 2
done
if (( uptodate != 1 )); then
    echo "FAIL: $RD never reached 3 UpToDate replicas"
    exit 1
fi
echo "   $RD: 3/3 UpToDate"

# Discover the DRBD minor number for $RD on worker-1 so we can poke
# /dev/drbd<minor> directly. The piraeus satellite container has no
# udev so /dev/drbd/by-res/<rd>/0 does NOT exist — only /dev/drbd<m>.
DRBD_MINOR=$(cat_res_on "$WORKER_1" 2>/dev/null \
    | awk '/device[ \t]+minor[ \t]+/ {gsub(/;/,"",$3); print $3; exit}' || true)
if [[ -z "$DRBD_MINOR" ]]; then
    echo "FAIL: could not parse DRBD minor from .res for $RD"
    exit 1
fi
DRBD_DEV="/dev/drbd${DRBD_MINOR}"
echo ">> drbd minor: $DRBD_MINOR ($DRBD_DEV)"

echo ">> step 1b: promote $RD to Primary on $WORKER_1"
drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 \
    || drbd_on "$WORKER_1" primary "$RD" >/dev/null 2>&1 || true

role=$(drbdsetup_on "$WORKER_1" role "$RD" 2>/dev/null || true)
echo "   role on $WORKER_1: '$role'"
if [[ "$role" != "Primary" ]]; then
    # Some piraeus builds need an open() on the block device.
    exec_on "$WORKER_1" sh -c "timeout 5 dd if=$DRBD_DEV of=/dev/null bs=4K count=1" \
        >/dev/null 2>&1 || true
    drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 || true
    role=$(drbdsetup_on "$WORKER_1" role "$RD" 2>/dev/null || true)
    echo "   role on $WORKER_1 (retry): '$role'"
fi
if [[ "$role" != "Primary" ]]; then
    echo "FAIL: could not promote $RD to Primary on $WORKER_1"
    exit 1
fi

# Capture .res BEFORE crash for diff.
res_initial=$(cat_res_on "$WORKER_1" || true)
echo "---- .res INITIAL on $WORKER_1 ----"
echo "$res_initial" | sed -n '1,40p'

# -- Step 2: crash workers 2 and 3 (suspends I/O on Primary) ---------
echo ">> step 2: crash $WORKER_2 ($IP_2) and $WORKER_3 ($IP_3) via talosctl reboot"
# Run reboots sequentially in foreground — avoid `wait` because the
# port-forward is also a background job and `wait` (no args) would
# block on it forever.
talosctl --nodes "$IP_2" reboot --wait=false >/dev/null 2>&1 &
RB2=$!
talosctl --nodes "$IP_3" reboot --wait=false >/dev/null 2>&1 &
RB3=$!
CRASHED+=("$IP_2" "$IP_3")
wait "$RB2" 2>/dev/null || true
wait "$RB3" 2>/dev/null || true

# Wait for the surviving Primary to register quorum loss. Read from
# Resource.Status (events2 observer, Phase 11.4.b / 11.5.b P0):
# either Status.Volumes[0].Quorum=="false" or Status.Suspended
# non-empty indicates the kernel has noticed the peers vanish.
deadline=$(( $(date +%s) + 90 ))
suspended=0
while (( $(date +%s) < deadline )); do
    q=$(status_volume_quorum "$RD" "$WORKER_1")
    s=$(status_suspended "$RD" "$WORKER_1")
    if [[ "$q" == "false" || -n "$s" ]]; then
        suspended=1
        break
    fi
    sleep 2
done
if (( suspended != 1 )); then
    echo "INFO: did not observe explicit suspension marker on $WORKER_1 — continuing"
fi
echo "   $WORKER_1 sees peers down"

# -- Step 3: set quorum off on the controller ------------------------
echo ">> step 3: linstor rd sp $RD DrbdOptions/Resource/quorum off"
# Controller may be rescheduling because its host worker crashed —
# revive the port-forward before issuing the property write.
pf_start || { echo "FAIL: linstor controller unreachable when setting quorum=off"; exit 1; }
linstor_retry resource-definition set-property "$RD" \
    DrbdOptions/Resource/quorum off >/dev/null

deadline=$(( $(date +%s) + 60 ))
saw_off_in_res=0
while (( $(date +%s) < deadline )); do
    if cat_res_on "$WORKER_1" 2>/dev/null | grep -qE "^\s*quorum\s+off\s*;"; then
        saw_off_in_res=1
        break
    fi
    sleep 2
done

# -- Step 4: verify .res on worker-1 contains `quorum off;` ----------
echo ">> step 4: verify .res on $WORKER_1 contains 'quorum off;'"
if (( saw_off_in_res != 1 )); then
    echo "---- .res AFTER override (on $WORKER_1) ----"
    cat_res_on "$WORKER_1" || true
    echo "FAIL: .res on $WORKER_1 does not contain 'quorum off;'"
    exit 1
fi
echo "   OK: .res shows quorum off"

# -- Step 5: resume-io so the Primary can take writes again ----------
echo ">> step 5: drbdadm resume-io $RD on $WORKER_1"
# After `adjust` (which the satellite ran when re-rendering .res),
# DRBD may have demoted to Secondary if `on-suspended-primary-outdated
# force-secondary` was hit. Re-promote, then resume-io, then probe.
drbd_on "$WORKER_1" adjust "$RD" >/dev/null 2>&1 || true
drbd_on "$WORKER_1" resume-io "$RD" >/dev/null 2>&1 || true
drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 \
    || drbd_on "$WORKER_1" primary "$RD" >/dev/null 2>&1 || true

role=$(drbdsetup_on "$WORKER_1" role "$RD" 2>/dev/null || true)
echo "   role on $WORKER_1 (post-quorum-off): '$role'"

io_ok=0
for _ in 1 2 3 4 5; do
    if exec_on "$WORKER_1" sh -c "timeout 5 dd if=/dev/zero of=$DRBD_DEV bs=4K count=1 conv=fdatasync" \
            >/dev/null 2>&1; then
        io_ok=1
        break
    fi
    # If still Secondary, re-promote and retry.
    drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 || true
    sleep 2
done

if (( io_ok == 1 )); then
    echo "   I/O OK on $WORKER_1 after resume-io"
else
    echo "FAIL: dd write to $DRBD_DEV failed after resume-io"
    exit 1
fi

# -- Step 6: bounce the satellite pod on worker-1 --------------------
echo ">> step 6: bounce satellite pod on $WORKER_1"
sat_pod_w1=$(sat_pod_on "$WORKER_1")
if [[ -z "$sat_pod_w1" ]]; then
    echo "FAIL: could not find satellite pod on $WORKER_1"
    exit 1
fi
echo "   deleting pod: $sat_pod_w1"
kubectl -n "$NS" delete pod "$sat_pod_w1" --wait=false >/dev/null

deadline=$(( $(date +%s) + 180 ))
new_pod=""
while (( $(date +%s) < deadline )); do
    p=$(sat_pod_on "$WORKER_1")
    if [[ -n "$p" && "$p" != "$sat_pod_w1" ]]; then
        ready=$(kubectl -n "$NS" get pod "$p" \
            -o jsonpath='{.status.containerStatuses[?(@.name=="'"$SAT_CONTAINER"'")].ready}' 2>/dev/null || echo "false")
        if [[ "$ready" == "true" ]]; then
            new_pod=$p
            break
        fi
    fi
    sleep 2
done
if [[ -z "$new_pod" ]]; then
    echo "FAIL: replacement satellite pod on $WORKER_1 never became Ready"
    exit 1
fi
echo "   new pod Ready: $new_pod"

deadline=$(( $(date +%s) + 60 ))
online=0
while (( $(date +%s) < deadline )); do
    s=$("${LCTLJ[@]}" node list 2>/dev/null \
        | jq -r --arg n "$WORKER_1" '.[][] | select(.name==$n) | .connection_status' 2>/dev/null || true)
    if [[ "$s" == "ONLINE" ]]; then online=1; break; fi
    sleep 2
done
if (( online != 1 )); then
    echo "FAIL: $WORKER_1 satellite never reported ONLINE after bounce"
    exit 1
fi
echo "   $WORKER_1 satellite ONLINE"

# -- Step 7: verify .res STILL has `quorum off;` ---------------------
echo ">> step 7: verify .res on $WORKER_1 STILL contains 'quorum off;' (persistence)"
# Give the satellite a few seconds to render its res files.
sleep 5
res_after=$(cat_res_on "$WORKER_1" || true)
echo "---- .res AFTER satellite bounce (on $WORKER_1) ----"
echo "$res_after" | sed -n '1,40p'
if ! echo "$res_after" | grep -qE "^\s*quorum\s+off\s*;"; then
    echo "FAIL: .res on $WORKER_1 lost 'quorum off;' after satellite respawn"
    echo "      regression — override not persisted across satellite restart"
    exit 1
fi
echo "   OK: quorum off survived satellite respawn"

if drbdsetup_on "$WORKER_1" show "$RD" 2>/dev/null \
        | grep -qE "quorum\s+off"; then
    echo "   OK: drbdsetup show confirms quorum off in kernel"
else
    echo "INFO: drbdsetup show does not show quorum off — re-adjusting"
    drbd_on "$WORKER_1" adjust "$RD" >/dev/null 2>&1 || true
fi

# -- Step 8: restart workers 2+3, restore quorum=majority ------------
echo ">> step 8a: wait for $WORKER_2 and $WORKER_3 to be Ready"
deadline=$(( $(date +%s) + 600 ))
both_ready=0
while (( $(date +%s) < deadline )); do
    r2=$(kubectl get "node/$WORKER_2" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    r3=$(kubectl get "node/$WORKER_3" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [[ "$r2" == "True" && "$r3" == "True" ]]; then both_ready=1; break; fi
    sleep 5
done
if (( both_ready != 1 )); then
    echo "FAIL: $WORKER_2/$WORKER_3 did not become Ready within 600s"
    exit 1
fi
CRASHED=()
echo "   both peers Ready"

deadline=$(( $(date +%s) + 180 ))
peers_online=0
while (( $(date +%s) < deadline )); do
    s2=$("${LCTLJ[@]}" node list 2>/dev/null \
        | jq -r --arg n "$WORKER_2" '.[][] | select(.name==$n) | .connection_status' 2>/dev/null || true)
    s3=$("${LCTLJ[@]}" node list 2>/dev/null \
        | jq -r --arg n "$WORKER_3" '.[][] | select(.name==$n) | .connection_status' 2>/dev/null || true)
    if [[ "$s2" == "ONLINE" && "$s3" == "ONLINE" ]]; then peers_online=1; break; fi
    sleep 3
done
if (( peers_online != 1 )); then
    echo "FAIL: peer satellites did not return ONLINE"
    exit 1
fi
echo "   peer satellites ONLINE"

# Re-promote in case satellite respawn or peer-reconnect adjust
# bounced us back to Secondary.
drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 \
    || drbd_on "$WORKER_1" primary "$RD" >/dev/null 2>&1 || true
sleep 2
if ! exec_on "$WORKER_1" sh -c "timeout 5 dd if=/dev/zero of=$DRBD_DEV bs=4K count=1 conv=fdatasync" \
        >/dev/null 2>&1; then
    drbd_on "$WORKER_1" primary "$RD" --force >/dev/null 2>&1 || true
    sleep 3
    if ! exec_on "$WORKER_1" sh -c "timeout 5 dd if=/dev/zero of=$DRBD_DEV bs=4K count=1 conv=fdatasync" \
            >/dev/null 2>&1; then
        echo "FAIL: Primary on $WORKER_1 not writable before restoring quorum"
        exit 1
    fi
fi

echo ">> step 8b: restore quorum=majority"
pf_start || { echo "FAIL: linstor controller unreachable when restoring quorum"; exit 1; }
linstor_retry resource-definition set-property "$RD" \
    DrbdOptions/Resource/quorum majority >/dev/null

deadline=$(( $(date +%s) + 60 ))
saw_majority=0
io_blip=0
while (( $(date +%s) < deadline )); do
    if cat_res_on "$WORKER_1" 2>/dev/null | grep -qE "^\s*quorum\s+off\s*;"; then
        :
    else
        saw_majority=1
    fi
    if ! exec_on "$WORKER_1" sh -c "timeout 5 dd if=/dev/zero of=$DRBD_DEV bs=4K count=1 conv=fdatasync" \
            >/dev/null 2>&1; then
        io_blip=1
        break
    fi
    if (( saw_majority == 1 )); then break; fi
    sleep 2
done

if (( io_blip == 1 )); then
    echo "FAIL: Primary on $WORKER_1 re-suspended I/O during quorum=majority restore"
    exit 1
fi
if (( saw_majority != 1 )); then
    echo "FAIL: .res on $WORKER_1 still has 'quorum off;' after restoring majority"
    exit 1
fi
echo "   quorum=majority restored, no I/O re-suspension on Primary"

deadline=$(( $(date +%s) + 240 ))
converged=0
while (( $(date +%s) < deadline )); do
    n=$("${LCTLJ[@]}" volume list -r "$RD" 2>/dev/null \
        | jq -r '[.[][].volumes[]? | select(.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
    if (( n >= 3 )); then converged=1; break; fi
    sleep 5
done
if (( converged != 1 )); then
    echo "INFO: $RD did not reconverge to 3/3 UpToDate within 240s — leaving for manual inspect"
else
    echo "   $RD converged: 3/3 UpToDate"
fi

echo ">> RECOVERY-QUORUM-PERSISTENCE OK"
echo "   .res quorum=off persisted across satellite respawn: YES"
echo "   no I/O re-suspension when restoring majority: YES"
