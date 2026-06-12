#!/usr/bin/env bash
#
# usage: quorum-loss-2tb-recovery.sh WORK_DIR
#
# COV-011 release gate — 2 TiB quorum-loss + recovery WITHOUT node reboot.
#
# Production fear being pinned: a DRBD deadlock where losing quorum under
# active IO wedges the volume such that only a node reboot recovers it.
# This scenario proves the opposite contract on a large (2 TiB, zfs-thick
# zvol-backed) volume:
#
#   * losing quorum mid-IO SUSPENDS IO (on-no-quorum=suspend-io: the
#     writer blocks in the kernel, no EIO surfaces), and
#   * once quorum returns, the suspended IO RESUMES by itself, the
#     cluster heals to all-UpToDate, and NO node was rebooted —
#     asserted via /proc/sys/kernel/random/boot_id captured before the
#     outage and re-read after recovery on ALL workers (plus a
#     monotonically-increasing /proc/uptime corroboration).
#
# Topology: the canonical 2-diskful + auto-tiebreaker witness shape.
#   N1 = Primary diskful (zfs-thick), N2 = Secondary diskful (zfs-thick),
#   N3 = DISKLESS TIE_BREAKER witness (auto-added by the RD reconciler).
#   quorum=majority over 3 voters; on-no-quorum=suspend-io.
#
# OUTAGE MECHANISM (and why): per-link iptables DROP of the resource's
# DRBD mesh port (all four src/dst x sport/dport combinations, exactly
# the drop_pair recipe proven by quorum-tiebreaker-no-return.sh /
# state-standalone-partition.sh / network-partition.sh). This is the
# strongest node-outage model available to a scenario on this stand:
#   - it kills the DRBD replication links at the network layer the same
#     way a dead node does (no graceful `drbdadm disconnect` handshake,
#     DRBD has to detect the loss via ping-timeout/ko-count);
#   - there is no VM-level kill helper reachable from a scenario
#     (stand/up.sh drives talosctl+qemu on the stand HOST; scenarios
#     only have kubectl), and stopping the satellite pod would NOT
#     break the kernel-level replication links anyway (DRBD lives in
#     the host kernel, not the pod);
#   - kubectl/API traffic is untouched (different ports), so the test
#     can keep observing kernel truth from every node throughout the
#     outage — which is precisely what "no reboot needed" recovery
#     looks like from an operator's seat.
#
# Flow:
#   1. Preflight: 3 workers; ${POOL} StoragePool CR on ALL workers with
#      providerKind=ZFS (stand-"big"-specific — SKIP otherwise); free
#      capacity headroom for a thick 2 TiB zvol on both diskful nodes.
#   2. Create RD + 2 TiB VD on N1+N2 (place-count 2) + auto-tiebreaker.
#      zfs-thick is in the skip-initial-sync class (pkg/satellite/
#      providerkind.go IsThinOrZFS), so creation must reach 2/2
#      UpToDate inside CREATE_TIMEOUT — a full 2 TiB initial sync here
#      is a regression and fails the gate. skipInitialSync=true stamp
#      is asserted explicitly for a sharp failure mode.
#   3. Seed an 8 MiB marker region at offset 0 on the Primary (md5
#      captured), then start a continuous 1 tick/s direct-IO writer at
#      offsets >= 128 MiB (never overlapping the marker).
#   4. Sever the SECONDARY diskful (N2) from both peers -> quorum must
#      HOLD on the Primary (N1+witness = 2/3 voters): writer keeps
#      ticking, no suspension. Heal, wait resync back to UpToDate.
#   5. Sever BOTH the secondary diskful AND the witness mid-IO -> the
#      Primary drops to 1/3 voters: assert quorum lost, IO suspended
#      (writer ticks FREEZE, zero write errors — suspend-io must block,
#      not fail), writer process still alive, no kernel crash.
#   6. Restore the links -> within bounded windows: quorum returns,
#      the frozen writer RESUMES (ticks advance again) and shuts down
#      cleanly with zero errors, both diskful replicas return to
#      UpToDate, marker md5 intact, drbdsetup clean on all workers,
#      single-Primary invariant holds, boot_id unchanged on ALL
#      workers (the no-reboot assert).
#   7. Teardown + no-orphans assert (CRDs gone, no leaked ${RD}_* zvol
#      on either diskful node). dump_diag on any failure.
#
# Every wait below is bounded by an explicit deadline on a concrete
# condition — no blind sleeps on the critical path.
#
# NOTE for the lane harness: this scenario is gated on the zfs-thick
# pool (stand "big") and SKIPs elsewhere, so it must stay in
# stand/run-scenarios-only.sh SKIP_ALLOWLIST. Worst-case budget exceeds
# the generic 600 s lane timeout; on stand "big" invoke it directly:
#   ./tests/e2e/quorum-loss-2tb-recovery.sh .work/big

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

# Short RD name: zvol becomes <zpool>/q2tb_00000 — stays well clear of
# the ZFS dataset-name length ceiling exercised by rd-name-length-48.
RD=q2tb
POOL=${STORPOOL:-zfs-thick}
SIZE_KIB=${SIZE_KIB:-2147483648}                   # 2 TiB
REQUIRED_FREE_KIB=$(( SIZE_KIB + SIZE_KIB / 20 ))  # +5% zvol/meta headroom
MARKER_BYTES=$(( 8 * 1024 * 1024 ))                # marker at offset 0
WRITER_BASE_BLOCK=32768                            # 128 MiB in 4K blocks
WRITER_STOP=/tmp/q2tb-e2e.stop
WRITER_LOG=/tmp/q2tb-writer.log
WRITER_ERR=/tmp/q2tb-writer.err
WRITER_PID=""

N1=$WORKER_1   # Primary diskful
N2=$WORKER_2   # Secondary diskful — the step-4 outage target
N3=$WORKER_3   # auto-tiebreaker witness — joins the step-5 outage

# Bounded-wait budgets (seconds). All sized for a busy QEMU stand.
CREATE_TIMEOUT=300    # 2 TiB thick zvol create + skip-init UpToDate
WITNESS_TIMEOUT=120   # RD reconciler stamps the TIE_BREAKER witness
SEVER_TIMEOUT=90      # DRBD ping-timeout/ko-count detects a dead link
HOLD_PROBE=20         # quorum-hold observation window in step 4
RESYNC_TIMEOUT=300    # post-outage bitmap resync of the writer deltas
QLOSS_TIMEOUT=90      # Primary notices 1/3 voters -> quorum lost
FREEZE_CONFIRM=20     # window proving the writer is frozen, not erroring
RESUME_TIMEOUT=240    # quorum return + suspended IO resumes
FINAL_TIMEOUT=300     # full reconvergence (UpToDate + clean status)
ORPHAN_TIMEOUT=60     # teardown: CRDs drained

# ---------------------------------------------------------------------------
# iptables sever helpers — same proven recipe as
# quorum-tiebreaker-no-return.sh: drop by peer IP + DRBD port, all four
# direction/port combinations, with BLOCKED_PAIRS bookkeeping so the
# EXIT trap always heals the stand no matter where the test died.
# ---------------------------------------------------------------------------
declare -a BLOCKED_PAIRS=()
DRBD_PORT=""

drop_pair() {   # node peer_ip
    local node=$1 peer_ip=$2
    on_node "$node" sh -c "
        iptables -A INPUT  -p tcp -s $peer_ip --sport $DRBD_PORT -j DROP
        iptables -A OUTPUT -p tcp -d $peer_ip --dport $DRBD_PORT -j DROP
        iptables -A INPUT  -p tcp -s $peer_ip --dport $DRBD_PORT -j DROP
        iptables -A OUTPUT -p tcp -d $peer_ip --sport $DRBD_PORT -j DROP
    "
    BLOCKED_PAIRS+=("$node|$peer_ip")
}

undrop_pair() {   # node peer_ip
    local node=$1 peer_ip=$2
    on_node "$node" sh -c "
        iptables -D INPUT  -p tcp -s $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D OUTPUT -p tcp -d $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D INPUT  -p tcp -s $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D OUTPUT -p tcp -d $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
    " 2>/dev/null || true
    local kept=() p
    for p in "${BLOCKED_PAIRS[@]:-}"; do
        [[ "$p" == "$node|$peer_ip" ]] || kept+=("$p")
    done
    BLOCKED_PAIRS=("${kept[@]:-}")
}

undrop_all() {
    [[ -n "$DRBD_PORT" ]] || return 0
    local p node peer_ip
    for p in "${BLOCKED_PAIRS[@]:-}"; do
        [[ -z "$p" ]] && continue
        node=${p%%|*}
        peer_ip=${p##*|}
        on_node "$node" sh -c "
            iptables -D INPUT  -p tcp -s $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp -d $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
            iptables -D INPUT  -p tcp -s $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp -d $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
        " 2>/dev/null || true
    done
    BLOCKED_PAIRS=()
}

# quorum_of <node> — per-volume quorum bool with kernel-truth fallback
# (the CRD events2 projection lags / reads empty under partition churn).
quorum_of() {
    local node=$1 q
    q=$(status_volume_quorum "$RD" "$node")
    if [[ -z "$q" ]]; then
        q=$(on_node "$node" drbdsetup status "$RD" --json 2>/dev/null \
            | jq -r '.[0].devices[0].quorum // empty' 2>/dev/null || true)
    fi
    printf '%s' "$q"
}

ticks() {
    local t
    t=$(wc -l <"$WRITER_LOG" 2>/dev/null | tr -d ' ')
    echo "${t:-0}"
}

stop_writer() {
    # Cooperative stop via the in-pod stop file; bounded wait for the
    # kubectl-exec wrapper to exit, then SIGTERM as last resort.
    on_node "$N1" touch "$WRITER_STOP" 2>/dev/null || true
    if [[ -n "$WRITER_PID" ]]; then
        for _ in $(seq 1 30); do
            kill -0 "$WRITER_PID" 2>/dev/null || { WRITER_PID=""; return 0; }
            sleep 1
        done
        kill "$WRITER_PID" 2>/dev/null || true
        wait "$WRITER_PID" 2>/dev/null || true
        WRITER_PID=""
        return 1
    fi
    return 0
}

dump_diag() {
    echo "==== dump_diag: quorum-loss-2tb-recovery ===="
    echo "---- writer stderr (tail 40) ----"
    tail -n 40 "$WRITER_ERR" 2>/dev/null || true
    echo "---- writer stdout (tail 20) ----"
    tail -n 20 "$WRITER_LOG" 2>/dev/null || true
    echo "---- Resource CRDs ----"
    kubectl get resourcedefinitions.blockstor.cozystack.io "$RD" -o yaml 2>/dev/null || true
    kubectl get resources.blockstor.cozystack.io -o yaml 2>/dev/null \
        | sed -n '/name: '"$RD"'\./,/^---/p' | head -120 || true
    local n
    for n in "$N1" "$N2" "$N3"; do
        echo "---- $n: drbdsetup status --verbose ----"
        on_node "$n" drbdsetup status "$RD" --verbose 2>&1 | sed "s/^/   /" || true
        echo "---- $n: drbdsetup status --json ----"
        on_node "$n" drbdsetup status "$RD" --json 2>&1 | sed "s/^/   /" || true
        echo "---- $n: dmesg drbd tail ----"
        on_node "$n" sh -c 'dmesg 2>/dev/null | grep -iE "drbd" | tail -25' 2>&1 \
            | sed "s/^/   /" || true
        echo "---- $n: iptables -S (filter) ----"
        on_node "$n" iptables -S 2>&1 | sed "s/^/   /" || true
    done
    echo "---- satellite pods ----"
    kubectl -n "$NS" get pods -l app=blockstor-satellite -o wide 2>/dev/null || true
    echo "==== end dump_diag ===="
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    # Heal the network FIRST so teardown can reach all peers.
    undrop_all
    stop_writer || true
    on_node "$N1" rm -f "$WRITER_STOP" 2>/dev/null || true
    on_node "$N1" drbdadm secondary "$RD" 2>/dev/null || true
    delete_rd "$RD"
    rm -f "$WRITER_LOG" "$WRITER_ERR" 2>/dev/null || true
    exit "$rc"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: preflight — stand-"big" gate.
# ---------------------------------------------------------------------------
echo ">> step 1: preflight — ${POOL} (providerKind=ZFS) on all 3 workers + headroom"
for n in "$N1" "$N2" "$N3"; do
    if ! kubectl get storagepools.blockstor.cozystack.io "${POOL}.${n}" >/dev/null 2>&1; then
        skip "StoragePool ${POOL}.${n} absent — 2TiB quorum-loss gate is stand-'big'-specific (3 workers, zpool blockstor-zfs on 2.2T disks, ${POOL} CRs staged)"
    fi
    kind=$(kubectl get storagepools.blockstor.cozystack.io "${POOL}.${n}" \
        -o jsonpath='{.spec.providerKind}' 2>/dev/null)
    if [[ "$kind" != "ZFS" ]]; then
        skip "StoragePool ${POOL}.${n} has providerKind='${kind}', need ZFS (thick zvols) — not stand 'big'"
    fi
done

# Headroom on the two diskful nodes: prefer the satellite-reported CRD
# status; fall back to asking the zpool directly when the projection
# has not populated yet (fresh satellite deploy at test time).
for n in "$N1" "$N2"; do
    free_kib=$(kubectl get storagepools.blockstor.cozystack.io "${POOL}.${n}" \
        -o jsonpath='{.status.freeCapacity}' 2>/dev/null)
    if [[ -z "$free_kib" || "$free_kib" == "0" ]]; then
        zp=$(kubectl get storagepools.blockstor.cozystack.io "${POOL}.${n}" -o json \
            | jq -r '.spec.props["StorDriver/ZPool"] // .spec.props["StorDriver/ZPoolThin"] // empty')
        if [[ -n "$zp" ]]; then
            free_bytes=$(on_node "$n" zpool list -Hpo free "$zp" 2>/dev/null || echo "")
            [[ -n "$free_bytes" ]] && free_kib=$(( free_bytes / 1024 ))
        fi
    fi
    if [[ -z "$free_kib" ]]; then
        skip "cannot determine free capacity of ${POOL}.${n} (no CRD status, no zpool read) — refusing to thick-allocate ${SIZE_KIB} KiB blind"
    fi
    if (( free_kib < REQUIRED_FREE_KIB )); then
        skip "${POOL}.${n} free=${free_kib} KiB < required ${REQUIRED_FREE_KIB} KiB (2 TiB thick zvol + 5% headroom)"
    fi
    echo "   ${POOL}.${n}: free=${free_kib} KiB (need ${REQUIRED_FREE_KIB})"
done

# ---------------------------------------------------------------------------
# Step 2: create RD + 2 TiB VD, place-count 2 + auto-tiebreaker witness.
# ---------------------------------------------------------------------------
echo ">> step 2: apply ${RD} (${SIZE_KIB} KiB, pool=${POOL}) on ${N1}+${N2} + auto-TB"
create_t0=$(date +%s)
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "true"
    DrbdOptions/Resource/quorum: "majority"
    DrbdOptions/Resource/on-no-quorum: "suspend-io"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${POOL}}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: ${POOL}}
EOF

echo "   wait up to ${CREATE_TIMEOUT}s for 2/2 diskful UpToDate (thick 2TiB zvol + skip-init)"
deadline=$(( create_t0 + CREATE_TIMEOUT ))
created=0
d1=""; d2=""
while (( $(date +%s) < deadline )); do
    d1=$(status_disk_state "$RD" "$N1")
    d2=$(status_disk_state "$RD" "$N2")
    if [[ "$d1" == "UpToDate" && "$d2" == "UpToDate" ]]; then
        created=1
        break
    fi
    # CRD projection can lag — accept kernel ground truth too.
    if [[ "$(kernel_pair_uptodate "$RD" "$N1" "$N2")" == "ok" ]]; then
        created=1
        break
    fi
    sleep 3
done
create_secs=$(( $(date +%s) - create_t0 ))
if (( created != 1 )); then
    echo "FAIL: ${RD} (2 TiB) not 2/2 UpToDate within ${CREATE_TIMEOUT}s (N1=$d1 N2=$d2)"
    echo "      a bounded-time create is part of the COV-011 gate — a full 2 TiB"
    echo "      initial sync here means the zfs-thick skip-init contract regressed"
    exit 1
fi
echo "   2/2 UpToDate in ${create_secs}s (budget ${CREATE_TIMEOUT}s)"

# Sharp-edged corroboration of WHY the create was fast: fresh replicas
# of a day0 RD must be stamped skipInitialSync=true (controller latch).
for n in "$N1" "$N2"; do
    sis=$(kubectl get resource "${RD}.${n}" -o jsonpath='{.spec.skipInitialSync}' 2>/dev/null)
    echo "   ${RD}.${n} spec.skipInitialSync=${sis:-<unset>}"
    if [[ "$sis" != "true" ]]; then
        echo "FAIL: fresh 2TiB replica ${RD}.${n} not stamped skipInitialSync=true (got '${sis:-<unset>}')"
        exit 1
    fi
done

# The suspend-io policy must actually be in the rendered config —
# otherwise step 5 reduces to "did DRBD happen to suspend by default".
res_pre=$(on_node "$N1" cat "/etc/drbd.d/${RD}.res" 2>/dev/null || true)
if ! echo "$res_pre" | grep -qE "on-no-quorum[[:space:]]+suspend-io"; then
    echo "FAIL: .res on ${N1} lacks 'on-no-quorum suspend-io'"
    echo "$res_pre" | sed -n '1,40p'
    exit 1
fi
echo "   .res confirms on-no-quorum suspend-io"

echo "   wait up to ${WITNESS_TIMEOUT}s for the auto-tiebreaker witness on ${N3}"
deadline=$(( $(date +%s) + WITNESS_TIMEOUT ))
tb_ok=0
while (( $(date +%s) < deadline )); do
    tbdisk=$(status_disk_state "$RD" "$N3")
    conn=$(status_connection_state "$RD" "$N1" "$N3")
    if [[ "$tbdisk" == "Diskless" && "$conn" =~ ^(Connected|Established)$ ]]; then
        tb_ok=1
        break
    fi
    sleep 3
done
if (( tb_ok != 1 )); then
    echo "FAIL: witness on ${N3} never came up Diskless+Connected within ${WITNESS_TIMEOUT}s"
    exit 1
fi
tb_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || true)
if [[ "$tb_flags" != *"TIE_BREAKER"* ]]; then
    echo "FAIL: ${RD}.${N3} is not a TIE_BREAKER witness (flags=$tb_flags)"
    exit 1
fi
echo "   witness up on ${N3} (Diskless, flags=$tb_flags)"

# DRBD mesh port + per-node IPs from the rendered .res (for drop_pair).
RESFILE="/etc/drbd.d/${RD}.res"
DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address[^:]*:[0-9]+' ${RESFILE} | head -1 | grep -oE '[0-9]+\$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from ${RESFILE} on ${N1}"
    exit 1
fi
node_ip() {
    local node=$1
    on_node "$N1" bash -c "awk '/^[[:space:]]*on ${node} /{f=1} f&&/address/{print \$2; exit}' ${RESFILE} | grep -oE '^[0-9.]+'"
}
IP1=$(node_ip "$N1"); IP2=$(node_ip "$N2"); IP3=$(node_ip "$N3")
if [[ -z "$IP1" || -z "$IP2" || -z "$IP3" ]]; then
    echo "FAIL: could not parse node IPs (N1=$IP1 N2=$IP2 N3=$IP3) from ${RESFILE}"
    exit 1
fi
echo "   DRBD port=${DRBD_PORT}  IPs: ${N1}=${IP1} ${N2}=${IP2} ${N3}(TB)=${IP3}"

# boot_id baseline on ALL workers — the no-reboot assert reference.
declare -A BOOT_ID_BEFORE UPTIME_BEFORE
for n in "$N1" "$N2" "$N3"; do
    BOOT_ID_BEFORE[$n]=$(on_node "$n" cat /proc/sys/kernel/random/boot_id)
    UPTIME_BEFORE[$n]=$(on_node "$n" cut -d. -f1 /proc/uptime)
    if [[ -z "${BOOT_ID_BEFORE[$n]}" ]]; then
        echo "FAIL: could not read boot_id on ${n}"
        exit 1
    fi
    echo "   ${n}: boot_id=${BOOT_ID_BEFORE[$n]} uptime=${UPTIME_BEFORE[$n]}s"
done

# ---------------------------------------------------------------------------
# Step 3: marker + continuous direct-IO writer on the Primary.
# ---------------------------------------------------------------------------
echo ">> step 3: promote ${N1}, seed ${MARKER_BYTES}-byte marker, start writer"
on_node "$N1" drbdadm primary "$RD"
wait_role "$RD" "$N1" "Primary" 30

DEV=$(device_for_rd "$RD" "$N1")
if [[ -z "$DEV" ]]; then
    echo "FAIL: could not resolve /dev/drbdN on ${N1} for ${RD}"
    exit 1
fi
echo "   device ${DEV} on ${N1}"

md5_before=$(write_random "$N1" "$DEV" "$MARKER_BYTES")
if [[ -z "$md5_before" || "$md5_before" == "d41d8cd98f00b204e9800998ecf8427e" ]]; then
    echo "FAIL: marker write produced empty/zero md5"
    exit 1
fi
echo "   marker md5_before=${md5_before} (offset 0, ${MARKER_BYTES} bytes)"

: >"$WRITER_LOG"
: >"$WRITER_ERR"
on_node "$N1" rm -f "$WRITER_STOP" 2>/dev/null || true

start_writer() {
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        --field-selector "spec.nodeName=${N1},status.phase=Running" \
        -o "jsonpath={.items[0].metadata.name}")
    if [[ -z "$pod" ]]; then
        echo "FAIL: no Running satellite pod on ${N1} for the writer" >&2
        return 1
    fi
    # 1 tick/s pattern writer, direct IO, offsets >= WRITER_BASE_BLOCK
    # (128 MiB) so the offset-0 marker region is never touched. dd
    # failures (EIO/ENODATA — anything but a clean block) land in
    # WRITER_ERR; suspend-io must BLOCK the write instead, which shows
    # up as a tick freeze with WRITER_ERR staying empty.
    kubectl -n "$NS" exec "$pod" -- bash -c "
        set +e
        i=0
        while true; do
            if [[ -f ${WRITER_STOP} ]]; then
                exit 0
            fi
            ts=\$(date +%s)
            off=\$(( ${WRITER_BASE_BLOCK} + (i % 64) ))
            if err=\$(dd if=/dev/urandom of=${DEV} bs=4096 count=1 seek=\$off \\
                conv=notrunc oflag=direct status=none 2>&1); then
                echo \"\$ts OK i=\$i off=\$off\"
            else
                echo \"\$ts ERR i=\$i off=\$off: \$err\" >&2
            fi
            i=\$(( i + 1 ))
            sleep 1
        done
    " >"$WRITER_LOG" 2>"$WRITER_ERR" &
    WRITER_PID=$!
}
start_writer

# Bounded warmup: the writer must produce >=3 clean ticks within 30s.
deadline=$(( $(date +%s) + 30 ))
warm=0
while (( $(date +%s) < deadline )); do
    if [[ -s "$WRITER_ERR" ]]; then
        echo "FAIL: writer errored during warmup on a healthy volume"
        cat "$WRITER_ERR"
        exit 1
    fi
    if (( $(ticks) >= 3 )); then
        warm=1
        break
    fi
    sleep 1
done
if (( warm != 1 )); then
    echo "FAIL: writer produced <3 ticks within 30s warmup"
    exit 1
fi
echo "   writer warmed up: $(ticks) ticks, zero errors"

# ---------------------------------------------------------------------------
# Step 4: secondary-only outage — quorum must HOLD (2/3 with witness).
# ---------------------------------------------------------------------------
echo ">> step 4: sever ${N2} from both peers — quorum must HOLD on ${N1}"
drop_pair "$N2" "$IP1"
drop_pair "$N2" "$IP3"
drop_pair "$N1" "$IP2"
drop_pair "$N3" "$IP2"

echo "   wait up to ${SEVER_TIMEOUT}s for ${N1}->${N2} link to break"
deadline=$(( $(date +%s) + SEVER_TIMEOUT ))
sev_ok=0
c12=""
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    if [[ -n "$c12" && ! "$c12" =~ ^(Connected|Established)$ ]]; then
        sev_ok=1
        break
    fi
    sleep 2
done
if (( sev_ok != 1 )); then
    echo "FAIL: ${N1}->${N2} never broke within ${SEVER_TIMEOUT}s (last='$c12')"
    exit 1
fi
echo "   ${N1}->${N2} severed ($c12)"

ticks_at_sever=$(ticks)
echo "   probe ${HOLD_PROBE}s: quorum holds, no suspension, writer keeps ticking"
probe_end=$(( $(date +%s) + HOLD_PROBE ))
while (( $(date +%s) < probe_end )); do
    q1=$(quorum_of "$N1")
    s1=$(status_suspended "$RD" "$N1")
    if [[ "$q1" == "false" || -n "$s1" ]]; then
        echo "FAIL: ${N1} lost quorum / suspended during SECONDARY-only outage (quorum=$q1 suspended=$s1)"
        echo "      witness must keep the Primary at 2/3 voters"
        exit 1
    fi
    sleep 2
done
ticks_after_hold=$(ticks)
if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer errored during the quorum-hold window"
    cat "$WRITER_ERR"
    exit 1
fi
if (( ticks_after_hold < ticks_at_sever + 5 )); then
    echo "FAIL: writer stalled during secondary-only outage (ticks ${ticks_at_sever} -> ${ticks_after_hold}, expected +>=5 over ${HOLD_PROBE}s)"
    exit 1
fi
echo "   quorum HELD: ticks ${ticks_at_sever} -> ${ticks_after_hold}, suspended=<none>"

echo ">> heal step-4 outage, wait up to ${RESYNC_TIMEOUT}s for clean resync"
undrop_pair "$N2" "$IP1"
undrop_pair "$N2" "$IP3"
undrop_pair "$N1" "$IP2"
undrop_pair "$N3" "$IP2"

deadline=$(( $(date +%s) + RESYNC_TIMEOUT ))
resync_ok=0
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    if [[ "$c12" =~ ^(Connected|Established)$ ]] \
        && [[ "$(kernel_pair_uptodate "$RD" "$N1" "$N2")" == "ok" ]] \
        && [[ "$(quorum_of "$N1")" == "true" ]] \
        && [[ -z "$(status_suspended "$RD" "$N1")" ]]; then
        resync_ok=1
        break
    fi
    sleep 3
done
if (( resync_ok != 1 )); then
    echo "FAIL: ${N2} did not resync clean within ${RESYNC_TIMEOUT}s after step-4 heal"
    exit 1
fi
echo "   step-4 heal complete: ${N1}<->${N2} Connected, pair UpToDate"

# ---------------------------------------------------------------------------
# Step 5: lose quorum on the Primary mid-IO — IO must SUSPEND, not fail.
# ---------------------------------------------------------------------------
echo ">> step 5: sever BOTH ${N2} (diskful) AND ${N3} (witness) — ${N1} drops to 1/3"
drop_pair "$N1" "$IP2"
drop_pair "$N1" "$IP3"
drop_pair "$N2" "$IP1"
drop_pair "$N2" "$IP3"
drop_pair "$N3" "$IP1"
drop_pair "$N3" "$IP2"

echo "   wait up to ${QLOSS_TIMEOUT}s for quorum loss on ${N1}"
deadline=$(( $(date +%s) + QLOSS_TIMEOUT ))
qlost=0
while (( $(date +%s) < deadline )); do
    q1=$(quorum_of "$N1")
    s1=$(status_suspended "$RD" "$N1")
    if [[ "$q1" == "false" || -n "$s1" ]]; then
        qlost=1
        break
    fi
    sleep 2
done
if (( qlost != 1 )); then
    echo "FAIL: ${N1} never reported quorum loss within ${QLOSS_TIMEOUT}s of full isolation"
    exit 1
fi
echo "   ${N1} LOST quorum (quorum=$(quorum_of "$N1") suspended=$(status_suspended "$RD" "$N1"))"
on_node "$N1" sh -c 'dmesg 2>/dev/null | grep -iE "drbd.*quorum" | tail -5' 2>&1 \
    | sed "s/^/   ${N1} dmesg: /" || true

# Suspension semantics: ticks FREEZE (in-flight dd blocks in the kernel
# awaiting quorum), zero ERR lines (suspend-io must block, never EIO),
# writer wrapper still alive, no kernel crash. A short settle absorbs
# any tick that completed in flight between sever and suspension.
sleep 3
ticks_frozen_a=$(ticks)
echo "   confirm freeze over ${FREEZE_CONFIRM}s (ticks pinned at ${ticks_frozen_a})"
sleep "$FREEZE_CONFIRM"
ticks_frozen_b=$(ticks)
if (( ticks_frozen_b != ticks_frozen_a )); then
    echo "FAIL: writer kept ticking under lost quorum (${ticks_frozen_a} -> ${ticks_frozen_b}) — IO was NOT suspended"
    exit 1
fi
if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer surfaced errors under suspend-io — policy must BLOCK, not fail"
    cat "$WRITER_ERR"
    exit 1
fi
if ! kill -0 "$WRITER_PID" 2>/dev/null; then
    echo "FAIL: writer wrapper died during the suspension window (cannot prove resume)"
    exit 1
fi
if on_node "$N1" sh -c 'dmesg 2>/dev/null | tail -200 | grep -qE "kernel BUG|Oops|general protection|Kernel panic"'; then
    echo "FAIL: kernel crash signature in dmesg on ${N1} during quorum loss"
    exit 1
fi
echo "   IO SUSPENDED cleanly: ticks frozen, zero errors, writer alive, no crash"

# ---------------------------------------------------------------------------
# Step 6: restore -> quorum returns, suspended IO RESUMES, NO reboot.
# ---------------------------------------------------------------------------
echo ">> step 6: restore all links — IO must resume WITHOUT any node reboot"
undrop_all

echo "   wait up to ${RESUME_TIMEOUT}s for quorum return + writer resume on ${N1}"
deadline=$(( $(date +%s) + RESUME_TIMEOUT ))
resumed=0
while (( $(date +%s) < deadline )); do
    q1=$(quorum_of "$N1")
    s1=$(status_suspended "$RD" "$N1")
    if [[ "$q1" == "true" && -z "$s1" && $(ticks) -gt $ticks_frozen_b ]]; then
        resumed=1
        break
    fi
    sleep 2
done
if (( resumed != 1 )); then
    echo "FAIL: suspended IO did not resume within ${RESUME_TIMEOUT}s of quorum return"
    echo "      (quorum=$(quorum_of "$N1") suspended=$(status_suspended "$RD" "$N1") ticks=$(ticks), frozen at ${ticks_frozen_b})"
    echo "      THIS IS THE COV-011 DEADLOCK: recovery would have required a node reboot"
    exit 1
fi
echo "   IO RESUMED: ticks ${ticks_frozen_b} -> $(ticks), quorum=true, suspended=<none>"

# Writer must shut down cleanly (the blocked write COMPLETED, none errored).
if ! stop_writer; then
    echo "FAIL: writer did not exit within 30s of the stop file — still wedged"
    exit 1
fi
if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer recorded errors across the suspend/resume cycle"
    cat "$WRITER_ERR"
    exit 1
fi
echo "   writer completed cleanly: $(ticks) total ticks, zero errors"

echo "   wait up to ${FINAL_TIMEOUT}s for full reconvergence"
deadline=$(( $(date +%s) + FINAL_TIMEOUT ))
converged=0
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    c13=$(status_connection_state "$RD" "$N1" "$N3")
    if [[ "$c12" =~ ^(Connected|Established)$ ]] \
        && [[ "$c13" =~ ^(Connected|Established)$ ]] \
        && [[ "$(kernel_pair_uptodate "$RD" "$N1" "$N2")" == "ok" ]] \
        && [[ "$(quorum_of "$N1")" == "true" && "$(quorum_of "$N2")" == "true" ]] \
        && [[ -z "$(status_suspended "$RD" "$N1")" && -z "$(status_suspended "$RD" "$N2")" ]]; then
        converged=1
        break
    fi
    sleep 3
done
if (( converged != 1 )); then
    echo "FAIL: cluster did not fully reconverge within ${FINAL_TIMEOUT}s after heal"
    exit 1
fi
echo "   reconverged: both diskful UpToDate, witness Connected, quorum clean everywhere"

# Split-brain guard: at most one Primary post-heal.
prim_count=0
for n in "$N1" "$N2" "$N3"; do
    r=$(status_role "$RD" "$n")
    echo "   role on ${n}: ${r:-<empty>}"
    [[ "$r" == "Primary" ]] && prim_count=$(( prim_count + 1 ))
done
if (( prim_count > 1 )); then
    echo "FAIL: ${prim_count} Primaries post-heal — split-brain"
    exit 1
fi

# Marker integrity: the offset-0 region seeded before any outage must
# read back bit-identical on the Primary after the full cycle.
md5_after=$(read_md5 "$N1" "$DEV" "$MARKER_BYTES")
if [[ "$md5_after" != "$md5_before" ]]; then
    echo "FAIL: marker md5 mismatch after recovery (before=${md5_before} after=${md5_after})"
    exit 1
fi
echo "   marker intact: md5=${md5_after}"

# THE no-reboot assert: boot_id unchanged + uptime strictly increased
# on EVERY worker. A reboot anywhere during the cycle fails the gate —
# the whole point of COV-011 is recovery without rebooting nodes.
echo "   no-reboot assert (boot_id + uptime on all workers)"
for n in "$N1" "$N2" "$N3"; do
    boot_after=$(on_node "$n" cat /proc/sys/kernel/random/boot_id)
    uptime_after=$(on_node "$n" cut -d. -f1 /proc/uptime)
    echo "     ${n}: boot_id ${BOOT_ID_BEFORE[$n]} -> ${boot_after}, uptime ${UPTIME_BEFORE[$n]}s -> ${uptime_after}s"
    if [[ "$boot_after" != "${BOOT_ID_BEFORE[$n]}" ]]; then
        echo "FAIL: ${n} REBOOTED during the scenario (boot_id changed) — recovery must not need reboots"
        exit 1
    fi
    if (( uptime_after <= ${UPTIME_BEFORE[$n]} )); then
        echo "FAIL: ${n} uptime did not increase monotonically (${UPTIME_BEFORE[$n]} -> ${uptime_after}) — reboot suspected"
        exit 1
    fi
done
echo "   NO node rebooted"

# ---------------------------------------------------------------------------
# Step 7: teardown + no-orphans assert.
# ---------------------------------------------------------------------------
echo ">> step 7: teardown ${RD} + assert no orphans"
on_node "$N1" drbdadm secondary "$RD" 2>/dev/null || true
delete_rd "$RD"

assert_no_orphans() {
    # CRD layer: nothing named after the RD may survive teardown.
    local deadline=$(( $(date +%s) + ORPHAN_TIMEOUT ))
    local leftover="" line
    while (( $(date +%s) < deadline )); do
        leftover=$( {
            kubectl get resourcedefinitions.blockstor.cozystack.io "$RD" --no-headers 2>/dev/null
            kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
                | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}'
        } | grep -v '^$' || true )
        [[ -z "$leftover" ]] && break
        sleep 2
    done
    if [[ -n "$leftover" ]]; then
        echo "FAIL: orphan CRDs survived teardown after ${ORPHAN_TIMEOUT}s:"
        while IFS= read -r line; do echo "   $line"; done <<<"$leftover"
        return 1
    fi
    # Storage layer: the 2 TiB thick zvol must be gone on both diskful
    # nodes — a leaked thick zvol permanently pins 2 TiB of the pool.
    local n zorphan
    for n in "$N1" "$N2"; do
        zorphan=$(on_node "$n" bash -c "zfs list -H -t volume -o name 2>/dev/null | grep -F '${RD}_' || true")
        if [[ -n "$zorphan" ]]; then
            echo "FAIL: leaked zvol(s) on ${n} after teardown:"
            while IFS= read -r line; do echo "   $line"; done <<<"$zorphan"
            return 1
        fi
    done
    # Kernel layer: no DRBD slot left for the RD anywhere.
    for n in "$N1" "$N2" "$N3"; do
        if on_node "$n" drbdsetup status "$RD" >/dev/null 2>&1; then
            echo "FAIL: ${n} still holds a DRBD kernel slot for ${RD} after teardown"
            return 1
        fi
    done
    return 0
}

if ! assert_no_orphans; then
    exit 1
fi
echo "   no orphans: CRDs drained, zvols reclaimed, kernel slots gone"

echo ">> QUORUM-LOSS-2TB-RECOVERY OK"
echo "   2 TiB zfs-thick create bounded: YES (${create_secs}s, skip-init)"
echo "   secondary-only outage, quorum held + IO continued: YES"
echo "   full quorum loss mid-IO -> IO suspended, zero errors: YES"
echo "   quorum return -> suspended IO resumed, writer completed: YES"
echo "   marker md5 intact: YES (${md5_after})"
echo "   all replicas UpToDate, single Primary, status clean: YES"
echo "   NO node reboot (boot_id stable on all workers): YES"
