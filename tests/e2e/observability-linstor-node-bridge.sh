#!/usr/bin/env bash
#
# usage: observability-linstor-node-bridge.sh WORK_DIR
#
# Scenario 7.11 — LINSTOR ↔ Node bridge: iptables drop ↔ drbdadm
# status agreement.
#
# Goal: when peer-to-peer DRBD TCP is silently dropped, the two
# observability layers must agree:
#   - Level 2 (LINSTOR REST / `linstor r l`): the Primary's view of
#     the secondary peer flips into a non-Connected state class
#     (StandAlone / Connecting / NetworkFailure / BrokenPipe).
#   - Level 3 (kernel / `drbdsetup status --verbose`): the same peer
#     appears in a non-Connected state class on the Primary satellite.
#
# Both views must point at the same broken peer AND agree on the
# state class (non-Connected). The State / disk_state column must
# never land in `Unknown` — that would mean the satellite stopped
# reporting altogether, which is a different failure mode (observer
# crash, not bridge drop) and is out of scope here.
#
# Heal: drop iptables rules → both views must return to Connected /
# UpToDate within 60s (DRBD reconnect handshake + satellite observer
# propagation).
#
# Pre-flight workarounds (per CLAUDE notes 2026-05-13):
#   - apiserver/controller no longer take `--store=k8s` (refactor
#     69ec9c9 dropped the flag). Sanity-check that the running
#     pods don't list it as an arg.
#   - Node CRDs must be present (install-blockstor.sh stamps them).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=bridge-test
SIZE_KIB=65536  # 64 MiB
# DRBD-9 default ping-timeout × ping-int adds up to ~30-45 s before
# the kernel flips connection:Connected -> Connecting under a silent
# tcp DROP (no RST). The satellite observer then needs another
# ~1-2 s to propagate that into the CRD Status and the REST view.
# 60 s gives a comfortable margin without masking a genuine
# observability bug (the satellite reconciler runs every ~2 s, so
# any propagation gap >5 s is a bug we'd still catch).
ASSERT_TIMEOUT=60
HEAL_TIMEOUT=60

# Port-forward to the REST service. The `blockstor-controller`
# Service selects apiserver pods post-Phase-11 split (the controller
# Deployment itself no longer serves :3370), so the URL stays stable
# but we still go through the Service. Random ephemeral port so this
# scenario can co-run with sibling iter scenarios on the same host.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "${PF_PORT}:3370" \
    >/tmp/bridge-test-pf.log 2>&1 &
PF_PID=$!

# Wait for the port-forward to actually answer before any GET.
for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

REST_BASE="http://127.0.0.1:${PF_PORT}"

# --- Workaround sanity checks -------------------------------------------------

echo ">> sanity: --store=k8s must not appear on controller/apiserver pods"
if kubectl -n "$NS" get pods -o yaml 2>/dev/null | grep -q -- '--store=k8s'; then
    echo "FAIL: stray --store=k8s arg on a controller/apiserver pod (refactor 69ec9c9 drops it)"
    kubectl -n "$NS" get pods -o yaml | grep -B2 -A2 -- '--store=k8s' >&2 || true
    exit 1
fi

echo ">> sanity: Node CRDs present for every worker"
for w in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    [[ -z "$w" ]] && continue
    if ! kubectl get node.blockstor.io.blockstor.io "$w" >/dev/null 2>&1; then
        echo "FAIL: Node CRD blockstor.io.blockstor.io/$w missing"
        exit 1
    fi
done

trap 'cleanup_bridge; kill "$PF_PID" 2>/dev/null || true; wait "$PF_PID" 2>/dev/null || true; delete_rd "$RD"' EXIT

# Track which node we firewalled so cleanup hits the right satellite
# even if we exit between block + heal.
BLOCKED_NODE=""
BLOCKED_PORT=""

cleanup_bridge() {
    if [[ -n "$BLOCKED_NODE" && -n "$BLOCKED_PORT" ]]; then
        on_node "$BLOCKED_NODE" sh -c "
            iptables -D INPUT  -p tcp --dport $BLOCKED_PORT -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp --dport $BLOCKED_PORT -j DROP 2>/dev/null || true
        " 2>/dev/null || true
    fi
}

# --- Step 1: create 2-replica RD with autoplace=2 ---------------------------

echo ">> apply 2-replica RD $RD (autoplace=2, AutoAddQuorumTiebreaker=false)"
# Explicit 2-replica diskful placement on $WORKER_1+$WORKER_2.
# AutoAddQuorumTiebreaker=false on the RD itself so the RD reconciler
# does NOT stamp a 3rd diskless witness Resource — we want a clean
# 2-node mesh so the connection-state view is unambiguous.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${WORKER_1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${WORKER_1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${WORKER_2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${WORKER_2}
  props: {StorPoolName: stand}
EOF

# --- Step 2: wait UpToDate on both peers ------------------------------------

echo ">> wait UpToDate on $WORKER_1 + $WORKER_2"
RD="$RD" wait_uptodate "$RD" "$WORKER_1" "$WORKER_2"

# --- Step 3: identify the Primary and the DRBD port -------------------------

# Promote $WORKER_1 explicitly so the test owns which side is Primary.
# Without this, neither peer is Primary (Resource flags don't auto-
# promote on bring-up), and `drbdsetup status` shows both as
# Secondary — the test would have no "Primary view" to assert from.
on_node "$WORKER_1" drbdadm primary "$RD" 2>/dev/null || true
sleep 2

PRIMARY="$WORKER_1"
SECONDARY="$WORKER_2"

echo ">> primary=$PRIMARY secondary=$SECONDARY"

# Discover DRBD port from rendered .res — same trick as
# network-partition.sh. DRBD-9 uses a single mesh listen port per
# replica, identical on every peer in the .res, so reading from
# either node works.
DRBD_PORT=$(on_node "$PRIMARY" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+\$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from /etc/drbd.d/${RD}.res on $PRIMARY"
    on_node "$PRIMARY" cat "/etc/drbd.d/${RD}.res" >&2 || true
    exit 1
fi

echo ">> DRBD port = $DRBD_PORT"

# --- Step 4: drop tcp/<port> on the SECONDARY (bridge break) ----------------

echo ">> block tcp/$DRBD_PORT in+out on secondary ($SECONDARY)"
# Block both directions: INPUT alone leaves outbound keep-alives
# flowing, so the kernel keeps the TCP socket half-open well past
# the 30s assertion window. OUTPUT DROP closes the symmetric side
# so the DRBD ping-timeout fires within ~10s on both peers.
on_node "$SECONDARY" sh -c "
    iptables -I INPUT  -p tcp --dport $DRBD_PORT -j DROP &&
    iptables -I OUTPUT -p tcp --dport $DRBD_PORT -j DROP
"
BLOCKED_NODE="$SECONDARY"
BLOCKED_PORT="$DRBD_PORT"

# Record timestamp on the Primary so the dmesg cross-check (step 6)
# has a known earliest-possible-timestamp anchor.
BLOCK_TS_EPOCH=$(on_node "$PRIMARY" date +%s)
echo "   block applied at epoch $BLOCK_TS_EPOCH on $PRIMARY"

# --- Step 5: assert two-level agreement within $ASSERT_TIMEOUT --------------

echo ">> assert within ${ASSERT_TIMEOUT}s: linstor view + drbdsetup view agree on broken peer"

# Acceptable non-Connected state classes (the four DRBD-9 transitional
# states the .gen.go enum lists). State must NOT be `Unknown` — that
# means the satellite stopped reporting altogether.
ACCEPT_STATES_RE='StandAlone|Connecting|NetworkFailure|BrokenPipe'

deadline=$(( $(date +%s) + ASSERT_TIMEOUT ))
linstor_state=""
drbd_state=""
linstor_peer=""
drbd_peer=""

while (( $(date +%s) < deadline )); do
    # --- Level 2: LINSTOR REST via `linstor r l -m` -----------------------
    # `linstor r l -m -r <rd>` machine-readable JSON has the per-resource
    # array at .[0]; each entry has a .layer_object.drbd.connections map
    # keyed by peer-node-name. We want the PRIMARY's view: find the entry
    # where node_name == $PRIMARY and inspect its connections[$SECONDARY].
    rest_json=$(curl -fsS -m 3 "${REST_BASE}/v1/resource-definitions/${RD}/resources?with_volume_data=true" 2>/dev/null || true)

    if [[ -n "$rest_json" ]]; then
        # Connection message on the Primary's view of the Secondary.
        linstor_state=$(echo "$rest_json" | jq -r \
            --arg primary "$PRIMARY" --arg secondary "$SECONDARY" \
            '.[] | select(.node_name==$primary) | .layer_object.drbd.connections[$secondary].message // ""' 2>/dev/null || true)
        # NOTE: do NOT use `// empty` here — jq's // treats `false`
        # as needing the default, which would strip `connected:false`
        # entries (the exact case we're trying to detect).
        linstor_connected=$(echo "$rest_json" | jq -r \
            --arg primary "$PRIMARY" --arg secondary "$SECONDARY" \
            '.[] | select(.node_name==$primary) | .layer_object.drbd.connections[$secondary].connected' 2>/dev/null || true)
        # If linstor_state is non-empty AND connected==false, this is the
        # broken peer from LINSTOR's POV.
        if [[ -n "$linstor_state" && "$linstor_connected" == "false" ]]; then
            linstor_peer="$SECONDARY"
        fi
        # Disk-state on Primary side — must NOT be Unknown.
        primary_disk=$(echo "$rest_json" | jq -r \
            --arg primary "$PRIMARY" \
            '.[] | select(.node_name==$primary) | .layer_object.drbd.drbd_volumes[0].disk_state // ""' 2>/dev/null || true)
        if [[ "$primary_disk" == "Unknown" ]]; then
            echo "FAIL: Primary disk_state landed in Unknown — observability lost (bridge bug)"
            echo "rest_json: $rest_json" >&2
            exit 1
        fi
    fi

    # --- Level 3: kernel `drbdsetup status --verbose` on Primary ----------
    # Output shape (DRBD-9):
    #   <rd> node-id:N role:Primary suspended:no
    #     volume:0 minor:NNN disk:UpToDate ...
    #     <peer> node-id:M connection:Connecting role:Unknown
    #       volume:0 replication:Off peer-disk:DUnknown ...
    drbd_raw=$(on_node "$PRIMARY" drbdsetup status "$RD" --verbose 2>/dev/null || true)

    # Parse: find the <peer-node-name> line(s) and the `connection:` token.
    # Lines that start with the peer node name (after leading whitespace)
    # carry the `connection:<state>` field. `Established` is DRBD-9's
    # Connected synonym.
    drbd_state=$(echo "$drbd_raw" | awk -v peer="$SECONDARY" '
        $0 ~ "^[[:space:]]*"peer"[[:space:]]" {
            for (i=1; i<=NF; i++) {
                if ($i ~ /^connection:/) {
                    split($i, a, ":")
                    print a[2]
                    exit
                }
            }
        }')

    if [[ -n "$drbd_state" && "$drbd_state" != "Established" && "$drbd_state" != "Connected" ]]; then
        drbd_peer="$SECONDARY"
    fi

    # Both views must report the same broken peer AND a recognised
    # non-Connected state-class label.
    if [[ "$linstor_peer" == "$SECONDARY" && "$drbd_peer" == "$SECONDARY" \
          && "$linstor_state" =~ ^($ACCEPT_STATES_RE)$ \
          && "$drbd_state" =~ ^($ACCEPT_STATES_RE)$ ]]; then
        echo "   linstor view: peer=$SECONDARY message=$linstor_state connected=false"
        echo "   drbd view:    peer=$SECONDARY connection=$drbd_state"
        break
    fi

    sleep 2
done

if [[ ! ( "$linstor_state" =~ ^($ACCEPT_STATES_RE)$ ) ]]; then
    echo "FAIL: linstor view never settled to non-Connected within ${ASSERT_TIMEOUT}s"
    echo "  last linstor_state=$linstor_state linstor_peer=$linstor_peer"
    echo "  rest_json (tail): ${rest_json: -400}"
    exit 1
fi
if [[ ! ( "$drbd_state" =~ ^($ACCEPT_STATES_RE)$ ) ]]; then
    echo "FAIL: drbdsetup view never settled to non-Connected within ${ASSERT_TIMEOUT}s"
    echo "  last drbd_state=$drbd_state drbd_peer=$drbd_peer"
    echo "  drbd_raw:"
    echo "$drbd_raw" | sed 's/^/    /'
    exit 1
fi
if [[ "$linstor_peer" != "$drbd_peer" ]]; then
    echo "FAIL: views disagree on which peer is broken (linstor=$linstor_peer drbd=$drbd_peer)"
    exit 1
fi

echo ">> two-level agreement OK: both views point at $SECONDARY, state class non-Connected"

# Persist the observed state name so the final report can quote it.
echo "$linstor_state" > "$WORK_DIR/.7.11-linstor-state"
echo "$drbd_state"    > "$WORK_DIR/.7.11-drbd-state"

# --- Step 6: dmesg sanity on Primary ----------------------------------------

echo ">> dmesg cross-check on $PRIMARY (rough TCP-failure trace)"
# Permissive: DRBD-9 emits one of several messages depending on which
# side hits the timeout first ("PingAck did not arrive in time",
# "sock was shut down", "Connection closed", "ack_receiver: receiver
# stopped"). Any of those count as the kernel having noticed.
dmesg_hit=$(on_node "$PRIMARY" bash -c "
    dmesg --since '1 minute ago' 2>/dev/null \
      | grep -E 'PingAck|sock.*shut|Connection.*closed|receiver.*stopped|drbd.*${RD}' \
      | tail -5
" 2>/dev/null || true)

if [[ -z "$dmesg_hit" ]]; then
    # Soft check — kernel-log rotation / ring-buffer pressure can
    # evict the line on a busy stand. Warn, don't fail.
    echo "   WARN: no matching dmesg trace on $PRIMARY (ring buffer may have rotated)"
else
    echo "   dmesg trace:"
    echo "$dmesg_hit" | sed 's/^/    /'
fi

# --- Step 7: heal -----------------------------------------------------------

echo ">> heal: remove iptables rules on $SECONDARY"
cleanup_bridge
BLOCKED_NODE=""
BLOCKED_PORT=""
HEAL_TS_EPOCH=$(date +%s)

# --- Step 8: assert recovery within $HEAL_TIMEOUT ---------------------------

echo ">> wait up to ${HEAL_TIMEOUT}s for both views to return to Connected/UpToDate"
recover_deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
recover_ok=0
HEAL_ELAPSED=""

while (( $(date +%s) < recover_deadline )); do
    rest_json=$(curl -fsS -m 3 "${REST_BASE}/v1/resource-definitions/${RD}/resources?with_volume_data=true" 2>/dev/null || true)
    lstate=$(echo "$rest_json" | jq -r \
        --arg primary "$PRIMARY" --arg secondary "$SECONDARY" \
        '.[] | select(.node_name==$primary) | .layer_object.drbd.connections[$secondary].connected' 2>/dev/null || true)
    pdisk=$(echo "$rest_json" | jq -r \
        --arg primary "$PRIMARY" \
        '.[] | select(.node_name==$primary) | .layer_object.drbd.drbd_volumes[0].disk_state // ""' 2>/dev/null || true)

    drbd_raw=$(on_node "$PRIMARY" drbdsetup status "$RD" --verbose 2>/dev/null || true)
    dstate=$(echo "$drbd_raw" | awk -v peer="$SECONDARY" '
        $0 ~ "^[[:space:]]*"peer"[[:space:]]" {
            for (i=1; i<=NF; i++) {
                if ($i ~ /^connection:/) {
                    split($i, a, ":")
                    print a[2]
                    exit
                }
            }
        }')

    # The state column must never land in Unknown during recovery either.
    if [[ "$pdisk" == "Unknown" ]]; then
        echo "FAIL: Primary disk_state landed in Unknown during recovery"
        exit 1
    fi

    if [[ "$lstate" == "true" && "$pdisk" == "UpToDate" \
          && ( "$dstate" == "Established" || "$dstate" == "Connected" ) ]]; then
        HEAL_ELAPSED=$(( $(date +%s) - HEAL_TS_EPOCH ))
        recover_ok=1
        break
    fi

    sleep 2
done

if (( recover_ok == 0 )); then
    echo "FAIL: views did not return to Connected/UpToDate within ${HEAL_TIMEOUT}s"
    echo "  last linstor_connected=$lstate  primary_disk=$pdisk  drbd_connection=$dstate"
    echo "  drbd_raw:"
    echo "$drbd_raw" | sed 's/^/    /'
    exit 1
fi

echo ">> recovered in ${HEAL_ELAPSED}s — both views back to Connected/UpToDate"
echo "$HEAL_ELAPSED" > "$WORK_DIR/.7.11-heal-seconds"

# --- Step 9: cleanup --------------------------------------------------------

echo ">> OBSERVABILITY-LINSTOR-NODE-BRIDGE OK"
echo "   linstor view state during outage: $linstor_state"
echo "   drbd view state during outage:    $drbd_state"
echo "   heal recovery: ${HEAL_ELAPSED}s"
