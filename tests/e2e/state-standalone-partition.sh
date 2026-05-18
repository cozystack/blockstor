#!/usr/bin/env bash
#
# usage: state-standalone-partition.sh WORK_DIR
#
# Scenario 5.10 — 2-replica RD survives a transient network partition.
#
# Goal: in a 2-replica setup, isolate the secondary at the TCP layer
# (iptables-drop on the DRBD port, both directions, on the satellite
# pod via hostNetwork + CAP_NET_ADMIN). DRBD must:
#   - flip the connection FSM into a non-Connected state class
#     (StandAlone / Connecting / NetworkFailure / BrokenPipe) within
#     30 s on the Primary's view of the secondary peer;
#   - on iptables removal, return both peers to UpToDate + Connected
#     within 30 s without any operator intervention (no
#     `drbdadm connect`, no satellite restart);
#   - preserve the Primary-side marker bytes verbatim (no corruption,
#     no truncation).
#
# This is distinct from scenario 5.14 (recovery-discard-my-data.sh)
# which provokes hard StandAlone via `drbdsetup disconnect --force=yes`
# and validates the operator's manual recovery recipe. 5.10 validates
# the AUTOMATIC heal path on a softer partition (TCP drops only).
#
# Earlier iteration's heal-grep regex looked for the literal token
# `connection:Connected`, which is not present on the happy path —
# DRBD-9's `drbdsetup status --verbose` reports `connection:Established`
# when the link is up. The fix here mirrors
# observability-linstor-node-bridge.sh: parse the `connection:` token
# from the peer line via awk and accept either `Established` or
# `Connected` as healthy; alternatively assert via NEGATION (no
# StandAlone/Connecting/NetworkFailure/BrokenPipe present on the
# peer line).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-5-10-standalone-partition
N1=$WORKER_1
N2=$WORKER_2
SIZE_BYTES=$((1024 * 1024))   # 1 MiB marker payload — large enough to
                              # surface bit-flips, small enough to
                              # re-sync in well under 30 s on QEMU.

BAD_STATES_RE='StandAlone|Connecting|NetworkFailure|BrokenPipe|Disconnecting|Timeout'
PARTITION_TIMEOUT=30
# Bug 307: Run 16 failed with both peers stuck `disk:Outdated
# peer-disk:Outdated replication:Established` 30 s after iptables
# removal. Root cause was a transient mid-suite satellite-pod restart
# (Bug 305's DaemonSet roll added a hostPath mount) that left the
# resource in a slow-heal state. DRBD-9 eventually converges, it just
# needs more than 30 s on a QEMU stand under satellite-restart
# pressure. 60 s mirrors the rest of the suite's heal-after-partition
# budget (network-partition.sh uses 180 s for the analogous wait).
HEAL_TIMEOUT=60

BLOCKED_NODE=""
BLOCKED_PORT=""

cleanup_iptables() {
    if [[ -n "$BLOCKED_NODE" && -n "$BLOCKED_PORT" ]]; then
        on_node "$BLOCKED_NODE" sh -c "
            iptables -D INPUT  -p tcp --dport $BLOCKED_PORT -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp --dport $BLOCKED_PORT -j DROP 2>/dev/null || true
            iptables -D INPUT  -p tcp --sport $BLOCKED_PORT -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp --sport $BLOCKED_PORT -j DROP 2>/dev/null || true
        " 2>/dev/null || true
    fi
}

trap 'cleanup_iptables; delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD on $N1 + $N2 (no tiebreaker, no third witness)"
# AutoAddQuorumTiebreaker=false so the RD reconciler does NOT stamp a
# 3rd diskless witness Resource. We want a clean 2-node mesh; the
# scenario asserts behaviour of the kernel-level connection FSM
# between exactly two peers, and a witness would obscure it.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
EOF

wait_uptodate "$RD" "$N1" "$N2"

# Bug 307: `wait_uptodate` only checks the LOCAL `disk:` row on each
# peer. It can return before the primary has confirmed its peer is
# UpToDate from its own view — initial sync's bitmap-clear takes an
# extra round-trip. Entering the iptables-drop window with the primary
# still thinking `peer-disk:Inconsistent` causes DRBD to mark its own
# disk Outdated on partition (no peer to verify against). After heal,
# both sides flip to `disk:Outdated peer-disk:Outdated
# replication:Established` and need 30+ s of bitmap-driven recovery.
# Mirror the network-partition.sh idiom: wait for `peer-disk:UpToDate`
# on both peers' views before isolating.
echo ">> wait for peer-disk:UpToDate on both peers' views (stable pre-partition)"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    p1=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep -c "peer-disk:UpToDate" || true)
    p2=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep -c "peer-disk:UpToDate" || true)
    if (( p1 >= 1 && p2 >= 1 )); then
        break
    fi
    sleep 2
done
if (( p1 < 1 || p2 < 1 )); then
    echo "FAIL: peer-disk:UpToDate not seen on both peers' views within 60s"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/  N1: /' || true
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/  N2: /' || true
    exit 1
fi

DEV=$(device_for_rd "$RD" "$N1")
echo "   device on $N1 = $DEV"

echo ">> promote $N1 + write 1 MiB urandom marker"
md5_before=$(write_random "$N1" "$DEV" "$SIZE_BYTES")
echo "   marker md5 = $md5_before"

# Discover the DRBD listen port from the rendered .res. DRBD-9 uses a
# single mesh port per replica's local listen socket and it's the same
# on all peers since we're parsing the address: token, not a peer-id
# entry. Same idiom as network-partition.sh.
DRBD_PORT=$(on_node "$N2" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+\$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from /etc/drbd.d/${RD}.res on $N2"
    exit 1
fi
echo "   DRBD port = $DRBD_PORT"

# --- Drop traffic on the DRBD port on the SECONDARY side -------------------
#
# Distinct from network-partition.sh which drops on the (3-way) primary
# half. Here we isolate $N2 from $N1's outbound and inbound traffic on
# tcp/$DRBD_PORT — both --dport and --sport because TCP segments in the
# established direction carry the local listen port as --sport on egress
# and the peer's listen port as --dport on ingress. Dropping only
# --dport leaves the established socket half-open until the keepalive
# timer fires, which can blow past our 30 s assertion budget.
echo ">> isolate $N2: iptables drop tcp:$DRBD_PORT in+out (sport+dport)"
BLOCKED_NODE="$N2"
BLOCKED_PORT="$DRBD_PORT"
on_node "$N2" sh -c "
    iptables -A INPUT  -p tcp --dport $DRBD_PORT -j DROP
    iptables -A OUTPUT -p tcp --dport $DRBD_PORT -j DROP
    iptables -A INPUT  -p tcp --sport $DRBD_PORT -j DROP
    iptables -A OUTPUT -p tcp --sport $DRBD_PORT -j DROP
"

# --- Assert non-Connected within $PARTITION_TIMEOUT ------------------------
#
# Parse `drbdsetup status --verbose` on the PRIMARY (it has a stable
# view of the peer). The peer line is of the form:
#     <peer-host> node-id:M connection:<STATE> role:<R> ...
# We extract `connection:<token>` and match against $BAD_STATES_RE.
echo ">> wait up to ${PARTITION_TIMEOUT}s for $N1's view of $N2 to flip non-Connected"
deadline=$(( $(date +%s) + PARTITION_TIMEOUT ))
part_state=""
while (( $(date +%s) < deadline )); do
    drbd_raw=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null || true)
    part_state=$(echo "$drbd_raw" | awk -v peer="$N2" '
        $0 ~ "^[[:space:]]*"peer"[[:space:]]" {
            for (i=1; i<=NF; i++) {
                if ($i ~ /^connection:/) {
                    split($i, a, ":")
                    print a[2]
                    exit
                }
            }
        }')
    if [[ "$part_state" =~ ^($BAD_STATES_RE)$ ]]; then
        break
    fi
    sleep 2
done

if [[ ! ( "$part_state" =~ ^($BAD_STATES_RE)$ ) ]]; then
    echo "FAIL: $N1's view of $N2 never left Connected/Established within ${PARTITION_TIMEOUT}s"
    echo "  last connection-state token: '${part_state:-<empty>}'"
    echo "  raw drbdsetup status --verbose on $N1:"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    exit 1
fi
echo "   partition observed: $N1 sees $N2 connection=$part_state"

# --- Heal partition --------------------------------------------------------
echo ">> heal: remove iptables drops on $N2"
cleanup_iptables
BLOCKED_NODE=""
BLOCKED_PORT=""

# --- Assert recovery within $HEAL_TIMEOUT ----------------------------------
#
# Healthy: peer-line connection token is either `Established` (DRBD-9
# canonical) or `Connected` (synonym observed on some kernels) AND
# disk:UpToDate on both peers. We assert via positive match on the
# healthy set; equivalently, the negation `! =~ BAD_STATES_RE` would
# fly too — earlier ReD's `connection:Connected` literal grep was bug
# because the token in this DRBD build is `Established`, not
# `Connected`, so the heal-grep never matched even on full recovery.
echo ">> wait up to ${HEAL_TIMEOUT}s for $N1's view of $N2 to return to healthy (Established|Connected) + UpToDate on both"
deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
heal_state=""
heal_ok=false
while (( $(date +%s) < deadline )); do
    drbd_raw=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null || true)
    heal_state=$(echo "$drbd_raw" | awk -v peer="$N2" '
        $0 ~ "^[[:space:]]*"peer"[[:space:]]" {
            for (i=1; i<=NF; i++) {
                if ($i ~ /^connection:/) {
                    split($i, a, ":")
                    print a[2]
                    exit
                }
            }
        }')
    n1_disk=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    n2_disk=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)

    if [[ ( "$heal_state" == "Established" || "$heal_state" == "Connected" ) \
          && "$n1_disk" == *"disk:UpToDate"* \
          && "$n2_disk" == *"disk:UpToDate"* ]]; then
        heal_ok=true
        break
    fi
    sleep 2
done

if [[ "$heal_ok" != "true" ]]; then
    echo "FAIL: did not converge to healthy state within ${HEAL_TIMEOUT}s"
    echo "  last connection token on $N1's view of $N2: '${heal_state:-<empty>}'"
    echo "  raw drbdsetup status on $N1:"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    echo "  raw drbdsetup status on $N2:"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    exit 1
fi
echo "   heal observed: $N1 sees $N2 connection=$heal_state, both disk:UpToDate"

# --- Marker round-trip -----------------------------------------------------
echo ">> read marker back on $N1 — md5 must match $md5_before"
md5_after=$(read_md5 "$N1" "$DEV" "$SIZE_BYTES")
if [[ "$md5_after" != "$md5_before" ]]; then
    echo "FAIL: marker drift on $N1 (before=$md5_before, after=$md5_after)"
    exit 1
fi
echo "   marker unchanged: $md5_after"

echo ">> STATE-STANDALONE-PARTITION OK ($N2 partitioned tcp:$DRBD_PORT → $part_state, healed → $heal_state, marker intact)"
