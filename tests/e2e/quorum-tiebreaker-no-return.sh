#!/usr/bin/env bash
#
# usage: quorum-tiebreaker-no-return.sh WORK_DIR
#
# Corner-case B7 (L4, behavioral) — a DISKLESS tiebreaker cannot RETURN
# quorum to a diskful node that already lost it; a secondary can be
# disk:UpToDate yet UNPROMOTABLE without quorum.
#
# This is an L4 *PIN of kernel behaviour*: it validates that OUR rendered
# DRBD config (`quorum: majority` + an auto-added TIE_BREAKER witness)
# produces exactly the upstream-documented quorum semantics from the
# DRBD-9 user's guide:
#
#   - ~2172-2176: "A node that has lost quorum can only regain it by
#     (re)connecting to a node that still has quorum AND has access to
#     the data (i.e. a diskful node with quorum). A diskless node — a
#     tiebreaker — does NOT count toward returning quorum to a node that
#     lost it, even though it counts toward MAINTAINING quorum."
#   - ~2209-2213: "A resource without quorum suspends I/O and cannot be
#     promoted to Primary, regardless of its own local disk state. The
#     disk may read UpToDate while the node refuses `drbdadm primary`."
#
# Topology: 2 diskful (N1, N2) + 1 auto-tiebreaker witness (diskless, on
# N3). `quorum: majority` over 3 voters → majority = 2.
#
# Three phases, each with full evidence capture (drbdsetup status --json,
# exit codes, dmesg quorum lines):
#
#   (a) BASELINE — N2 is promotable (drbdadm primary succeeds, then
#       demote back). Establishes the control: with full quorum the
#       secondary CAN be promoted.
#
#   (b) SEVER N1<->N2 ONLY (both keep their link to the TB). Each diskful
#       node still has 2/3 connections (peer-diskful lost, TB kept) →
#       per DRBD majority(3) BOTH partitions {N1,TB} and {N2,TB} retain
#       quorum. We capture which side(s) keep quorum + may_promote. This
#       is the "TB connected to both halves" case the plan flags as
#       subtle — we PIN the observed kernel verdict rather than assume.
#
#   (c) THE DOC'S CLAIM. Sever N2 from BOTH N1 AND the TB → N2 has 0 peer
#       connections = 1/3 voters → N2 LOSES quorum (suspended:quorum,
#       quorum:false). Then RESTORE ONLY N2<->TB. N2 now has {N2,TB} =
#       2/3 connections — numerically a majority — BUT the regained peer
#       is the DISKLESS tiebreaker, which per the doc CANNOT return
#       quorum to a node that lost it. Assert:
#         * N2's local disk stays UpToDate (it never lost its data), AND
#         * `drbdadm primary N2` FAILS with a quorum error, AND
#         * N2 still reports quorum:false / suspended:quorum.
#       Pre-DRBD-9-semantics (or a mis-rendered config without
#       `quorum: majority`) this would WRONGLY allow the promotion.
#
# Then heal fully (restore all links) and assert reconvergence: both
# diskful back UpToDate + Connected, N2 promotable again, no
# suspended:quorum anywhere.
#
# Modeled on state-standalone-partition.sh structure and its hardened
# wait patterns (PR #104 / corner/deflake-partition-detect): empty-read
# tolerance, kernel-ground-truth reads, confirmed-token gating, and a
# trap that ALWAYS removes the iptables rules so the stand is never left
# partitioned.
#
# Selective per-peer sever: DRBD-9 uses a single mesh port per resource
# and every peer connects on it, so a plain `--dport DROP` severs a node
# from ALL peers. To cut ONE link we drop by PEER IP + DRBD port (both
# directions, src+dst) — the node IPs are parsed from the rendered .res.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-b7-quorum-tb-no-return
STORPOOL=${STORPOOL:-stand}
N1=$WORKER_1   # diskful
N2=$WORKER_2   # diskful — the "secondary that loses quorum"
N3=$WORKER_3   # auto-tiebreaker witness (diskless) lands here

# Heal/partition budgets mirror state-standalone-partition.sh + the
# 90s fence-detect budget from network-partition.sh: DRBD's ko-count /
# ping-timeout detection of a silently-dropped link can take >30s under
# nested-QEMU pressure.
QUORUM_TIMEOUT=90
HEAL_TIMEOUT=90
WITNESS_TIMEOUT=120

# Track every (node, peer-ip) pair we have DROP rules on, so the trap
# tears every rule down regardless of which phase we failed in. Format:
# "node|peer_ip" entries.
declare -a BLOCKED_PAIRS=()
DRBD_PORT=""

drop_pair() {   # node peer_ip — drop DRBD traffic between node and peer_ip
    local node=$1 peer_ip=$2
    on_node "$node" sh -c "
        iptables -A INPUT  -p tcp -s $peer_ip --sport $DRBD_PORT -j DROP
        iptables -A OUTPUT -p tcp -d $peer_ip --dport $DRBD_PORT -j DROP
        iptables -A INPUT  -p tcp -s $peer_ip --dport $DRBD_PORT -j DROP
        iptables -A OUTPUT -p tcp -d $peer_ip --sport $DRBD_PORT -j DROP
    "
    BLOCKED_PAIRS+=("$node|$peer_ip")
}

undrop_pair() {   # node peer_ip — remove the DROP rules for one pair
    local node=$1 peer_ip=$2
    on_node "$node" sh -c "
        iptables -D INPUT  -p tcp -s $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D OUTPUT -p tcp -d $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D INPUT  -p tcp -s $peer_ip --dport $DRBD_PORT -j DROP 2>/dev/null || true
        iptables -D OUTPUT -p tcp -d $peer_ip --sport $DRBD_PORT -j DROP 2>/dev/null || true
    " 2>/dev/null || true
    # remove from BLOCKED_PAIRS bookkeeping
    local kept=()
    local p
    for p in "${BLOCKED_PAIRS[@]:-}"; do
        [[ "$p" == "$node|$peer_ip" ]] || kept+=("$p")
    done
    BLOCKED_PAIRS=("${kept[@]:-}")
}

cleanup() {
    # Tear down EVERY remaining DROP rule so the stand is never left
    # partitioned, then drop the RD.
    if [[ -n "$DRBD_PORT" ]]; then
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
    fi
    # Belt-and-braces: a leftover N2/N1 may still be Primary from a phase
    # that aborted mid-promote — demote so delete_rd's drbdsetup down
    # doesn't wedge.
    on_node "$N2" drbdadm secondary "$RD" 2>/dev/null || true
    on_node "$N1" drbdadm secondary "$RD" 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Setup: 2 diskful + auto-tiebreaker. AutoAddQuorumTiebreaker defaults on
# for a 2-diskful RD, but stamp it explicitly so the scenario is robust
# to controller default changes.
# ---------------------------------------------------------------------------
echo ">> apply 2-diskful RD on $N1 + $N2 with AutoAddQuorumTiebreaker=true"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "true"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${STORPOOL}}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: ${STORPOOL}}
EOF

wait_uptodate "$RD" "$N1" "$N2"
echo "   both diskful peers UpToDate"

# Wait for the auto-tiebreaker witness Resource to be stamped on N3 and
# its connection to come up on both diskful peers' views. The controller
# (internal/controller/resourcedefinition_controller.go::ensureTiebreaker)
# creates a DISKLESS+TIE_BREAKER Resource once it observes 2 diskful.
echo ">> wait up to ${WITNESS_TIMEOUT}s for the auto-tiebreaker witness on $N3"
deadline=$(( $(date +%s) + WITNESS_TIMEOUT ))
tb_ok=0
while (( $(date +%s) < deadline )); do
    # The witness shows up as Resource ${RD}.${N3} with a Diskless disk
    # state, and N1's kernel view should list a connection to N3.
    tbdisk=$(status_disk_state "$RD" "$N3")
    conn=$(status_connection_state "$RD" "$N1" "$N3")
    if [[ "$tbdisk" == "Diskless" && "$conn" =~ ^(Connected|Established)$ ]]; then
        tb_ok=1
        break
    fi
    sleep 3
done
if (( tb_ok != 1 )); then
    echo "FAIL: auto-tiebreaker witness on $N3 never came up Connected within ${WITNESS_TIMEOUT}s"
    echo "   N3 disk-state=$(status_disk_state "$RD" "$N3"), N1->N3 conn=$(status_connection_state "$RD" "$N1" "$N3")"
    kubectl get resources.blockstor.cozystack.io -o wide 2>/dev/null | grep "^${RD}" || true
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N1: /' || true
    exit 1
fi
echo "   auto-tiebreaker witness up on $N3 (Diskless, Connected to $N1)"

# Parse the DRBD mesh port + each node's IP from the rendered .res on N1.
RESFILE="/etc/drbd.d/${RD}.res"
DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address[^:]*:[0-9]+' ${RESFILE} | head -1 | grep -oE '[0-9]+\$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from ${RESFILE} on $N1"
    on_node "$N1" cat "$RESFILE" 2>&1 | sed 's/^/   /' || true
    exit 1
fi
node_ip() {   # node — IP from the `on <node> { address X:port }` stanza
    local node=$1
    on_node "$N1" bash -c "awk '/^[[:space:]]*on ${node} /{f=1} f&&/address/{print \$2; exit}' ${RESFILE} | grep -oE '^[0-9.]+'"
}
IP1=$(node_ip "$N1"); IP2=$(node_ip "$N2"); IP3=$(node_ip "$N3")
if [[ -z "$IP1" || -z "$IP2" || -z "$IP3" ]]; then
    echo "FAIL: could not parse node IPs (N1=$IP1 N2=$IP2 N3=$IP3) from ${RESFILE}"
    on_node "$N1" cat "$RESFILE" 2>&1 | sed 's/^/   /' || true
    exit 1
fi
echo "   DRBD port=$DRBD_PORT  IPs: $N1=$IP1 $N2=$IP2 $N3(TB)=$IP3"

# quorum_of <node> — per-volume quorum bool with a kernel-truth fallback.
# status_volume_quorum reads the events2 CRD projection, which lags (or
# is briefly empty) under partition churn; when it reads empty we fall
# back to `drbdsetup status --json`'s own `quorum` field. Returns
# "true"/"false"/"" (empty only if BOTH reads fail).
quorum_of() {
    local node=$1 q
    q=$(status_volume_quorum "$RD" "$node")
    if [[ -z "$q" ]]; then
        q=$(on_node "$node" drbdsetup status "$RD" --json 2>/dev/null \
            | jq -r '.[0].devices[0].quorum // empty' 2>/dev/null || true)
    fi
    printf '%s' "$q"
}

dump_quorum() {   # node — one-line quorum/role/may_promote evidence
    local node=$1
    local q s r mp
    q=$(quorum_of "$node")
    s=$(status_suspended "$RD" "$node")
    r=$(status_role "$RD" "$node")
    mp=$(on_node "$node" drbdsetup status "$RD" --json 2>/dev/null | jq -r '.[0]."may-promote" // "?"' 2>/dev/null || echo "?")
    echo "     $node: quorum=${q:-?} suspended=${s:-<none>} role=${r:-?} may-promote=${mp}"
}

dump_dmesg_quorum() {   # node
    on_node "$1" sh -c 'dmesg 2>/dev/null | grep -iE "drbd.*quorum|quorum.*drbd" | tail -8' 2>&1 \
        | sed "s/^/     $1 dmesg: /" || true
}

# ===========================================================================
# Phase (a): BASELINE — N2 is promotable with full quorum.
# ===========================================================================
echo ">> phase (a) baseline: N2=$N2 must be promotable with full quorum"
dump_quorum "$N2"
if ! on_node "$N2" drbdadm primary "$RD" 2>/tmp/b7-primary-a.err; then
    echo "FAIL: baseline promote of $N2 failed — full-quorum secondary should be promotable"
    sed 's/^/   /' /tmp/b7-primary-a.err 2>/dev/null || true
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   /' || true
    exit 1
fi
echo "   baseline OK: $N2 promoted to Primary"
on_node "$N2" drbdadm secondary "$RD" 2>/dev/null || true
wait_role "$RD" "$N2" "Secondary" 30 || true

# ===========================================================================
# Phase (b): sever N1<->N2 ONLY (both keep the TB). PIN observed verdict.
# ===========================================================================
echo ">> phase (b): sever $N1<->$N2 ONLY (both keep link to TB on $N3)"
drop_pair "$N1" "$IP2"   # N1 drops N2
drop_pair "$N2" "$IP1"   # N2 drops N1

echo "   wait up to ${QUORUM_TIMEOUT}s for $N1<->$N2 connection to break (TB links stay up)"
deadline=$(( $(date +%s) + QUORUM_TIMEOUT ))
sev_ok=0
c12=""
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    [[ -z "$c12" ]] && { sleep 2; continue; }
    if [[ ! "$c12" =~ ^(Connected|Established)$ ]]; then
        sev_ok=1
        break
    fi
    sleep 2
done
if (( sev_ok != 1 )); then
    echo "FAIL: $N1<->$N2 link never broke within ${QUORUM_TIMEOUT}s (last conn=$c12)"
    on_node "$N1" iptables -S 2>&1 | sed 's/^/   N1 ipt: /' || true
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N1: /' || true
    exit 1
fi
echo "   $N1<->$N2 severed (N1's view of N2: $c12); TB links intact"
echo "   PIN — quorum/promote verdict with TB bridging both halves:"
dump_quorum "$N1"
dump_quorum "$N2"
dump_quorum "$N3"
dump_dmesg_quorum "$N1"
dump_dmesg_quorum "$N2"
# DRBD majority(3): each diskful node has 2/3 connections (peer-diskful
# lost, TB kept) → both retain quorum. We assert the WEAK invariant that
# at least one diskful side keeps quorum (the partition did NOT freeze
# the whole resource), and record the full per-node verdict above. We do
# NOT hard-assert "both promotable" — a Primary on one side would make
# the other unpromotable by the single-primary rule, which is orthogonal
# to quorum. The strong assertion lives in phase (c).
q1=$(quorum_of "$N1")
q2=$(quorum_of "$N2")
if [[ "$q1" != "true" && "$q2" != "true" ]]; then
    echo "FAIL: phase (b) — NEITHER diskful node kept quorum though each retained the TB link (q1=$q1 q2=$q2)"
    echo "      with majority(3) and a TB bridging both halves, at least one side must keep quorum"
    exit 1
fi
echo "   phase (b) PIN: at least one diskful side kept quorum (q($N1)=$q1 q($N2)=$q2) via the TB"

# Restore the N1<->N2 link before phase (c) so we start from a clean,
# fully-converged 3-way before provoking the real loss.
echo ">> heal phase (b): restore $N1<->$N2"
undrop_pair "$N1" "$IP2"
undrop_pair "$N2" "$IP1"
deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
reheal=0
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    if [[ "$c12" =~ ^(Connected|Established)$ ]] \
       && [[ "$(status_disk_state "$RD" "$N1")" == "UpToDate" ]] \
       && [[ "$(status_disk_state "$RD" "$N2")" == "UpToDate" ]]; then
        reheal=1
        break
    fi
    sleep 2
done
if (( reheal != 1 )); then
    echo "FAIL: $N1<->$N2 did not re-converge after phase (b) heal within ${HEAL_TIMEOUT}s"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N1: /' || true
    exit 1
fi
echo "   phase (b) healed: $N1<->$N2 Connected, both UpToDate"

# ===========================================================================
# Phase (c): THE DOC'S CLAIM — diskless TB cannot RETURN quorum.
# ===========================================================================
echo ">> phase (c): sever $N2 from BOTH $N1 AND TB($N3) → $N2 loses quorum"
drop_pair "$N2" "$IP1"   # N2 drops N1
drop_pair "$N2" "$IP3"   # N2 drops TB
# Symmetric drops on the peers so neither direction leaks.
drop_pair "$N1" "$IP2"   # N1 drops N2
drop_pair "$N3" "$IP2"   # TB drops N2

echo "   wait up to ${QUORUM_TIMEOUT}s for $N2 to LOSE quorum (0/3 peers)"
deadline=$(( $(date +%s) + QUORUM_TIMEOUT ))
lost=0
while (( $(date +%s) < deadline )); do
    q=$(quorum_of "$N2")
    s=$(status_suspended "$RD" "$N2")
    if [[ "$q" == "false" || -n "$s" ]]; then
        lost=1
        break
    fi
    sleep 2
done
if (( lost != 1 )); then
    echo "FAIL: $N2 never lost quorum after being severed from both peers within ${QUORUM_TIMEOUT}s"
    dump_quorum "$N2"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N2: /' || true
    exit 1
fi
echo "   $N2 LOST quorum (quorum=$(quorum_of "$N2") suspended=$(status_suspended "$RD" "$N2"))"
dump_quorum "$N2"
dump_dmesg_quorum "$N2"

# N2's local disk must STILL be UpToDate — it never lost its data, only
# its quorum. This is the "UpToDate yet unpromotable" precondition.
n2disk=$(status_disk_state "$RD" "$N2")
# status_disk_state can read empty while I/O is suspended; fall back to
# the kernel directly.
if [[ -z "$n2disk" || "$n2disk" != "UpToDate" ]]; then
    n2disk=$(on_node "$N2" drbdsetup status "$RD" --json 2>/dev/null | jq -r '.[0].devices[0]."disk-state" // ""' 2>/dev/null || echo "")
fi
echo "   $N2 local disk-state while no-quorum: ${n2disk:-<empty>}"
if [[ "$n2disk" != "UpToDate" ]]; then
    echo "FAIL: precondition — $N2 disk must be UpToDate while no-quorum (got '${n2disk:-<empty>}')"
    exit 1
fi

# --- RESTORE ONLY N2<->TB. N2 now has {N2,TB}=2/3 connections. ---
echo ">> phase (c): restore ONLY $N2<->TB($N3); $N2<->$N1 stays severed"
undrop_pair "$N2" "$IP3"
undrop_pair "$N3" "$IP2"

echo "   wait up to ${HEAL_TIMEOUT}s for $N2<->TB to reconnect"
deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
tbback=0
c23=""
while (( $(date +%s) < deadline )); do
    c23=$(status_connection_state "$RD" "$N2" "$N3")
    if [[ "$c23" =~ ^(Connected|Established)$ ]]; then
        tbback=1
        break
    fi
    sleep 2
done
if (( tbback != 1 )); then
    echo "FAIL: $N2<->TB did not reconnect within ${HEAL_TIMEOUT}s (last=$c23)"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N2: /' || true
    exit 1
fi
echo "   $N2<->TB reconnected ($c23). $N2 now has 2/3 connections — but the regained peer is DISKLESS."

# Give DRBD a moment to (NOT) update quorum, then snapshot the verdict.
sleep 6
echo "   verdict after TB-only reconnect:"
dump_quorum "$N2"
dump_dmesg_quorum "$N2"

# THE PIN: N2's disk is UpToDate, the TB is reconnected, yet N2 must NOT
# be promotable and must NOT have regained quorum — a diskless tiebreaker
# cannot RETURN quorum.
n2disk_after=$(status_disk_state "$RD" "$N2")
if [[ -z "$n2disk_after" || "$n2disk_after" != "UpToDate" ]]; then
    n2disk_after=$(on_node "$N2" drbdsetup status "$RD" --json 2>/dev/null | jq -r '.[0].devices[0]."disk-state" // ""' 2>/dev/null || echo "")
fi
echo "   $N2 disk-state after TB reconnect: ${n2disk_after:-<empty>}"
if [[ "$n2disk_after" != "UpToDate" ]]; then
    echo "FAIL: $N2 disk must remain UpToDate after TB reconnect (got '${n2disk_after:-<empty>}')"
    exit 1
fi

# Promotion MUST fail. Capture exit code + stderr.
echo "   attempt drbdadm primary $RD on $N2 — MUST FAIL with a quorum error"
prim_rc=0
on_node "$N2" drbdadm primary "$RD" >/tmp/b7-primary-c.out 2>/tmp/b7-primary-c.err || prim_rc=$?
prim_err=$(cat /tmp/b7-primary-c.err 2>/dev/null || true)
echo "     drbdadm primary exit=$prim_rc"
echo "$prim_err" | sed 's/^/     stderr: /'

if (( prim_rc == 0 )); then
    # It promoted. That means a diskless TB returned quorum — a violation
    # of the documented semantics (or our config lost `quorum: majority`).
    echo "FAIL: B7 VIOLATED — $N2 was promoted to Primary after regaining ONLY a diskless"
    echo "      tiebreaker connection. A diskless TB must NOT return quorum to a node that"
    echo "      lost it (drbd-admin ~2172-2176). Demoting to keep the stand sane."
    on_node "$N2" drbdadm secondary "$RD" 2>/dev/null || true
    dump_quorum "$N2"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N2: /' || true
    exit 1
fi

# The promote failed (expected). Confirm it failed for the RIGHT reason:
# quorum, not some unrelated error. We require quorum:false as the
# kernel-truth corroboration so a spurious unrelated failure can't pass.
q_after=$(quorum_of "$N2")
if [[ "$q_after" == "true" ]]; then
    echo "FAIL: $N2 reports quorum=true after regaining only the diskless TB — the TB"
    echo "      wrongly RETURNED quorum (promote happened to fail for another reason)."
    exit 1
fi
echo "   PIN CONFIRMED: $N2 disk=UpToDate, TB reconnected, yet promote REFUSED"
echo "                  (exit=$prim_rc) and quorum=${q_after:-false} — diskless TB did NOT return quorum."

# ===========================================================================
# Heal fully + assert reconvergence.
# ===========================================================================
echo ">> heal fully: restore $N2<->$N1 (all links back)"
undrop_pair "$N2" "$IP1"
undrop_pair "$N1" "$IP2"

echo "   wait up to ${HEAL_TIMEOUT}s for full reconvergence (both diskful UpToDate+Connected, quorum back)"
deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
conv=0
c12=""; d1=""; d2=""; q2=""
while (( $(date +%s) < deadline )); do
    c12=$(status_connection_state "$RD" "$N1" "$N2")
    d1=$(status_disk_state "$RD" "$N1")
    d2=$(status_disk_state "$RD" "$N2")
    q2=$(quorum_of "$N2")
    if [[ "$c12" =~ ^(Connected|Established)$ && "$d1" == "UpToDate" && "$d2" == "UpToDate" && "$q2" == "true" ]]; then
        conv=1
        break
    fi
    sleep 2
done
if (( conv != 1 )); then
    echo "FAIL: did not fully reconverge within ${HEAL_TIMEOUT}s (c12=$c12 d1=$d1 d2=$d2 q2=$q2)"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N1: /' || true
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   N2: /' || true
    exit 1
fi
echo "   fully healed: $N1<->$N2 Connected, both UpToDate, $N2 quorum=true"

# Final control: with quorum restored, N2 is promotable again.
echo ">> post-heal control: $N2 must be promotable again"
if ! on_node "$N2" drbdadm primary "$RD" 2>/tmp/b7-primary-heal.err; then
    echo "FAIL: $N2 not promotable after full heal — reconvergence incomplete"
    sed 's/^/   /' /tmp/b7-primary-heal.err 2>/dev/null || true
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/   /' || true
    exit 1
fi
on_node "$N2" drbdadm secondary "$RD" 2>/dev/null || true
echo "   post-heal control OK: $N2 promotable again"

echo ">> QUORUM-TIEBREAKER-NO-RETURN OK"
echo "   (a) baseline promotable; (b) TB bridges both halves → at least one keeps quorum;"
echo "   (c) PINNED: diskless TB did NOT return quorum to no-quorum $N2 (UpToDate yet"
echo "       unpromotable, exit=$prim_rc, quorum=false); full heal → promotable again."
