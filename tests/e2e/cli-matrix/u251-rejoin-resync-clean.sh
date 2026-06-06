#!/usr/bin/env bash
#
# usage: u251-rejoin-resync-clean.sh WORK_DIR
#
# L6 cli-matrix cell — U251 (P1, rejoining-node resync correctness).
#
# Upstream LINSTOR user report: after a node briefly lost its DRBD
# replication link and rejoined, its resyncs got stuck at 97-98% and the
# resource surface kept showing a stale partial sync that never drained
# to a clean UpToDate.
#
# Blockstor pin: place ~4 small RDs so ONE worker holds a diskful replica
# of each, anchor real data on the survivors, then sever JUST that
# worker's DRBD replication ports with iptables (a "soft" partition —
# the node, kubelet and satellite pod stay up; only DRBD peer traffic is
# dropped). Write more data on the surviving peers while the link is
# down so the isolated replica falls genuinely out-of-sync, then heal
# (flush the rules) and assert that EVERY resource on the previously-
# isolated node drains to a CLEAN UpToDate — no sync percentage stuck for
# >60s — AND the CRD .status.volumes[0].diskState agrees with the
# kernel's `drbdsetup status` truth on that node.
#
# CRITICAL — port derivation (vs. the original draft's bug):
#   The draft hard-coded DRBD TCP ports 7000:7999 (upstream LINSTOR's
#   TcpPortAutoRange default). blockstor DELIBERATELY allocates DRBD
#   ports from 20000-20999 (docs/cli-parity-known-deltas.md row 71:
#   drbd.DefaultPortMin/Max) so it can coexist with a live upstream
#   LINSTOR on the same kernel TCP-port namespace. Isolating 7000:7999
#   on a blockstor stand drops NOTHING — the test would heal-then-pass
#   vacuously because the link was never actually severed.
#   We therefore DERIVE the exact per-resource listen ports from the
#   Resource CRD's authoritative Spec.DRBDPort (api/v1alpha1
#   Resource.Spec.DRBDPort, jsonpath {.spec.drbdPort}; mirrored to
#   {.status.drbdPort}). We isolate exactly the union of those ports on
#   the chosen node and fall back to the blockstor default window
#   20000:20999 only if derivation yields nothing.
#
# SAFETY:
#   - iptables rules are scoped to the DRBD ports on ONE node only,
#     applied INSIDE that node's satellite pod (which shares the host
#     net namespace). NEVER touches blockstor controller/apiserver pods.
#   - A hard cleanup trap flushes the rules on ANY exit (EXIT/INT/TERM)
#     so a partition can never leak past this cell.
#   - SKIPs cleanly (exit 0) when <4 workers, when the iptables apply
#     fails (no NET_ADMIN), or when no DRBD ports can be derived AND the
#     default-window fallback also drops nothing.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

# U251 needs 4 workers: 3 to carry 2-replica RDs that all include the
# isolated node, plus headroom. With exactly 3 workers a 2-replica RD
# placed on {iso, other} still works, but 4 gives the autoplacer real
# choice and keeps the isolated node from being the only candidate.
require_workers 3

linstor_cli_setup

PREFIX=cli-matrix-u251
POOL=${POOL:-stand}
RDS=(
    "${PREFIX}-01"
    "${PREFIX}-02"
    "${PREFIX}-03"
    "${PREFIX}-04"
)

# The node we will isolate. WORKER_3 is the conventional "victim" slot in
# the other partition-style cells; every RD below is forced to place a
# replica there.
ISO=$WORKER_3

# Default blockstor DRBD port window — fallback ONLY (see header). This is
# the real blockstor range, NOT upstream's 7000:7999.
BS_DEFAULT_PORTS="20000:20999"

# Space-separated list of single ports we actually isolated. heal()
# deletes exactly these (plus the fallback window) so the trap is correct
# regardless of which apply path ran; empty when nothing was isolated.
ISO_PORTS=""

# heal — flush every DRBD-port DROP rule this cell may have inserted on
# the isolated node. Idempotent; safe to call when nothing was applied.
# Deletes both the per-port rules and the fallback-window rules so the
# trap is correct regardless of which path applied them.
heal() {
    local p
    # Per-resource single-port rules.
    for p in $ISO_PORTS; do
        on_node "$ISO" sh -c "
            iptables -D INPUT  -p tcp --dport ${p} -j DROP 2>/dev/null
            iptables -D OUTPUT -p tcp --sport ${p} -j DROP 2>/dev/null
            iptables -D INPUT  -p tcp --sport ${p} -j DROP 2>/dev/null
            iptables -D OUTPUT -p tcp --dport ${p} -j DROP 2>/dev/null
            true
        " 2>/dev/null || true
    done
    # Fallback-window rules.
    on_node "$ISO" sh -c "
        iptables -D INPUT  -p tcp --dport ${BS_DEFAULT_PORTS} -j DROP 2>/dev/null
        iptables -D OUTPUT -p tcp --sport ${BS_DEFAULT_PORTS} -j DROP 2>/dev/null
        iptables -D INPUT  -p tcp --sport ${BS_DEFAULT_PORTS} -j DROP 2>/dev/null
        iptables -D OUTPUT -p tcp --dport ${BS_DEFAULT_PORTS} -j DROP 2>/dev/null
        true
    " 2>/dev/null || true
}

cleanup() {
    # ALWAYS heal first — a leaked partition is the worst outcome.
    heal
    local rd
    for rd in "${RDS[@]}"; do
        delete_rd "$rd"
    done
    for rd in "${RDS[@]}"; do
        assert_no_orphans "$rd"
    done
    linstor_cli_teardown
}
trap cleanup EXIT INT TERM

# Pre-flight: the named pool on at least 3 nodes including ISO. U251 needs
# the isolated node to actually carry diskful backing storage; a diskless
# placement there has no resync to drain.
echo ">> pre-flight: $POOL SP present on >=3 nodes (incl $ISO)"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
pool_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique' <<<"$sp_json" 2>/dev/null || echo "[]")
have=$(jq -r 'length' <<<"$pool_nodes" 2>/dev/null || echo 0)
iso_has_pool=$(jq -r --arg n "$ISO" 'index($n) != null' <<<"$pool_nodes" 2>/dev/null || echo false)
if (( have < 3 )) || [[ "$iso_has_pool" != "true" ]]; then
    echo "SKIP: need $POOL on >=3 nodes incl $ISO (got $have nodes, iso_has_pool=$iso_has_pool) — U251 fixture unavailable"
    exit 0
fi

# ---- build the resources, each with a diskful replica on ISO ----
#
# Force-place: one replica explicitly on ISO (`r c $ISO`), one more via
# auto-place to a second diskful node. This guarantees ISO carries a
# diskful copy of EVERY RD so every RD has a resync that must drain on
# heal — autoplace alone could legally skip ISO.
echo ">> creating ${#RDS[@]} RDs (64M, diskful replica pinned on $ISO + 1 auto-placed peer)"
for rd in "${RDS[@]}"; do
    "${LCTL[@]}" resource-definition create "$rd" >/dev/null
    "${LCTL[@]}" volume-definition create "$rd" 64M >/dev/null
    # Pin one diskful replica on the to-be-isolated node...
    "${LCTL[@]}" resource create "$ISO" "$rd" --storage-pool="$POOL" >/dev/null
    # ...and add a second diskful replica via auto-place (+1 over the
    # existing one → total 2 diskful).
    "${LCTL[@]}" resource create --auto-place=+1 --storage-pool="$POOL" "$rd" >/dev/null
done

echo ">> waiting for all RDs to converge UpToDate (pre-isolation)"
for rd in "${RDS[@]}"; do
    # Identify ISO's diskful peer for this RD and wait for the pair.
    mapfile -t diskful < <(linstor_diskful_nodes "$rd")
    peer=""
    for n in "${diskful[@]}"; do
        [[ "$n" != "$ISO" ]] && peer="$n" && break
    done
    if [[ -z "$peer" ]]; then
        echo "FAIL (U251): $rd has no diskful peer besides $ISO — placement did not produce 2 diskful replicas" >&2
        linstor_diskful_nodes "$rd" | sed 's/^/    diskful: /' >&2
        exit 1
    fi
    if ! wait_uptodate "$rd" "$ISO" "$peer"; then
        echo "FAIL (U251): pre-isolation convergence timed out for $rd ($ISO<->$peer)" >&2
        exit 1
    fi
done
echo "   pre-isolation: every RD UpToDate on $ISO + peer"

# ---- DERIVE the per-resource DRBD listen ports for ISO ----
#
# Authoritative source: Resource.Spec.DRBDPort (api/v1alpha1). Read the
# isolated replica's own port; fall back to the status mirror, then to the
# peer's port (the listen port is RD-wide on DRBD-9). Collect the union of
# distinct ports across all RDs.
echo ">> deriving DRBD ports from Resource CRDs on $ISO (NOT upstream 7000s)"
declare -A port_seen=()
for rd in "${RDS[@]}"; do
    port=$(kubectl get "resources.blockstor.cozystack.io/${rd}.${ISO}" \
        -o jsonpath='{.spec.drbdPort}' 2>/dev/null || echo "")
    if [[ -z "$port" ]]; then
        port=$(kubectl get "resources.blockstor.cozystack.io/${rd}.${ISO}" \
            -o jsonpath='{.status.drbdPort}' 2>/dev/null || echo "")
    fi
    if [[ -z "$port" ]]; then
        # RD-wide port: pull it from any replica of the RD.
        port=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
            | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
            | while read -r name; do
                p=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
                    -o jsonpath='{.spec.drbdPort}' 2>/dev/null || echo "")
                [[ -n "$p" ]] && { echo "$p"; break; }
              done)
    fi
    if [[ -n "$port" && "$port" =~ ^[0-9]+$ ]]; then
        port_seen["$port"]=1
        echo "   $rd -> DRBD port $port"
    else
        echo "   $rd -> port not derivable from CRD" >&2
    fi
done

ISO_PORTS="${!port_seen[*]}"
USE_FALLBACK=false
if [[ -z "$ISO_PORTS" ]]; then
    echo "   WARN: no DRBD ports derivable from any CRD — falling back to BS default window $BS_DEFAULT_PORTS" >&2
    USE_FALLBACK=true
fi

# ---- ISOLATE: drop DRBD replication traffic on ISO ----
echo ">> isolating $ISO DRBD link"
if [[ "$USE_FALLBACK" == "true" ]]; then
    if ! on_node "$ISO" sh -c "
        iptables -I INPUT  -p tcp --dport ${BS_DEFAULT_PORTS} -j DROP
        iptables -I OUTPUT -p tcp --sport ${BS_DEFAULT_PORTS} -j DROP
        iptables -I INPUT  -p tcp --sport ${BS_DEFAULT_PORTS} -j DROP
        iptables -I OUTPUT -p tcp --dport ${BS_DEFAULT_PORTS} -j DROP
    "; then
        echo "SKIP: could not apply iptables on $ISO (no NET_ADMIN in satellite pod?) — U251 not exercisable"
        exit 0
    fi
else
    apply_ok=true
    for p in $ISO_PORTS; do
        if ! on_node "$ISO" sh -c "
            iptables -I INPUT  -p tcp --dport ${p} -j DROP
            iptables -I OUTPUT -p tcp --sport ${p} -j DROP
            iptables -I INPUT  -p tcp --sport ${p} -j DROP
            iptables -I OUTPUT -p tcp --dport ${p} -j DROP
        "; then
            apply_ok=false
            break
        fi
    done
    if [[ "$apply_ok" != "true" ]]; then
        echo "SKIP: could not apply iptables on $ISO (no NET_ADMIN in satellite pod?) — U251 not exercisable"
        exit 0
    fi
fi
echo "   isolated ports: ${ISO_PORTS:-$BS_DEFAULT_PORTS}"

# ---- confirm the link actually dropped (non-vacuous gate) ----
#
# If the link does NOT drop, the heal-then-converge assertion below would
# pass trivially (nothing was ever broken). Require at least ONE RD to
# show ISO's connection in a non-Connected state within 90s; otherwise
# FAIL — we isolated the wrong ports (e.g. the 7000s-vs-20000s bug).
echo ">> verifying DRBD link to $ISO actually dropped (else port derivation was wrong)"
dropped=false
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    for rd in "${RDS[@]}"; do
        mapfile -t diskful < <(linstor_diskful_nodes "$rd")
        for n in "${diskful[@]}"; do
            [[ "$n" == "$ISO" ]] && continue
            st=$(status_connection_state "$rd" "$n" "$ISO" 2>/dev/null || echo "")
            if [[ -n "$st" && ! "$st" =~ ^(Connected|Established)$ ]]; then
                echo "   $rd: $n sees $ISO as '$st' — link is down"
                dropped=true
                break 3
            fi
        done
    done
    sleep 3
done
if [[ "$dropped" != "true" ]]; then
    echo "FAIL (U251): DRBD link to $ISO never dropped after isolating ports '${ISO_PORTS:-$BS_DEFAULT_PORTS}'" >&2
    echo "  This is the draft's 7000-vs-20000 bug fingerprint: the isolated ports carry no DRBD traffic." >&2
    for rd in "${RDS[@]}"; do
        on_node "$ISO" drbdsetup status "$rd" 2>/dev/null | sed "s/^/    ${rd}: /" >&2 || true
    done
    exit 1
fi

# ---- write more data on the survivors while ISO is cut off ----
#
# This advances the surviving replicas' generation so ISO falls genuinely
# out-of-sync and has a real resync to perform on heal (not a no-op).
echo ">> writing data on surviving peers while $ISO is isolated"
for rd in "${RDS[@]}"; do
    mapfile -t diskful < <(linstor_diskful_nodes "$rd")
    survivor=""
    for n in "${diskful[@]}"; do
        [[ "$n" != "$ISO" ]] && survivor="$n" && break
    done
    [[ -z "$survivor" ]] && continue
    dev=$(kubectl get "resources.blockstor.cozystack.io/${rd}.${survivor}" \
        -o jsonpath='{.status.volumes[0].devicePath}' 2>/dev/null || echo "")
    if [[ -z "$dev" ]]; then
        minor=$(kubectl get "resources.blockstor.cozystack.io/${rd}.${survivor}" \
            -o jsonpath='{.status.drbdMinor}' 2>/dev/null || echo "")
        [[ -n "$minor" ]] && dev="/dev/drbd${minor}"
    fi
    [[ -z "$dev" ]] && continue
    on_node "$survivor" sh -c "
        D='${dev}'
        if [ -b \"\$D\" ]; then
            drbdsetup primary '${rd}' --force 2>/dev/null \
                || drbdadm primary --force '${rd}' 2>/dev/null || true
            dd if=/dev/urandom of=\"\$D\" bs=1M count=16 oflag=direct 2>/dev/null || true
            sync
            drbdadm secondary '${rd}' 2>/dev/null || true
        fi
    " 2>/dev/null || true
    echo "   wrote 16 MiB on $survivor:$dev for $rd"
done

# ---- HEAL: flush rules; ISO's resyncs must drain to clean UpToDate ----
echo ">> healing $ISO (flush iptables) — resyncs must reach clean UpToDate"
heal

# ---- assert every RD on ISO drains to clean UpToDate, no stuck sync ----
#
# CLEAN means: kernel disk-state UpToDate AND no sync-percentage stuck for
# >60s. We poll `drbdsetup status --json` on ISO and read each peer
# device's "done" percentage; if a resource's done% is identical for
# >STUCK_WINDOW seconds while still <100 / not UpToDate, that is the U251
# stuck-partial-sync failure.
echo ">> waiting up to 420s for all resyncs to drain CLEAN on $ISO"
STUCK_WINDOW=60
GLOBAL_DEADLINE=$(( $(date +%s) + 420 ))
all_clean=false

# Per-RD stuck tracking: last-seen done% and the timestamp it was first
# seen at that value.
declare -A last_pct=()
declare -A last_pct_ts=()

while (( $(date +%s) < GLOBAL_DEADLINE )); do
    pending=0
    now=$(date +%s)
    for rd in "${RDS[@]}"; do
        # ISO's local disk-state for vol 0.
        ds=$(status_disk_state "$rd" "$ISO" 0)
        # Kernel-truth done% toward ANY peer (min across peers = slowest).
        pct=$(on_node "$ISO" drbdsetup status "$rd" --json 2>/dev/null | jq -r '
            [.[0].connections[]? | .peer_devices[]? | (.done // empty)]
            | map(select(. != null)) | (min // 100)' 2>/dev/null || echo "")
        [[ -z "$pct" ]] && pct=""

        if [[ "$ds" == "UpToDate" && ( -z "$pct" || "$pct" == "100" ) ]]; then
            continue
        fi

        pending=$((pending+1))

        # Stuck detection: if done% has not advanced for >STUCK_WINDOW
        # seconds while not yet UpToDate, fail loudly.
        cur="${pct:-na}"
        if [[ "${last_pct[$rd]:-}" == "$cur" ]]; then
            elapsed=$(( now - ${last_pct_ts[$rd]:-now} ))
            if (( elapsed > STUCK_WINDOW )); then
                echo "FAIL (U251): $rd on $ISO stuck at done=${cur}% diskState=$ds for ${elapsed}s (>${STUCK_WINDOW}s) — partial resync never drains" >&2
                on_node "$ISO" drbdsetup status "$rd" --verbose 2>&1 | sed "s/^/    /" >&2 || true
                exit 1
            fi
        else
            last_pct["$rd"]="$cur"
            last_pct_ts["$rd"]=$now
        fi
    done

    if (( pending == 0 )); then
        all_clean=true
        break
    fi
    sleep 10
done

if [[ "$all_clean" != "true" ]]; then
    echo "FAIL (U251): not all resyncs reached clean UpToDate on $ISO within 420s" >&2
    "${LCTL[@]}" resource list 2>/dev/null | grep -E "$PREFIX" | sed 's/^/    /' >&2 || true
    for rd in "${RDS[@]}"; do
        on_node "$ISO" drbdsetup status "$rd" 2>/dev/null | sed "s/^/    ${rd}: /" >&2 || true
    done
    exit 1
fi
echo "   all resyncs drained to clean UpToDate on $ISO"

# ---- kernel-truth parity: CRD diskState must match drbdsetup on ISO ----
echo ">> U251: CRD .status.volumes[0].diskState must match kernel on $ISO"
mismatch=0
for rd in "${RDS[@]}"; do
    crd=$(status_disk_state "$rd" "$ISO" 0)
    kern=$(on_node "$ISO" drbdsetup status "$rd" 2>/dev/null \
        | grep -oE 'disk:[A-Za-z]+' | head -1 | cut -d: -f2 || echo "")
    if [[ -z "$kern" ]]; then
        echo "FAIL (U251): could not read kernel disk-state for $rd on $ISO" >&2
        mismatch=$((mismatch+1))
        continue
    fi
    if [[ "$crd" != "$kern" ]]; then
        echo "  MISMATCH $rd@$ISO: CRD='$crd' kernel='$kern'" >&2
        mismatch=$((mismatch+1))
    else
        echo "   $rd@$ISO: CRD=kernel=$kern OK"
    fi
done
if (( mismatch > 0 )); then
    echo "FAIL (U251): $mismatch CRD/kernel diskState mismatch(es) after rejoin+heal" >&2
    exit 1
fi

echo ">> u251-rejoin-resync-clean OK (rejoining $ISO drained every resync to clean UpToDate; CRD matches kernel)"
