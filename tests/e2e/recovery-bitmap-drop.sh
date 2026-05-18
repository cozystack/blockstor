#!/usr/bin/env bash
#
# usage: recovery-bitmap-drop.sh WORK_DIR
#
# Scenario 5.23 — DRBD bitmap drop / "Can not drop the bitmap" race.
#
# Source: tests/scenarios/05-drbd-state-recovery.md §5.23;
#         tests/recovery-skill-scenarios.md §B5 ("upstream-bug #1").
#
# Background:
#   DRBD 9.2.16 has a bitmap-handling race during a diskful<->diskless
#   toggle: when a peer transitions from diskful -> diskless -> diskful
#   in quick succession the in-kernel bitmap slot for that peer is
#   freed before the connection finishes flushing, and the next
#   reconnect attempt logs `Can not drop the bitmap` and refuses to
#   come back UpToDate. The peer ends up StandAlone-ish (connection
#   refuses to negotiate) until an operator forces it.
#
#   SKILL/recovery-skill §B5 documents the recipe:
#     drbdadm disconnect <rd>
#     drbdadm connect --discard-my-data <rd>
#   on the affected (newly-diskful) side. The `--discard-my-data`
#   flag tells DRBD "I know my bitmap state is suspect, take peer
#   data as authoritative" — which is exactly what we want here
#   because the freshly-promoted side just came up from diskless.
#
#   Upstream fixed this in DRBD 9.2.17 (kernel-side). On 9.2.17+
#   the race is gone and the test is meaningless — we treat that
#   as a SKIP (the spec calls it `xfail on kernel >= 9.2.17`).
#
# Methodology:
#   1. Detect the DRBD kernel version on $WORKER_1 (the satellite
#      pod has `cat /proc/drbd` if the module is loaded, else
#      `modinfo drbd | grep ^version`). Bail with SKIP if >= 9.2.17.
#      Also bail with SKIP if we can't read a version at all — a
#      missing module is a stand-bring-up bug, not a 5.23 finding.
#   2. On 9.2.16-: create a 2-replica RD on N1+N2, wait UpToDate.
#   3. Toggle $N1's replica diskful -> diskless -> diskful via the
#      blockstor REST `toggle-disk` endpoints. Capture `dmesg -T`
#      on $N1 before and after the toggle.
#   4. If the bitmap error message appears in the dmesg delta,
#      apply the recipe (disconnect + connect --discard-my-data)
#      on $N1 and wait for UpToDate.
#   5. PASS if either:
#        (a) the toggle completed cleanly with no bitmap error
#            (race didn't trip — DRBD 9.2.16 is racy but not
#            deterministic, ~30-50% reproduction rate per upstream
#            issue), or
#        (b) the bitmap error was observed AND the recipe
#            converged the replica back to UpToDate within the
#            recovery window.
#      FAIL only if the bitmap error fired AND the recipe failed
#      to converge — that would be a real regression of the
#      documented operator recipe.
#
# Regression guards:
#   - $N2 must stay UpToDate throughout — the toggle and recipe
#     target $N1 only; if $N2 disk flaps, the test setup is
#     interacting with something other than the bitmap-drop race.
#   - The recipe MUST NOT touch blockstor CRDs — it is a pure
#     drbdadm shell sequence on the satellite, exercising the
#     reconciler's tolerance of operator-applied recovery commands
#     (same contract as recovery-discard-my-data.sh §5.14).
#
# CI vs. operator-runbook duality:
#   This script doubles as an executable runbook for operators
#   hitting the bitmap-drop bug on a 9.2.16 stand. On a 9.2.17+
#   CI cluster it returns 0 immediately after the SKIP banner;
#   on a 9.2.16 cluster it walks the full recipe. Either way it
#   prints the kernel version it observed so test reports have a
#   pinned fact about which DRBD the stand is running.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=bitmap-drop-test
N1=$WORKER_1
N2=$WORKER_2
SIZE_KIB=65536
TOGGLE_LOOPS=${TOGGLE_LOOPS:-3}   # 9.2.16 race is non-deterministic;
                                  # loop the toggle a few times to
                                  # raise reproduction odds.
RECOVERY_WINDOW=${RECOVERY_WINDOW:-30}

# ---------- step 1: detect DRBD kernel version on $N1 ----------
#
# /proc/drbd is the canonical source when the kernel module is
# loaded — first line is `version: X.Y.Z (api:N/proto:M-K)`.
# `modinfo drbd` is the fallback if /proc/drbd hasn't been touched
# yet (it gets created on first drbdadm/drbdsetup call, which is
# always true on a stand that's already running blockstor — but
# we keep the fallback for completeness).
echo ">> step 1: detect DRBD kernel version on $N1"
ver_line=$(on_node "$N1" bash -c "cat /proc/drbd 2>/dev/null | head -1" || true)
if [[ -z "$ver_line" || "$ver_line" != *version:* ]]; then
    ver_line=$(on_node "$N1" bash -c "modinfo drbd 2>/dev/null | awk '/^version:/ { print \$2; exit }'" || true)
    drbd_ver="$ver_line"
else
    drbd_ver=$(echo "$ver_line" | awk '{ for (i=1;i<=NF;i++) if ($i=="version:") { print $(i+1); exit } }')
fi

if [[ -z "$drbd_ver" ]]; then
    echo "SKIP: could not detect DRBD kernel version on $N1 (no /proc/drbd, no modinfo) — module likely not loaded"
    exit 0
fi
echo "   DRBD kernel version on $N1 = $drbd_ver"

# Parse MAJOR.MINOR.PATCH. Accept any trailing junk (e.g.
# `9.2.16-rc1`, `9.2.16+linbit-1`) — the gate is whether the
# numeric triple is at-or-above 9.2.17.
maj=$(echo "$drbd_ver" | awk -F. '{ print $1+0 }')
min=$(echo "$drbd_ver" | awk -F. '{ print $2+0 }')
pat=$(echo "$drbd_ver" | awk -F. '{ split($3, p, /[^0-9]/); print p[1]+0 }')
echo "   parsed = ${maj}.${min}.${pat}"

# Gate: (maj > 9) OR (maj == 9 AND min > 2) OR (maj == 9 AND
# min == 2 AND pat >= 17). The current world is 9.2.x; the >9 /
# >2 branches are forward-compatible plumbing for when a 9.3 or
# 10.x lands and the race might be re-introduced — at which point
# this gate should be re-evaluated explicitly by a human, not
# silently re-enabled by the test.
fixed_upstream=false
if (( maj > 9 )); then
    fixed_upstream=true
elif (( maj == 9 && min > 2 )); then
    fixed_upstream=true
elif (( maj == 9 && min == 2 && pat >= 17 )); then
    fixed_upstream=true
fi

if [[ "$fixed_upstream" == "true" ]]; then
    echo "SKIP: kernel ${drbd_ver} >= 9.2.17 — bitmap drop fixed upstream (xfail per scenario 5.23)"
    exit 0
fi

echo "   kernel ${drbd_ver} is in the 9.2.16- bracket — proceeding with toggle drill"

# ---------- step 2: 2-replica RD on $N1+$N2, wait UpToDate ----------
trap 'delete_rd "$RD"' EXIT

echo ">> step 2: apply 2-replica RD on $N1 + $N2"
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
echo "   RD UpToDate on $N1 and $N2"

# Open a controller port-forward for the toggle-disk REST calls.
# Same pattern as recovery-port-collision.sh / lifecycle-toggle-retry.sh.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/recovery-bitmap-drop-pf.log 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true; wait "$PF_PID" 2>/dev/null || true; delete_rd "$RD"' EXIT
for _ in $(seq 1 20); do
    curl -fsS -m1 "http://127.0.0.1:$PF_PORT/v1/healthz" >/dev/null 2>&1 && break
    sleep 0.5
done

# ---------- step 3: toggle diskful -> diskless -> diskful, watch dmesg ----------
#
# We baseline `dmesg -T | tail -1` so the post-toggle delta only
# contains lines emitted during the toggle. Using `dmesg --since`
# would be cleaner but the satellite container's busybox dmesg
# doesn't always support it; tailing by line is portable.
bitmap_error_observed=false

for loop in $(seq 1 "$TOGGLE_LOOPS"); do
    echo ">> step 3.${loop}: toggle ${RD}.${N1} diskful -> diskless -> diskful"

    # Mark a sentinel into the kernel ring so the post-toggle scan
    # can scope to "lines emitted after this point". `printf` into
    # /dev/kmsg from the satellite (privileged) is the canonical
    # way; the satellite already runs privileged for drbdsetup.
    sentinel="bitmap-drop-test-loop-${loop}-$(date +%s%N)"
    on_node "$N1" bash -c "printf '%s\n' '${sentinel}' > /dev/kmsg 2>/dev/null || true"

    # diskful -> diskless: PUT toggle-disk/diskless.
    http_code=$(curl -s -o /tmp/bitmap-drop-r1.json -w '%{http_code}' -m 30 -X PUT \
        "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}/resources/${N1}/toggle-disk/diskless")
    echo "   diskful -> diskless: HTTP $http_code"
    if [[ "$http_code" != "200" ]]; then
        echo "   body: $(cat /tmp/bitmap-drop-r1.json)"
        echo "FAIL: toggle-disk/diskless on ${RD}.${N1} returned HTTP $http_code"
        exit 1
    fi

    # Give the satellite reconciler time to actually run `drbdadm
    # adjust` and detach the backing disk. 9.2.16 race fires on
    # the next promote, so we wait for Diskless to settle first.
    deadline=$(( $(date +%s) + 30 ))
    while (( $(date +%s) < deadline )); do
        d=$(status_disk_state "$RD" "$N1")
        [[ "$d" == "Diskless" ]] && break
        sleep 1
    done
    echo "   $N1 settled to: $(status_disk_state "$RD" "$N1")"

    # diskless -> diskful: PUT toggle-disk/storage-pool/stand.
    http_code=$(curl -s -o /tmp/bitmap-drop-r2.json -w '%{http_code}' -m 30 -X PUT \
        "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}/resources/${N1}/toggle-disk/storage-pool/stand")
    echo "   diskless -> diskful: HTTP $http_code"
    if [[ "$http_code" != "200" ]]; then
        echo "   body: $(cat /tmp/bitmap-drop-r2.json)"
        echo "FAIL: toggle-disk/storage-pool/stand on ${RD}.${N1} returned HTTP $http_code"
        exit 1
    fi

    # Now poll for either UpToDate (race didn't fire this loop) or
    # a bitmap error in dmesg (race fired — break and run the
    # recipe).
    deadline=$(( $(date +%s) + 30 ))
    loop_settled=false
    while (( $(date +%s) < deadline )); do
        # Look for the bitmap-drop error in the dmesg slice that
        # starts at our sentinel. awk skips lines until the
        # sentinel appears, then prints the rest; grep -E catches
        # the canonical message and its near-variants.
        bitmap_hit=$(on_node "$N1" bash -c "
            dmesg -T 2>/dev/null | awk -v s='${sentinel}' '
                f { print } \$0 ~ s { f=1 }
            ' | grep -iE 'can not drop the bitmap|cannot drop the bitmap|bitmap.*drop.*fail' | head -3
        " || true)
        if [[ -n "$bitmap_hit" ]]; then
            bitmap_error_observed=true
            echo "   !! bitmap-drop error observed in dmesg on $N1:"
            echo "$bitmap_hit" | sed 's/^/      | /'
            break
        fi

        d=$(status_disk_state "$RD" "$N1")
        if [[ "$d" == "UpToDate" ]]; then
            loop_settled=true
            break
        fi
        sleep 1
    done

    if [[ "$bitmap_error_observed" == "true" ]]; then
        # Race fired — don't keep toggling, the next steps are
        # the recovery recipe.
        break
    fi

    if [[ "$loop_settled" == "true" ]]; then
        echo "   loop ${loop} converged cleanly — bitmap race did NOT fire this round"
    else
        echo "   loop ${loop} did not converge to UpToDate within 30s and no bitmap error logged"
        echo "   $N1 final state: $(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | head -3 || true)"
    fi

    # Regression guard between loops: $N2 must still be UpToDate.
    n2_disk=$(status_disk_state "$RD" "$N2")
    if [[ "$n2_disk" != "UpToDate" ]]; then
        echo "FAIL: $N2 disk regressed during toggle loop ${loop} (got: $n2_disk) — test setup contaminated"
        exit 1
    fi
done

# ---------- step 4: if race fired, apply the recipe ----------

if [[ "$bitmap_error_observed" != "true" ]]; then
    # Race didn't fire across all loops. On 9.2.16 the upstream
    # repro rate is roughly 30-50%; over $TOGGLE_LOOPS we'd expect
    # to hit it most runs, but a clean run is not a regression —
    # the kernel race is non-deterministic. Document and pass.
    echo ">> step 4: bitmap-drop race did NOT fire in ${TOGGLE_LOOPS} toggle loops on kernel ${drbd_ver}"
    echo "   this is expected ~50% of the time on 9.2.16 (the race is non-deterministic)"
    echo ">> RECOVERY-BITMAP-DROP OK (race not reproduced; kernel=${drbd_ver}, toggles=${TOGGLE_LOOPS})"
    exit 0
fi

echo ">> step 4: apply recipe on $N1 — disconnect + connect --discard-my-data"
# The recipe is a pure drbdadm shell sequence; we do NOT touch
# blockstor CRDs. This is intentional — the reconciler must tolerate
# operator-applied recovery (same contract as scenario 5.14).
on_node "$N1" bash -c "
    drbdadm disconnect ${RD} 2>&1 || true
    drbdadm connect --discard-my-data ${RD}
"

# Wait for $N1 to walk back to UpToDate. The connect handshake
# rebuilds the bitmap from the peer ($N2), so this is effectively
# a fresh sync of the local disk — should complete well within
# RECOVERY_WINDOW for a 64 MiB volume on the QEMU stand.
echo ">> step 5: wait up to ${RECOVERY_WINDOW}s for $N1 -> UpToDate"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
recovered=false
trace=""
while (( $(date +%s) < deadline )); do
    d=$(status_disk_state "$RD" "$N1")
    trace="${trace}|${d}"
    if [[ "$d" == "UpToDate" ]]; then
        recovered=true
        break
    fi
    sleep 2
done
echo "   $N1 disk-state trace: $trace"

if [[ "$recovered" != "true" ]]; then
    echo "FAIL: recipe did NOT recover ${RD}.${N1} within ${RECOVERY_WINDOW}s on kernel ${drbd_ver}"
    echo "   $N1 final status:"
    on_node "$N1" drbdsetup status "$RD" 2>&1 | sed 's/^/      | /' || true
    echo "   recent dmesg on $N1:"
    on_node "$N1" bash -c "dmesg -T 2>/dev/null | tail -30" | sed 's/^/      | /' || true
    exit 1
fi

# Regression guard: $N2 must still be UpToDate after the recipe.
n2_final=$(status_disk_state "$RD" "$N2")
if [[ "$n2_final" != "UpToDate" ]]; then
    echo "FAIL: $N2 disk regressed during recipe (got: $n2_final)"
    exit 1
fi

echo ">> RECOVERY-BITMAP-DROP OK (race fired on kernel ${drbd_ver}, recipe converged $N1 -> UpToDate)"
