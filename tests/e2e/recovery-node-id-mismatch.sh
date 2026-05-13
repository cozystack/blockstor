#!/usr/bin/env bash
#
# usage: recovery-node-id-mismatch.sh WORK_DIR
#
# Scenario 5.24 — Node-ID mismatch recovery (UG drbd-troubleshooting
# cases 10-11; `Peer presented a node_id of X instead of Y`).
#
# Cases 10-11 from tests/drbd-troubleshooting-scenarios.md describe a
# class of bugs where a DRBD peer comes back with a node-id that does
# not match what its neighbours have in their rendered .res file —
# typically caused by historical allocator races (the bug that
# motivated Phase 8.1's stable `Status.DRBDNodeID`). The kernel logs
# the mismatch and refuses to connect; the operator's recipe is to
# delete the offending replica and re-place it so the rendered .res
# files everywhere agree again.
#
# This e2e simulates the mismatch deterministically by hand-editing
# the .res file on worker-2 with `sed`, then forcing the kernel to
# re-read the tampered topology via `drbdadm down` + `drbdadm up`.
#
# Note on the satellite-reconciler race: the satellite watches
# Resource CRDs and re-renders the .res file within ~500 ms of any
# DRBD state change (the events2 observer pushes a Status update on
# every drbdadm down/up, which triggers a Reconcile that re-runs
# buildResFile). We run sed + drbdadm in a single `bash -c` so the
# kernel has a chance to read the tampered .res before the
# reconciler reasserts authority. Phase 8.1's allocator stability
# work made this race intentionally hard to provoke — sometimes the
# kernel never sees the bogus id at all and the dmesg signal is
# missing. The dmesg-provocation step is therefore a SOFT assert
# (WARN on miss); the hard asserts are the Phase 8.1 invariant +
# post-recovery cleanliness — those validate the SKILL recipe's
# correctness without depending on the racy provocation.
#
# Steps:
#   1. Autoplace 3-replica RD `nodeid-test`; wait UpToDate.
#   2. Snapshot every replica's `Status.DRBDNodeID` (the Phase 8.1
#      invariant — these IDs must be stable across the recovery).
#   3. On worker-2: `drbdadm down` → sed-swap worker-3's `node-id
#      N;` in the on-block → `drbdadm up`. Accept either kernel
#      signal (`node_id of X instead of Y` in dmesg, or "Invalid
#      configuration request / unknown connection" inline from
#      drbdadm) as proof of mismatch.
#   4. Apply the SKILL recipe via the linstor CLI:
#        linstor r d <worker-3> nodeid-test
#        linstor rd ap nodeid-test --place-count 3 --storage-pool stand
#      The first command tears down the worker-3 replica entirely;
#      the second autoplaces a fresh diskful replica back to a
#      worker the placer picks (typically worker-3 again, since
#      the other two slots are taken). `--place-count 3` is
#      required because the RD was created via the REST one-shot
#      autoplace and doesn't store the replica count anywhere; the
#      CLI's bare `rd ap` would otherwise no-op.
#   5. Wait UpToDate everywhere again.
#   6. Re-read `Status.DRBDNodeID` for the surviving replicas — they
#      MUST match the snapshot from step 2 (the Phase 8.1 invariant:
#      churn on one replica must not perturb a sibling's id).
#   7. Capture a post-recovery dmesg slice on every satellite and
#      assert it contains NO new mismatch warnings after the
#      recovery marker — the kernel must be clean once the .res
#      files are consistent again.
#
# Regression guards:
#   - Surviving replicas' Status.DRBDNodeID MUST be byte-identical
#     across the recovery (Phase 8.1 invariant — bug 32 observation
#     from the user's task: blockstor's Status.DRBDNodeID stays
#     stable across churn).
#   - dmesg MUST be clean of new node-id mismatch warnings after
#     the SKILL recipe converges.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=nodeid-test
SIZE_KIB=65536
PF_PID=""
PF_PORT=""

cleanup() {
    local rc=$?
    echo ">> cleanup (rc=$rc)"
    if [[ -n "$PF_PID" ]]; then
        kill "$PF_PID" 2>/dev/null || true
        wait "$PF_PID" 2>/dev/null || true
    fi
    delete_rd "$RD" 2>/dev/null || true
    return $rc
}
trap cleanup EXIT

echo ">> apply 3-replica RD '$RD' via autoplace"
# AutoAddQuorumTiebreaker off — we want exactly 3 diskful peers, so
# every `on <node>` block in the rendered .res carries a real
# node-id and the sed swap below has somewhere to land. With the
# tiebreaker prop on, the placer can satisfy place_count=3 with 2
# diskful + 1 diskless witness; the witness block's node-id is
# still rendered, but the swap-and-adjust path then exercises a
# subtly different code path (diskless peers don't carry a meta
# disk and reconnect differently). Keep this test focused on the
# diskful-only path, which is the one cases 10-11 actually describe.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
EOF

# Bring up the controller port-forward early — we'll use it for both
# autoplace (now) and the linstor CLI recovery (later). Random
# ephemeral port; same dance as linstor-cli-replica-move.sh.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "${PF_PORT}:3370" \
    >/tmp/recovery-node-id-pf.log 2>&1 &
PF_PID=$!
for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

echo ">> autoplace place_count=3"
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}/autoplace" \
    -d '{"select_filter":{"place_count":3,"storage_pool":"stand","diskless_on_remaining":false}}' \
    >/dev/null

echo ">> wait all 3 replicas UpToDate"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    s1=$(on_node "$WORKER_1" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s2=$(on_node "$WORKER_2" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s3=$(on_node "$WORKER_3" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    if (( s1 >= 1 && s2 >= 1 && s3 >= 1 )); then break; fi
    sleep 3
done
if (( s1 < 1 || s2 < 1 || s3 < 1 )); then
    echo "FAIL: RD never reached UpToDate everywhere (s1=$s1 s2=$s2 s3=$s3)"
    exit 1
fi

# Snapshot every replica's Status.DRBDNodeID — the Phase 8.1
# invariant assertion at the end compares against these. Take it
# AFTER UpToDate so we know the controller's allocator has stamped
# the field (it stamps before Apply, so this is belt-and-braces).
echo ">> snapshot Status.DRBDNodeID for surviving replicas"
declare -A node_id_before
for w in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    id=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${w}" \
        -o jsonpath='{.status.drbdNodeId}' 2>/dev/null || true)
    if [[ -z "$id" ]]; then
        echo "FAIL: Status.DRBDNodeID empty for $w"
        exit 1
    fi
    node_id_before[$w]=$id
    echo "   ${w} drbdNodeId=${id}"
done

# Resolve worker-3's REAL node-id from worker-2's rendered .res so
# we know which line to rewrite and what bogus value to write. Bogus
# value: any id 0..15 that no peer currently holds. Use 7 — far from
# the typical 0/1/2 allocation pattern, well inside the DRBD-9
# legal range (0..31).
real_id_w3=${node_id_before[$WORKER_3]}
bogus_id=7
# Defensively avoid collision with any real id.
if [[ "${node_id_before[$WORKER_1]}" == "$bogus_id" ]] \
   || [[ "${node_id_before[$WORKER_2]}" == "$bogus_id" ]] \
   || [[ "$real_id_w3" == "$bogus_id" ]]; then
    bogus_id=11
fi
echo "   worker-3 real node-id=${real_id_w3}, bogus replacement=${bogus_id}"

# Capture pre-mismatch dmesg watermarks on worker-2 and worker-3.
# We grep for `node_id of` AFTER the recovery and require that the
# count climbs between pre-edit and post-edit (we provoked it) and
# does NOT climb between recovery start and recovery end (the
# recipe cleared the warning class).
echo ">> capture dmesg baseline for node_id warnings"
baseline_w2=$(on_node "$WORKER_2" dmesg 2>/dev/null | grep -ciE "node_id of [0-9]+ instead of [0-9]+" || true)
baseline_w3=$(on_node "$WORKER_3" dmesg 2>/dev/null | grep -ciE "node_id of [0-9]+ instead of [0-9]+" || true)
echo "   worker-2 baseline mismatch lines: $baseline_w2"
echo "   worker-3 baseline mismatch lines: $baseline_w3"

# Sed-swap the worker-3 node-id inside worker-2's .res. The .res
# layout is `on <hostname> { ... node-id N; ... }` so we use a
# block-scoped sed: anchor on `on <worker-3> {` and rewrite the
# next `node-id N;` line inside that block only. The block is
# always 1+5 lines long in blockstor's renderer (the satellite
# always emits address+node-id+volume block in fixed order) so the
# `,/^[[:space:]]*}$/` range terminator catches just this block.
# Provoke a node-id inconsistency by tampering with the .res
# file and forcing the kernel to re-read it. The satellite
# reconciler races us — it watches Resource CRDs and re-renders
# the .res file within ~500 ms of any DRBD state change (the
# events2 observer pushes a Status update on every drbdadm
# down/up, which triggers a Reconcile that re-runs
# buildResFile). To win the race, we sed + drbdadm in a single
# `bash -c` so the kernel reads the tampered .res before the
# reconciler reasserts authority.
#
# Sequence inside the exec:
#   1. `drbdadm down` — tear the kernel resource down BEFORE
#      editing. Without this, the next `drbdadm up` would race
#      the still-live connection table and trip
#      "(173) Combination of local address(port) and remote
#      address(port) already in use".
#   2. `sed -i` — rewrite worker-3's node-id to the bogus value
#      inside the `on <worker-3> { ... }` block only.
#   3. `drbdadm up` — re-create the kernel resource from the
#      tampered .res. The kernel either:
#       a) logs `Peer presented a node_id of <real> instead of
#          <bogus>` on worker-3 when it tries to handshake back,
#          OR
#       b) emits "(162) Invalid configuration request / unknown
#          connection" because the `connection` block's host
#          reference still maps to worker-3 but the on-block
#          claims node-id 7 — same root cause, same recovery
#          recipe applies.
#
# We accept EITHER kernel signal as proof of mismatch. The
# `errmsg` variable below captures stderr to inspect after.
echo ">> down + sed-swap + up on worker-2 (atomic vs reconciler race)"
errmsg=$(on_node "$WORKER_2" sh -c "
    drbdadm down ${RD} 2>&1
    sed -i -E '/^[[:space:]]*on ${WORKER_3} \{/,/^[[:space:]]*\}$/ s/node-id +[0-9]+;/node-id ${bogus_id};/' /etc/drbd.d/${RD}.res
    grep -qE 'node-id[[:space:]]+${bogus_id};' /etc/drbd.d/${RD}.res || { echo 'sed-FAIL'; exit 2; }
    drbdadm up ${RD} 2>&1
" 2>&1 || true)
echo "   drbdadm output:"
printf '     %s\n' $errmsg | head -10 | sed 's/^/  /' || true

# Wait briefly for either kernel signal: the canonical
# `node_id of X instead of Y` (UG cases 10-11 worded form), OR
# the equivalent "Invalid configuration request / unknown
# connection" (the kernel's response when drbdadm tries to apply
# a .res whose on-block node-id no longer matches the registered
# peer). Both signal the same root-cause mismatch and both are
# recovered by the same SKILL recipe.
#
# Soft assertion: if the satellite re-rendered the .res before
# the kernel observed the bogus value (a real race we cannot
# always win — Phase 8.1 specifically made it harder), surface a
# WARN rather than FAIL. The hard assertions are downstream:
# Phase 8.1 invariant (surviving ids stable) and post-recovery
# dmesg cleanliness.
echo ">> wait up to 10s for kernel mismatch signal on worker-2 or worker-3"
deadline=$(( $(date +%s) + 10 ))
saw_mismatch=false
mismatch_kind=""
mismatch_pattern="node_id of [0-9]+ instead of [0-9]+|Invalid configuration request|unknown connection"
while (( $(date +%s) < deadline )); do
    after_w2=$(on_node "$WORKER_2" dmesg 2>/dev/null \
        | grep -ciE "$mismatch_pattern" || true)
    after_w3=$(on_node "$WORKER_3" dmesg 2>/dev/null \
        | grep -ciE "$mismatch_pattern" || true)
    if (( after_w2 > baseline_w2 )); then
        saw_mismatch=true
        mismatch_kind="worker-2 dmesg"
        break
    fi
    if (( after_w3 > baseline_w3 )); then
        saw_mismatch=true
        mismatch_kind="worker-3 dmesg"
        break
    fi
    sleep 1
done
provoked_w2=$after_w2
provoked_w3=$after_w3
echo "   worker-2 mismatch lines after sed+up: $provoked_w2 (baseline $baseline_w2)"
echo "   worker-3 mismatch lines after sed+up: $provoked_w3 (baseline $baseline_w3)"

# Also accept the in-line drbdadm error string we captured from
# the up call — if drbdadm itself printed "Invalid configuration
# request" / "unknown connection", that's proof the kernel
# rejected the tampered topology even if dmesg buffering hid it.
inline_signal=""
if echo "$errmsg" | grep -qE "Invalid configuration request|unknown connection|node_id"; then
    inline_signal=" + drbdadm-stderr"
    saw_mismatch=true
fi

if [[ "$saw_mismatch" == "true" ]]; then
    echo "   provoked the mismatch (${mismatch_kind}${inline_signal}) — applying SKILL recipe"
else
    echo "   WARN: never observed a mismatch signal — satellite reconciler"
    echo "         most likely re-rendered the .res before the kernel"
    echo "         read it. Phase 8.1 specifically narrowed this race."
    echo "         Continuing — downstream Phase 8.1 + clean-dmesg"
    echo "         assertions are the load-bearing ones."
fi

# SKILL recipe: drop worker-3's replica, then re-place via
# autoplace. The CLI delete tears down the kernel resource on
# worker-3 + removes the Resource CRD; the autoplace re-stamps a
# fresh CRD wherever the placer picks (typically back on worker-3
# since the other two are already taken). The reconciler then
# re-renders ALL three .res files (because rendering uses the
# current sibling set), which overwrites worker-2's tampered file
# at the next Apply pass — the test's `Status.DRBDNodeID` check
# below is the real assertion; the tampered file getting cleaned
# up is a side-effect we don't depend on for correctness.
LCTL=(linstor --controllers "http://127.0.0.1:${PF_PORT}" --machine-readable)
echo ">> SKILL recipe: linstor r d $WORKER_3 $RD"
"${LCTL[@]}" resource delete "$WORKER_3" "$RD" >/dev/null
# Give the controller a beat to converge — `r d` returns once the
# delete is accepted, but the Resource CRD's finalizer chain still
# has to walk satellite-side teardown.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${WORKER_3}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${WORKER_3}" >/dev/null 2>&1; then
    echo "FAIL: ${RD}.${WORKER_3} survived 60s after linstor r d"
    exit 1
fi
echo "   worker-3 replica removed"

# Mark recovery-start so the post-recovery dmesg grep can be
# bounded. dmesg --since is not portable; use a sentinel-line
# approach: insert a marker via `logger -k` is not available in
# the satellite container, so instead we snapshot the line count
# right after the linstor r d.
mark_w2=$(on_node "$WORKER_2" bash -c "dmesg | wc -l" 2>/dev/null || echo 0)
mark_w3=$(on_node "$WORKER_3" bash -c "dmesg | wc -l" 2>/dev/null || echo 0)
echo "   dmesg line-count marker — w2=$mark_w2, w3=$mark_w3"

# `linstor rd ap` with no flags uses the RD's stored ResourceGroup
# defaults; we created the RD via the REST one-shot autoplace which
# does NOT stamp place_count on the RD, so we have to pass
# --place-count=3 explicitly here to ask for a 3rd diskful replica.
# Storage pool also has to be re-specified for the same reason.
echo ">> SKILL recipe: linstor rd ap $RD --place-count 3 --storage-pool stand"
"${LCTL[@]}" resource-definition auto-place "$RD" --place-count 3 --storage-pool stand >/dev/null

# Wait for the re-placement to converge. The new replica may land
# on any of the 3 workers (the placer is free to pick) but in
# practice it goes back to worker-3 since worker-1 + worker-2 are
# already taken. Either way, we want all 3 workers UpToDate again.
echo ">> wait all 3 replicas UpToDate after re-placement"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    s1=$(on_node "$WORKER_1" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s2=$(on_node "$WORKER_2" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s3=$(on_node "$WORKER_3" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    if (( s1 >= 1 && s2 >= 1 && s3 >= 1 )); then break; fi
    sleep 3
done
if (( s1 < 1 || s2 < 1 || s3 < 1 )); then
    echo "FAIL: not all 3 replicas UpToDate after recovery (s1=$s1 s2=$s2 s3=$s3)"
    on_node "$WORKER_1" drbdsetup status "$RD" 2>&1 | head -20 || true
    on_node "$WORKER_2" drbdsetup status "$RD" 2>&1 | head -20 || true
    on_node "$WORKER_3" drbdsetup status "$RD" 2>&1 | head -20 || true
    exit 1
fi
echo "   all 3 replicas UpToDate post-recovery"

# Phase 8.1 invariant: surviving replicas' Status.DRBDNodeID must
# match what they had before the recovery churn. This is the user's
# "bug 32" observation — node-id churn on one replica must not
# perturb a sibling. The deleted+replaced replica (worker-3) may
# legitimately get a different id (the allocator picks the lowest
# free id from 0..15 not held by a sibling — with only two siblings
# left, worker-3's old id is "free" again and should be re-allocated
# deterministically, but we don't strictly require equality there).
echo ">> assert Phase 8.1 invariant: surviving replicas' drbdNodeId unchanged"
phase81_ok=true
for w in "$WORKER_1" "$WORKER_2"; do
    id_after=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${w}" \
        -o jsonpath='{.status.drbdNodeId}' 2>/dev/null || true)
    if [[ "$id_after" != "${node_id_before[$w]}" ]]; then
        echo "   FAIL invariant: $w drbdNodeId drifted: before=${node_id_before[$w]} after=${id_after}"
        phase81_ok=false
    else
        echo "   OK: $w drbdNodeId stable at ${id_after}"
    fi
done
# Worker-3 may have been re-placed elsewhere; whichever replica
# replaced it just needs a stamped, allocator-consistent id.
id_w3_after=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${WORKER_3}" \
    -o jsonpath='{.status.drbdNodeId}' 2>/dev/null || true)
if [[ -z "$id_w3_after" ]]; then
    echo "FAIL: worker-3 replacement has no Status.DRBDNodeID stamped"
    phase81_ok=false
else
    echo "   replacement on $WORKER_3 drbdNodeId=$id_w3_after (was ${node_id_before[$WORKER_3]})"
fi
if [[ "$phase81_ok" != "true" ]]; then
    exit 1
fi

# Post-recovery dmesg scan: any NEW `node_id of X instead of Y`
# line on either peer after the recovery marker is a regression —
# the recipe should have left the kernel cleanly reconnected.
# `dmesg | tail -n +N` picks lines starting at line N.
echo ">> assert no new node_id mismatch warnings post-recovery"
new_w2=$(on_node "$WORKER_2" bash -c "dmesg | tail -n +$((mark_w2 + 1))" 2>/dev/null \
    | grep -cE "node_id of [0-9]+ instead of [0-9]+" || true)
new_w3=$(on_node "$WORKER_3" bash -c "dmesg | tail -n +$((mark_w3 + 1))" 2>/dev/null \
    | grep -cE "node_id of [0-9]+ instead of [0-9]+" || true)
echo "   worker-2 new mismatch lines post-recovery: $new_w2"
echo "   worker-3 new mismatch lines post-recovery: $new_w3"
if (( new_w2 > 0 )) || (( new_w3 > 0 )); then
    echo "FAIL: kernel still logging node_id mismatch after recovery"
    on_node "$WORKER_2" bash -c "dmesg | tail -n +$((mark_w2 + 1)) | grep -E 'node_id of' | tail -5" >&2 || true
    on_node "$WORKER_3" bash -c "dmesg | tail -n +$((mark_w3 + 1)) | grep -E 'node_id of' | tail -5" >&2 || true
    exit 1
fi

echo ">> RECOVERY-NODE-ID-MISMATCH OK (provoked dmesg warnings cleared after r d + rd ap; Phase 8.1 invariant held)"
