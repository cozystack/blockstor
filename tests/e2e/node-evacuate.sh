#!/usr/bin/env bash
#
# usage: node-evacuate.sh WORK_DIR
#
# Scenario 4.20 — `linstor node evacuate <node>` drains a worker by
# stamping the EVICTED flag onto the Node CRD; the NodeReconciler
# then re-places every Resource on that worker onto a non-evicted
# peer via the placer (same code path as REST autoplace). The
# original UpToDate replicas on the survivors must stay UpToDate
# throughout — drain is a placement-policy change, not a data move.
#
# Steps:
#   1. Spawn 3 RDs (test-evac-a/b/c, 100M each, place-count=2) so
#      each one lands on a different pair of workers — at least one
#      will land on WORKER_3.
#   2. Snapshot which RDs have a replica on WORKER_3.
#   3. `linstor node evacuate WORKER_3`. Verify the EVICTED flag
#      appears on the Node CRD within 30 s (the CLI returns as soon
#      as REST persists the flag; reconciler runs async).
#   4. Within 5 min: every Resource that had a WORKER_3 entry must
#      either have a replacement on a survivor or stay co-located
#      with the existing two diskful peers (placer fills the gap
#      honouring place-count). Survivors stay UpToDate the whole
#      time — sample once at the end as the cheapest pin.
#   5. **Refusal branch:** spawn `test-evac-mounted`, mount via a Pod
#      on WORKER_3, wait Running. Promote it on WORKER_3 to drive
#      InUse=true. Re-fire `linstor node evacuate WORKER_3`.
#
#      *Documented gap:* the blockstor REST handler currently does
#      NOT enforce the InUse refusal that upstream LINSTOR docs
#      (UG9 §"Evacuating a node", lines 2383-2386) call out — it
#      just stamps the EVICTED flag again (idempotent). The test
#      pins the current behaviour (CLI exits 0) and reports it
#      explicitly as INFO in the script output so the reviewer
#      sees the gap rather than misreading a green run as full
#      coverage of the refusal path.
#   6. Cleanup: unmount, `node restore WORKER_3`, delete all RDs.
#
# CLI vs REST: this test hits POST/PUT endpoints via curl directly
# rather than via `linstor n evac` / `n rst`. Reason: blockstor's
# success envelope uses `maskInfo = 0x0001_0000_0000` (see
# pkg/rest/api_call_rc.go), but upstream LINSTOR's `MASK_INFO` is
# `0x0040_0000_0000_0000`. The python-linstor CLI treats the
# wrong-mask value as an error, then tries to parse the JSON body
# as XML and crashes with `ParseError: syntax error: line 1`. The
# REST layer itself is fine (returns 200 + valid JSON); the
# mismatch is a documented gap to fix on the blockstor side.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RDS=(test-evac-a test-evac-b test-evac-c)
MOUNTED_RD=test-evac-mounted

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/node-evac-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor n l ----"
    linstor --controllers "http://localhost:$PF_PORT" node list || true
    echo "---- dump: linstor r l ----"
    linstor --controllers "http://localhost:$PF_PORT" resource list || true
    echo "---- dump: kubectl get nodes.blockstor.io.blockstor.io -o yaml ----"
    kubectl get nodes.blockstor.io.blockstor.io -o yaml | tail -80 || true
    echo "---- dump: kubectl get resources.blockstor.io.blockstor.io ----"
    kubectl get resources.blockstor.io.blockstor.io || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    # Drop any primary role we may have stamped on WORKER_3 so
    # delete_rd's drbdsetup down doesn't trip on "currently Primary".
    on_node "$WORKER_3" drbdadm secondary "$MOUNTED_RD" 2>/dev/null || true

    # Restore WORKER_3 so node-restore.sh / follow-up tests have a
    # usable cluster — leaving EVICTED behind blocks autoplace.
    curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/restore" >/dev/null 2>&1 || true
    kubectl patch "nodes.blockstor.io.blockstor.io/$WORKER_3" --type=merge \
        -p '{"spec":{"flags":null}}' >/dev/null 2>&1 || true

    for rd in "${RDS[@]}" "$MOUNTED_RD"; do
        delete_rd "$rd" 2>/dev/null || true
    done

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

echo ">> spawn 3 RDs (place-count=2, 100M each) and autoplace"
for rd in "${RDS[@]}"; do
    "${LCTL[@]}" resource-definition create "$rd" >/dev/null
    "${LCTL[@]}" volume-definition create "$rd" 100M >/dev/null
    "${LCTL[@]}" resource-definition auto-place "$rd" --place-count 2 >/dev/null
done

echo ">> wait up to 180s for each RD to have 2 UpToDate diskful replicas"
for rd in "${RDS[@]}"; do
    deadline=$(( $(date +%s) + 180 ))
    ok=0
    while (( $(date +%s) < deadline )); do
        up=$("${LCTLJ[@]}" resource list-volumes -r "$rd" 2>/dev/null \
            | jq '[.[][] | select(.volumes[]?.state.disk_state == "UpToDate")] | length' 2>/dev/null || echo 0)
        if (( up >= 2 )); then
            ok=1
            break
        fi
        sleep 2
    done
    if (( ok != 1 )); then
        echo "FAIL: $rd never reached 2 UpToDate diskful replicas"
        exit 1
    fi
done

# Capture which RDs have a diskful replica on WORKER_3 BEFORE the
# evacuate so we can verify the placer moved them off.
echo ">> snapshot replica placement on $WORKER_3"
declare -a on_w3=()
for rd in "${RDS[@]}"; do
    nodes=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
        | jq -r --arg n "$WORKER_3" '.[][] | select(.node_name == $n) | .node_name')
    if [[ -n "$nodes" ]]; then
        on_w3+=("$rd")
    fi
done
echo "   RDs with replica on $WORKER_3 before evacuate: ${on_w3[*]:-<none>}"

# If autoplace didn't pick WORKER_3 for any RD (e.g. round-robin gave
# us workers 1+2 three times in a row), the evacuate is still
# meaningful — the flag still has to stamp — but the migration
# branch can't fire. Force at least one replica onto WORKER_3 by
# adding an explicit Resource if none of the three landed there.
if (( ${#on_w3[@]} == 0 )); then
    echo ">> no RD landed on $WORKER_3 from autoplace; pin test-evac-a there"
    "${LCTL[@]}" resource-definition set-property "${RDS[0]}" \
        DrbdOptions/AutoAddQuorumTiebreaker false >/dev/null
    "${LCTL[@]}" resource create "$WORKER_3" "${RDS[0]}" --storage-pool stand >/dev/null
    on_w3=("${RDS[0]}")
    # Wait for the pinned replica to converge so we have a real
    # migration source rather than a half-attached lower disk.
    deadline=$(( $(date +%s) + 180 ))
    while (( $(date +%s) < deadline )); do
        state=$("${LCTLJ[@]}" resource list-volumes -r "${RDS[0]}" 2>/dev/null \
            | jq -r --arg n "$WORKER_3" '.[][] | select(.node_name == $n) | .volumes[]?.state.disk_state')
        [[ "$state" == "UpToDate" ]] && break
        sleep 2
    done
fi

echo ">> sample pre-evacuate UpToDate state for assertion at end"
declare -A pre_state=()
for rd in "${RDS[@]}"; do
    pre_state[$rd]=$("${LCTLJ[@]}" resource list-volumes -r "$rd" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '[.[][] | select(.node_name != $w3) | .node_name + ":" + (.volumes[0].state.disk_state // "?")] | sort | join(",")')
done

echo ">> linstor node evacuate $WORKER_3"
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/evacuate"

echo ">> within 30s, expect EVICTED flag on Node CRD"
deadline=$(( $(date +%s) + 30 ))
got_evicted=0
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" == *"EVICTED"* ]]; then
        got_evicted=1
        break
    fi
    sleep 1
done
if (( got_evicted != 1 )); then
    echo "FAIL: EVICTED flag never appeared on $WORKER_3 (flags=$flags)"
    exit 1
fi

echo ">> within 5min, each WORKER_3 replica must be re-placed off worker-3"
deadline=$(( $(date +%s) + 300 ))
all_moved=0
while (( $(date +%s) < deadline )); do
    pending=()
    for rd in "${on_w3[@]}"; do
        # Count diskful (non-DISKLESS / non-TIE_BREAKER) replicas
        # outside WORKER_3 — the placer should have reached place-count
        # = 2 on the surviving pair (workers 1+2). The replica on
        # WORKER_3 itself may still hang around with a DELETE flag
        # pending the satellite-finalizer cleanup; we only assert the
        # replacement side, not the deletion side.
        n_surv=$("${LCTLJ[@]}" resource list -r "$rd" 2>/dev/null \
            | jq -r --arg w3 "$WORKER_3" \
            '[.[][] | select(.node_name != $w3) | select((.flags // []) | index("DISKLESS") | not)] | length')
        if (( n_surv < 2 )); then
            pending+=("$rd($n_surv/2)")
        fi
    done
    if (( ${#pending[@]} == 0 )); then
        all_moved=1
        break
    fi
    sleep 5
done
if (( all_moved != 1 )); then
    echo "FAIL: replacement replicas did not converge to place-count=2: ${pending[*]}"
    exit 1
fi

echo ">> verify survivors stayed UpToDate throughout"
for rd in "${RDS[@]}"; do
    post=$("${LCTLJ[@]}" resource list-volumes -r "$rd" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '[.[][] | select(.node_name != $w3) | .node_name + ":" + (.volumes[0].state.disk_state // "?")] | sort | join(",")')
    # Compare ONLY the workers that were in pre_state (could differ
    # if placer added a new node). What matters: every node listed
    # in BOTH pre and post must still report UpToDate in post.
    bad=""
    while IFS=, read -ra entries; do
        for e in "${entries[@]}"; do
            node=${e%%:*}; state=${e##*:}
            if [[ "$state" != "UpToDate" ]]; then
                bad+="$node=$state "
            fi
        done
    done <<< "$post"
    if [[ -n "$bad" ]]; then
        echo "FAIL: $rd survivors drifted off UpToDate post-evacuate: $bad (pre=${pre_state[$rd]} post=$post)"
        exit 1
    fi
done
echo "   survivors stayed UpToDate"

# ---- Refusal branch -------------------------------------------------
# Per UG9 lines 2383-2386, upstream LINSTOR refuses evacuate when a
# resource on the node is InUse (Primary). We don't go through
# piraeus-csi here because that would test the JAVA-LINSTOR (oracle)
# stack, not blockstor — the stand wires piraeus-csi against the
# Java controller. Instead we drive InUse directly: promote a
# replica on WORKER_3 to Primary via `drbdadm primary` inside the
# satellite Pod. That sets the satellite-observed `InUse=true` on
# the Resource Status, which is what the REST handler would inspect
# if/when the UG9 refusal is implemented.

echo ">> restore $WORKER_3 so it can host the InUse test replica"
curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/restore" >/dev/null

deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" != *"EVICTED"* ]]; then
        break
    fi
    sleep 1
done

echo ">> spawn $MOUNTED_RD pinned on $WORKER_1 + $WORKER_3 and wait UpToDate"
"${LCTL[@]}" resource-definition create "$MOUNTED_RD" >/dev/null
"${LCTL[@]}" volume-definition create "$MOUNTED_RD" 100M >/dev/null
"${LCTL[@]}" resource-definition set-property "$MOUNTED_RD" \
    DrbdOptions/AutoAddQuorumTiebreaker false >/dev/null
"${LCTL[@]}" resource create "$WORKER_1" "$MOUNTED_RD" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$WORKER_3" "$MOUNTED_RD" --storage-pool stand >/dev/null

wait_uptodate "$MOUNTED_RD" "$WORKER_1" "$WORKER_3"

echo ">> drive InUse=true on $WORKER_3 via drbdadm primary"
on_node "$WORKER_3" drbdadm primary "$MOUNTED_RD"

# Quick sanity-check: REST view should report InUse=true on the
# WORKER_3 row for this RD. If the satellite hasn't propagated yet
# the polling loop below retries.
deadline=$(( $(date +%s) + 30 ))
inuse=""
while (( $(date +%s) < deadline )); do
    inuse=$(curl -sf -m5 "http://localhost:$PF_PORT/v1/view/resources?resource=$MOUNTED_RD" 2>/dev/null \
        | jq -r --arg w3 "$WORKER_3" \
        '.[]? | select(.node_name == $w3) | .state.in_use' 2>/dev/null || true)
    [[ "$inuse" == "true" ]] && break
    sleep 1
done
echo "   in_use on $WORKER_3 = $inuse"

echo ">> linstor node evacuate $WORKER_3 with InUse replica present"
set +e
out=$(curl -fsS -XPOST "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/evacuate" 2>&1)
rc=$?
set -e
echo "   exit=$rc output: $out"

# Pin pin (regardless of outcome): re-firing evacuate must not
# disrupt the Primary's local IO. We treat this as the operator
# safety contract — even if the EVICTED flag stamps idempotently,
# the kernel-side DRBD state must not be torn down on the InUse
# replica.
state_w3=$(on_node "$WORKER_3" drbdsetup status "$MOUNTED_RD" 2>/dev/null | head -1 || true)
if [[ "$state_w3" != *"role:Primary"* ]]; then
    echo "FAIL: $MOUNTED_RD on $WORKER_3 lost Primary role after evacuate (state=$state_w3)"
    on_node "$WORKER_3" drbdsetup status "$MOUNTED_RD" || true
    exit 1
fi

if (( rc == 0 )); then
    echo "INFO: evacuate exited 0 despite InUse — blockstor does NOT enforce the upstream UG9 InUse refusal (documented gap; see node_lifecycle.go handleNodeEvacuate)"
else
    if echo "$out" | grep -qiE 'in.?use|primary|busy'; then
        echo "OK: evacuate refused with actionable text on InUse (rc=$rc)"
    else
        echo "INFO: evacuate exited $rc but the message did not mention InUse: $out"
    fi
fi

# Release the Primary so cleanup can drbdadm down cleanly.
on_node "$WORKER_3" drbdadm secondary "$MOUNTED_RD" 2>/dev/null || true

echo ">> NODE-EVACUATE OK"
