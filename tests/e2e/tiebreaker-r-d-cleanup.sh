#!/usr/bin/env bash
#
# usage: tiebreaker-r-d-cleanup.sh WORK_DIR
#
# Bug 338 regression catcher: after `linstor r d <one-of-many-diskful>`
# the auto-managed TIE_BREAKER witness must converge to the shape the
# DRBD-9 quorum invariant promises for the new diskful count — not lag
# behind a stale topology.
#
# Reproduces, on a 3-node cluster, the exact sequence the user ran by
# hand on a stand where Bug 338 had been silently re-introduced:
#
#   1. Create an RD with 3 diskful replicas (workers 1+2+3, no diskless
#      yet). Wait for all three UpToDate.
#   2. `linstor r d <worker-3>` (remove one diskful). diskful drops 3 → 2,
#      so the upstream `shouldTieBreakerExist` contract says a witness
#      must appear — and the only "uninvolved" node in a 3-node cluster
#      is worker-3 (which we just removed). That's the expected and
#      documented placement (see shouldKeepExistingWitness comment block
#      around resourcedefinition_controller.go:331). NOT a bug — we
#      assert the controller HAS converged to "exactly one TIE_BREAKER,
#      and it's the diskless witness shape".
#   3. `linstor r d <worker-2>` (remove the second diskful). diskful
#      drops 2 → 1 with no user-added diskless co-resident. Per the
#      Bug 338 carve-out in shouldKeepExistingWitness, the witness must
#      collapse: 1 diskful + 1 diskless witness is a 2-voter quorum that
#      loses majority the moment EITHER node dies — strictly worse than
#      the lone diskful (always quorate on `quorum: off`). The orphan
#      TIE_BREAKER must be removed and the lone diskful left running
#      under `quorum: off`. THIS is the Bug 338 regression assert.
#
# This scenario lives in tests/e2e/ (not unit) because Bugs 104/108/267/
# 271/338/342 have ALL been in the TIE_BREAKER cleanup class — and every
# one of them slipped past the unit-handler tests that returned 200 OK
# on a mocked LINSTOR while the real Status convergence regressed. The
# only way to catch this class is to drive the actual REST handler
# against the actual reconciler against the actual satellite-rendered
# Resource CRDs, on a real QEMU stand. See
# feedback_tiebreaker_e2e_must_exist.md in MEMORY.md.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "FAIL: linstor CLI not in PATH (apt install linstor-client)" >&2
    exit 1
fi

RD=tb-rd-cleanup
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Per-step convergence budget. The witness add/remove path is event-
# driven (RD reconciler watches Resource CRDs) plus a 5s requeue tick,
# so 60s is comfortable on the QEMU stand.
CONVERGE=${CONVERGE:-60}

# blockstor's REST/apiserver is exposed inside the cluster on :3370
# (LinstorPlain). Use port-forward + a host-side `linstor` CLI so the
# scenario can drive the same REST endpoints the user reaches by
# `kubectl exec`-ing into the satellite — no satellite-pod coupling.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/tb-r-d-cleanup-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    "${LCTL[@]}" r l -r "$RD" 2>&1 || true
    echo "---- dump: kubectl get resources.blockstor.cozystack.io -A ----"
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd' || true
    echo "---- dump: Resource flags ----"
    for n in "$N1" "$N2" "$N3"; do
        kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
            -o jsonpath='{.metadata.name}{" flags="}{.spec.flags}{"\n"}' \
            2>/dev/null || true
    done
    echo "---- dump: kubectl logs -n $NS deploy/blockstor-controller --tail=120 ----"
    kubectl logs -n "$NS" deploy/blockstor-controller --tail=120 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    # Trap-time cleanup. delete_rd is idempotent and drives the same
    # cascade `linstor rd d` would.
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to actually bind before invoking the CLI.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

# Wipe any leftover state from a prior run of THIS scenario.
# delete_all_rds is deliberately NOT used here — the stand may carry
# unrelated workloads (Cozystack demo PVCs, csi-sanity residue, etc.)
# whose owning RDs would get torn down by a blanket wipe. Bug 338 is
# scoped to a single auto-managed RD; cleaning our own name is enough.
delete_rd "$RD" 2>/dev/null || true

# tb_witness_count <rd> — prints the number of Resources for $rd that
# carry the TIE_BREAKER spec flag. Reads the controller-authoritative
# Spec, not the satellite-derived .Status (`linstor r l` State column
# joins on Spec.Flags too).
tb_witness_count() {
    local rd=$1
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            kubectl get "resources.blockstor.cozystack.io/${name}" \
                -o jsonpath='{.spec.flags}' 2>/dev/null
            echo
        done \
        | grep -c "TIE_BREAKER" || true
}

# tb_witness_nodes <rd> — prints the comma-separated node names hosting
# a TIE_BREAKER witness for $rd (empty when none). Used for diagnostic
# messages on assertion failure.
tb_witness_nodes() {
    local rd=$1
    local out=()
    while read -r name; do
        local flags
        flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null)
        if [[ "$flags" == *"TIE_BREAKER"* ]]; then
            out+=("${name#${rd}.}")
        fi
    done < <(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}')
    (IFS=,; echo "${out[*]}")
}

# diskful_count <rd> — number of non-diskless replicas for $rd. A
# DISKLESS spec flag (with or without TIE_BREAKER) means "not diskful".
diskful_count() {
    local rd=$1
    local n=0
    while read -r name; do
        local flags
        flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null)
        if [[ "$flags" != *"DISKLESS"* ]]; then
            n=$((n + 1))
        fi
    done < <(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}')
    echo "$n"
}

# wait_witness_count <rd> <want> [timeout] — poll until exactly $want
# TIE_BREAKER witnesses exist for $rd, or timeout. Tries one extra
# settle iteration before returning (the controller is event-driven
# off Resource watches, so the witness can briefly oscillate during
# the per-cycle 5s requeue while the diskful count is observed).
wait_witness_count() {
    local rd=$1 want=$2 timeout=${3:-$CONVERGE}
    local deadline=$(( $(date +%s) + timeout ))
    local got=""
    while (( $(date +%s) < deadline )); do
        got=$(tb_witness_count "$rd")
        if [[ "$got" == "$want" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "FAIL: $rd witness count never reached $want (last=$got)" >&2
    echo "  diskful=$(diskful_count "$rd") witness-on=$(tb_witness_nodes "$rd")" >&2
    return 1
}

# ---- STEP 1: 3 diskful replicas on workers 1+2+3 -------------------

echo ">> create RD $RD with 3 diskful replicas on $N1, $N2, $N3"
# Create-class CLI calls go through lctl_idempotent so a dropped
# port-forward read that triggers python-linstor's blind POST resend
# (see lctl_idempotent in lib.sh) does not flake the scenario with a
# spurious "already diskful: object already exists" 409 on a create that
# already landed server-side.
lctl_idempotent resource-definition create "$RD"
lctl_idempotent volume-definition create "$RD" 64M
lctl_idempotent resource create "$N1" "$RD" --storage-pool stand
lctl_idempotent resource create "$N2" "$RD" --storage-pool stand
lctl_idempotent resource create "$N3" "$RD" --storage-pool stand

echo ">> wait all 3 replicas UpToDate (<=180s)"
wait_disk_state "$RD" "$N1" UpToDate 180 0
wait_disk_state "$RD" "$N2" UpToDate 180 0
wait_disk_state "$RD" "$N3" UpToDate 180 0

# Baseline assert: with 3 diskful, no auto-managed witness should
# exist. shouldTieBreakerExist gates on `diskful % 2 == 0` AND
# `diskful < witnessUnnecessaryDiskfulCount` — both flip the result
# false at diskful=3.
init_witness=$(tb_witness_count "$RD")
if [[ "$init_witness" != "0" ]]; then
    echo "FAIL: pre-step baseline already has $init_witness witness(es); test cannot trust step asserts"
    dump_diag
    exit 1
fi
echo "   baseline: 3 diskful, 0 witnesses (OK)"

# ---- STEP 2: r d worker-3 → 2 diskful, witness MUST exist ----------

echo ">> linstor r d $N3 $RD (delete worker-3 diskful replica)"
"${LCTL[@]}" resource delete "$N3" "$RD"

echo ">> wait up to ${CONVERGE}s for ensureTiebreaker to add exactly one TIE_BREAKER witness"
if ! wait_witness_count "$RD" 1 "$CONVERGE"; then
    echo "ASSERT 1 FAILED: after r d $N3, expected exactly 1 TIE_BREAKER witness,"
    echo "  diskful=$(diskful_count "$RD") witness-on=$(tb_witness_nodes "$RD")"
    exit 1
fi

# Sanity: the freshly stamped witness must be DISKLESS too. A diskful
# replica that the satellite hasn't yet observed (post-create races)
# could otherwise sneak past tb_witness_count above; pin the shape.
witness_node=$(tb_witness_nodes "$RD")
witness_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${witness_node}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null)
if [[ "$witness_flags" != *"DISKLESS"* ]]; then
    echo "ASSERT 1 FAILED: TIE_BREAKER on ${witness_node} missing DISKLESS flag (got: ${witness_flags})"
    exit 1
fi
echo "   step 2 OK: 2 diskful + 1 TIE_BREAKER witness on ${witness_node}, flags=${witness_flags}"

# ---- STEP 3: r d worker-2 → 1 diskful, witness MUST collapse -------
#
# This is the Bug 338 regression. The controller intent (Bug 338
# carve-out in shouldKeepExistingWitness, around
# resourcedefinition_controller.go:323-342) is to collapse the
# orphan witness when diskful drops to 1 AND no non-witness diskless
# replica is present. The pre-fix symptom is a witness that never
# converges — `linstor r l` shows 1 diskful + 1 TieBreaker
# indefinitely.
#
# We pick the second deletion target deliberately: the witness landed
# on worker-3 in step 2, and one of the diskful replicas needs to go.
# Removing worker-2 (the diskful that is NOT the witness host) makes
# the topology unambiguous — there's no race with "is the user
# converting a diskful to diskless?" because there was no diskless
# user-replica.

echo ">> linstor r d $N2 $RD (delete worker-2 diskful, leaving 1 diskful + 1 TB orphan)"
"${LCTL[@]}" resource delete "$N2" "$RD"

# Sanity: diskful must reach 1 before we assert the collapse. The
# orphan-collapse decision is gated on observed diskful count.
echo ">> wait up to ${CONVERGE}s for diskful to drop to 1"
deadline=$(( $(date +%s) + CONVERGE ))
df=""
while (( $(date +%s) < deadline )); do
    df=$(diskful_count "$RD")
    if [[ "$df" == "1" ]]; then
        break
    fi
    sleep 2
done
if [[ "$df" != "1" ]]; then
    echo "FAIL: diskful never reached 1 after r d $N2 (last=$df)"
    dump_diag
    exit 1
fi

echo ">> wait up to ${CONVERGE}s for ensureTiebreaker to collapse the orphan witness"
if ! wait_witness_count "$RD" 0 "$CONVERGE"; then
    echo "ASSERT 2 FAILED (Bug 338 regression): after r d $N2 with diskful=1,"
    echo "  the orphan TIE_BREAKER witness was NOT removed."
    echo "  diskful=$(diskful_count "$RD") witness-on=$(tb_witness_nodes "$RD")"
    echo "  expected: diskful=1, 0 witnesses (lone diskful runs under quorum=off)"
    echo "  see shouldKeepExistingWitness Bug 338 carve-out for the contract"
    exit 1
fi

# Stability tail: hold for 15s and re-check. The pre-fix symptom of
# Bug 338 was a witness that oscillated between collapse and resurrect
# — a "willRemove: true → willCreate: true → willRemove: true" thrash
# in the controller logs. A single point-in-time read could catch the
# witness in the collapsed half of the oscillation and pass; force a
# tail-window stability check so the regression catcher actually sees
# the bounce. The Bug-342 node-id-occupied invariant now closes the
# in-flight relocate race that the prior grace-window guarded, so the
# collapse fires on the first observation — the stability tail still
# proves it stays collapsed across a 5s requeue cycle without bouncing.
echo ">> stability tail: hold 15s, re-check witness count"
deadline=$(( $(date +%s) + 15 ))
while (( $(date +%s) < deadline )); do
    cur=$(tb_witness_count "$RD")
    if [[ "$cur" != "0" ]]; then
        echo "ASSERT 2 FAILED (Bug 338 oscillation regression): witness reappeared"
        echo "  during 15s stability tail (got=$cur on $(tb_witness_nodes "$RD"))"
        echo "  diskful=$(diskful_count "$RD")"
        exit 1
    fi
    sleep 2
done

echo ">> step 3 OK: 1 diskful, 0 TIE_BREAKER witnesses (stable for 15s)"
echo "PASS: tiebreaker cleanup converged correctly after both r d operations"
