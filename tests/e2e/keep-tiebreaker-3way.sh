#!/usr/bin/env bash
#
# usage: keep-tiebreaker-3way.sh WORK_DIR
#
# Bug B.1 (hunt-v3) regression catcher: `linstor r d --keep-tiebreaker
# <one-of-diskful>` on a 2-diskful + 1-witness shape must NOT collapse
# the auto-managed TIE_BREAKER witness. The CLI's `--keep-tiebreaker`
# flag promises "Keeps the tiebreaker instead of accidentally deleting
# it"; without the REST/controller plumbing the witness gets reaped in
# the same reconcile that observes diskful drop from 2 → 1.
#
# Pre-fix symptom (live on the dev stand 2026-06-02):
#   - 2 diskful (w-1 + w-2) + 1 auto-TIE_BREAKER (w-3)
#   - linstor r d --keep-tiebreaker w-2 hunt3-b1
#   - controller log: "ensureTiebreaker rd=... replicas=2 diskful=1
#     witness=1 willCreate=false willRemove=true"
#   - resulting topology: solo diskful on w-1, NO witness — quorum=1
#     for ~5 minutes until stampTiebreakerSuppression expires.
#
# Post-fix expected: the witness on w-3 survives, the lone diskful on
# w-1 stays in a "1 diskful + 1 TIE_BREAKER witness" shape until the
# operator explicitly tears the rest down or the 5-minute deadline
# expires (at which point the Bug-338 carve-out resumes its normal
# collapse path).
#
# Per the MEMORY.md `tiebreaker e2e MUST exist` rule (Bugs 104/108/
# 267/271/338/342 all slipped past unit-handler tests), every fix in
# the TIE_BREAKER class ships with a real-DRBD scenario on the QEMU
# stand. This is that scenario for Bug B.1.

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

RD=tb-keep-3way
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Per-step convergence budget. The witness add/keep path is event-
# driven (RD reconciler watches Resource CRDs) plus a 5s requeue tick,
# so 60s is comfortable on the QEMU stand.
CONVERGE=${CONVERGE:-60}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/keep-tb-3way-pf.log 2>&1 &
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
    echo "---- dump: RD annotations ----"
    kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" \
        -o jsonpath='{.metadata.annotations}{"\n"}' 2>/dev/null || true
    echo "---- dump: kubectl logs -n $NS deploy/blockstor-controller --tail=120 ----"
    kubectl logs -n "$NS" deploy/blockstor-controller --tail=120 2>/dev/null || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to actually bind.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

# Wipe any leftover state from a prior run of THIS scenario.
delete_rd "$RD" 2>/dev/null || true

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
# TIE_BREAKER witnesses exist for $rd, or timeout.
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

# ---- STEP 1: 2 diskful (w-1 + w-2), auto-TB on w-3 ----------------

echo ">> create RD $RD with 2 diskful replicas on $N1, $N2 (auto-TB lands on $N3)"
"${LCTL[@]}" resource-definition create "$RD"
"${LCTL[@]}" volume-definition create "$RD" 64M
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool stand
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool stand

echo ">> wait for both diskful replicas to reach UpToDate (<=180s)"
wait_disk_state "$RD" "$N1" UpToDate 180 0
wait_disk_state "$RD" "$N2" UpToDate 180 0

echo ">> wait up to ${CONVERGE}s for the auto-TIE_BREAKER witness to land on $N3"
if ! wait_witness_count "$RD" 1 "$CONVERGE"; then
    echo "PRECONDITION FAILED: auto-TIE_BREAKER did not land"
    echo "  diskful=$(diskful_count "$RD") witness-on=$(tb_witness_nodes "$RD")"
    exit 1
fi

witness_node=$(tb_witness_nodes "$RD")
if [[ "$witness_node" != "$N3" ]]; then
    echo "PRECONDITION FAILED: auto-TIE_BREAKER landed on $witness_node, expected $N3"
    exit 1
fi
echo "   step 1 OK: 2 diskful ($N1, $N2) + 1 auto-TIE_BREAKER on $N3"

# ---- STEP 2: r d --keep-tiebreaker $N2 → witness MUST survive ------

echo ">> linstor r d --keep-tiebreaker $N2 $RD (operator opt-in to keep the witness)"
"${LCTL[@]}" resource delete --keep-tiebreaker "$N2" "$RD"

# Sanity: diskful must reach 1 before we assert the keep behaviour.
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
    echo "FAIL: diskful never reached 1 after r d --keep-tiebreaker $N2 (last=$df)"
    dump_diag
    exit 1
fi

# Bug B.1 assert: across a 30s window the witness must NOT be reaped.
# Without the fix, the controller hits the Bug-338 carve-out
# (diskful=1, witness=1, no non-witness diskless) and removes the
# witness within ~5s of the diskful drop.
echo ">> stability tail: hold 30s, assert TIE_BREAKER witness on $N3 survives"
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    cur=$(tb_witness_count "$RD")
    if [[ "$cur" != "1" ]]; then
        echo "ASSERT FAILED (Bug B.1): TIE_BREAKER witness was reaped"
        echo "  during 30s stability tail after r d --keep-tiebreaker"
        echo "  (got=$cur on $(tb_witness_nodes "$RD"))"
        echo "  diskful=$(diskful_count "$RD")"
        echo "  expected: 1 diskful + 1 TIE_BREAKER witness on $N3 (operator opted in)"
        exit 1
    fi

    # Also assert the witness stays on the original node — we should
    # not be regenerating it elsewhere.
    where=$(tb_witness_nodes "$RD")
    if [[ "$where" != "$N3" ]]; then
        echo "ASSERT FAILED: witness migrated from $N3 to $where during keep window"
        exit 1
    fi
    sleep 2
done

# Sanity on the surviving witness flags — must be DISKLESS + TIE_BREAKER
witness_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null)
if [[ "$witness_flags" != *"DISKLESS"* ]] || [[ "$witness_flags" != *"TIE_BREAKER"* ]]; then
    echo "ASSERT FAILED: surviving witness on $N3 has wrong flags: ${witness_flags}"
    echo "  expected: both DISKLESS and TIE_BREAKER"
    exit 1
fi

echo ">> step 2 OK: TIE_BREAKER witness on $N3 survived 30s after r d --keep-tiebreaker, flags=${witness_flags}"
echo "PASS: --keep-tiebreaker preserved the auto-managed witness across the diskful=1 transition"
