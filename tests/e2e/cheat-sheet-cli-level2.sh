#!/usr/bin/env bash
#
# usage: cheat-sheet-cli-level2.sh WORK_DIR
#
# Scenario 1.24 (tests/scenarios/01-api-contract.md):
#   Level-2 LINSTOR CLI matrix against blockstor REST. The cheat-sheet
#   (tests/observability-cheat-sheet-scenarios.md §11) lists:
#
#     linstor node list
#     linstor storage-pool list
#     linstor resource-definition list
#     linstor volume-definition list
#     linstor resource list
#     linstor volume list
#     linstor resource create [--diskless]
#     linstor resource delete
#
# linstor-cli.sh / linstor-cli-replica-move.sh already cover most of
# the side-effecting commands individually (1.1-1.6, 1.17). This
# scenario is the integration smoke called out at line 208: every
# command must exit 0 against a fresh stand inside a single run.
#
# Crucially this script also exercises `r c --diskless` distinctly
# from autoplacer-stamped tiebreaker (cheat-sheet §11 last paragraph)
# — those share kernel-level DRBD-9 diskless but render different
# LINSTOR-side semantics (Diskless vs TieBreaker).

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

RD=cheat-l2

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/cheat-l2-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTL_M=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# Macro: run a command, capture stderr+stdout, ensure exit 0. Some
# `linstor` commands print 'No nodes' on empty clusters but still
# exit 0 — we tolerate empty content. Note: `local out=$(...)` would
# mask the inner exit under set -e via `local`'s own return value,
# so we declare + assign on separate lines and disable set -e for the
# capture only.
run_ok() {
    local label=$1; shift
    local out rc=0
    set +e
    out=$("$@" 2>&1)
    rc=$?
    set -e
    if (( rc != 0 )); then
        echo "FAIL: $label exited $rc"
        echo "--- output ---"
        echo "$out"
        return 1
    fi
    echo "   $label OK"
    return 0
}

fail=0

# --- list-style commands (1.1-1.6 already cover them individually; this
# is the back-to-back integration smoke) ---
run_ok "linstor node list"                   "${LCTL[@]}" node list                  || fail=1
run_ok "linstor storage-pool list"           "${LCTL[@]}" storage-pool list          || fail=1
run_ok "linstor resource-definition list"    "${LCTL[@]}" resource-definition list   || fail=1

# Side-effecting commands — provisioning an RD, then walking through
# vd list / r list / v list / r delete.
run_ok "linstor resource-definition create $RD"  "${LCTL[@]}" resource-definition create "$RD" || fail=1
run_ok "linstor volume-definition create $RD 32M" "${LCTL[@]}" volume-definition create "$RD" 32M || fail=1
run_ok "linstor resource create $WORKER_1 $RD"   "${LCTL[@]}" resource create "$WORKER_1" "$RD" --storage-pool stand || fail=1
run_ok "linstor resource create $WORKER_2 $RD"   "${LCTL[@]}" resource create "$WORKER_2" "$RD" --storage-pool stand || fail=1

wait_uptodate "$RD" "$WORKER_1" "$WORKER_2" || fail=1

run_ok "linstor volume-definition list -r $RD"   "${LCTL[@]}" volume-definition list -r "$RD" || fail=1
run_ok "linstor resource list -r $RD"            "${LCTL[@]}" resource list -r "$RD" || fail=1
run_ok "linstor volume list -r $RD"              "${LCTL[@]}" volume list -r "$RD" || fail=1

# --- r c --diskless: cheat-sheet's "audit" call from §11 last paragraph.
# WORKER_3 is uninvolved at this point — we add a diskless replica
# there and assert the new row's LINSTOR flag is DISKLESS (and NOT
# TIE_BREAKER, which is auto-placed by blockstor's RD reconciler for
# a 2-replica RD without an operator-requested 3rd).
echo ">> linstor resource create $WORKER_3 $RD --diskless"
if ! "${LCTL[@]}" resource create "$WORKER_3" "$RD" --diskless >/dev/null 2>&1; then
    echo "FAIL: linstor resource create --diskless errored"
    fail=1
else
    echo "   --diskless create accepted"

    # Wait up to 30s for WORKER_3's replica to appear in the listing.
    found=""
    for _ in $(seq 1 15); do
        json=$("${LCTL_M[@]}" resource list -r "$RD" 2>/dev/null || true)
        if echo "$json" | grep -q "\"$WORKER_3\""; then
            found=1
            break
        fi
        sleep 2
    done
    if [[ -z "$found" ]]; then
        echo "FAIL: diskless replica on $WORKER_3 never appeared in resource list"
        "${LCTL[@]}" resource list -r "$RD" || true
        fail=1
    else
        # Inspect the row's flags — must be DISKLESS, and not the
        # autoplacer-stamped TIE_BREAKER (the §11 distinction).
        # blockstor returns [[ {res-entry}, ... ]] (outer wrap is the
        # /v1/view/resources envelope shape). jq's .. recursive walk
        # finds the entry without us having to commit to an exact
        # nesting level.
        if command -v jq >/dev/null 2>&1; then
            flags=$(echo "$json" | jq -r --arg n "$WORKER_3" '
                [.. | objects | select(.node_name? == $n) | .flags // []]
                | first // [] | join(",")
            ' 2>/dev/null || true)
            if [[ "$flags" != *"DISKLESS"* ]]; then
                echo "FAIL: $WORKER_3 row flags='$flags' (expected DISKLESS)"
                fail=1
            elif [[ "$flags" == *"TIE_BREAKER"* ]]; then
                echo "FAIL: $WORKER_3 row has TIE_BREAKER flag (operator-DISKLESS, not autoplacer-tiebreaker): '$flags'"
                fail=1
            else
                echo "   --diskless row flags=$flags OK"
            fi
        else
            # jq unavailable — fall back to substring match. Weaker
            # guard but better than skipping the assertion entirely.
            if echo "$json" | grep -q '"DISKLESS"'; then
                echo "   --diskless row contains DISKLESS flag (jq absent — coarse match)"
            else
                echo "FAIL: $WORKER_3 resource list output has no DISKLESS flag (jq absent — coarse match)"
                fail=1
            fi
        fi
    fi
fi

# --- r d ---
run_ok "linstor resource delete $WORKER_2 $RD"   "${LCTL[@]}" resource delete "$WORKER_2" "$RD" || fail=1

# Allow the satellite's finalizer-strip path to settle.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
if kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1; then
    echo "FAIL: Resource $RD.$WORKER_2 still present after CLI delete"
    fail=1
fi

if (( fail != 0 )); then
    echo "CHEAT-SHEET-CLI-LEVEL2: FAIL"
    exit 1
fi

echo ">> CHEAT-SHEET-CLI-LEVEL2 OK (n l, sp l, rd l, vd l, r l, v l, r c, r c --diskless, r d all round-trip)"
