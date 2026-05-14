#!/usr/bin/env bash
#
# usage: recovery-late-vd-real-drbd.sh WORK_DIR
#
# Tier 4 real-DRBD regression guard for **Bug 79** (late VolumeDefinition
# → Unintentional Diskless). The Tier 2 sibling lives in
# tests/integration/group_e_test.go::TestGroupEVDLateAddTriggersReconcile
# and pins the CRD-/controller-shape half of the bug. Tier 4 is here
# because the second half — "did the satellite actually run drbdadm
# create-md against the now-present backing storage, or did it pin the
# .md-created marker on the empty-volume first pass?" — can only be
# observed with a real kernel: a mock satellite cannot tell us whether
# `drbdsetup status` reports disk:UpToDate vs disk:Diskless on a real
# /dev/drbdN minor.
#
# Production trigger (reproduced verbatim by this script):
#
#   1. linstor rd c bug79-e2e
#   2. linstor r c <worker-1> bug79-e2e --storage-pool stand
#   3. linstor r c <worker-2> bug79-e2e --storage-pool stand
#   4. (5s settle so the satellite reconciler observes the empty-volume
#      first pass — without this the test can race past the exact branch
#      Bug 79 lived in)
#   5. linstor vd c bug79-e2e 32M
#
# Pass contract:
#
#   - Within 60s of the late-VD add, both diskful replicas reach
#     disk_state == "UpToDate" in `linstor r l --machine-readable`.
#   - Neither replica ever reports the "Unintentional Diskless" flag in
#     `linstor r l` (the operator-visible smoking gun for Bug 79).
#   - On both satellite pods, `drbdsetup status bug79-e2e` confirms the
#     resource is present in the kernel with real metadata (disk: line
#     is something other than Diskless/Unattached/Negotiating). This
#     is the bit that distinguishes a real fix from a controller-side
#     paper-over that hides the diskless flag but leaves the kernel
#     unattached.
#
# Pairs with tests/e2e/client-compat.sh §B.1 — that one is the CLI-
# surface smoke (no Unintentional Diskless on the CLI wire); THIS one
# is the kernel-truth check.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=bug79-e2e

# Random ephemeral port so back-to-back runs against the same stand
# don't collide on a stale TIME_WAIT socket from a previous attempt.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/recovery-late-vd-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    linstor --controllers "http://localhost:$PF_PORT" resource list -r "$RD" || true
    echo "---- dump: linstor --machine-readable r l -r $RD ----"
    linstor --controllers "http://localhost:$PF_PORT" --machine-readable resource list -r "$RD" || true
    echo "---- dump: linstor vd l -r $RD ----"
    linstor --controllers "http://localhost:$PF_PORT" volume-definition list -r "$RD" || true
    for n in "$WORKER_1" "$WORKER_2"; do
        echo "---- dump: drbdsetup status $RD on $n ----"
        on_node "$n" drbdsetup status "$RD" 2>/dev/null || true
        echo "---- dump: /etc/drbd.d/${RD}.res on $n ----"
        on_node "$n" cat "/etc/drbd.d/${RD}.res" 2>/dev/null || true
        echo "---- dump: ls /etc/drbd.d/${RD}.md-created on $n ----"
        on_node "$n" ls -la "/etc/drbd.d/${RD}.md-created" 2>/dev/null || true
    done
    echo "---- dump: kubectl logs satellite tail=120 ----"
    for pod in $(kubectl -n blockstor-system get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        echo "-- $pod --"
        kubectl -n blockstor-system logs "$pod" --tail=120 2>/dev/null || true
    done
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    # delete_rd from lib.sh: cascades through Snapshots, Resources, the
    # RD CRD, then force-cleans the satellite-side .res and .md-created
    # markers so a failed run leaves no kernel residue for the next test.
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# linstor_safe — same wire-shape wrapper as tests/e2e/client-compat.sh.
# Catches python-linstor crashes (xml.etree.ElementTree.ParseError on
# the dreaded empty-body 405) and "ERROR:" envelopes that exit 0.
linstor_safe() {
    local mode=$1
    shift
    local err_file rc out
    err_file=$(mktemp)

    if out=$("${LCTL[@]}" "$@" 2>"$err_file"); then
        rc=0
    else
        rc=$?
    fi

    local err
    err=$(cat "$err_file")
    rm -f "$err_file"

    if grep -qiE 'xml.*ParseError|Traceback|HTTPConnectionPool' <<<"$err"; then
        echo "FAIL: linstor $* crashed the python client (wire-shape bug, like Bug 78)" >&2
        echo "----- stderr -----" >&2
        echo "$err" >&2
        echo "------------------" >&2
        exit 1
    fi

    if [[ "$mode" == "MUST_PASS" && $rc -ne 0 ]]; then
        echo "FAIL: linstor $* exited $rc but was expected to succeed" >&2
        echo "----- stderr -----" >&2
        echo "$err" >&2
        echo "----- stdout -----" >&2
        echo "$out" >&2
        echo "------------------" >&2
        exit 1
    fi

    printf '%s' "$out"
}

# wait_both_uptodate_no_diskless polls `linstor r l --machine-readable`
# for $RD until BOTH $WORKER_1 and $WORKER_2 show
# disk_state == "UpToDate" AND neither row carries the
# "Unintentional Diskless" flag (either as a flag entry or as the
# in_use / state string). Returns the last JSON for the caller to
# print on failure.
#
# `linstor r l` machine-readable shape is a list-of-lists:
#   [[{node_name, volumes:[{state:{disk_state:"..."}}], flags:[...]}, ...]]
# — flatten with `.[][]` to iterate replicas.
wait_both_uptodate_no_diskless() {
    local rd=$1 deadline=$(( $(date +%s) + ${2:-60} ))
    local last unintentional uptodate_count
    while (( $(date +%s) < deadline )); do
        last=$(linstor_safe MAY_FAIL resource list --resources "$rd")

        # Bug 79 surface 1: the literal "Unintentional Diskless" string
        # surfaced by `linstor r l` (driven by `.flags[]` containing the
        # DRBD_DISKLESS flag on a Resource whose Spec did NOT request
        # diskless). A grep across the full machine-readable JSON is
        # robust to schema drift between linstor-client versions.
        if grep -qi 'Unintentional Diskless' <<<"$last"; then
            sleep 3
            continue
        fi

        # Bug 79 surface 2: actual disk_state on both diskful replicas.
        # Filter to only our two named workers; any other node (e.g. a
        # tiebreaker if the cluster auto-stamped one) is ignored here
        # because Bug 79 is about the two original diskful replicas
        # silently demoting to diskless, not about tiebreaker presence.
        uptodate_count=$(jq -r --arg w1 "$WORKER_1" --arg w2 "$WORKER_2" '
            [ .[][]
              | select(.node_name == $w1 or .node_name == $w2)
              | select((.volumes // [])[]?.state.disk_state == "UpToDate")
            ] | length' <<<"$last" 2>/dev/null || echo 0)

        if (( uptodate_count >= 2 )); then
            return 0
        fi

        sleep 3
    done

    printf '%s' "$last"
    return 1
}

# verify_kernel_has_metadata asserts `drbdsetup status $RD` on the named
# node reports a disk: line that's NOT Diskless/Unattached/Negotiating.
# This is the kernel-truth half of Bug 79: a controller-side fix that
# clears the "Unintentional Diskless" flag without actually attaching
# the backing device would pass the CLI assertion above but fail here.
verify_kernel_has_metadata() {
    local node=$1
    local status disk_line
    status=$(on_node "$node" drbdsetup status "$RD" 2>&1 || true)
    # Expect at least one `disk:` line in the local-volume row that is
    # NOT Diskless/Unattached/Negotiating. drbdsetup prints peers as
    # `peer-disk:` so a plain `disk:` grep targets the local volume.
    disk_line=$(grep -m1 -E '^\s*disk:' <<<"$status" || true)

    if [[ -z "$disk_line" ]]; then
        echo "FAIL: drbdsetup status on $node had no 'disk:' line — kernel never saw $RD" >&2
        echo "----- drbdsetup status -----" >&2
        echo "$status" >&2
        echo "----------------------------" >&2
        return 1
    fi

    if grep -qE 'disk:(Diskless|Unattached|Negotiating)' <<<"$disk_line"; then
        echo "FAIL: Bug 79 regression on $node — kernel reports '$disk_line' "\
"(expected real metadata: Inconsistent/UpToDate/Outdated/Consistent)" >&2
        echo "----- drbdsetup status -----" >&2
        echo "$status" >&2
        echo "----------------------------" >&2
        return 1
    fi

    echo "   kernel on $node: $disk_line"
}

# ----------------------------------------------------------------------
# The repro: rd + r×2 (no VD) + 5s settle + late vd.
# ----------------------------------------------------------------------

echo ">> [1] linstor rd c $RD"
linstor_safe MUST_PASS resource-definition create "$RD" >/dev/null

echo ">> [2] linstor r c $WORKER_1 $RD --storage-pool stand"
linstor_safe MUST_PASS resource create "$WORKER_1" "$RD" --storage-pool stand >/dev/null

echo ">> [3] linstor r c $WORKER_2 $RD --storage-pool stand"
linstor_safe MUST_PASS resource create "$WORKER_2" "$RD" --storage-pool stand >/dev/null

# Without this settle the satellite reconciler can race past the exact
# empty-volume apply pass where Bug 79 used to pin .md-created — the
# test would then succeed against an unfixed build because the empty
# pass never ran. 5s matches the production repro from 2026-05-14.
echo ">> [4] sleep 5s so satellite reconciler observes the empty-volume state"
sleep 5

echo ">> [5] linstor vd c $RD 32M (the trigger — must NOT leave replicas Unintentional Diskless)"
linstor_safe MUST_PASS volume-definition create "$RD" 32M >/dev/null

echo ">> [6] poll up to 60s: both diskful replicas reach disk_state=UpToDate and never 'Unintentional Diskless'"
if ! out=$(wait_both_uptodate_no_diskless "$RD" 60); then
    echo "FAIL (Bug 79 regression): after late VD add, replicas of $RD did not reach UpToDate" >&2
    echo "      OR carried the 'Unintentional Diskless' flag past the 60s deadline" >&2
    echo "----- last linstor r l --machine-readable -----" >&2
    echo "$out" >&2
    echo "------------------------------------------------" >&2
    exit 1
fi
echo "   both replicas UpToDate, neither flagged Unintentional Diskless"

echo ">> [7] verify kernel-truth on both satellites: drbdsetup status shows real metadata"
verify_kernel_has_metadata "$WORKER_1"
verify_kernel_has_metadata "$WORKER_2"

echo "PASS recovery-late-vd-real-drbd"
