#!/usr/bin/env bash
#
# usage: client-compat.sh WORK_DIR
#
# Wire-shape smoke for the production `linstor` CLI (python-linstor)
# against blockstor's REST API. The reason this script exists, even
# though tests/e2e/linstor-cli.sh already covers the happy path: the
# user's day-of-use shell session keeps hitting bugs that unit-level
# httptest assertions miss because they exercise Go-side `httpPost`
# helpers, not the actual python-linstor client. Examples surfaced
# by hand-tested sessions (and now pinned here):
#
#   - Bug 78: POST /v1/nodes/{n}/restore was the only registered
#     verb; python-linstor's node_restore() uses PUT and crashes on
#     the empty-body 405 with xml.etree.ElementTree.ParseError.
#   - Bug 79: rd-create + r-create + late vd-create flipped both
#     replicas to "Unintentional Diskless" because the empty-volume
#     first activation pinned the .md-created marker.
#   - Bug 80: rd-create + vd-create + r-create --auto-place=2 left
#     both replicas stuck in Inconsistent forever, no one electing
#     itself as initial sync source.
#
# Run on the dev-kvaps stand (or any cluster with the standard 3
# storage pools provisioned): this is a true integration smoke, not
# a hermetic unit test. The script will refuse to clobber a cluster
# that has non-test ResourceDefinitions and is safe to re-run.

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

# Per-scenario unique RDs so a partial-failure rerun doesn't trip on
# leftover state from the previous attempt.
RD_LATE_VD=cc-late-vd
RD_AUTOPLACE=cc-autoplace
RD_HAPPY=cc-happy

# port-forward to the apiserver — same dance as linstor-cli.sh.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/client-compat-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    for rd in "$RD_LATE_VD" "$RD_AUTOPLACE" "$RD_HAPPY"; do
        "${LCTL[@]}" resource-definition delete "$rd" >/dev/null 2>&1 || true
    done
    # Belt-and-braces: a stuck satellite finalizer would leave the
    # Resource CRDs behind even after `rd delete` returns success.
    # `kubectl delete --ignore-not-found` is harmless against an
    # already-gone object and clears any straggler.
    for rd in "$RD_LATE_VD" "$RD_AUTOPLACE" "$RD_HAPPY"; do
        kubectl delete resourcedefinition "$rd" --ignore-not-found >/dev/null 2>&1 || true
    done
    kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# linstor_safe runs a linstor CLI command and fails the script on
# any of the wire-shape pathologies that historically slipped past
# unit-level handler tests:
#
#   - python traceback (xml.etree.ElementTree.ParseError is the
#     classic "405 returned an empty body" symptom — Bug 78)
#   - "ERROR:" prefix on a command we expected to succeed
#   - non-zero exit on a command labelled MUST_PASS
#
# Usage:  linstor_safe MUST_PASS|MAY_FAIL <cmd...>
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

# wait_no_unintentional_diskless polls `linstor r l --machine-readable`
# until none of $RD's replicas reports state == "Unintentional Diskless"
# OR the deadline expires. Returns the last r-l output for the caller
# to print on failure.
wait_no_unintentional_diskless() {
    local rd=$1 deadline=$(( SECONDS + ${2:-90} ))
    local last
    while (( SECONDS < deadline )); do
        last=$(linstor_safe MAY_FAIL resource list --resources "$rd")
        if ! grep -qi 'Unintentional Diskless' <<<"$last"; then
            return 0
        fi
        sleep 3
    done
    printf '%s' "$last"
    return 1
}

# wait_any_uptodate polls until at least one replica of $rd reports
# disk:UpToDate. Catches Bug 80 (both stuck Inconsistent forever).
wait_any_uptodate() {
    local rd=$1 deadline=$(( SECONDS + ${2:-180} ))
    local last
    while (( SECONDS < deadline )); do
        last=$(linstor_safe MAY_FAIL resource list --resources "$rd")
        if grep -q '"disk_state"\s*:\s*"UpToDate"' <<<"$last"; then
            return 0
        fi
        sleep 3
    done
    printf '%s' "$last"
    return 1
}

# ----------------------------------------------------------------------
# Section A — wire-shape smoke: every command we claim to support
# returns a parseable response, not a 405-empty-body that crashes
# the python client. This is the first-defence regression net for
# the next Bug-78-class bug.
# ----------------------------------------------------------------------

echo ">> [A.1] linstor node list"
linstor_safe MUST_PASS node list >/dev/null

echo ">> [A.2] linstor storage-pool list"
linstor_safe MUST_PASS storage-pool list >/dev/null

echo ">> [A.3] linstor resource-group list"
linstor_safe MUST_PASS resource-group list >/dev/null

echo ">> [A.4] linstor resource-definition list"
linstor_safe MUST_PASS resource-definition list >/dev/null

echo ">> [A.5] linstor volume-definition list"
linstor_safe MUST_PASS volume-definition list >/dev/null

echo ">> [A.6] linstor resource list"
linstor_safe MUST_PASS resource list >/dev/null

echo ">> [A.7] linstor physical-storage list (Bug 51 — must not crash on empty)"
linstor_safe MUST_PASS physical-storage list >/dev/null

echo ">> [A.8] linstor controller list-properties"
linstor_safe MUST_PASS controller list-properties >/dev/null

echo ">> [A.9] linstor error-reports list"
linstor_safe MUST_PASS error-reports list >/dev/null

# Node lifecycle PUTs (Bug 78 pin): all three of these used to crash
# python-linstor with xml.etree.ElementTree.ParseError because they
# hit a POST-only route and the 405 came back with an empty body.
echo ">> [A.10] linstor node restore (PUT compat — Bug 78)"
linstor_safe MUST_PASS node restore "$WORKER_1" >/dev/null

# ----------------------------------------------------------------------
# Section B — the user's actual repro sessions, pinned forever.
# ----------------------------------------------------------------------

echo ">> [B.1 — Bug 79] rd create + r create + late vd create — replicas MUST NOT end up Unintentional Diskless"
linstor_safe MUST_PASS resource-definition create "$RD_LATE_VD" >/dev/null
linstor_safe MUST_PASS resource create "$WORKER_1" "$RD_LATE_VD" --storage-pool stand >/dev/null
linstor_safe MUST_PASS resource create "$WORKER_2" "$RD_LATE_VD" --storage-pool stand >/dev/null
# Brief settling delay so the satellite reconciler at least sees the
# empty-volume first pass before we add the VD. Without this the test
# can race past the "first apply with empty volumes" branch entirely
# and miss what Bug 79 was about.
sleep 5
linstor_safe MUST_PASS volume-definition create "$RD_LATE_VD" 32M >/dev/null

if ! out=$(wait_no_unintentional_diskless "$RD_LATE_VD" 90); then
    echo "FAIL (Bug 79 regression): replicas of $RD_LATE_VD stuck Unintentional Diskless after VD-add" >&2
    echo "$out" >&2
    exit 1
fi
echo "   B.1 OK — no Unintentional Diskless after VD-add"

echo ">> [B.2 — Bug 80] rd + vd + r --auto-place=2 — at least one replica MUST reach UpToDate"
linstor_safe MUST_PASS resource-definition create "$RD_AUTOPLACE" >/dev/null
linstor_safe MUST_PASS volume-definition create "$RD_AUTOPLACE" 32M >/dev/null
linstor_safe MUST_PASS resource create "$RD_AUTOPLACE" --auto-place 2 --storage-pool stand >/dev/null

if ! out=$(wait_any_uptodate "$RD_AUTOPLACE" 180); then
    echo "FAIL (Bug 80 regression): no replica of $RD_AUTOPLACE reached UpToDate within 180s" >&2
    echo "$out" >&2
    exit 1
fi
echo "   B.2 OK — at least one replica reached UpToDate"

echo ">> [B.3] happy path: rd + vd + r WORKER_1 + r WORKER_2 — both replicas to UpToDate"
linstor_safe MUST_PASS resource-definition create "$RD_HAPPY" >/dev/null
linstor_safe MUST_PASS volume-definition create "$RD_HAPPY" 32M >/dev/null
linstor_safe MUST_PASS resource create "$WORKER_1" "$RD_HAPPY" --storage-pool stand >/dev/null
linstor_safe MUST_PASS resource create "$WORKER_2" "$RD_HAPPY" --storage-pool stand >/dev/null
RD="$RD_HAPPY" wait_uptodate "$RD_HAPPY" "$WORKER_1" "$WORKER_2"
echo "   B.3 OK — full happy path"

echo ">> client-compat OK"
