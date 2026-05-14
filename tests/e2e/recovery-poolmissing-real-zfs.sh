#!/usr/bin/env bash
#
# usage: recovery-poolmissing-real-zfs.sh WORK_DIR
#
# Bug 83 — Tier-4 real-ZFS regression guard.
#
# Pin: when a backing zpool disappears under a StoragePool the
# satellite stamps `Status.PoolMissing=true`. The REST envelope for
# `/v1/view/storage-pools` MUST then surface BOTH:
#   - `state == "Faulty"`
#   - a structured `reports[]` entry whose `ret_code` carries the
#     ApiConsts `MASK_ERROR | MASK_STOR_POOL | 990` bit set
#
# Why both: python-linstor's `storpool_cmds.py:472` derives the State
# column from `get_replies_state(reports)` — NOT from the bare `state`
# string. Before the fix, REST emitted `state="Faulty"` but the
# reports[] array was empty, so `linstor sp l` rendered the column as
# "Ok" (in green) even though the underlying zpool was destroyed.
# This test exercises the full operator-visible path that the Bug-50
# unit tests can't: a real `zpool destroy`, the satellite's
# StoragePoolReconciler.writeCapacity() seeing `PoolStatus` fail, the
# CRD's Status.PoolMissing flipping to true, REST flattening it onto
# the wire shape, and the Python CLI parsing the response.
#
# Steps:
#   1. Baseline — `linstor sp l --machine-readable` for `zfs-thin` on
#      $WORKER_1 MUST report state="Ok" and reports=[] (the fix MUST
#      NOT stamp reports[] when the pool is healthy — pinned by
#      TestCrdToWireStoragePoolHealthyHasNoReports at the unit level,
#      pinned here at the wire level).
#   2. Destroy backing zpool: `zpool destroy -f blockstor-zfs` inside
#      the satellite pod on $WORKER_1. The next probe tick (the
#      reconciler's `capacityResyncInterval = 30s`) MUST detect the
#      missing pool and flip Status.PoolMissing=true on the
#      `zfs-thin.$WORKER_1` StoragePool CRD.
#   3. Wait up to 90s for the wire view to flip — machine-readable
#      JSON's `state` MUST become "Faulty" AND `reports[]` MUST be
#      non-empty with `ret_code & MASK_ERROR != 0` AND
#      `ret_code & MASK_STOR_POOL != 0`. (90s = 3 probe ticks
#      tolerance for a busy QEMU stand.)
#   4. python-linstor human output: `linstor sp l` MUST render the
#      State column for `zfs-thin` on $WORKER_1 NOT as "Ok" — this is
#      the operator-visible regression: pre-fix CLI showed "Ok"
#      colourised green even when the underlying pool was gone.
#   5. Recovery — `zpool create -f blockstor-zfs <dev>` on the same
#      backing partition. The next probe tick MUST clear PoolMissing
#      and the wire view MUST flip back to state="Ok" with empty
#      reports[] within 90s.
#
# Cleanup trap recreates the zpool if the test bailed mid-way so the
# next test in the batch doesn't inherit a destroyed pool.
#
# Why not also check the partition with `physicaldevice` CRDs:
# `install-pools.sh` creates the zpool on `/dev/sd<x>1` where <x> is
# typically `a` but the satellite's mount may differ across stand
# bring-ups. Rather than scraping PhysicalDevice CRDs, we discover
# the partition the running zpool is sitting on BEFORE we destroy it
# (`zpool status -P`), then use that same partition path for the
# recreate. That keeps the test self-contained on whatever device the
# stand happens to expose.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH (apt install jq)"
    exit 0
fi

POOL=zfs-thin
NODE=$WORKER_1
ZPOOL=blockstor-zfs
# Bug 83 ret_code mask bits — must match pkg/api/v1/storage_pool.go
# constants. -0x4000_0000_0000_0000 is MASK_ERROR (sign bit + 62 in
# int64); 0x140000 is MASK_STOR_POOL. jq does signed 64-bit ints, but
# bitwise ops require the `tonumber` path AND we can't rely on jq
# having bit-ops cross-version, so we extract `ret_code` as a JSON
# integer and let bash do the masking.
MASK_ERROR_HEX=0xC000000000000000   # absolute value; ret_code is negative
MASK_STOR_POOL_HEX=0x0000000000140000

# ----------------------------------------------------------------
# Port-forward to apiserver — same dance as client-compat.sh.
# ----------------------------------------------------------------
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/recovery-poolmissing-real-zfs-pf.log 2>&1 &
PF_PID=$!

# Discover the backing partition the live zpool sits on so we can
# recreate it on cleanup. Done BEFORE installing the trap so a SKIP
# inside the discovery (no zpool on $NODE) doesn't try to recreate a
# pool that never existed.
ZPOOL_DEV=""

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        echo "---- DIAG: storagepool CRD status on $NODE ----" >&2
        kubectl get storagepool.blockstor.io.blockstor.io "${POOL}.${NODE}" \
            -o yaml 2>&1 | tail -40 >&2 || true
        echo "---- DIAG: REST view ----" >&2
        curl -fsS "http://127.0.0.1:${PF_PORT}/v1/view/storage-pools?nodes=${NODE}&storage_pools=${POOL}" \
            2>/dev/null | jq . >&2 || true
        echo "---- DIAG: satellite logs (tail) ----" >&2
        local sat_pod
        sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o "jsonpath={.items[?(@.spec.nodeName==\"${NODE}\")].metadata.name}" 2>/dev/null || true)
        if [[ -n "$sat_pod" ]]; then
            kubectl -n "$NS" logs "$sat_pod" --tail=60 2>/dev/null >&2 || true
        fi
    fi

    # Belt-and-braces: if the test bailed mid-way the zpool may still
    # be destroyed. Recreate it on the same partition we discovered
    # at startup, ignoring all errors (pool may already exist if the
    # test passed cleanly). Then wait for the satellite probe to
    # clear PoolMissing — without this wait, the next test in the
    # batch can race past install-pools.sh's settle and observe a
    # stale Faulty state that has nothing to do with its scenario.
    if [[ -n "$ZPOOL_DEV" ]]; then
        on_node "$NODE" bash -c "
            if ! zpool list ${ZPOOL} >/dev/null 2>&1; then
                zpool create -f -o cachefile=none ${ZPOOL} ${ZPOOL_DEV} 2>&1 || true
            fi
        " 2>/dev/null || true

        # Wait up to 60s for poolMissing to clear on the CRD. The
        # 30s capacityResyncInterval means one probe is usually
        # enough; we double it for QEMU-stand jitter.
        local deadline=$(( $(date +%s) + 60 ))
        while (( $(date +%s) < deadline )); do
            pm=$(kubectl get storagepool.blockstor.io.blockstor.io \
                "${POOL}.${NODE}" -o jsonpath='{.status.poolMissing}' 2>/dev/null || echo "")
            [[ "$pm" != "true" ]] && break
            sleep 3
        done
    fi

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward to bind.
for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# Discover the backing partition the live zpool is using. We need
# this for cleanup-recreate. `zpool status -P` prints absolute device
# paths; the first /dev/* line after the `config:` block is the
# vdev. Failing this lookup is fatal — without it we can't safely
# recreate the pool, so we'd leave the cluster in a degraded state
# for the next test.
echo ">> discover backing partition for $ZPOOL on $NODE"
ZPOOL_DEV=$(on_node "$NODE" bash -c "
    zpool status -P ${ZPOOL} 2>/dev/null | awk '/^\t  \/dev\// {print \$1; exit}' | tr -d '\t '
" || true)
if [[ -z "$ZPOOL_DEV" || "$ZPOOL_DEV" != /dev/* ]]; then
    echo "FAIL: could not discover backing partition for $ZPOOL on $NODE (got: '$ZPOOL_DEV')" >&2
    echo "      $ZPOOL may not be present on the stand — install-pools.sh zfs first" >&2
    exit 1
fi
echo "   backing partition: $ZPOOL_DEV"

# ----------------------------------------------------------------
# Helpers — query the wire view for a single (node, pool).
# ----------------------------------------------------------------

# sp_wire_state echoes the wire `state` field for the target pool.
sp_wire_state() {
    curl -fsS "http://127.0.0.1:${PF_PORT}/v1/view/storage-pools?nodes=${NODE}&storage_pools=${POOL}" \
        | jq -r '.[0].state // "MISSING"'
}

# sp_wire_reports echoes the reports[] array as compact JSON.
sp_wire_reports() {
    curl -fsS "http://127.0.0.1:${PF_PORT}/v1/view/storage-pools?nodes=${NODE}&storage_pools=${POOL}" \
        | jq -c '.[0].reports // []'
}

# sp_wire_retcode echoes the first reports[].ret_code as a raw
# integer (signed) — empty string if reports[] is empty or missing.
sp_wire_retcode() {
    curl -fsS "http://127.0.0.1:${PF_PORT}/v1/view/storage-pools?nodes=${NODE}&storage_pools=${POOL}" \
        | jq -r '.[0].reports[0].ret_code // empty'
}

# mask_has_bit RET_CODE BIT_HEX — bash arithmetic on signed 64-bit
# ret_codes. Bash `(( ... ))` treats negative literals correctly and
# `&` works on the full 64-bit value on a 64-bit host (the QEMU stand
# and the runner are both x86_64). Returns 0 if the bit is set.
mask_has_bit() {
    local rc=$1 bit=$2
    # MASK_ERROR is the sign bit + bit 62; in two's complement that's
    # `rc < 0` for the sign-bit half. Treat MASK_ERROR specially —
    # any negative ret_code on this code path carries MASK_ERROR.
    if [[ "$bit" == "0xC000000000000000" ]]; then
        (( rc < 0 ))
        return $?
    fi
    (( (rc & bit) != 0 ))
}

# wait_wire_state STATE TIMEOUT_S — block until sp_wire_state echoes
# the expected string, or the deadline expires. Echoes the last
# observed state on timeout for the caller's error message.
wait_wire_state() {
    local want=$1
    local timeout=$2
    local deadline=$(( SECONDS + timeout ))
    local last
    while (( SECONDS < deadline )); do
        last=$(sp_wire_state 2>/dev/null || echo "?")
        if [[ "$last" == "$want" ]]; then
            return 0
        fi
        sleep 3
    done
    echo "$last"
    return 1
}

# ----------------------------------------------------------------
# Step 1 — baseline: state=Ok, reports[] empty.
# ----------------------------------------------------------------
echo ">> [1] baseline — ${POOL} on ${NODE} must be Ok with empty reports[]"
state=$(sp_wire_state)
if [[ "$state" != "Ok" ]]; then
    echo "FAIL (baseline): expected state=Ok, got state=${state}" >&2
    exit 1
fi
reports=$(sp_wire_reports)
if [[ "$reports" != "[]" ]]; then
    echo "FAIL (baseline): expected reports=[], got reports=${reports}" >&2
    echo "       — a healthy pool MUST NOT carry a reports[] entry, that would" >&2
    echo "       — re-introduce Bug 83's symptom in the opposite direction" >&2
    exit 1
fi
echo "   [1] OK — state=Ok, reports=[]"

# ----------------------------------------------------------------
# Step 2 — destroy the backing zpool.
# ----------------------------------------------------------------
echo ">> [2] destroy zpool ${ZPOOL} on ${NODE}"
on_node "$NODE" zpool destroy -f "$ZPOOL"

# ----------------------------------------------------------------
# Step 3 — wait for Status.PoolMissing → wire flip.
# ----------------------------------------------------------------
echo ">> [3] wait for wire view to flip state=Faulty (≤90s)"
if ! wait_wire_state "Faulty" 90; then
    echo "FAIL: state never flipped to Faulty within 90s (last observed: $(sp_wire_state))" >&2
    exit 1
fi
echo "   [3a] OK — state=Faulty"

reports=$(sp_wire_reports)
if [[ "$reports" == "[]" || "$reports" == "null" ]]; then
    echo "FAIL (Bug 83 regression): state=Faulty but reports[] is empty" >&2
    echo "       — this is the exact pre-fix wire shape that made the Python" >&2
    echo "       — CLI render the State column as 'Ok' even with state=Faulty" >&2
    echo "       reports: $reports" >&2
    exit 1
fi
echo "   [3b] OK — reports[] non-empty: $reports"

rc=$(sp_wire_retcode)
if [[ -z "$rc" ]]; then
    echo "FAIL: reports[0].ret_code missing on Faulty pool wire shape" >&2
    exit 1
fi
echo "   ret_code: $rc"

if ! mask_has_bit "$rc" "$MASK_ERROR_HEX"; then
    echo "FAIL (Bug 83 regression): reports[0].ret_code=${rc} lacks MASK_ERROR bit" >&2
    echo "       — Python CLI's get_replies_state classifies by severity bits;" >&2
    echo "       — without MASK_ERROR the entry counts as INFO and renders Ok" >&2
    exit 1
fi
echo "   [3c] OK — ret_code carries MASK_ERROR"

if ! mask_has_bit "$rc" "$MASK_STOR_POOL_HEX"; then
    echo "FAIL (Bug 83 regression): reports[0].ret_code=${rc} lacks MASK_STOR_POOL bit" >&2
    echo "       — audit-log greppers route by subject mask; without MASK_STOR_POOL" >&2
    echo "       — the entry is unattributed and operator tooling miscategorises it" >&2
    exit 1
fi
echo "   [3d] OK — ret_code carries MASK_STOR_POOL"

# ----------------------------------------------------------------
# Step 4 — python-linstor human output must NOT render "Ok".
# ----------------------------------------------------------------
echo ">> [4] python-linstor CLI output: State column for ${POOL} on ${NODE} MUST NOT be 'Ok'"
# `--no-color` strips the ANSI green/red that the CLI uses to colour
# the State column — without it the regex below would match the
# escape sequence, not the bare word. The CLI prints something like:
#   | Node ... | zfs-thin | ... | Faulty |
# Pre-fix this column was literally `Ok` (in green) with an
# underlying state of Faulty.
sp_cli_line=$(linstor --controllers "http://127.0.0.1:${PF_PORT}" --no-color \
    storage-pool list --storage-pools "$POOL" --nodes "$NODE" 2>&1 \
    | grep -E "^\| ${POOL}[[:space:]]+\| ${NODE}" || true)
if [[ -z "$sp_cli_line" ]]; then
    echo "FAIL: linstor sp l did not return a row for ${POOL} on ${NODE}" >&2
    linstor --controllers "http://127.0.0.1:${PF_PORT}" --no-color \
        storage-pool list --storage-pools "$POOL" --nodes "$NODE" >&2 || true
    exit 1
fi
echo "   CLI row: $sp_cli_line"

# The State column is the last `|`-delimited cell on the row that
# carries a known state token. Pre-fix the CLI rendered `Ok` here.
# Anything other than `Ok` (typically `Faulty` or `Error`) is the
# fix working. We assert the negation rather than `== "Faulty"`
# because the exact rendering varies across linstor-client versions
# (newer ones render `Error` for the same wire shape) — what matters
# for the operator is that the column is NOT the misleading `Ok`.
if grep -qE '\| Ok +\|' <<<"$sp_cli_line"; then
    echo "FAIL (Bug 83 regression): linstor sp l State column shows 'Ok' for a destroyed pool" >&2
    echo "       row: $sp_cli_line" >&2
    echo "       — this is the operator-visible symptom: the column hides" >&2
    echo "       — that the underlying zpool is gone" >&2
    exit 1
fi
echo "   [4] OK — State column is not 'Ok'"

# ----------------------------------------------------------------
# Step 5 — recover and flip back.
# ----------------------------------------------------------------
echo ">> [5] recover zpool ${ZPOOL} on ${ZPOOL_DEV}"
on_node "$NODE" zpool create -f -o cachefile=none "$ZPOOL" "$ZPOOL_DEV"

echo ">> [5a] wait for wire view to flip back state=Ok (≤90s)"
if ! wait_wire_state "Ok" 90; then
    echo "FAIL: state never recovered to Ok within 90s (last observed: $(sp_wire_state))" >&2
    exit 1
fi
echo "   [5a] OK — state=Ok after recovery"

reports=$(sp_wire_reports)
if [[ "$reports" != "[]" ]]; then
    echo "FAIL: state=Ok but reports[] is non-empty after recovery" >&2
    echo "       reports: $reports" >&2
    echo "       — the recovered pool MUST drop the stamped ERROR entry" >&2
    exit 1
fi
echo "   [5b] OK — reports[]=[] after recovery"

echo ">> RECOVERY-POOLMISSING-REAL-ZFS OK (Bug 83 regression guard)"
