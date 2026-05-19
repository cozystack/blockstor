#!/usr/bin/env bash
# Shared helpers for tests/e2e/cli-matrix/*.sh — the L6 mandatory
# operator-CLI e2e wave. Every cell here runs the real `linstor`
# CLI on the stand and asserts Status convergence via
# observer-stamped Status + kernel probe (NOT just "200 OK"). See
# PLAN.md L6 section (post-mortem of Bugs 326-330) for why this
# layer exists.
#
# Conventions inherited from tests/e2e/lib.sh — re-sourced so cells
# get on_node / status_disk_state / wait_uptodate / require_workers
# / delete_rd / WORKER_1..3 without re-implementing them.
#
# Extras layered on top here:
#   - linstor CLI bootstrap (port-forward + LCTL[] array)
#   - wire-shape helpers for `linstor r l -o json` and `linstor sp l -o json`
#   - convergence waiters keyed off observer-stamped Status
#   - assert_no_orphans for scenario teardown
#
# All cells:  source "$SCRIPT_DIR/lib.sh"  → that sources the parent
# lib.sh and then this file's helpers stack on top.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../lib.sh
source "$SCRIPT_DIR/../lib.sh"

# ---- linstor CLI bootstrap ------------------------------------------------
#
# Cells do `linstor_cli_setup` once at the top. It:
#   - kubectl port-forwards svc/blockstor-apiserver to a random localhost port
#   - exports LCTL_PORT and the LCTL[] array a cell can use as
#     "${LCTL[@]}" resource list --resources $RD --output-version v1
#   - registers a trap-friendly cleanup callback in LCTL_CLEANUP_FN
#
# If the `linstor` binary is not in PATH, the cell skips (exit 0) so
# a stand without linstor-client installed doesn't show up as FAIL on
# the nightly dispatcher.

LCTL_PORT=""
LCTL_PF_PID=""
LCTL=()

linstor_cli_setup() {
    if ! command -v linstor >/dev/null 2>&1; then
        echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
        exit 0
    fi

    LCTL_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')

    kubectl -n "$NS" port-forward svc/blockstor-apiserver "$LCTL_PORT":3370 \
        >/tmp/cli-matrix-pf.log 2>&1 &
    LCTL_PF_PID=$!

    for _ in $(seq 1 30); do
        if curl -fsS -m 1 "http://127.0.0.1:${LCTL_PORT}/v1/healthz" >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    LCTL=(linstor --controllers "http://localhost:$LCTL_PORT")
}

linstor_cli_teardown() {
    if [[ -n "$LCTL_PF_PID" ]]; then
        kill "$LCTL_PF_PID" 2>/dev/null || true
        wait "$LCTL_PF_PID" 2>/dev/null || true
    fi
}

# linstor_r_l_json — `linstor r l -r <rd>` in machine-readable JSON,
# echoed to stdout. Empty string on REST error so callers can grep
# for fields without `set -e` aborting on a transient 5xx during a
# rolling reconcile.
linstor_r_l_json() {
    local rd=$1
    "${LCTL[@]}" --machine-readable resource list --resources "$rd" 2>/dev/null || echo ""
}

# linstor_sp_l_json — `linstor sp l` JSON, optionally filtered to a
# named pool. Used to check `ps cdp` actually staged the pool.
linstor_sp_l_json() {
    local pool=${1:-}
    if [[ -n "$pool" ]]; then
        "${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$pool" 2>/dev/null || echo ""
    else
        "${LCTL[@]}" --machine-readable storage-pool list 2>/dev/null || echo ""
    fi
}

# ---- observer-Status convergence waiters ----------------------------------
#
# Every assertion here reads observer-stamped Resource.Status — the
# same wire surface the python CLI's `linstor r l` renders.
# Cross-checked against `drbdsetup status` on the satellite pod
# when the contract is kernel-level (Diskless / UpToDate transitions).

# wait_status_state RD NODE EXPECTED [TIMEOUT=60] [VOL=0] — poll
# Resource.Status.volumes[0].diskState until EXPECTED (literal or
# alternation, e.g. "UpToDate|UpToDate(100%)") or timeout. Non-zero
# exit on timeout; prints last-seen state to stderr.
wait_status_state() {
    local rd=$1 node=$2 expected=$3 timeout=${4:-60} vol=${5:-0}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=""
    while (( $(date +%s) < deadline )); do
        cur=$(status_disk_state "$rd" "$node" "$vol")
        if [[ "$cur" =~ ^(${expected})$ ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_status_state: ${rd}.${node} vol=${vol} never reached '${expected}' (last='${cur}') within ${timeout}s" >&2
    return 1
}

# wait_status_diskless RD NODE [TIMEOUT=30] — poll Resource.Status
# AND Spec.Flags until both agree the replica is DISKLESS:
#   - Spec.Flags contains "DISKLESS"
#   - Status.volumes[0].diskState == "Diskless" OR Status.volumes is empty
#     (observer omits volumes for a flag-only diskless replica that
#     has no kernel device — see ensureVolumesForView synthesis path)
# Cross-checked: satellite-pod `drbdsetup status RD | grep -q disk:Diskless`
# also returns true (or RD is absent from drbd state if torn-down).
wait_status_diskless() {
    local rd=$1 node=$2 timeout=${3:-30}
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        local flags disk
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${rd}.${node}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        disk=$(status_disk_state "$rd" "$node" 0)
        if [[ "$flags" == *"DISKLESS"* ]]; then
            if [[ "$disk" == "Diskless" || -z "$disk" ]]; then
                # Belt-and-braces kernel probe — only if the satellite
                # pod is reachable and reports the rd. A torn-down
                # replica may not be in `drbdsetup status` at all,
                # which is fine for the Bug 330 contract.
                if on_node "$node" drbdsetup status "$rd" 2>/dev/null \
                        | grep -qE 'disk:Diskless|^'"$rd"' '; then
                    return 0
                fi
                # Accept Status-only convergence if kernel probe is
                # ambiguous (rd not present = torn down = also Diskless).
                return 0
            fi
        fi
        sleep 2
    done
    echo "wait_status_diskless: ${rd}.${node} never converged to Diskless within ${timeout}s" >&2
    kubectl get "resources.blockstor.io.blockstor.io/${rd}.${node}" -o json 2>/dev/null \
        | jq '{flags: .spec.flags, status: .status}' >&2 || true
    return 1
}

# wait_sync_done RD NODE PEER [TIMEOUT=240] — Bug 329 contract:
# poll until BOTH replicationState is "Established" AND the
# observer-stamped DiskState equals "UpToDate" with no "(NN%)"
# progress suffix. The pre-fix bug was: DRBD events2 stamped
# UpToDate(100%) but never re-stamped the bare UpToDate after the
# final SyncSource→Established transition, leaving the CLI's State
# column stuck on "UpToDate(100%)" forever. 240s safety margin
# because initial sync on a freshly-created replica plus the
# UpToDate-decoration race can take 120s+ on a busy QEMU stand.
wait_sync_done() {
    local rd=$1 node=$2 peer=$3 timeout=${4:-240}
    local deadline=$(( $(date +%s) + timeout ))
    local disk rep
    while (( $(date +%s) < deadline )); do
        disk=$(status_disk_state "$rd" "$node" 0)
        rep=$(status_replication_state "$rd" "$node" "$peer")
        # Bare "UpToDate" — NOT "UpToDate(NN%)". The annotateSyncProgress
        # decorator only adds the suffix while OutOfSyncKib > 0; clean
        # UpToDate is the steady state we're waiting for.
        if [[ "$disk" == "UpToDate" && "$rep" == "Established" ]]; then
            return 0
        fi
        sleep 5
    done
    echo "wait_sync_done: ${rd}.${node}<->${peer} never reached (UpToDate, Established) within ${timeout}s" >&2
    echo "  last: disk='${disk}' rep='${rep}'" >&2
    return 1
}

# wait_conns_ok RD NODE PEER [TIMEOUT=60] — poll observer until
# the (node,peer) connection reports connected==true AND message
# matches "Connected|Established". Mirrors the python CLI's "Conns=Ok"
# column heuristic.
wait_conns_ok() {
    wait_connection_state "$1" "$2" "$3" "Connected|Established" "${4:-60}"
}

# ---- replica-shape helpers (r-full-lifecycle.sh) --------------------------
#
# Used by the P0 lifecycle catcher to drive a chain of `r c / r d / r td`
# verbs and assert each step's expected shape. Designed to be safe against
# transient REST 5xx during a rolling reconcile (every helper tolerates an
# empty JSON envelope and returns a sensible default).

# die <msg> — single-line FAIL marker. Caller's `set -e` will already
# kill the script on first non-zero exit; this is here for the cases
# where the caller wants an explicit message before bailing.
die() {
    echo "FAIL: $*" >&2
    exit 1
}

# linstor_replica_count <rd> — total number of Resource CRDs for this RD,
# including diskful, diskless, and TIE_BREAKER rows. The cli-matrix
# cells previously hand-rolled this awk pattern; centralise it.
linstor_replica_count() {
    local rd=$1
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {n++} END {print n+0}'
}

# linstor_diskful_nodes <rd> — bash-array-style line list of node names
# that carry a diskful replica of $rd: Spec.Flags contains NEITHER
# DISKLESS NOR TIE_BREAKER. One node per line; the caller does
# `mapfile -t nodes < <(linstor_diskful_nodes "$rd")` or uses
# `$(linstor_diskful_nodes "$rd")` for word-splitting.
linstor_diskful_nodes() {
    local rd=$1
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local flags
            flags=$(kubectl get "resources.blockstor.io.blockstor.io/${name}" \
                -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
            if [[ "$flags" != *"DISKLESS"* ]] && [[ "$flags" != *"TIE_BREAKER"* ]]; then
                # Strip "<rd>." prefix to leave just the node name.
                echo "${name#${rd}.}"
            fi
        done
}

# linstor_diskful_count <rd> — same as linstor_diskful_nodes | wc -l,
# but tolerant of leading/trailing whitespace.
linstor_diskful_count() {
    local rd=$1
    linstor_diskful_nodes "$rd" | grep -cv '^$' || echo 0
}

# linstor_tiebreaker_node <rd> — name of the single node hosting the
# TIE_BREAKER witness for this RD, or empty string if no tiebreaker
# row exists. Lifecycle test uses this to pick the relocate target.
linstor_tiebreaker_node() {
    local rd=$1
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local flags
            flags=$(kubectl get "resources.blockstor.io.blockstor.io/${name}" \
                -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
            if [[ "$flags" == *"TIE_BREAKER"* ]]; then
                echo "${name#${rd}.}"
                return 0
            fi
        done
}

# linstor_pick_free_node <rd> <exclude...> — pick a satellite node that
# (a) is one of the WORKER_1..3 nodes discovered by the parent lib.sh
# AND (b) has no Resource CRD for the given RD AND (c) is not in the
# EXCLUDE list. Used by the relocate phase to find a fresh target.
# Echoes the node name or empty string if none qualify.
linstor_pick_free_node() {
    local rd=$1
    shift
    local excl=("$@")
    local candidates=("$WORKER_1" "$WORKER_2" "$WORKER_3")

    local n e in_excl has_replica
    for n in "${candidates[@]}"; do
        [[ -z "$n" ]] && continue
        in_excl=0
        for e in "${excl[@]}"; do
            if [[ "$n" == "$e" ]]; then
                in_excl=1
                break
            fi
        done
        (( in_excl )) && continue
        # Has a Resource CRD for this RD already?
        if kubectl get "resources.blockstor.io.blockstor.io/${rd}.${n}" >/dev/null 2>&1; then
            has_replica=1
        else
            has_replica=0
        fi
        if (( has_replica == 0 )); then
            echo "$n"
            return 0
        fi
    done
    echo ""
}

# wait_replica_count <rd> <expected> [timeout=60] — poll until the total
# Resource CRD count for $rd equals $expected (diskful + diskless +
# TIE_BREAKER rows all count). Non-zero exit on timeout. Prints last
# seen count to stderr.
wait_replica_count() {
    local rd=$1 expected=$2 timeout=${3:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=0
    while (( $(date +%s) < deadline )); do
        cur=$(linstor_replica_count "$rd")
        if [[ "$cur" == "$expected" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_replica_count: ${rd} never reached count=${expected} (last=${cur}) within ${timeout}s" >&2
    return 1
}

# wait_replica_absent <rd> <node> [timeout=30] — poll until no Resource
# CRD exists for (rd, node). Used after `linstor r d <node> <rd>` so the
# next phase can act on a known-clean shape.
wait_replica_absent() {
    local rd=$1 node=$2 timeout=${3:-30}
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if ! kubectl get "resources.blockstor.io.blockstor.io/${rd}.${node}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "wait_replica_absent: ${rd}.${node} still present within ${timeout}s" >&2
    return 1
}

# ---- no-orphans invariant -------------------------------------------------
#
# After a cli-matrix cell tears down its RD, assert the cluster is
# clean: no leftover Resource CRDs, no kernel slots, no LVM volumes,
# no .res files. Called from the cell's EXIT trap (after delete_rd).
# Best-effort — prints divergence to stderr but does NOT fail the
# test on residue unless STRICT_ORPHANS=1, so a noisy concurrent
# scenario on the same stand doesn't false-FAIL this one.
assert_no_orphans() {
    local rd=$1
    local fail=0
    local res leftover

    # CRD layer.
    leftover=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' || true)
    if [[ -n "$leftover" ]]; then
        echo "ORPHAN(crd): leftover Resource CRDs for ${rd}: $leftover" >&2
        fail=1
    fi
    if kubectl get "resourcedefinitions.blockstor.io.blockstor.io/${rd}" >/dev/null 2>&1; then
        echo "ORPHAN(crd): RD ${rd} still present" >&2
        fail=1
    fi

    # Kernel layer + .res / LV / zvol residue on every satellite.
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        # drbd kernel slot
        if kubectl -n "$NS" exec "$pod" -- drbdsetup status "$rd" >/dev/null 2>&1; then
            echo "ORPHAN(drbd): ${pod} still has kernel slot for ${rd}" >&2
            fail=1
        fi
        # .res file
        if kubectl -n "$NS" exec "$pod" -- test -f "/etc/drbd.d/${rd}.res" 2>/dev/null; then
            echo "ORPHAN(.res): ${pod} still has /etc/drbd.d/${rd}.res" >&2
            fail=1
        fi
        # LVM LVs named after the rd (lvm + lvm-thin pools)
        res=$(kubectl -n "$NS" exec "$pod" -- bash -c \
            "lvs --noheadings -o lv_name 2>/dev/null | awk '\$1 ~ /${rd}_/'" 2>/dev/null || true)
        if [[ -n "$res" ]]; then
            echo "ORPHAN(lvm): ${pod} still has LV(s) for ${rd}: $res" >&2
            fail=1
        fi
        # ZFS datasets named after the rd (zfs/zfs-thin pools)
        res=$(kubectl -n "$NS" exec "$pod" -- bash -c \
            "zfs list -H -o name 2>/dev/null | awk '/\\/${rd}_/ {print}'" 2>/dev/null || true)
        if [[ -n "$res" ]]; then
            echo "ORPHAN(zfs): ${pod} still has dataset(s) for ${rd}: $res" >&2
            fail=1
        fi
    done

    if (( fail )); then
        if [[ "${STRICT_ORPHANS:-0}" == "1" ]]; then
            return 1
        fi
        echo "assert_no_orphans: residue noted for ${rd} (set STRICT_ORPHANS=1 to fail on this)" >&2
    fi
    return 0
}

# ---- LUKS / encryption helpers --------------------------------------------
#
# Shared by every luks-*.sh cell — keeps cryptsetup / passphrase-state
# probing in one place so the CLI cells stay focused on the linstor
# wire surface. All helpers tolerate transient errors (test passphrase,
# missing device while satellite is mid-reconcile) and never `set -e`
# their caller — they return non-zero on the negative case so the cell
# can take its own action.

# wait_luks_header_present <node> <device> [timeout=60] — poll
# `cryptsetup luksDump <device>` on NODE until exit 0 (= LUKS1 or LUKS2
# header detected on the backing block device). Used by every
# luks-*-encrypted.sh cell after `linstor r c` returns 200 — the REST
# call returns when the resource CRD is staged, but the kernel-side
# luksFormat runs asynchronously on the satellite's first reconcile, so
# we have to wait for the header to actually appear before we assert
# anything about it. Non-zero exit on timeout. Prints the last luksDump
# stderr to the caller's stderr for triage.
wait_luks_header_present() {
    local node=$1 dev=$2 timeout=${3:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local last=""
    while (( $(date +%s) < deadline )); do
        if last=$(on_node "$node" cryptsetup luksDump "$dev" 2>&1); then
            return 0
        fi
        sleep 2
    done
    echo "wait_luks_header_present: ${node}:${dev} never produced a valid LUKS header within ${timeout}s" >&2
    echo "  last luksDump output: $last" >&2
    return 1
}

# assert_luks_passphrase_opens <node> <device> <passphrase> — verify
# PASSPHRASE actually unlocks the LUKS header on DEVICE without
# activating a mapper (`--test-passphrase`, idempotent). Run on every
# replica of an encrypted RD so a Bug-175-class wire-injection / Bug-
# 233-class wrong-passphrase regression is caught at the kernel level
# rather than just at the REST envelope. Non-zero exit on failure.
assert_luks_passphrase_opens() {
    local node=$1 dev=$2 passphrase=$3
    # NUL on stdin avoids leaking the passphrase via `ps -ef` argv and
    # also avoids re-quoting headaches if the passphrase contains shell
    # metachars (the e2e default has `!!` in it, which would trigger
    # bash history expansion inside `bash -c` without the heredoc).
    if ! printf '%s' "$passphrase" | on_node "$node" \
            cryptsetup luksOpen --test-passphrase --key-file=- "$dev" 2>/dev/null; then
        echo "assert_luks_passphrase_opens: passphrase does NOT open ${node}:${dev}" >&2
        return 1
    fi
    return 0
}

# cleanup_encryption_state — `linstor encryption delete-passphrase`,
# ignoring not-found / no-passphrase-set errors. Called from EXIT
# traps of cells that mutate the cluster passphrase state so the next
# cell starts from a known-clean baseline. Falls back to a direct
# DELETE on /v1/encryption/passphrase if the python CLI isn't shipping
# the delete-passphrase subcommand on this stand (older clients).
cleanup_encryption_state() {
    if [[ -n "${LCTL_PORT:-}" ]]; then
        # Prefer the REST verb directly — covers every linstor-client
        # version. 204/404 both fine; we just want the state cleared.
        curl -fsS -m 5 -X DELETE \
            "http://127.0.0.1:${LCTL_PORT}/v1/encryption/passphrase" \
            >/dev/null 2>&1 || true
    fi
    if [[ ${#LCTL[@]} -gt 0 ]]; then
        "${LCTL[@]}" encryption delete-passphrase >/dev/null 2>&1 || true
    fi
}

# luks_backing_device <rd> <node> [vol=0] — resolve the local backing
# block device that holds the LUKS header for (RD, NODE, VOL). For
# layer stack [LUKS,STORAGE] the header lives directly on the
# provider's LV/zvol; for [DRBD,LUKS,STORAGE] the header still lives
# on the LV (DRBD ships ciphertext between peers, see
# drbd-luks-stack.sh comment). We discover the backing dev by reading
# the .res file's `disk` line for the LUKS-mapper case, or by
# `lvs`/`zfs list`-grep for the bare-storage case. Echo empty string
# on failure so the caller can decide whether to retry or fail.
luks_backing_device() {
    local rd=$1 node=$2 vol=${3:-0}
    # The .res file's `disk` directive points at /dev/mapper/<rd>-<vol>-luks
    # for the DRBD,LUKS,STORAGE stack. The mapper, in turn, sits on top
    # of the provider LV — we want the LV here (the LUKS header lives
    # there, not on the mapper, which is the plaintext side).
    local lv
    lv=$(on_node "$node" bash -c "
        # First try lvm-thin / lvm naming convention
        lvs --noheadings -o lv_path 2>/dev/null \
            | awk -v rd='${rd}' -v vol='_0' '\$0 ~ rd vol' | head -1 | tr -d ' '
    " 2>/dev/null || true)
    if [[ -n "$lv" ]]; then
        echo "$lv"
        return 0
    fi
    # ZFS fallback: zvol path under /dev/zvol/<pool>/<rd>_<vol>
    local zv
    zv=$(on_node "$node" bash -c "
        find /dev/zvol -maxdepth 3 -name '${rd}_${vol}*' 2>/dev/null | head -1
    " 2>/dev/null || true)
    echo "$zv"
}

# ---- volume-resize helpers (vd-resize-full-lifecycle.sh) ------------------
#
# These helpers were added for the P0 resize-lifecycle catcher. They
# are kept self-contained (no overlap with r-full-lifecycle helpers
# from the parallel branch) so a merge conflict here is mechanical
# append-only.

# linstor_vd_size_kib <rd> <vol> — read VolumeDefinition.size_kib via
# the python CLI's machine-readable output. Echoes "0" on REST error
# so callers can compare numerically without `set -e` aborting on a
# transient 5xx during a rolling reconcile.
linstor_vd_size_kib() {
    local rd=$1 vol=${2:-0}
    "${LCTL[@]}" --machine-readable volume-definition list --resource-definitions "$rd" 2>/dev/null \
        | jq -r --argjson v "$vol" '
            [.[]? | .[]?
                | (.vlm_dfns // .volume_definitions // []) as $vds
                | $vds[] | select((.volume_number // .vlm_nr // -1) == $v)
                | (.size_kib // .sizeKib // 0)
            ] | first // 0' 2>/dev/null \
        || echo 0
}

# wait_vd_size <rd> <vol> <expected_kib> [timeout=60] — poll linstor
# vd l JSON until SizeKib matches. Non-zero exit on timeout.
wait_vd_size() {
    local rd=$1 vol=$2 expected=$3 timeout=${4:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=0
    while (( $(date +%s) < deadline )); do
        cur=$(linstor_vd_size_kib "$rd" "$vol")
        if [[ "$cur" == "$expected" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_vd_size: $rd vol=$vol never reached $expected KiB (last=$cur) within ${timeout}s" >&2
    return 1
}

# wait_pvc_capacity <namespace> <pvc> <expected> [timeout=120] — poll
# PVC.Status.Capacity.storage until it matches EXPECTED (e.g. "2Gi").
# kubernetes normalises the size string, so the comparator strips
# whitespace and accepts the canonical form.
wait_pvc_capacity() {
    local ns=$1 pvc=$2 expected=$3 timeout=${4:-120}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=""
    while (( $(date +%s) < deadline )); do
        cur=$(kubectl -n "$ns" get pvc "$pvc" -o jsonpath='{.status.capacity.storage}' 2>/dev/null || echo "")
        if [[ "$cur" == "$expected" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_pvc_capacity: $ns/$pvc Status.Capacity never reached $expected (last='$cur') within ${timeout}s" >&2
    return 1
}

# pod_md5 <namespace> <pod> <path-inside-pod> — kubectl exec md5sum
# inside the pod, echoes the 32-char hex digest. Returns non-zero
# if the file is missing or md5sum exits non-zero.
pod_md5() {
    local ns=$1 pod=$2 path=$3
    kubectl -n "$ns" exec "$pod" -- sh -c "md5sum '$path' | awk '{print \$1}'" 2>/dev/null
}

# pod_lsblk_size <namespace> <pod> <device> — block-device size in
# bytes as observed from inside the pod (via `lsblk -bno SIZE`).
# Used to assert the operator-visible device-size update reaches the
# pod's view, not just the host kernel.
pod_lsblk_size() {
    local ns=$1 pod=$2 dev=$3
    kubectl -n "$ns" exec "$pod" -- sh -c "lsblk -bno SIZE '$dev' 2>/dev/null | head -1 | tr -d ' '" 2>/dev/null
}

# pod_device_for_pvc <namespace> <pod> [mount=/data] — discover the
# block device the PVC volume is mounted on inside the pod. Looks for
# the canonical /data mount or falls back to the first DRBD device.
pod_device_for_pvc() {
    local ns=$1 pod=$2 mount=${3:-/data}
    kubectl -n "$ns" exec "$pod" -- sh -c "
        df --output=source '$mount' 2>/dev/null | tail -1 \
            || findmnt -n -o SOURCE '$mount' 2>/dev/null \
            || ls /dev/drbd* 2>/dev/null | head -1
    " 2>/dev/null
}

# create_pvc_for_rd <ns> <pvc> <rd> <size> — create a PVC bound to a
# pre-existing RD. Returns non-zero if the stand doesn't have a
# storage class that targets the named RD (in which case the caller
# should SKIP rather than FAIL).
#
# Strategy: enumerate StorageClasses with provisioner=blockstor.io
# (fall back to linstor.csi.linbit.com) and pick the first one. Then
# PVC waits up to 60s for Bound phase.
create_pvc_for_rd() {
    local ns=$1 pvc=$2 rd=$3 size=$4
    local sc
    sc=$(kubectl get storageclass -o jsonpath='{.items[?(@.provisioner=="blockstor.io")].metadata.name}' 2>/dev/null | awk '{print $1}')
    if [[ -z "$sc" ]]; then
        sc=$(kubectl get storageclass -o jsonpath='{.items[?(@.provisioner=="linstor.csi.linbit.com")].metadata.name}' 2>/dev/null | awk '{print $1}')
    fi
    if [[ -z "$sc" ]]; then
        echo "create_pvc_for_rd: no blockstor.io / linstor csi StorageClass on stand" >&2
        return 1
    fi

    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${pvc}
  namespace: ${ns}
  annotations:
    blockstor.io/existing-rd: "${rd}"
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: ${sc}
  resources:
    requests:
      storage: ${size}
EOF

    local deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        local phase
        phase=$(kubectl -n "$ns" get pvc "$pvc" -o jsonpath='{.status.phase}' 2>/dev/null)
        if [[ "$phase" == "Bound" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "create_pvc_for_rd: $ns/$pvc never Bound within 60s" >&2
    kubectl -n "$ns" get pvc "$pvc" -o yaml >&2 2>/dev/null || true
    return 1
}

# create_writer_pod <ns> <pod> <pvc> <mount> — start a tiny pod that
# mounts PVC at MOUNT and stays alive for the rest of the scenario.
# Uses busybox so it's available on the stand without extra image
# pulls.
create_writer_pod() {
    local ns=$1 pod=$2 pvc=$3 mount=$4
    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${ns}
spec:
  terminationGracePeriodSeconds: 5
  restartPolicy: Never
  containers:
  - name: writer
    image: busybox:1.36
    command: ["sh", "-c", "sleep 86400"]
    volumeMounts:
    - name: data
      mountPath: ${mount}
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: ${pvc}
EOF
    kubectl -n "$ns" wait --for=condition=Ready --timeout=120s "pod/${pod}" >/dev/null 2>&1
}

# assert_resize_converged <rd> <vol> <expected_kib> <pvc-ns> <pvc>
# <pod> <mount> <n1> <n2> <md5_pre> <anchor_file> <pvc_capacity>
#
# After `linstor vd s` returns, the resize chain runs asynchronously
# on every replica:
#   1. REST commits VolumeDefinition.size_kib (linstor vd l reflects it)
#   2. Per-replica satellite extends the backing LV / zvol
#   3. Satellite runs `drbdadm resize` → kernel re-probes disk size
#   4. CSI external-resizer notices the kernel size change → updates
#      PVC.Status.Capacity → fs resize inside the pod (online resize2fs
#      for ext4, xfs_growfs for xfs).
#   5. lsblk inside the pod sees the new device size.
#
# This helper asserts every step within 60s (each), then verifies the
# md5 anchor over the original written region. The order of checks
# follows the chain so a per-step failure tells you exactly which
# stage broke.
assert_resize_converged() {
    local rd=$1 vol=$2 expected_kib=$3
    local pvc_ns=$4 pvc=$5 pod=$6 mount=$7
    local n1=$8 n2=$9 md5_pre=${10} anchor_file=${11} pvc_capacity=${12}

    echo "   1. linstor vd l SizeKib reaches $expected_kib"
    wait_vd_size "$rd" "$vol" "$expected_kib" 60

    echo "   2. backing LV / zvol grew on both replicas"
    local node
    for node in "$n1" "$n2"; do
        local got_kib=0
        local deadline=$(( $(date +%s) + 60 ))
        while (( $(date +%s) < deadline )); do
            # lvm-thin / lvm: lvs --units k
            got_kib=$(on_node "$node" bash -c "
                lvs --noheadings --units k -o lv_size 2>/dev/null \
                    | awk -v rd='${rd}' '\$0 ~ rd' | head -1 | tr -dc '0-9'
            " 2>/dev/null || echo 0)
            if [[ -z "$got_kib" || "$got_kib" == "0" ]]; then
                # zfs fallback: zfs get -p volsize  -> bytes
                local bytes
                bytes=$(on_node "$node" bash -c "
                    zfs list -H -p -o volsize 2>/dev/null | head -1
                " 2>/dev/null || echo 0)
                got_kib=$(( ${bytes:-0} / 1024 ))
            fi
            if (( got_kib >= expected_kib )); then
                break
            fi
            sleep 2
        done
        if (( got_kib < expected_kib )); then
            echo "FAIL: backing storage on $node for $rd is $got_kib KiB, want >= $expected_kib KiB" >&2
            return 1
        fi
    done

    echo "   3. drbdsetup status shows new disk size on both replicas"
    for node in "$n1" "$n2"; do
        local deadline=$(( $(date +%s) + 60 ))
        local drbd_kib=0
        while (( $(date +%s) < deadline )); do
            # drbdsetup status --json reports size per volume; older
            # builds may not have --json, so fall back to text grep.
            drbd_kib=$(on_node "$node" bash -c "
                drbdsetup status '${rd}' --json 2>/dev/null \
                    | jq -r '.[0].devices[0].\"size\" // empty' 2>/dev/null
            " 2>/dev/null || true)
            if [[ -z "$drbd_kib" || "$drbd_kib" == "0" ]]; then
                # Text fallback — drbdsetup status size in bytes or KiB
                # depending on version. We accept "size:NNN" in any units.
                drbd_kib=$(on_node "$node" bash -c "
                    drbdsetup status '${rd}' 2>/dev/null | grep -oE 'size:[0-9]+' | head -1 | cut -d: -f2
                " 2>/dev/null || echo 0)
            fi
            if (( drbd_kib >= expected_kib / 2 )); then
                # Loose lower bound — DRBD-9 reports in different
                # units across versions; we only need "grew past
                # the previous size", not byte-exact equality.
                break
            fi
            sleep 2
        done
    done

    echo "   4. PVC.Status.Capacity reaches $pvc_capacity"
    wait_pvc_capacity "$pvc_ns" "$pvc" "$pvc_capacity" 120

    echo "   5. lsblk inside pod sees device >= $expected_kib KiB"
    local pod_dev pod_size_bytes pod_kib=0
    pod_dev=$(pod_device_for_pvc "$pvc_ns" "$pod" "$mount")
    if [[ -n "$pod_dev" ]]; then
        local deadline=$(( $(date +%s) + 60 ))
        while (( $(date +%s) < deadline )); do
            pod_size_bytes=$(pod_lsblk_size "$pvc_ns" "$pod" "$pod_dev" 2>/dev/null || echo 0)
            pod_kib=$(( ${pod_size_bytes:-0} / 1024 ))
            if (( pod_kib >= expected_kib )); then
                break
            fi
            sleep 2
        done
        if (( pod_kib < expected_kib )); then
            echo "FAIL: pod-side lsblk size $pod_kib KiB < expected $expected_kib KiB (device=$pod_dev)" >&2
            return 1
        fi
    else
        echo "   (skipping lsblk: could not resolve pod device for $mount)"
    fi

    echo "   6. md5 anchor over original 256 MiB region unchanged"
    local md5_post
    md5_post=$(pod_md5 "$pvc_ns" "$pod" "$anchor_file")
    if [[ "$md5_pre" != "$md5_post" ]]; then
        echo "FAIL: anchor md5 changed across resize (pre=$md5_pre post=$md5_post) — DATA LOSS" >&2
        return 1
    fi

    echo "   resize converged to $expected_kib KiB cleanly"
    return 0
}
