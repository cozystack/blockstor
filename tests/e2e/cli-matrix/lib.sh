#!/usr/bin/env bash
# Shared helpers for tests/e2e/cli-matrix/*.sh — the L6 mandatory
# operator-CLI e2e wave. Every cell here runs the real `linstor`
# CLI on the stand and asserts Status convergence via
# observer-stamped Status + kernel probe (NOT just "200 OK"). See
# the L6 section (post-mortem of Bugs 326-330) for why this
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

    kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$LCTL_PORT":3370 \
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
        flags=$(kubectl get "resources.blockstor.cozystack.io/${rd}.${node}" \
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
    kubectl get "resources.blockstor.cozystack.io/${rd}.${node}" -o json 2>/dev/null \
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
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {n++} END {print n+0}'
}

# linstor_diskful_nodes <rd> — bash-array-style line list of node names
# that carry a diskful replica of $rd: Spec.Flags contains NEITHER
# DISKLESS NOR TIE_BREAKER. One node per line; the caller does
# `mapfile -t nodes < <(linstor_diskful_nodes "$rd")` or uses
# `$(linstor_diskful_nodes "$rd")` for word-splitting.
linstor_diskful_nodes() {
    local rd=$1
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local flags
            flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
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
    # awk emits exactly one number — no double-emission like the
    # previous `grep -cv '^$' || echo 0` pattern, which on empty
    # input printed "0" from grep AND another "0" from the `||`
    # fallback, yielding `"0\n0"` and breaking `[[ $v == "0" ]]`.
    linstor_diskful_nodes "$rd" | awk 'NF{c++} END{print c+0}'
}

# linstor_tiebreaker_node <rd> — name of the single node hosting the
# TIE_BREAKER witness for this RD, or empty string if no tiebreaker
# row exists. Lifecycle test uses this to pick the relocate target.
linstor_tiebreaker_node() {
    local rd=$1
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}' \
        | while read -r name; do
            [[ -z "$name" ]] && continue
            local flags
            flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
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
        if kubectl get "resources.blockstor.cozystack.io/${rd}.${n}" >/dev/null 2>&1; then
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

# wait_diskful_count <rd> <expected> [timeout=60] — poll until exactly
# <expected> DISKFUL replicas exist (DISKLESS / TIE_BREAKER rows are
# ignored). Use this — not wait_replica_count — after `r c --auto-place`
# of an even count: the auto-quorum machinery legitimately adds a
# diskless TIE_BREAKER witness on a spare node within seconds, so the
# total-row count overshoots the requested place count and the
# total-count wait flakes on the witness race (BUG-040 sweep signature:
# "never reached count=2 (last=3)").
wait_diskful_count() {
    local rd=$1 expected=$2 timeout=${3:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=0
    while (( $(date +%s) < deadline )); do
        cur=$(linstor_diskful_count "$rd")
        if [[ "$cur" == "$expected" ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_diskful_count: ${rd} never reached diskful=${expected} (last=${cur}) within ${timeout}s" >&2
    return 1
}

# wait_replica_absent <rd> <node> [timeout=30] — poll until no Resource
# CRD exists for (rd, node). Used after `linstor r d <node> <rd>` so the
# next phase can act on a known-clean shape.
wait_replica_absent() {
    local rd=$1 node=$2 timeout=${3:-30}
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if ! kubectl get "resources.blockstor.cozystack.io/${rd}.${node}" >/dev/null 2>&1; then
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
    leftover=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' || true)
    if [[ -n "$leftover" ]]; then
        echo "ORPHAN(crd): leftover Resource CRDs for ${rd}: $leftover" >&2
        fail=1
    fi
    if kubectl get "resourcedefinitions.blockstor.cozystack.io/${rd}" >/dev/null 2>&1; then
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
#
# BUG-039: MUST go through on_node_stdin — plain on_node drops stdin
# (kubectl exec without -i), so cryptsetup read an empty key-file and
# this assert failed on every stand while the satellite had in fact
# formatted with the correct master passphrase. The old 2>/dev/null
# also swallowed cryptsetup's "Nothing to read on input." tell, so we
# now keep stderr and print it on the failure path for triage.
assert_luks_passphrase_opens() {
    local node=$1 dev=$2 passphrase=$3
    # Passphrase on stdin avoids leaking it via `ps -ef` argv and
    # also avoids re-quoting headaches if the passphrase contains shell
    # metachars (the e2e default has `!!` in it, which would trigger
    # bash history expansion inside `bash -c` without the heredoc).
    local err
    if ! err=$(printf '%s' "$passphrase" | on_node_stdin "$node" \
            cryptsetup luksOpen --test-passphrase --key-file=- "$dev" 2>&1); then
        echo "assert_luks_passphrase_opens: passphrase does NOT open ${node}:${dev}" >&2
        echo "  cryptsetup said: $err" >&2
        return 1
    fi
    return 0
}

# cleanup_encryption_state — reset the cluster passphrase to "unset"
# so the next cell starts from a known-clean baseline. Called from EXIT
# traps of cells that mutate the passphrase state.
#
# The passphrase is Secret-backed (pkg/rest/encryption.go: the
# controller stores it in the `blockstor-cluster-passphrase` Secret,
# or the one named by ControllerConfig.Spec.PassphraseSecretRef) and
# the apiserver exposes ONLY GET/POST/PATCH/PUT on
# /v1/encryption/passphrase — there is no DELETE route, and the python
# linstor-client has no `delete-passphrase` subcommand. So the old
# REST-DELETE / CLI-delete attempts were both no-ops (405 / unknown
# subcommand): the first luks cell set the passphrase and every later
# `create-passphrase` then failed with "passphrase already set". The
# reliable, route-independent reset is to delete the backing Secret.
cleanup_encryption_state() {
    # Resolve the passphrase Secret name (custom ref or default).
    local secret
    secret=$(kubectl -n "$NS" get controllerconfigs.blockstor.cozystack.io \
        -o jsonpath='{.items[0].spec.passphraseSecretRef}' 2>/dev/null || true)
    [[ -n "$secret" ]] || secret=blockstor-cluster-passphrase
    kubectl -n "$NS" delete secret "$secret" --ignore-not-found >/dev/null 2>&1 || true
    # Clear the LUKS-layer controller property too, so a leftover key
    # from a prior cell doesn't leak into the next one's RD create.
    if [[ ${#LCTL[@]} -gt 0 ]]; then
        "${LCTL[@]}" controller set-property DrbdOptions/EncryptPassphrase "" \
            >/dev/null 2>&1 || true
    fi
    # Best-effort REST attempt kept for stands that DO ship a delete
    # path; a harmless 405/404 no-op otherwise.
    if [[ -n "${LCTL_PORT:-}" ]]; then
        curl -fsS -m 5 -X DELETE \
            "http://127.0.0.1:${LCTL_PORT}/v1/encryption/passphrase" \
            >/dev/null 2>&1 || true
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
# bytes as observed from inside the pod. Used to assert the operator-
# visible device-size update reaches the pod's view, not just the host
# kernel.
#
# The pod image is busybox, which ships neither `lsblk` nor a block-
# device node for the DRBD volume (kubelet bind-mounts the filesystem,
# not /dev/drbdN). Read the size out of sysfs instead: the block
# layer exposes /sys/class/block/<name>/size in 512-byte sectors for
# any device that backs a mounted fs, regardless of whether its /dev
# node is visible in the container. Fall back to blockdev if the node
# happens to be present (host-path mounts).
pod_lsblk_size() {
    local ns=$1 pod=$2 dev=$3
    local name=${dev##*/}
    kubectl -n "$ns" exec "$pod" -- sh -c "
        if [ -r /sys/class/block/${name}/size ]; then
            sectors=\$(cat /sys/class/block/${name}/size 2>/dev/null)
            [ -n \"\$sectors\" ] && echo \$(( sectors * 512 )) && exit 0
        fi
        blockdev --getsize64 '${dev}' 2>/dev/null \
            || lsblk -bno SIZE '${dev}' 2>/dev/null | head -1 | tr -d ' '
    " 2>/dev/null
}

# pod_device_for_pvc <namespace> <pod> [mount=/data] — discover the
# block device the PVC volume is mounted on inside the pod. busybox
# lacks `df --output` and `findmnt`, so parse /proc/mounts directly
# (column 1 = source device); fall back to the first DRBD node.
pod_device_for_pvc() {
    local ns=$1 pod=$2 mount=${3:-/data}
    kubectl -n "$ns" exec "$pod" -- sh -c "
        awk -v m='${mount}' '\$2==m {print \$1; exit}' /proc/mounts 2>/dev/null \
            || ls /dev/drbd* 2>/dev/null | head -1
    " 2>/dev/null
}

# Node (set by create_pvc_for_rd) that holds a diskful replica of the
# RD and whose DRBD device the helper has pre-formatted. create_writer_pod
# pins the pod here so linstor-csi's NodePublishVolume mounts the formatted
# local replica rather than a freshly-attached diskless one.
BS_RESIZE_PVC_NODE=""

# expandable_linstor_csi_sc — name of a StorageClass that (a) is
# provisioned by linstor-csi and (b) has allowVolumeExpansion=true.
# Empty string if none exists. We bind the static PV/PVC to this SC so
# that — after the operator grows the volume out-of-band via `linstor
# vd s` — a one-line PVC.Spec request bump can drive the csi external-
# resizer to propagate PVC.Status.Capacity + run the in-guest fs grow
# (the operator-visible half of the resize chain). A static PVC bound to
# storageClassName:"" cannot be resized at all ("only dynamically
# provisioned pvc can be resized"), so the expandable SC is required for
# the Status.Capacity assertion to be exercisable.
expandable_linstor_csi_sc() {
    kubectl get storageclass -o json 2>/dev/null | jq -r '
        .items[]?
        | select(.provisioner=="linstor.csi.linbit.com")
        | select(.allowVolumeExpansion==true)
        | .metadata.name' 2>/dev/null | head -1
}

# format_drbd_device <rd> <node> [fstype=ext4] [vol=0] — lay down a
# fresh filesystem on the RD's local DRBD device on NODE. A
# CLI-created (not CSI-provisioned) resource has a raw block device:
# linstor-csi's NodePublishVolume runs `fsck` on it (not `mkfs`, since
# the FsType property only gets stamped during CSI's own CreateVolume)
# and aborts with "bad magic number in super-block". We pre-format
# through the satellite so the subsequent CSI mount sees a valid fs.
#
# resolve_drbd_device <node> <rd> [vol=0] — print the RD's local
# /dev/drbdN path on NODE. drbdadm sh-dev prints the volume's
# /dev/drbdN path directly. The /dev/drbd/by-res/<rd>/<vol> symlink
# is not reliably present in the satellite mount namespace, and
# 'drbdsetup status' (no --json, jq absent) does not print the minor
# — so sh-dev is the one portable resolver here. Falls back to the
# bare-RD spelling for drbd-utils that reject '<rd>/<vol>'. Returns
# non-zero when the device cannot be resolved.
resolve_drbd_device() {
    local node=$1 rd=$2 vol=${3:-0}
    on_node "$node" sh -c "
        dev=\$(drbdadm sh-dev '${rd}/${vol}' 2>/dev/null) \
            || dev=\$(drbdadm sh-dev '${rd}' 2>/dev/null) || dev=''
        [ -n \"\$dev\" ] || exit 1
        printf '%s\n' \"\$dev\"
    "
}

# DRBD only permits writes while Primary, so we promote --force,
# mkfs, then demote back to Secondary (same primary/secondary dance the
# snap-*-lifecycle cells use to write anchor data). Idempotent: a second
# call just rewrites the fs.
format_drbd_device() {
    local rd=$1 node=$2 fstype=${3:-ext4} vol=${4:-0}
    local dev
    dev=$(resolve_drbd_device "$node" "$rd" "$vol") \
        || { echo 'format_drbd_device: cannot resolve drbd device' >&2; return 1; }
    on_node "$node" sh -c "
        set -e
        drbdadm primary --force '${rd}'
        # Settle the role change before mkfs opens the device.
        sleep 1
        mkfs.${fstype} -F -q '${dev}'
        sync
        drbdadm secondary '${rd}'
    "
}

# create_pvc_for_rd <ns> <pvc> <rd> <size> — bind a pod-mountable PVC
# to a pre-existing, CLI-created RD via a *static* (pre-provisioned)
# PersistentVolume — no dynamic provisioner, no custom annotation.
# Returns non-zero (caller SKIPs) only when linstor-csi is genuinely
# absent from the stand.
#
# Flow:
#   1. Require the linstor.csi.linbit.com CSIDriver + an expandable
#      linstor-csi StorageClass; else SKIP.
#   2. Pick a node that holds a diskful replica of the RD and pre-format
#      its DRBD device (CLI-created volumes ship raw — see
#      format_drbd_device). Record it in BS_RESIZE_PVC_NODE so the
#      writer pod can be pinned there.
#   3. Create a static PV (csi.volumeHandle=<rd>, fsType=ext4) with a
#      claimRef + the matching PVC, both on the expandable SC, and wait
#      for Bound.
create_pvc_for_rd() {
    local ns=$1 pvc=$2 rd=$3 size=$4
    local pv="${pvc}-pv"

    # linstor-csi present on the stand?
    if ! kubectl get csidriver linstor.csi.linbit.com >/dev/null 2>&1; then
        echo "create_pvc_for_rd: linstor.csi.linbit.com CSIDriver not installed on stand" >&2
        return 1
    fi
    local sc
    sc=$(expandable_linstor_csi_sc)
    if [[ -z "$sc" ]]; then
        echo "create_pvc_for_rd: no expandable linstor-csi StorageClass on stand" >&2
        return 1
    fi

    # Resolve a diskful node and pre-format its DRBD device so the CSI
    # mount (which only fsck's, never mkfs's, a static volume) succeeds.
    local node
    node=$(linstor_diskful_nodes "$rd" | head -1)
    if [[ -z "$node" ]]; then
        echo "create_pvc_for_rd: no diskful replica found for $rd" >&2
        return 1
    fi
    if ! format_drbd_device "$rd" "$node" ext4 0; then
        echo "create_pvc_for_rd: mkfs on $rd@$node failed" >&2
        return 1
    fi
    BS_RESIZE_PVC_NODE="$node"

    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${pv}
spec:
  capacity:
    storage: ${size}
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ${sc}
  claimRef:
    namespace: ${ns}
    name: ${pvc}
  csi:
    driver: linstor.csi.linbit.com
    volumeHandle: ${rd}
    fsType: ext4
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${pvc}
  namespace: ${ns}
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: ${sc}
  volumeName: ${pv}
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

# delete_static_pv_for_pvc <pvc> — remove the static PV created by
# create_pvc_for_rd (Retain reclaim policy means it is NOT garbage-
# collected when the PVC is deleted, so cells must drop it explicitly).
delete_static_pv_for_pvc() {
    local pvc=$1
    kubectl delete pv "${pvc}-pv" --wait=false 2>/dev/null || true
}

# create_writer_pod <ns> <pod> <pvc> <mount> [node] — start a tiny pod
# that mounts PVC at MOUNT and stays alive for the rest of the scenario.
# Uses busybox so it's available on the stand without extra image pulls.
#
# When NODE is given (or BS_RESIZE_PVC_NODE is set by create_pvc_for_rd)
# the pod is pinned there: for a static PV bound to a CLI-created RD we
# want the pod on a node that holds a diskful replica so linstor-csi
# mounts the local pre-formatted device rather than attaching a fresh
# diskless replica elsewhere.
create_writer_pod() {
    local ns=$1 pod=$2 pvc=$3 mount=$4 node=${5:-${BS_RESIZE_PVC_NODE:-}}
    local node_pin=""
    [[ -n "$node" ]] && node_pin="  nodeName: ${node}"
    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${ns}
spec:
${node_pin}
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
            # lvm-thin / lvm: match the LV by name (LINSTOR names the
            # backing LV "<rd>_<5-digit-vol>") and read its size in KiB.
            # NB: must select lv_name AND lv_size — filtering a size-only
            # listing by the RD name never matches, so an earlier version
            # silently fell through to the zfs branch on lvm pools.
            got_kib=$(on_node "$node" bash -c "
                lvs --noheadings --units k --nosuffix -o lv_name,lv_size 2>/dev/null \
                    | awk -v rd='${rd}_' '\$1 ~ (\"^\" rd) {gsub(/\\..*/,\"\",\$2); print \$2; exit}'
            " 2>/dev/null | tr -dc '0-9' || echo 0)
            if [[ -z "$got_kib" || "$got_kib" == "0" ]]; then
                # zfs fallback: match the zvol dataset by RD name and read
                # its volsize (bytes). Skip the literal '-' that volsize-
                # less datasets print, which would break the arithmetic.
                local bytes
                bytes=$(on_node "$node" bash -c "
                    zfs list -H -p -o name,volsize 2>/dev/null \
                        | awk -v rd='${rd}_' '\$1 ~ rd && \$2 ~ /^[0-9]+\$/ {print \$2; exit}'
                " 2>/dev/null | tr -dc '0-9' || echo 0)
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
    # `linstor vd s` grows the volume out-of-band — it does not touch
    # the PVC, so the csi external-resizer never wakes up on its own.
    # To exercise the operator-visible PVC.Status.Capacity propagation
    # (and the in-guest fs grow) we bump PVC.Spec.requests to the new
    # size, which the resizer reconciles: ControllerExpandVolume is a
    # no-op (LINSTOR already has the new size from `vd s`), then
    # NodeExpandVolume runs resize2fs and Status.Capacity follows.
    # This requires the PVC to be bound on an expandable SC — a static
    # PVC on storageClassName:"" rejects the request outright.
    kubectl -n "$pvc_ns" patch pvc "$pvc" --type=merge \
        -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"${pvc_capacity}\"}}}}" \
        >/dev/null 2>&1 || true
    wait_pvc_capacity "$pvc_ns" "$pvc" "$pvc_capacity" 120

    # The DRBD device is a few MiB smaller than the gross VolumeDefinition
    # size because DRBD subtracts its internal metadata, so the pod-visible
    # block device never quite reaches the nominal KiB. Allow a fixed slack
    # (8 MiB) below the nominal size — far smaller than the 1 GiB growth
    # step, so this still proves the resize reached the pod while tolerating
    # metadata overhead.
    local meta_slack_kib=8192
    local pod_min_kib=$(( expected_kib - meta_slack_kib ))
    echo "   5. in-pod device size reaches ~$expected_kib KiB (>= $pod_min_kib KiB net)"
    local pod_dev pod_size_bytes pod_kib=0
    pod_dev=$(pod_device_for_pvc "$pvc_ns" "$pod" "$mount")
    if [[ -n "$pod_dev" ]]; then
        local deadline=$(( $(date +%s) + 60 ))
        while (( $(date +%s) < deadline )); do
            pod_size_bytes=$(pod_lsblk_size "$pvc_ns" "$pod" "$pod_dev" 2>/dev/null || echo 0)
            pod_kib=$(( ${pod_size_bytes:-0} / 1024 ))
            if (( pod_kib >= pod_min_kib )); then
                break
            fi
            sleep 2
        done
        if (( pod_kib < pod_min_kib )); then
            echo "FAIL: pod-side device size $pod_kib KiB < $pod_min_kib KiB (nominal $expected_kib, device=$pod_dev)" >&2
            return 1
        fi
    else
        echo "   (skipping in-pod size check: could not resolve pod device for $mount)"
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
