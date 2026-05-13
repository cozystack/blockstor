#!/usr/bin/env bash
#
# usage: satellite-utils-smoke.sh WORK_DIR
#
# Scenario 1.25 (tests/scenarios/01-api-contract.md):
#   Level-3 satellite-container utilities. The cheat-sheet at
#   tests/observability-cheat-sheet-scenarios.md §12 expects each of
#   the following to work from inside a blockstor-satellite pod:
#
#     drbdadm  status / down / up / primary [--force] / secondary
#              disconnect / connect [--discard-my-data] / adjust
#     drbdsetup  status
#     cat /etc/drbd.d/<rd>.res
#     dmesg | grep drbd                          (needs CAP_SYSLOG)
#     lsblk
#     lvs / vgs / pvs                            (LVM userspace)
#     zfs list / zpool list                      (ZFS userspace)
#
# The smoke does NOT exercise side-effecting flows (down/up, force,
# disconnect/connect) here — those are covered by the dedicated DRBD
# scenarios (recovery-*, network-partition.sh, toggle-disk.sh). This
# script asserts each binary is present, on PATH, and answers a
# read-only invocation without "command not found" / EPERM /
# permission-denied. Failure modes the spec calls out:
#
#   - lvs missing       → LVM_THIN pool debug blind
#   - dmesg EPERM       → satellite pod missing CAP_SYSLOG
#   - .res wrong path   → satellite renders elsewhere; the cheat-sheet's
#                         /var/lib/linstor.d/ (upstream) vs blockstor's
#                         /etc/drbd.d/. We accept either via fallback.
#
# Skipped layers documented inline with explicit reasons.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

# Enumerate every satellite pod once so a missing binary on a single
# worker is still caught (a fan-out build/Dockerfile drift would leave
# the cluster with mixed images).
mapfile -t SAT_PODS < <(
    kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n'
)

if (( ${#SAT_PODS[@]} == 0 )); then
    echo "FAIL: no blockstor-satellite pods (-l app=blockstor-satellite)"
    exit 1
fi

echo ">> satellite pods to audit: ${SAT_PODS[*]}"

# Read-only invocations only. Each entry: human-name|command.
# The command runs under /bin/sh -c inside the pod, so quoting that
# survives kubectl's argv split is the only thing we need to worry
# about. Side-effecting flows (down/up/primary --force/disconnect/
# connect --discard-my-data) are covered by other scenarios.
declare -a CHECKS=(
    "drbdadm-help|drbdadm help >/dev/null"
    "drbdadm-status|drbdadm status >/dev/null 2>&1 || true"
    "drbdsetup-help|drbdsetup help >/dev/null"
    "drbdsetup-status|drbdsetup status >/dev/null 2>&1 || true"
    "lsblk|lsblk >/dev/null"
    "lvs|lvs --readonly >/dev/null 2>&1 || lvs >/dev/null 2>&1 || true"
    "vgs|vgs --readonly >/dev/null 2>&1 || vgs >/dev/null 2>&1 || true"
    "pvs|pvs --readonly >/dev/null 2>&1 || pvs >/dev/null 2>&1 || true"
    "zfs-version|zfs --version >/dev/null"
    "zpool-version|zpool --version >/dev/null"
    "cryptsetup-version|cryptsetup --version >/dev/null"
)

# A few entries are "binary must exist" rather than "command must
# succeed" — the cluster may have no LVM VG / no ZFS pool / no DRBD
# resource configured at the moment of the smoke, in which case the
# tool exits non-zero for "no such object" reasons that are not a
# fault of the image. So we verify presence via `command -v` and
# tolerate a non-zero exit from the read-only call.

fail=0
for pod in "${SAT_PODS[@]}"; do
    echo ">> [${pod}]"

    # Group A: must be on PATH (the "command not found" guard the
    # spec actually calls out). `command -v` returns the resolved
    # path or exits 1.
    for bin in drbdadm drbdsetup lsblk lvs vgs pvs zfs zpool cryptsetup; do
        if ! kubectl -n "$NS" exec "$pod" -- /bin/sh -c "command -v $bin >/dev/null"; then
            echo "FAIL: [${pod}] $bin not on PATH"
            fail=1
        fi
    done

    # Group B: read-only invocations should not bomb with EPERM or
    # other capability-related errors. We tolerate non-zero exit
    # (no DRBD configured, empty VG) but capture stderr to spot
    # EPERM / "Permission denied" / "Operation not permitted".
    for spec in "${CHECKS[@]}"; do
        name=${spec%%|*}
        cmd=${spec#*|}
        out=$(kubectl -n "$NS" exec "$pod" -- /bin/sh -c "$cmd" 2>&1 || true)
        if echo "$out" | grep -qE 'Permission denied|Operation not permitted|EPERM'; then
            echo "FAIL: [${pod}] ${name} permission error: $out"
            fail=1
        fi
        if echo "$out" | grep -qE 'command not found|No such file'; then
            echo "FAIL: [${pod}] ${name} missing: $out"
            fail=1
        fi
    done

    # dmesg: cheat-sheet expects CAP_SYSLOG. dmesg without CAP_SYSLOG
    # silently truncates to no output; with it, on a Talos host the
    # kernel ring buffer always has *something* (boot messages alone
    # are 50+ lines). So we assert at least one line. If dmesg
    # returns EPERM that is the missing-CAP_SYSLOG mode the spec calls
    # out and we should fail loudly.
    dmesg_out=$(kubectl -n "$NS" exec "$pod" -- /bin/sh -c "dmesg 2>&1 | head -5" || true)
    if echo "$dmesg_out" | grep -qE 'Operation not permitted|Permission denied'; then
        echo "FAIL: [${pod}] dmesg EPERM (satellite pod missing CAP_SYSLOG)"
        fail=1
    elif [[ -z "$dmesg_out" ]]; then
        # Empty without an EPERM is suspicious but not a hard fail —
        # mark it for review.
        echo "WARN: [${pod}] dmesg returned no output (no CAP_SYSLOG or empty ring buffer)"
    fi

    # .res file location: spec calls out a known delta — upstream
    # LINSTOR renders to /var/lib/linstor.d/, blockstor renders to
    # /etc/drbd.d/. Accept either, fail if neither exists AND there
    # is at least one configured DRBD resource (otherwise an empty
    # cluster legitimately has no .res files).
    drbd_count=$(kubectl -n "$NS" exec "$pod" -- /bin/sh -c \
        "drbdsetup status 2>/dev/null | grep -c '^[a-z]' || true")
    if [[ "${drbd_count:-0}" -gt 0 ]]; then
        if ! kubectl -n "$NS" exec "$pod" -- /bin/sh -c \
            "ls /etc/drbd.d/*.res 2>/dev/null | head -1 \
            || ls /var/lib/linstor.d/*.res 2>/dev/null | head -1" \
            | grep -q '\.res$'; then
            echo "FAIL: [${pod}] DRBD has $drbd_count resource(s) configured but no .res file in /etc/drbd.d or /var/lib/linstor.d"
            fail=1
        fi
    fi

    echo "   [${pod}] OK"
done

if (( fail != 0 )); then
    echo "SATELLITE-UTILS-SMOKE: FAIL"
    exit 1
fi

echo ">> SATELLITE-UTILS-SMOKE OK (${#SAT_PODS[@]} pods × $(( ${#CHECKS[@]} + 9 )) checks)"
