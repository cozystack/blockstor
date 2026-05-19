#!/usr/bin/env bash
#
# usage: ps-cdp-zfs-roundtrip.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 346.
#
# Reproduction from the e2e2 stand (user-reported, 2026-05-19):
#
# Issue 1 — `linstor ps cdp` returns success but `zpool create`
# inside the satellite container fails silently:
#
#   root@e2e2-worker-1:/# zpool create data /dev/sda
#   cannot label 'sda': failed to detect device partitions
#   on '/dev/sda1': 19
#   Error preparing/labeling disk.
#
# errno 19 (ENODEV) on /dev/sda1 means the kernel did not surface
# the new partition node into the satellite container's /dev. The
# usual culprits are:
#   - hostPath /dev not bind-mounted (or mounted with the wrong
#     mountPropagation — needs Bidirectional or HostToContainer)
#   - missing /dev/zfs char device in the container
#   - kmod-zfs not loaded on the host node
# Pre-fix the operator-facing surface was just "SP State=Error
# 60s after ps cdp" — no surfaced root cause. This cell makes
# the failure mode explicit so the daemonset misconfiguration
# surfaces immediately.
#
# Issue 2 — after `linstor sp d <node> <pool>` + manual
# `wipefs -af /dev/sda` the device does NOT reappear in
# `linstor ps l` output. The operator can't re-use the device
# without restarting the satellite pod (which forces a full
# udev re-discovery scan on startup). udev listener
# (pkg/uevent — Bug 341) wired in Phase 11 should pick up the
# `change` uevent from wipefs and re-enqueue the
# PhysicalDeviceDiscovery reconcile, but the device path stays
# absent from the controller-side `physical-storage list`
# output.
#
# This L6 cell drives the full ZFS cdp roundtrip on the stand:
#   1. Probe the satellite container for /dev/zfs + working
#      `zpool list` (catches the Issue 1 root cause before the
#      ps cdp call).
#   2. Pick the first free /dev/sdX from `linstor ps l` (works
#      on any stand layout, not just the /dev/sdb fixture).
#   3. Run `linstor ps cdp ZFS` against it.
#   4. Assert exit 0 AND `zpool list <pool>` succeeds on the
#      host AND `linstor sp l` shows non-zero free_capacity.
#   5. `linstor sp d` + manual wipefs + blockdev --rereadpt.
#   6. Assert the device reappears in `linstor ps l` within 60s.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup
trap linstor_cli_teardown EXIT

POOL=cli-matrix-cdp-roundtrip
NODE=$WORKER_1

SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "SKIP: satellite pod for $NODE not found"
    exit 0
fi

# ---- Issue 1 pre-flight: /dev/zfs + working `zpool list` -----------------
#
# zpool create inside the satellite container will fail with
# "failed to detect device partitions on '/dev/sdaN': 19" when
# either /dev/zfs is absent OR the host /dev is not bind-mounted
# so newly-created partition nodes never propagate into the
# container. Surface this here instead of via a generic
# "ps cdp returned non-zero" later.
echo ">> pre-flight: /dev/zfs and zpool available inside satellite $SAT_POD"
if ! kubectl -n "$NS" exec "$SAT_POD" -- test -c /dev/zfs 2>/dev/null; then
    echo "FAIL (Bug 346 — Issue 1): /dev/zfs missing in satellite container on $NODE" >&2
    echo "  DaemonSet likely missing 'hostPath: /dev/zfs' volume or kmod-zfs not loaded on host" >&2
    kubectl -n "$NS" exec "$SAT_POD" -- ls -la /dev/zfs 2>&1 >&2 || true
    kubectl -n "$NS" exec "$SAT_POD" -- ls -la /dev 2>&1 | grep -i zfs >&2 || true
    exit 1
fi
if ! kubectl -n "$NS" exec "$SAT_POD" -- bash -c 'zpool list >/dev/null 2>&1 || zpool list 2>&1' >/dev/null; then
    echo "FAIL (Bug 346 — Issue 1): zpool list fails inside satellite container" >&2
    kubectl -n "$NS" exec "$SAT_POD" -- zpool list 2>&1 >&2 || true
    kubectl -n "$NS" exec "$SAT_POD" -- bash -c "ls -la /dev/zfs; modprobe zfs 2>&1" >&2 || true
    exit 1
fi

# ---- pick first free /dev/sdX on $NODE -----------------------------------
#
# `linstor ps l` returns devices grouped by size/rotational; node names
# nested under `.nodes`. Pick the first /dev/sdX (skip nvme / md / loop
# — we want the canonical direct-attach disk path operators use).
echo ">> pick first free /dev/sdX on $NODE from linstor ps l"
# Why: wire schema is `[{size, rotational, nodes:{<node>:[{device,...}]}}]`
# — `.[0]` may be `[]` when no spare devices, so guard with `// empty`
# and never crash on a stand whose disks are all already attached.
DEV=$("${LCTL[@]}" --machine-readable physical-storage list 2>/dev/null \
    | jq -r --arg n "$NODE" '
        ( .[]? // empty )
        | .nodes[$n][]?
        | .device // empty
        | select(test("^/dev/sd[a-z]+$"))' \
    | head -1)
if [[ -z "$DEV" ]]; then
    echo "SKIP: no free /dev/sdX on $NODE in linstor ps l (already all attached or no spare disks on stand)"
    "${LCTL[@]}" physical-storage list 2>&1 | head -20
    exit 0
fi
echo "   picked $DEV"

# ---- cleanup trap ---------------------------------------------------------
cleanup() {
    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
    on_node "$NODE" bash -c "
        zpool destroy ${POOL} 2>/dev/null
        wipefs -af ${DEV} >/dev/null 2>&1
        blockdev --rereadpt ${DEV} >/dev/null 2>&1
        partprobe ${DEV} >/dev/null 2>&1
    " || true
    linstor_cli_teardown
}
trap cleanup EXIT

# ---- Step 1: linstor ps cdp ZFS ------------------------------------------
echo ">> linstor ps cdp ZFS $NODE $DEV --pool-name $POOL --storage-pool=$POOL"
out_file=$(mktemp)
err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" "$DEV" \
        --pool-name "$POOL" \
        --storage-pool="$POOL" \
        >"$out_file" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 346 — Issue 1): linstor ps cdp exited $rc" >&2
    echo "----- stdout -----" >&2
    cat "$out_file" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    rm -f "$out_file" "$err_file"
    exit 1
fi
# linstor server-side ERROR envelopes are routed to stdout (not
# stderr). Even on exit 0 the server may have stamped a non-fatal
# error report.
if grep -qiE 'error|fail' "$out_file"; then
    echo "note: ps cdp exit 0 but stdout contains error keyword — surfacing for diagnosis:" >&2
    cat "$out_file" >&2
fi
rm -f "$out_file" "$err_file"

# ---- Step 2: wait for SP free_capacity > 0 -------------------------------
echo ">> wait up to 60s for SP $POOL on $NODE to converge to non-zero free_capacity"
deadline=$(( $(date +%s) + 60 ))
cur_free=0
while (( $(date +%s) < deadline )); do
    cur_free=$("${LCTL[@]}" --machine-readable storage-pool list \
        --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
        | jq -r '.[0].stor_pools[0].free_capacity // 0' 2>/dev/null \
        || echo "0")
    if (( cur_free > 0 )); then
        break
    fi
    sleep 2
done

if (( cur_free == 0 )); then
    echo "FAIL (Bug 346 — Issue 1, post Bug 336 v2): SP $POOL free_capacity=0 after 60s — zpool create failed inside satellite" >&2
    echo "----- linstor sp l -----" >&2
    "${LCTL[@]}" storage-pool list --storage-pools "$POOL" --nodes "$NODE" 2>&1 | tail -20 >&2
    echo "----- satellite log (last 200 / zpool|attach|wipe lines) -----" >&2
    kubectl -n "$NS" logs "$SAT_POD" --tail=200 2>/dev/null \
        | grep -iE "zpool|attach|wipe|labelfail|detect.*partition" >&2 || true
    exit 1
fi

# ---- Step 3: cross-verify zpool exists on host ---------------------------
echo ">> cross-verify: zpool list $POOL on $NODE"
if ! on_node "$NODE" zpool list -H -o name "$POOL" >/dev/null 2>&1; then
    echo "FAIL (Bug 346 — Issue 1): satellite reported SP healthy but \`zpool list $POOL\` on $NODE failed" >&2
    on_node "$NODE" zpool list 2>&1 >&2 || true
    exit 1
fi

# ---- Step 4: Issue 2 — sp d + wipefs → device must re-appear in ps l ----
echo ">> [Issue 2] linstor sp d $NODE $POOL (operator cleanup)"
"${LCTL[@]}" storage-pool delete "$NODE" "$POOL" \
    || { echo "FAIL: linstor sp d returned non-zero"; exit 1; }

# Give Bug 340 self-heal a moment to clear PhysicalDevice.AttachTo.
sleep 5

echo ">> [Issue 2] manual wipefs -af + blockdev --rereadpt on $DEV"
on_node "$NODE" bash -c "
    zpool destroy $POOL 2>/dev/null
    wipefs -af $DEV
    blockdev --rereadpt $DEV
    partprobe $DEV 2>/dev/null || true
" || { echo "FAIL: host-side wipe of $DEV returned non-zero"; exit 1; }

echo ">> [Issue 2] wait up to 60s for $DEV to reappear in linstor ps l"
deadline=$(( $(date +%s) + 60 ))
found=false
while (( $(date +%s) < deadline )); do
    if "${LCTL[@]}" --machine-readable physical-storage list 2>/dev/null \
        | jq -e --arg n "$NODE" --arg d "$DEV" '
            .[0].physical_devices[$n][]?
            | select(.device_path==$d)' \
        >/dev/null 2>&1; then
        found=true
        break
    fi
    sleep 3
done

if ! $found; then
    echo "FAIL (Bug 346 — Issue 2): $DEV not re-discovered in \`linstor ps l\` within 60s after wipefs+rereadpt" >&2
    echo "  Expected: udev listener (pkg/uevent, Bug 341) re-enqueues PhysicalDevice discovery on \`change\` event" >&2
    echo "----- linstor ps l -----" >&2
    "${LCTL[@]}" physical-storage list 2>&1 | head -30 >&2
    echo "----- on-host blkid + lsblk for $DEV -----" >&2
    on_node "$NODE" bash -c "blkid $DEV 2>&1; lsblk $DEV 2>&1" >&2 || true
    echo "----- satellite log (last 150 / udev|uevent|physical|discover lines) -----" >&2
    kubectl -n "$NS" logs "$SAT_POD" --tail=200 2>/dev/null \
        | grep -iE "udev|uevent|physical|discover|change" >&2 || true
    exit 1
fi

echo ">> ps-cdp-zfs-roundtrip OK (Bug 346 pinned: zpool materialised on host + device re-discovered after wipefs)"
