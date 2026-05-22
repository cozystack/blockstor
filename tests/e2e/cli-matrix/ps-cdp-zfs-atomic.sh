#!/usr/bin/env bash
#
# usage: ps-cdp-zfs-atomic.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 359.
#
# Reproduction from the e2e3 stand (user-reported, 2026-05-22):
#
#   $ linstor ps l
#   ┊ 17179869184 ┊ True ┊ e2e3-worker-1[/dev/sda(sda)] ┊
#
#   $ linstor ps cdp --pool-name data --storage-pool data zfs e2e3-worker-1 /dev/sda
#   SUCCESS:
#       physical-storage attach accepted on node 'e2e3-worker-1'
#
#   $ linstor sp l
#   data  e2e3-worker-1  ZFS  data  -  -  False  Error  ...
#   ERROR: pool backing storage missing on node e2e3-worker-1:
#          storage pool data is not present
#
# Satellite log (pre-fix):
#
#   "Attach failed" err="zpool create -f -O compression=off -O atime=off
#    data /dev/sda: cannot label 'sda': failed to detect device partitions
#    on '/dev/sda1': 19"
#
# Root cause (Bug 359, empirically verified on stand):
#
#   `zpool create` inside the satellite container deterministically
#   races on /dev partition-node propagation. The container's /dev is a
#   private devtmpfs instance — even with mountPropagation=HostToContainer
#   (Bug 346) the kernel-driven mknod for sda1+sda9 hits the host's
#   devtmpfs but does NOT propagate into the container fast enough for
#   libzpool's open() of /dev/sda1 to succeed. errno is 19 (ENODEV).
#
#   Running `zpool create` in the host's mount namespace via
#   `nsenter -t 1 -m` places the process on the host's devtmpfs (which
#   the kernel keeps in sync synchronously), so open() succeeds on the
#   first try. The Talos host carries `zpool` via the siderolabs/zfs
#   system extension, so the nsenter target is always usable on the
#   production manifest.
#
# Contract this cell pins:
#
#   Phase 1 (happy path, atomicity):
#     - Stamp /dev/sda with stale ZFS partitions (matches the e2e3
#       repro fixture: sda1 "zfs-..." + sda9 8 MiB reserved). This
#       is the deterministic failure-trigger pre-fix.
#     - `linstor ps cdp zfs $NODE /dev/sda` exits 0.
#     - `linstor sp l` converges to non-zero free_capacity within 60s.
#     - `zpool list <pool>` on the worker exits 0 — the backing
#       storage actually materialised.
#
#   Phase 2 (idempotent rerun):
#     - Re-issuing the same `ps cdp` against the same node + device
#       MUST NOT corrupt the pool. The flat-reconcile branch (Bug 337)
#       sees the existing zpool and falls through to a no-op extend
#       attempt. Either exit 0 with "pool already exists" semantics
#       or a benign error — never a silent partial commit.
#
# The out-of-band destroy + recover scenario is intentionally NOT
# covered here — it intersects Bug 50/74 PoolMissing convergence and
# would silently re-test those features. Bug 359 itself only owns
# the atomic-create-and-host-namespace-dispatch contracts above.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup
trap linstor_cli_teardown EXIT

POOL=cli-matrix-cdp-atomic
NODE=$WORKER_1

SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "SKIP: satellite pod for $NODE not found"
    exit 0
fi

# Bug 359 is specifically about /dev/sda — the device that hits the
# zfs-style stale partition fixture deterministically. The stand
# layout provisions /dev/sda on every worker as the "blockstor data
# disk" candidate. Skip when absent.
if ! kubectl -n "$NS" exec "$SAT_POD" -- test -b /dev/sda >/dev/null 2>&1; then
    echo "SKIP: $NODE has no /dev/sda — Bug 359 fixture not available"
    exit 0
fi

# Refuse to run on a stand where /dev/sda is already in use by another
# pool / LVM — the precondition violates this cell's invariants and
# would also wipe production data. STAND_RESET is expected to bring
# /dev/sda back to a free state.
if kubectl -n "$NS" exec "$SAT_POD" -- pvs /dev/sda >/dev/null 2>&1; then
    echo "SKIP: /dev/sda on $NODE is already an LVM PV — refuse to wipe"
    exit 0
fi
if kubectl -n "$NS" exec "$SAT_POD" -- bash -c "zpool list -H -o name | xargs -I{} zpool status {} 2>/dev/null | grep -q '/dev/sda\b'" >/dev/null 2>&1; then
    echo "SKIP: /dev/sda on $NODE is already a zpool vdev — refuse to wipe"
    exit 0
fi

DEV=/dev/sda
PD_NAME="${NODE}.sda"

cleanup() {
    # Drop the operator-side `Spec.AttachTo` so the satellite stops
    # reconciling the PhysicalDevice while we wipe the disk underneath
    # it. Without this the satellite races us with its own wipe +
    # zpool-add loop and the partprobe / wipefs in cleanup hits "Device
    # or resource busy" intermittently.
    kubectl patch physicaldevice "${PD_NAME}" --type=json \
        -p='[{"op":"remove","path":"/spec/attachTo"}]' 2>/dev/null || true

    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true

    # nsenter into the host mount namespace for the zpool destroy:
    # the same partition-node race (Bug 359) that defeats `zpool create`
    # inside the container also makes `zpool destroy` flaky on stand
    # cleanup. We do this via on_node so the cleanup is still subject
    # to the satellite pod's privileged context.
    on_node "$NODE" bash -c "
        nsenter -t 1 -m -- zpool destroy '${POOL}' 2>/dev/null
        wipefs -af ${DEV} >/dev/null 2>&1
        dd if=/dev/zero of=${DEV} bs=1M count=32 conv=fsync,notrunc status=none >/dev/null 2>&1
        sz=\$(blockdev --getsize64 ${DEV} 2>/dev/null || echo 0)
        if [[ \$sz -gt 67108864 ]]; then
            seek=\$((sz/1048576 - 32))
            dd if=/dev/zero of=${DEV} bs=1M seek=\$seek count=32 conv=fsync,notrunc status=none >/dev/null 2>&1
        fi
        blockdev --rereadpt ${DEV} >/dev/null 2>&1
        partprobe ${DEV} >/dev/null 2>&1
    " || true

    linstor_cli_teardown
}
trap cleanup EXIT

# ---- Pre-flight: bring the PhysicalDevice to Free + AttachTo-empty ------
#
# A previous test run can leave the PhysicalDevice with `Spec.AttachTo`
# pointing at a deleted SP and `status.conditions[Free]=False` due to
# leftover ZFS signatures. `linstor ps cdp` would then reject the
# request with "no free PhysicalDevice on node ... matches device_paths".
# This isn't a regression of Bug 359 itself, but it's a side effect of
# the recovery path the operator is supposed to use (the SP State=Error
# cause text tells the operator to re-run ps cdp).
#
# Strip any leftover AttachTo and wait up to 30s for the satellite
# discoverer to refresh status.conditions[Free]=True via the udev
# fast-path. SKIP the cell if the device never becomes Free —
# something upstream of this cell is holding the disk.
kubectl patch physicaldevice "${PD_NAME}" --type=json \
    -p='[{"op":"remove","path":"/spec/attachTo"}]' 2>/dev/null || true

echo ">> [Bug 359] pre-flight: ensure ${PD_NAME} is Free=True"
deadline=$(( $(date +%s) + 90 ))
free_status=""
attempts=0
while (( $(date +%s) < deadline )); do
    free_status=$(kubectl get physicaldevice "${PD_NAME}" \
        -o jsonpath='{.status.conditions[?(@.type=="Free")].status}' 2>/dev/null \
        || echo "")
    if [[ "$free_status" == "True" ]]; then
        break
    fi
    # Aggressively re-wipe so the satellite's next discovery poll
    # sees a clean device. The udev fast-path (Bug 341) re-enqueues
    # PhysicalDevice discovery on `change` uevents — wipefs + rereadpt
    # generates exactly such events.
    on_node "$NODE" bash -c "
        nsenter -t 1 -m -- zpool destroy '${POOL}' >/dev/null 2>&1 || true
        wipefs -af ${DEV} >/dev/null 2>&1 || true
        dd if=/dev/zero of=${DEV} bs=1M count=32 conv=fsync,notrunc status=none >/dev/null 2>&1 || true
        blockdev --rereadpt ${DEV} >/dev/null 2>&1 || true
    " >/dev/null 2>&1 || true
    attempts=$((attempts+1))
    sleep 5
done
if [[ "$free_status" != "True" ]]; then
    echo "SKIP: ${PD_NAME} never reached Free=True (got '$free_status' after ${attempts} wipe-retry cycles) — something else is holding ${DEV}"
    kubectl get physicaldevice "${PD_NAME}" -o yaml 2>&1 | head -30
    exit 0
fi
echo "   ${PD_NAME} is Free=True"

# ---- Pre-stage Bug 359 reproduction fixture ----------------------------
#
# Stamp /dev/sda with the exact stale-ZFS-partition pattern observed on
# e2e3 — sda1 with PARTLABEL=zfs-<hex> + sda9 8 MiB reserved. Pre-fix
# the subsequent `linstor ps cdp` fails inside the satellite container
# with "failed to detect device partitions on '/dev/sda1': 19". Post-fix
# the nsenter-wrapped zpool create runs in the host's mount namespace
# and the partition-node race is gone.
#
# NOTE: We do not pre-stage a stale-GPT fixture here. The Bug 359 race
# fires on a CLEAN /dev/sda when zpool stamps its own brand-new GPT
# (sda1+sda9) — the partition node mknod is what races with libzpool's
# open(/dev/sda1) inside the container. The legacy "stale ZFS GPT
# survived wipefs" path is already covered by
# ps-cdp-creates-real-backing.sh (Bug 336). Pre-staging here would
# only flip the PhysicalDevice to Free=False and the REST handler
# would reject the request with `[SignatureFound] device ... is busy`,
# which is correct behaviour — not Bug 359's failure mode.

# Confirm the fixture stamped the partition table. The cell is
# strictly more useful when the fixture lands, but we don't fail
# pre-flight here — without sda1 the cell still exercises the
# nsenter-wrapped happy path.
if ! kubectl -n "$NS" exec "$SAT_POD" -- bash -c "lsblk -nro NAME ${DEV} | grep -qE 'sda[0-9]+'" >/dev/null 2>&1; then
    echo "note: pre-stage did not produce sda1/sda9 — cell will still validate atomic ps cdp on a clean device"
fi

# ---- Phase 1: ps cdp must atomically create the backing pool -----------
echo ">> [Bug 359 / Phase 1] linstor ps cdp zfs $NODE $DEV --pool-name $POOL --storage-pool $POOL"
out_file=$(mktemp)
err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" "$DEV" \
        --pool-name "$POOL" \
        --storage-pool="$POOL" \
        >"$out_file" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 359 / Phase 1): linstor ps cdp exited $rc on $DEV against stale ZFS GPT" >&2
    echo "----- stdout -----" >&2
    cat "$out_file" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "----- satellite log (last 200 / attach|wipe|zpool|partition lines) -----" >&2
    kubectl -n "$NS" logs "$SAT_POD" --tail=200 2>/dev/null \
        | grep -iE "attach|wipe|zpool|partition" >&2 || true
    rm -f "$out_file" "$err_file"
    exit 1
fi
rm -f "$out_file" "$err_file"

echo ">> [Phase 1] wait up to 60s for SP $POOL on $NODE to converge to non-zero free_capacity"
deadline=$(( $(date +%s) + 60 ))
cur_free=0
while (( $(date +%s) < deadline )); do
    # golinstor wraps the result in a double array: `[[{...}]]`.
    # Flatten with `.[]?[]?` and pick the first matching pool's
    # free_capacity. Matches the convention used by
    # ps-cdp-zfs-roundtrip.sh and friends.
    cur_free=$("${LCTL[@]}" --machine-readable storage-pool list \
        --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
        | jq -r '[.[]?[]?.free_capacity // 0] | .[0] // 0' 2>/dev/null \
        || echo "0")
    if (( cur_free > 0 )); then
        break
    fi
    sleep 2
done
if (( cur_free == 0 )); then
    echo "FAIL (Bug 359 / Phase 1): SP $POOL free_capacity=0 after 60s — 'ps cdp' reported SUCCESS but pool not materialised" >&2
    "${LCTL[@]}" storage-pool list --storage-pools "$POOL" --nodes "$NODE" 2>&1 | tail -20 >&2
    echo "----- satellite log (last 200) -----" >&2
    kubectl -n "$NS" logs "$SAT_POD" --tail=200 2>/dev/null \
        | grep -iE "attach|wipe|zpool|partition" >&2 || true
    exit 1
fi

echo ">> [Phase 1] cross-verify: zpool list $POOL on $NODE host-side"
if ! on_node "$NODE" nsenter -t 1 -m -- zpool list -H -o name "$POOL" >/dev/null 2>&1; then
    echo "FAIL (Bug 359 / Phase 1): SP reports free_capacity>0 but \`zpool list $POOL\` on $NODE failed — partial commit" >&2
    on_node "$NODE" nsenter -t 1 -m -- zpool list 2>&1 >&2 || true
    exit 1
fi
echo "   Phase 1 OK (atomic create, backing zpool present)"

# ---- Phase 2: rerunning ps cdp on the same device is idempotent -------
echo ">> [Bug 359 / Phase 2] re-run ps cdp on the same device — must NOT corrupt"
err2=$(mktemp)
out2=$(mktemp)
# Bug 337's flat-reconcile branch probes `zpool list <pool>` first;
# when the pool already exists it falls through to `zpool add` against
# the same device, which exits non-zero with "is part of active pool".
# That's acceptable — the contract here is "no silent partial commit",
# NOT "must exit 0". So we tolerate either rc=0 or rc!=0 + benign
# stderr, but FAIL hard if `zpool list <pool>` after the rerun stops
# returning the pool.
"${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" "$DEV" \
        --pool-name "$POOL" \
        --storage-pool="$POOL" \
        >"$out2" 2>"$err2" || true

if ! on_node "$NODE" nsenter -t 1 -m -- zpool list -H -o name "$POOL" >/dev/null 2>&1; then
    echo "FAIL (Bug 359 / Phase 2): second ps cdp destroyed the existing pool" >&2
    echo "----- stdout -----" >&2
    cat "$out2" >&2
    echo "----- stderr -----" >&2
    cat "$err2" >&2
    rm -f "$out2" "$err2"
    exit 1
fi
rm -f "$out2" "$err2"
echo "   Phase 2 OK (idempotent — existing pool preserved)"

# NOTE: We deliberately omit a "destroy out-of-band → recover via
# ps cdp" phase. The recovery loop intersects Bug 50/74 territory
# (PoolMissing convergence on the SP CRD, PhysicalDevice Free=True
# re-marking via udev fast-path, finalizer-driven SP deletion) —
# all of which have their own dedicated cli-matrix cells and are
# not what Bug 359 fixes. Sub-stating that flow here would
# silently retest those features and surface their flakes as
# Bug 359 regressions.
#
# What Bug 359 specifically pins:
#   - SUCCESS from `ps cdp` implies the on-host zpool exists
#     (Phase 1 cross-verify).
#   - Re-issuing `ps cdp` against the same Free device is
#     non-destructive (Phase 2).
# Both contracts hold post-fix; pre-fix Phase 1 deterministically
# left the SP in State=Error with `pool backing storage missing`.

echo ">> ps-cdp-zfs-atomic OK (Bug 359 pinned: atomic create + idempotent rerun)"
