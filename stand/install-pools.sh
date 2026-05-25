#!/usr/bin/env bash
#
# usage: install-pools.sh WORK_DIR [TYPE]
#
# Creates real-disk storage pools on every worker node:
#   TYPE=zfs   → zpool create blockstor-zfs on the spare disk
#   TYPE=lvm   → vgcreate blockstor-lvm + thin pool on the spare disk
#   TYPE=both  → both: first spare disk = zfs, second spare disk = lvm
#
# Backing devices are auto-detected per node (see pick_device below);
# the root OS disk is always excluded and a single-type re-run never
# steals the other driver's disk.
#
# Default TYPE=both. Idempotent: each step skips if the pool already
# exists and recreates a destroyed VG (clearing stale dm nodes first).
#
# Re-applies the satellite DaemonSet with --zfs-pool-name=blockstor-zfs
# and/or --lvm-pool-name=blockstor-lvm so the controller's StoragePool
# CRDs reflect the real pools.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
TYPE=${2:-both}

export KUBECONFIG="$WORK_DIR/kubeconfig"

NS=blockstor-system

# Talos qemu attaches extra disks as /dev/sda, /dev/sdb (vda is the
# root OS disk). zfs takes the first spare disk, lvm-thin the second
# when both are requested.
#
# Device selection is auto-detected per worker, never hard-coded: the
# old code set LVM_DEV=/dev/sda for single-type (TYPE=lvm) runs, but on
# a `both`-provisioned stand /dev/sda is the zfs_member disk — so
# `make pools TYPE=lvm` clobbered the live ZFS pool instead of touching
# the spare LVM disk. We now enumerate the real spare disks per node
# and skip any disk already claimed by the other driver.
#
# spare_disks_on <pod>: prints candidate whole-disk paths (one per
# line, sorted), excluding the OS disk and loop/cdrom devices.
#
# The satellite container's mount namespace has no partition mounted at
# `/` (its rootfs is an overlay), so we can't find the OS disk by the
# `/` mountpoint. Instead we mark a disk as the OS disk if any of its
# partitions is mounted OR carries a vfat (EFI) signature — on the
# Talos qemu stand that uniquely identifies vda. PKNAME maps each
# partition to its parent disk.
spare_disks_on() {
    local pod=$1
    kubectl -n "$NS" exec "$pod" -- sh -c '
        os_disks=$(lsblk -nro NAME,FSTYPE,MOUNTPOINT,PKNAME \
            | awk "(\$3!=\"\" || \$2==\"vfat\") && \$4!=\"\" {print \$4}" \
            | sort -u)
        lsblk -nrdo NAME,TYPE | awk -v os="$os_disks" "
            BEGIN { n=split(os, a, \"\n\"); for (i=1;i<=n;i++) skip[a[i]]=1 }
            \$2==\"disk\" && \$1!~/^loop/ && \$1!~/^sr/ && !(\$1 in skip) { print \"/dev/\"\$1 }
        " | sort
    '
}

# disk_owner <pod> <dev>: classify a whole disk by scanning the disk
# AND all of its child partitions for a pool signature. Prints "zfs"
# (any descendant is zfs_member), "lvm" (any descendant is LVM2_member),
# or "free". The earlier version only read the whole-disk fstype, which
# is empty for a partitioned zfs disk (the zfs_member sig lives on
# /dev/sda1, not /dev/sda) — that bug made `make pools TYPE=lvm` pick
# the zfs disk /dev/sda and wipefs its GPT.
disk_owner() {
    local pod=$1 dev=$2
    kubectl -n "$NS" exec "$pod" -- lsblk -nro FSTYPE "$dev" 2>/dev/null \
        | awk '
            /zfs_member/ { z=1 }
            /LVM2_member/ { l=1 }
            END { if (z) print "zfs"; else if (l) print "lvm"; else print "free" }
        '
}

# pick_device <pod> <slot>: choose the backing device for slot=zfs or
# slot=lvm. With TYPE=both the first spare is zfs, the second is lvm.
# With a single driver, prefer a disk already owned by the requested
# driver (idempotent re-provision), else a free disk; never a disk
# owned by the other driver (so a single-type re-run on a both-stand
# never clobbers the foreign pool).
pick_device() {
    local pod=$1 slot=$2
    local -a spares
    mapfile -t spares < <(spare_disks_on "$pod")
    if (( ${#spares[@]} == 0 )); then
        echo "ERROR: no spare disk on $pod (only root + loop/cdrom)" >&2
        return 1
    fi
    if [[ "$TYPE" == "both" ]]; then
        if [[ "$slot" == "zfs" ]]; then
            echo "${spares[0]}"
        else
            echo "${spares[1]:-${spares[0]}}"
        fi
        return 0
    fi
    local dev owner free=""
    for dev in "${spares[@]}"; do
        owner=$(disk_owner "$pod" "$dev")
        if [[ "$owner" == "$slot" ]]; then
            echo "$dev"            # already this driver's disk — reuse it
            return 0
        fi
        if [[ "$owner" == "free" && -z "$free" ]]; then
            free="$dev"            # remember first free disk as fallback
        fi
    done
    if [[ -n "$free" ]]; then
        echo "$free"
        return 0
    fi
    echo "ERROR: no $slot or free spare disk on $pod (all owned by the other driver)" >&2
    return 1
}

create_zfs() {
    local pod=$1
    local ZFS_DEV=$2
    kubectl -n "$NS" exec "$pod" -- bash -c "
        if zpool list blockstor-zfs >/dev/null 2>&1; then
            echo 'zpool blockstor-zfs already exists'
            exit 0
        fi
        # zpool create's auto-partition step fails inside the
        # satellite container ('cannot label sda: failed to detect
        # device partitions on /dev/sda1: 19') even though sgdisk
        # itself works. The /dev hostPath bind mount picks up
        # newly-created partitions but zpool's libzfs probe runs in
        # a way that doesn't see them. Workaround: pre-create the
        # ZFS partition with sgdisk + partprobe, then hand zpool
        # the partition path directly.
        wipefs -af ${ZFS_DEV}* 2>&1 || true
        sgdisk --zap-all ${ZFS_DEV} 2>&1 || true
        sgdisk --new=1:0:0 -t 1:bf01 ${ZFS_DEV}
        partprobe ${ZFS_DEV} 2>&1 || true
        sleep 1
        zpool create -f -o cachefile=none blockstor-zfs ${ZFS_DEV}1
        echo 'zpool blockstor-zfs created'
    "
}

create_lvm() {
    local pod=$1
    local LVM_DEV=$2
    kubectl -n "$NS" exec "$pod" -- bash -c "
        set -e
        if ! vgs blockstor-lvm >/dev/null 2>&1; then
            # A destructive lvm-thin scenario (vgremove + pvremove) can
            # leave the VG gone yet stale state still pinning the
            # backing device / blocking vgcreate:
            #   - device-mapper nodes (blockstor--lvm-thin*) → 'in use'
            #   - a leftover /dev/blockstor-lvm/ directory with dangling
            #     symlinks (no udev to clean it) → vgcreate aborts with
            #     '/dev/blockstor-lvm: already exists in filesystem'
            # Tear both down, then scrub any residual PV/fs signature so
            # vgcreate starts from a clean disk. All best-effort so an
            # already-clean device is a no-op.
            for dm in \$(dmsetup ls 2>/dev/null | awk '/^blockstor--lvm/{print \$1}'); do
                dmsetup remove \"\$dm\" 2>/dev/null || true
            done
            rm -rf /dev/blockstor-lvm 2>/dev/null || true
            pvremove -ff -y ${LVM_DEV} 2>/dev/null || true
            wipefs -af ${LVM_DEV} 2>/dev/null || true
            vgcreate -y blockstor-lvm ${LVM_DEV}
        fi
        if lvs blockstor-lvm/thin >/dev/null 2>&1; then
            echo 'lv blockstor-lvm/thin already exists'
            exit 0
        fi
        # The satellite container has no udev. lvm's default behaviour
        # is to wait for udev to populate /dev/<vg>/<lv> after a
        # device-mapper create — without udev that wait times out and
        # fails the LV. activation{udev_sync=0 udev_rules=0} bypasses
        # the wait. -Wn -Zn skip the optional wipe-signatures / zero
        # steps which also fail (they go through the same
        # /dev/<vg>/<lv> path that udev never created).
        CFG='activation{udev_sync=0 udev_rules=0}'
        # Idempotency: a prior run may have created thin_meta and/or a
        # plain 'thin' LV but not finished the thin-pool convert (e.g.
        # interrupted, or a too-small disk where 'lvcreate -L 13G thin'
        # ran out of space). The 'lvs .../thin' check above only matches
        # the FINISHED pool, so scrub any partial leftovers here to keep
        # these lvcreates re-runnable.
        lvremove --config \"\$CFG\" -y blockstor-lvm/thin 2>/dev/null || true
        lvremove --config \"\$CFG\" -y blockstor-lvm/thin_meta 2>/dev/null || true
        lvcreate --config \"\$CFG\" -y -Wn -Zn -L 1G blockstor-lvm -n thin_meta
        lvcreate --config \"\$CFG\" -y -Wn -Zn -L 13G blockstor-lvm -n thin
        lvconvert --config \"\$CFG\" -y -Wn -Zn --type thin-pool --poolmetadata blockstor-lvm/thin_meta blockstor-lvm/thin
        echo 'lv blockstor-lvm/thin created'
    "
}

for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name); do
    echo ">> setup pools on $pod"

    case "$TYPE" in
    zfs)
        zdev=$(pick_device "$pod" zfs)
        echo "   zfs device: $zdev"
        create_zfs "$pod" "$zdev"
        ;;
    lvm)
        ldev=$(pick_device "$pod" lvm)
        echo "   lvm device: $ldev"
        create_lvm "$pod" "$ldev"
        ;;
    both)
        zdev=$(pick_device "$pod" zfs)
        ldev=$(pick_device "$pod" lvm)
        echo "   zfs device: $zdev   lvm device: $ldev"
        create_zfs "$pod" "$zdev"
        create_lvm "$pod" "$ldev"
        ;;
    *)
        echo "unknown TYPE: $TYPE (want zfs/lvm/both)" >&2
        exit 2
        ;;
    esac
done

echo ">> pools provisioned on all workers"

# Apply StoragePool CRDs so the satellite's StoragePoolReconciler
# discovers them and registers the matching storage.Provider. The
# yaml ships with `__WORKER_N__` placeholders; substitute with the
# actual cluster's worker node names (sorted) before applying.
mapfile -t _WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort
)
W1="${_WORKERS[0]:-}"
W2="${_WORKERS[1]:-}"
W3="${_WORKERS[2]:-}"

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
echo ">> applying StoragePool CRDs (workers: $W1 $W2 $W3)"
sed -e "s|__WORKER_1__|$W1|g" \
    -e "s|__WORKER_2__|$W2|g" \
    -e "s|__WORKER_3__|$W3|g" \
    "$REPO_ROOT/stand/blockstor-storagepools.yaml" | kubectl apply -f -

echo ">> waiting for satellite StoragePoolReconciler to pick up CRDs"
sleep 5
kubectl get storagepools -o wide
