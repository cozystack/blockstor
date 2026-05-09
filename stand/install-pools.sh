#!/usr/bin/env bash
#
# usage: install-pools.sh WORK_DIR [TYPE]
#
# Creates real-disk storage pools on every worker node:
#   TYPE=zfs   → zpool create blockstor-zfs /dev/sdb on each worker
#   TYPE=lvm   → vgcreate blockstor-lvm /dev/sdb + lvcreate -T -L 14G
#                blockstor-lvm/thin
#   TYPE=both  → both, on /dev/sdb (zfs) + /dev/sdc (lvm)
#
# Default TYPE=both. Idempotent: each step skips if the pool already
# exists.
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
# root). zfs gets the first, lvm-thin the second when both are
# requested; single-type stands use the first available.
ZFS_DEV=${ZFS_DEV:-/dev/sda}
LVM_DEV=${LVM_DEV:-/dev/sdb}

if [[ "$TYPE" == "zfs" || "$TYPE" == "lvm" ]]; then
    # single-type stand: both pool drivers default to the first
    # extra disk (since only one was provisioned).
    ZFS_DEV=/dev/sda
    LVM_DEV=/dev/sda
fi

create_zfs() {
    local pod=$1
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
    kubectl -n "$NS" exec "$pod" -- bash -c "
        set -e
        if ! vgs blockstor-lvm >/dev/null 2>&1; then
            wipefs -af ${LVM_DEV}
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
        create_zfs "$pod"
        ;;
    lvm)
        create_lvm "$pod"
        ;;
    both)
        create_zfs "$pod"
        create_lvm "$pod"
        ;;
    *)
        echo "unknown TYPE: $TYPE (want zfs/lvm/both)" >&2
        exit 2
        ;;
    esac
done

echo ">> pools provisioned on all workers"
