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

# zfs and lvm devices (per the up.sh extra-disks order).
ZFS_DEV=/dev/sdb
LVM_DEV=/dev/sdc

if [[ "$TYPE" == "zfs" ]]; then
    ZFS_DEV=/dev/sdb
fi

if [[ "$TYPE" == "lvm" ]]; then
    LVM_DEV=/dev/sdb
fi

create_zfs() {
    local pod=$1
    kubectl -n "$NS" exec "$pod" -- bash -c "
        if zpool list blockstor-zfs >/dev/null 2>&1; then
            echo 'zpool blockstor-zfs already exists'
        else
            zpool create -f blockstor-zfs ${ZFS_DEV}
            echo 'zpool blockstor-zfs created'
        fi
    "
}

create_lvm() {
    local pod=$1
    kubectl -n "$NS" exec "$pod" -- bash -c "
        if vgs blockstor-lvm >/dev/null 2>&1; then
            echo 'vg blockstor-lvm already exists'
        else
            vgcreate -y blockstor-lvm ${LVM_DEV}
            lvcreate -y -T -L 14G blockstor-lvm/thin
            echo 'vg blockstor-lvm + thin pool created'
        fi
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
