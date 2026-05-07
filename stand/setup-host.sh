#!/usr/bin/env bash
# Provisions a fresh Linux host (Ubuntu 24.04 tested) for running blockstor
# dev stands. Idempotent: safe to re-run.
#
# Installs:
#   - qemu-kvm + libvirt + ovmf (UEFI firmware) for the qemu provisioner
#   - drbd-utils (host inspection only; the actual DRBD kmod lives inside Talos VMs)
#   - zfsutils-linux, lvm2 for storage providers
#   - talosctl, kubectl, helm pinned to known versions
#   - mounts the largest unused block device at /var/lib/blockstor and
#     symlinks ~/blockstor/.work to it so VM disks live on fast storage.
set -euo pipefail

TALOS_VERSION=${TALOS_VERSION:-v1.10.5}
KUBECTL_VERSION=${KUBECTL_VERSION:-v1.34.1}
HELM_VERSION=${HELM_VERSION:-v3.18.4}
GO_VERSION=${GO_VERSION:-1.24.4}
GOLANGCI_LINT_VERSION=${GOLANGCI_LINT_VERSION:-v2.5.0}
WORK_MOUNT=${WORK_MOUNT:-/var/lib/blockstor}

if [[ $EUID -ne 0 ]]; then
    echo "re-exec under sudo"
    exec sudo -E bash "$0" "$@"
fi

INVOKING_USER=${SUDO_USER:-$(logname 2>/dev/null || echo ubuntu)}
INVOKING_HOME=$(getent passwd "$INVOKING_USER" | cut -d: -f6)

echo "==> apt update + install"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends \
    qemu-kvm qemu-system-x86 qemu-utils ovmf \
    libvirt-daemon-system libvirt-clients virtinst bridge-utils dnsmasq-base \
    drbd-utils lvm2 zfsutils-linux \
    jq curl ca-certificates xz-utils unzip make git \
    iproute2 dmidecode socat conntrack ipset \
    iptables-persistent

echo "==> firewall: allow traffic for talos qemu provisioner"
# Ubuntu OCI image ships a catch-all REJECT in FORWARD and INPUT chains;
# remove FORWARD's, and explicitly allow input/forward from talos+ bridges
# (each cluster gets a different bridge name like talos<hash>).
iptables -P FORWARD ACCEPT
iptables -D FORWARD -j REJECT --reject-with icmp-host-prohibited 2>/dev/null || true
# Idempotency: -C checks before -I inserts.
for spec in \
    "INPUT -i talos+ -j ACCEPT" \
    "INPUT -i virbr+ -j ACCEPT" \
    "FORWARD -i talos+ -j ACCEPT" \
    "FORWARD -o talos+ -j ACCEPT"; do
    iptables -C $spec 2>/dev/null || iptables -I $spec
done
netfilter-persistent save >/dev/null

echo "==> enabling libvirtd"
systemctl enable --now libvirtd virtlogd
usermod -aG libvirt,kvm "$INVOKING_USER"

echo "==> talosctl $TALOS_VERSION, kubectl $KUBECTL_VERSION, helm $HELM_VERSION"
curl -sfL "https://github.com/siderolabs/talos/releases/download/$TALOS_VERSION/talosctl-linux-amd64" -o /usr/local/bin/talosctl
chmod +x /usr/local/bin/talosctl
curl -sfL "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl
TMPHELM=$(mktemp -d)
curl -sfL "https://get.helm.sh/helm-$HELM_VERSION-linux-amd64.tar.gz" | tar -xz -C "$TMPHELM"
install -m0755 "$TMPHELM/linux-amd64/helm" /usr/local/bin/helm
rm -rf "$TMPHELM"

echo "==> go $GO_VERSION, golangci-lint $GOLANGCI_LINT_VERSION"
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "$GO_VERSION"; then
    rm -rf /usr/local/go
    curl -sfL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -C /usr/local -xz
fi
ln -sf /usr/local/go/bin/go /usr/local/bin/go
ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin "$GOLANGCI_LINT_VERSION" >/dev/null

echo "==> picking work device"
if mountpoint -q "$WORK_MOUNT"; then
    echo "   $WORK_MOUNT already mounted, skipping"
else
    # Find the largest unused block device.
    DEV=$(lsblk -dnpo NAME,TYPE,SIZE,MOUNTPOINT \
        | awk '$2=="disk" && $4=="" {print $3, $1}' \
        | sort -h | tail -1 | awk '{print $2}')
    if [[ -z "$DEV" ]]; then
        echo "WARN: no spare block device found; .work will live on root fs"
    else
        echo "   formatting $DEV as xfs (label: blockstor)"
        mkfs.xfs -L blockstor -f "$DEV"
        mkdir -p "$WORK_MOUNT"
        UUID=$(blkid -s UUID -o value "$DEV")
        grep -q "$UUID" /etc/fstab || \
            echo "UUID=$UUID $WORK_MOUNT xfs defaults,noatime 0 2" >> /etc/fstab
        mount "$WORK_MOUNT"
        chown "$INVOKING_USER:$INVOKING_USER" "$WORK_MOUNT"
    fi
fi

if [[ -d "$INVOKING_HOME/blockstor" ]]; then
    echo "==> wiring $INVOKING_HOME/blockstor/.work -> $WORK_MOUNT/work"
    sudo -u "$INVOKING_USER" bash -c "
        rm -rf '$INVOKING_HOME/blockstor/.work'
        mkdir -p '$WORK_MOUNT/work'
        ln -snf '$WORK_MOUNT/work' '$INVOKING_HOME/blockstor/.work'
    "
fi

echo
echo "==> setup complete. Re-login (or run \`newgrp libvirt\`) to pick up groups."
echo "    Then: cd ~/blockstor && make up NAME=test"
