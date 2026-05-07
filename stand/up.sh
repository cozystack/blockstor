#!/usr/bin/env bash
# usage: up.sh NAME CONTROLPLANES WORKERS EXTENSIONS WORK_DIR
# Brings up a Talos cluster on QEMU/KVM. When EXTENSIONS is set, uses Talos
# Image Factory to obtain a kernel+initramfs with those extensions baked in
# and boots the VMs from those artifacts.
set -euo pipefail

NAME=${1:?cluster name required}
CONTROLPLANES=${2:-1}
WORKERS=${3:-3}
EXTENSIONS=${4:-siderolabs/drbd}
WORK_DIR=${5:-.work/$NAME}
TALOS_VERSION=${TALOS_VERSION:-v1.10.5}
ARCH=${ARCH:-amd64}

mkdir -p "$WORK_DIR"
TALOSCONFIG="$WORK_DIR/talosconfig"
KUBECONFIG="$WORK_DIR/kubeconfig"
export TALOSCONFIG KUBECONFIG

# Resolve schematic id from extension list (cache per-extension-set).
SCHEMATIC_DIR=".work/_factory"
mkdir -p "$SCHEMATIC_DIR"
if [[ -n "$EXTENSIONS" ]]; then
    EXT_KEY=$(echo "$EXTENSIONS" | tr ',' '\n' | sort | tr '\n' ',' | sed 's/,$//')
    EXT_HASH=$(echo -n "$EXT_KEY" | sha256sum | cut -c1-12)
    SCHEMATIC_CACHE="$SCHEMATIC_DIR/$EXT_HASH.id"
    if [[ ! -f "$SCHEMATIC_CACHE" ]]; then
        echo ">> registering schematic for extensions: $EXT_KEY"
        YAML_EXTS=$(echo "$EXTENSIONS" | tr ',' '\n' | sed 's/^/        - /')
        SCHEMATIC=$(cat <<EOF
customization:
  systemExtensions:
    officialExtensions:
$YAML_EXTS
EOF
)
        SCHEMATIC_ID=$(curl -sX POST --data-binary "$SCHEMATIC" https://factory.talos.dev/schematics | jq -r .id)
        [[ -n "$SCHEMATIC_ID" && "$SCHEMATIC_ID" != "null" ]] || { echo "factory rejected schematic"; exit 1; }
        echo "$SCHEMATIC_ID" > "$SCHEMATIC_CACHE"
    fi
    SCHEMATIC_ID=$(cat "$SCHEMATIC_CACHE")
    BOOT_DIR="$SCHEMATIC_DIR/$SCHEMATIC_ID-$TALOS_VERSION-$ARCH"
else
    SCHEMATIC_ID=""
    BOOT_DIR="$SCHEMATIC_DIR/vanilla-$TALOS_VERSION-$ARCH"
fi

mkdir -p "$BOOT_DIR"
VMLINUZ="$BOOT_DIR/vmlinuz"
INITRD="$BOOT_DIR/initramfs.xz"
INSTALLER_IMG="$BOOT_DIR/installer.tar"

if [[ ! -s "$VMLINUZ" || ! -s "$INITRD" ]]; then
    if [[ -n "$SCHEMATIC_ID" ]]; then
        BASE="https://factory.talos.dev/image/$SCHEMATIC_ID/$TALOS_VERSION"
    else
        BASE="https://github.com/siderolabs/talos/releases/download/$TALOS_VERSION"
    fi
    echo ">> downloading kernel/initramfs from $BASE"
    curl -fL "$BASE/kernel-$ARCH"        -o "$VMLINUZ"
    curl -fL "$BASE/initramfs-$ARCH.xz"  -o "$INITRD"
fi

# Per-cluster CIDR offset to avoid collisions when running parallel stands.
HASH=$(echo -n "$NAME" | sha256sum | cut -c1-2)
SLOT=$((16#$HASH % 200 + 5))
NET_CIDR="10.${SLOT}.0.0/24"

STATE_DIR="$WORK_DIR/talos-state"
mkdir -p "$STATE_DIR"

# Preflight: kill any residual qemu/talosctl processes from a previous run of
# this same cluster name. Without this, two dhcpd-launch instances race on the
# bridge and VMs never get their config.
echo ">> preflight cleanup for '$NAME'"
sudo bash "$(dirname "$0")/down.sh" "$NAME" "$WORK_DIR" >/dev/null 2>&1 || true
mkdir -p "$STATE_DIR"

# Load DRBD modules inside every Talos node. The siderolabs/drbd extension
# packages the modules but does not auto-load them.
CONFIG_PATCH=$(cat <<'YAML'
machine:
  kernel:
    modules:
      - name: drbd
        parameters:
          - usermode_helper=disabled
      - name: drbd_transport_tcp
YAML
)
PATCH_FILE="$WORK_DIR/config-patch.yaml"
echo "$CONFIG_PATCH" > "$PATCH_FILE"

echo ">> creating cluster '$NAME' (CP=$CONTROLPLANES, workers=$WORKERS, net=$NET_CIDR)"
# talos qemu provisioner needs root for CNI bridge / netfilter; run via sudo -E
# and fix ownership afterwards so the user can read configs.
sudo -E talosctl cluster create \
    --name "$NAME" \
    --provisioner qemu \
    --state "$STATE_DIR" \
    --controlplanes "$CONTROLPLANES" \
    --workers "$WORKERS" \
    --cidr "$NET_CIDR" \
    --vmlinuz-path "$VMLINUZ" \
    --initrd-path "$INITRD" \
    --talosconfig "$TALOSCONFIG" \
    --kubernetes-version "${KUBERNETES_VERSION:-v1.34.1}" \
    --config-patch "@$PATCH_FILE" \
    --memory 4096 \
    --memory-workers 4096 \
    --cpus 2 \
    --cpus-workers 2 \
    --disk 20480 \
    --wait

sudo chown -R "$(id -u):$(id -g)" "$WORK_DIR"

talosctl --talosconfig "$TALOSCONFIG" kubeconfig --force "$KUBECONFIG"

echo
echo ">> cluster '$NAME' is up"
echo "   TALOSCONFIG=$(realpath "$TALOSCONFIG")"
echo "   KUBECONFIG=$(realpath "$KUBECONFIG")"
echo "   eval \"\$(make use NAME=$NAME)\"   # to use it from this shell"
