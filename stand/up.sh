#!/usr/bin/env bash
# usage: up.sh NAME CONTROLPLANES WORKERS EXTENSIONS WORK_DIR
# Brings up a Talos cluster on QEMU/KVM with the given system extensions baked in.
set -euo pipefail

NAME=${1:?cluster name required}
CONTROLPLANES=${2:-1}
WORKERS=${3:-3}
EXTENSIONS=${4:-siderolabs/drbd}
WORK_DIR=${5:-.work/$NAME}
TALOS_VERSION=${TALOS_VERSION:-v1.10.4}
CIDR=${CIDR:-10.5.0.0/24}

mkdir -p "$WORK_DIR"
TALOSCONFIG="$WORK_DIR/talosconfig"
KUBECONFIG="$WORK_DIR/kubeconfig"
export TALOSCONFIG KUBECONFIG

# Resolve a Talos image factory schematic for the requested extensions.
# Cached per-extension-set under .work/_factory.
EXT_KEY=$(echo "$EXTENSIONS" | tr ',' '\n' | sort | tr '\n' ',' | sed 's/,$//')
SCHEMATIC_CACHE=".work/_factory/$(echo "$EXT_KEY" | sha256sum | cut -c1-12)"
mkdir -p "$(dirname "$SCHEMATIC_CACHE")"
if [[ ! -f "$SCHEMATIC_CACHE" ]]; then
    echo ">> registering schematic with image factory for extensions: $EXT_KEY"
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
INSTALLER="factory.talos.dev/installer/$SCHEMATIC_ID:$TALOS_VERSION"
echo ">> using installer: $INSTALLER"

# Per-cluster CIDR offset to avoid collisions when running parallel stands.
HASH=$(echo -n "$NAME" | sha256sum | cut -c1-2)
SLOT=$((16#$HASH % 200 + 5))   # 5..204
NET_CIDR="10.${SLOT}.0.0/24"

echo ">> creating cluster '$NAME' (CP=$CONTROLPLANES, workers=$WORKERS, net=$NET_CIDR)"
talosctl cluster create \
    --name "$NAME" \
    --provisioner qemu \
    --controlplanes "$CONTROLPLANES" \
    --workers "$WORKERS" \
    --cidr "$NET_CIDR" \
    --installer-image "$INSTALLER" \
    --talosconfig "$TALOSCONFIG" \
    --kubernetes-version "${KUBERNETES_VERSION:-v1.34.1}" \
    --memory 4096 \
    --memory-workers 4096 \
    --cpus 2 \
    --cpus-workers 2 \
    --disk 20480 \
    --user-disk-size 10GB \
    --wait

talosctl --talosconfig "$TALOSCONFIG" kubeconfig --force "$KUBECONFIG"

echo
echo ">> cluster '$NAME' is up"
echo "   TALOSCONFIG=$(realpath "$TALOSCONFIG")"
echo "   KUBECONFIG=$(realpath "$KUBECONFIG")"
echo "   eval \"\$(make use NAME=$NAME)\"   # to use it from this shell"
