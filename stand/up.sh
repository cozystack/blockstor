#!/usr/bin/env bash
# usage: up.sh NAME CONTROLPLANES WORKERS EXTENSIONS WORK_DIR
# Brings up a Talos cluster on QEMU/KVM via `talosctl cluster create qemu`.
# When EXTENSIONS is set we register a Talos Image Factory schematic with
# those extensions and pass the resulting id as `--schematic-id`, so the
# factory ISO the cluster boots from has the DRBD/ZFS bits baked in.
set -euo pipefail

NAME=${1:?cluster name required}
CONTROLPLANES=${2:-1}
WORKERS=${3:-3}
EXTENSIONS=${4:-siderolabs/drbd,siderolabs/zfs}
WORK_DIR=${5:-.work/$NAME}
TALOS_VERSION=${TALOS_VERSION:-v1.13.2}
ARCH=${ARCH:-amd64}

# Provisioning Talos $TALOS_VERSION needs a talosctl whose version matches the
# target Talos release; the host-global talosctl (installed by setup-host.sh)
# may be pinned to an older version. Resolve a matching binary, downloading
# into a repo-local cache under .work/ (gitignored) when needed. An operator
# can still override with TALOSCTL=/path/to/talosctl.
ensure_talosctl() {
    # Honour an explicit override verbatim.
    if [[ -n "${TALOSCTL:-}" ]]; then
        echo "$TALOSCTL"
        return
    fi
    local want="${TALOS_VERSION#v}"
    # Reuse the on-PATH talosctl when its client version already matches.
    if command -v talosctl >/dev/null 2>&1; then
        local have
        have=$(talosctl version --client --short 2>/dev/null \
            | sed -n "s/.*Tag:[[:space:]]*v\{0,1\}\([0-9][^[:space:]]*\).*/\1/p" \
            | head -n1)
        if [[ "$have" == "$want" ]]; then
            echo "talosctl"
            return
        fi
    fi
    # Otherwise fetch a version-matched binary into the repo-local cache.
    local cache_dir=".work/_bin"
    local bin="$cache_dir/talosctl-$TALOS_VERSION"
    if [[ ! -x "$bin" ]]; then
        mkdir -p "$cache_dir"
        echo ">> fetching talosctl $TALOS_VERSION into $bin" >&2
        curl -fL "https://github.com/siderolabs/talos/releases/download/$TALOS_VERSION/talosctl-linux-$ARCH" \
            -o "$bin.tmp"
        chmod +x "$bin.tmp"
        mv "$bin.tmp" "$bin"
    fi
    echo "$bin"
}
TALOSCTL=$(ensure_talosctl)

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

# Pre-1.13 `talosctl cluster create --provisioner qemu` consumed
# `--vmlinuz-path`/`--initrd-path` directly, so up.sh used to fetch the
# kernel + initramfs into $BOOT_DIR. In 1.13 the deprecation shim
# behind `--provisioner qemu` no longer wires PKI SANs correctly and
# the bootstrap RPC fails cert verification on the controlplane IP.
# The replacement `talosctl cluster create qemu` subcommand handles
# boot-artifact retrieval itself via `--presets iso --schematic-id <id>`
# (pulls an ISO from the Image Factory) so the local kernel cache is
# no longer needed. $BOOT_DIR is still created as a sentinel for
# cleanup logic but stays empty.
mkdir -p "$BOOT_DIR"

# Per-cluster CIDR offset to avoid collisions when running parallel stands.
# Talos's default Kubernetes service CIDR is 10.96.0.0/12 (covers
# 10.96.0.0 – 10.111.255.255). If our slot lands inside that range,
# the controlplane's own bridge IP overlaps with the service-IP pool,
# Talos's `address-overlap` diagnostic fires, and apid refuses to put
# the IP into its API server cert SANs — so bootstrap fails with
# `cannot validate certificate for 10.X.0.2 because it doesn't
# contain any IP SANs`. Skip slots 96..111 to dodge the collision.
HASH=$(echo -n "$NAME" | sha256sum | cut -c1-2)
RAW_SLOT=$((16#$HASH % 200 + 5))
if [[ $RAW_SLOT -ge 96 && $RAW_SLOT -le 111 ]]; then
    SLOT=$(( RAW_SLOT - 16 ))
else
    SLOT=$RAW_SLOT
fi
NET_CIDR="10.${SLOT}.0.0/24"

STATE_DIR="$WORK_DIR/talos-state"
mkdir -p "$STATE_DIR"

# Preflight: kill any residual qemu/talosctl processes from a previous run of
# this same cluster name. Without this, two dhcpd-launch instances race on the
# bridge and VMs never get their config.
echo ">> preflight cleanup for '$NAME'"
sudo bash "$(dirname "$0")/down.sh" "$NAME" "$WORK_DIR" >/dev/null 2>&1 || true
mkdir -p "$STATE_DIR"

# The factory ISO carries our extensions for *boot*; the on-disk
# install uses ghcr.io/siderolabs/installer:* by default, which lacks
# them. Override machine.install.image to the same factory schematic
# so the installed Talos has DRBD bits, and tell it to load the modules.
if [[ -n "$SCHEMATIC_ID" ]]; then
    INSTALL_IMG="factory.talos.dev/installer/$SCHEMATIC_ID:$TALOS_VERSION"
else
    INSTALL_IMG="ghcr.io/siderolabs/installer:$TALOS_VERSION"
fi
PATCH_FILE="$WORK_DIR/config-patch.yaml"
cat > "$PATCH_FILE" <<YAML
machine:
  install:
    image: $INSTALL_IMG
  kernel:
    modules:
      # Replication layer.
      - name: drbd
        parameters:
          - usermode_helper=disabled
      - name: drbd_transport_tcp
      # Storage providers — load the kernel bits piraeus / blockstor
      # rely on so satellites can drive ZFS and LVM-thin pools without
      # an extra Talos config-patch step.
      # https://piraeus.io/docs/v2.10.5/how-to/talos/
      - name: zfs
      - name: dm_thin_pool
      - name: dm_snapshot
      - name: dm_crypt
  # Trust the host-side Docker registry on the bridge gateway (.1 of
  # this cluster's NET_CIDR) via plain HTTP. The blockstor-controller
  # / blockstor-satellite images we build live there; without this
  # patch containerd refuses to pull them ("http: server gave HTTP
  # response to HTTPS client"). The bridge IP varies per cluster
  # because parallel stands each get their own /24, hashed off NAME.
  registries:
    mirrors:
      "${NET_CIDR%.*}.1:5000":
        endpoints:
          - "http://${NET_CIDR%.*}.1:5000"
        skipFallback: true
YAML

# Build the `--disks` list: first disk is the boot disk (20 GiB), the
# rest are EXTRA_DISKS data disks at EXTRA_DISK_SIZE_GB each. The new
# subcommand expects a single comma-separated `<driver>:<size>` list
# instead of the old `--disk`/`--extra-disks`/`--extra-disks-size`
# triple.
# Backward compat: legacy `EXTRA_DISK_SIZE_MB` (MiB, e.g. 16384 = 16 GiB)
# wins if set so stand/ci-e2e.sh keeps working unchanged.
if [[ -n "${EXTRA_DISK_SIZE_MB:-}" ]]; then
    EXTRA_DISK_SIZE_GB=$(( EXTRA_DISK_SIZE_MB / 1024 ))
fi
EXTRA_DISK_SIZE_GB=${EXTRA_DISK_SIZE_GB:-16}
DISKS="virtio:20GiB"
for _ in $(seq 1 ${EXTRA_DISKS:-2}); do
    DISKS="$DISKS,virtio:${EXTRA_DISK_SIZE_GB}GiB"
done

echo ">> creating cluster '$NAME' (CP=$CONTROLPLANES, workers=$WORKERS, net=$NET_CIDR)"
# 1.13 `cluster create qemu` subcommand: presets ISO + factory
# schematic id give us a DRBD/ZFS-extension boot. Flag renames vs
# the legacy `cluster create --provisioner qemu` path:
#   --provisioner qemu        → (now the subcommand)
#   --vmlinuz-path/--initrd-path → --presets iso (factory ISO download)
#   --talosconfig             → --talosconfig-destination
#   --memory                  → --memory-controlplanes
#   --cpus                    → --cpus-controlplanes
#   --disk + --extra-disks*   → --disks "<driver>:<size>,..."
# `--wait` is implicit on the new subcommand. Run via `sudo -E`
# because the qemu provisioner still needs root for CNI / netfilter.
sudo -E "$TALOSCTL" cluster create qemu \
    --name "$NAME" \
    --state "$STATE_DIR" \
    --presets iso \
    --schematic-id "$SCHEMATIC_ID" \
    --talos-version "$TALOS_VERSION" \
    --talosconfig-destination "$TALOSCONFIG" \
    --controlplanes "$CONTROLPLANES" \
    --workers "$WORKERS" \
    --cidr "$NET_CIDR" \
    --kubernetes-version "${KUBERNETES_VERSION:-v1.34.1}" \
    --config-patch "@$PATCH_FILE" \
    --memory-controlplanes 4096 \
    --memory-workers 4096 \
    --cpus-controlplanes 2.0 \
    --cpus-workers 2.0 \
    --disks "$DISKS"

# `chown -R` does not follow a top-level symlink, so when WORK_DIR is
# a symlink (the dev stand redirects `.work/<name>` →
# `/var/lib/blockstor/work-<name>` for disk-space reasons) the
# kubeconfig/talosconfig written by the `sudo talosctl cluster create`
# above stay root-owned. Resolve via realpath so the recursion lands
# on the actual directory. Found by parallel e2e run on 2026-05-17.
sudo chown -R "$(id -u):$(id -g)" "$(realpath "$WORK_DIR")"

# Talos qemu provisioner allocates IPs deterministically: first controlplane
# is at .2 in the cluster CIDR.
CP_IP="${NET_CIDR%.*}.2"

"$TALOSCTL" --talosconfig "$TALOSCONFIG" \
    --endpoints "$CP_IP" --nodes "$CP_IP" \
    kubeconfig --force "$KUBECONFIG"

echo
echo ">> cluster '$NAME' is up"
echo "   TALOSCONFIG=$(realpath "$TALOSCONFIG")"
echo "   KUBECONFIG=$(realpath "$KUBECONFIG")"
echo "   eval \"\$(make use NAME=$NAME)\"   # to use it from this shell"
