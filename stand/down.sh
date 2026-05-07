#!/usr/bin/env bash
# usage: down.sh NAME WORK_DIR
set -uo pipefail
NAME=${1:?cluster name required}
WORK_DIR=${2:-.work/$NAME}
STATE_DIR="$WORK_DIR/talos-state"

echo ">> destroying cluster '$NAME'"
if [[ -d "$STATE_DIR" ]]; then
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu --state "$STATE_DIR" 2>/dev/null || true
else
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu 2>/dev/null || true
fi

# Belt-and-braces: kill any residual qemu/talos helpers that referenced this
# state dir. talosctl process names are >15 chars so plain pkill won't match —
# we have to use pkill -f.
ABS_WORK=$(realpath "$WORK_DIR" 2>/dev/null || echo "$WORK_DIR")
sudo pkill -9 -f "$ABS_WORK"   2>/dev/null || true
sudo pkill -9 -f "$WORK_DIR"   2>/dev/null || true
sudo pkill -9 -f "/$NAME/"     2>/dev/null || true
# Bridge interface name is talos<hash(NAME)>; talosctl makes this deterministic
# but we don't compute it here — destroy already handles it. Sweep any orphans.

sudo rm -rf "$WORK_DIR"
echo ">> done"
