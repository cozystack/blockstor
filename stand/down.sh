#!/usr/bin/env bash
# usage: down.sh NAME WORK_DIR
set -euo pipefail
NAME=${1:?cluster name required}
WORK_DIR=${2:-.work/$NAME}
STATE_DIR="$WORK_DIR/talos-state"

echo ">> destroying cluster '$NAME'"
if [[ -d "$STATE_DIR" ]]; then
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu --state "$STATE_DIR" || true
else
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu || true
fi
sudo rm -rf "$WORK_DIR"
echo ">> done"
