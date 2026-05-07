#!/usr/bin/env bash
# usage: down.sh NAME WORK_DIR
set -euo pipefail
NAME=${1:?cluster name required}
WORK_DIR=${2:-.work/$NAME}

echo ">> destroying cluster '$NAME'"
talosctl cluster destroy --name "$NAME" --provisioner qemu || true
rm -rf "$WORK_DIR"
echo ">> done"
