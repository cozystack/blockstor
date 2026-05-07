#!/usr/bin/env bash
# usage: down.sh NAME WORK_DIR
set -uo pipefail
NAME=${1:?cluster name required}
WORK_DIR=${2:-.work/$NAME}
STATE_DIR="$WORK_DIR/talos-state/$NAME"

echo ">> destroying cluster '$NAME'"
if [[ -d "$WORK_DIR/talos-state" ]]; then
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu --state "$WORK_DIR/talos-state" 2>/dev/null || true
else
    sudo talosctl cluster destroy --name "$NAME" --provisioner qemu 2>/dev/null || true
fi

# Belt-and-braces: any qemu/dhcpd/lb that didn't shut down gracefully — use
# the PID files Talos drops in the state dir. Avoids pkill -f path which would
# catch the parent ssh/make processes.
if [[ -d "$STATE_DIR" ]]; then
    for pidfile in "$STATE_DIR"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        pid=$(cat "$pidfile" 2>/dev/null)
        if [[ -n "$pid" ]] && sudo kill -0 "$pid" 2>/dev/null; then
            sudo kill -9 "$pid" 2>/dev/null || true
        fi
    done
fi

sudo rm -rf "$WORK_DIR"
echo ">> done"
