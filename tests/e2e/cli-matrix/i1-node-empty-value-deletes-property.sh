#!/usr/bin/env bash
#
# usage: i1-node-empty-value-deletes-property.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case I1 on a NON-RD object (node).
#
# PR #97 implemented the upstream empty-value=delete semantic
# (linstor-administration.adoc NOTE ~4277-4279: "Setting a property to
# an empty value deletes the property from the object entirely") only
# for the resource-definition / resource-group set-property handlers.
# I1 routes the remaining CLI-reachable GenericPropsModify handlers
# (node, storage-pool, controller, resource, volume-definition,
# volume-group, storage-pool-definition) through the same shared core.
#
# This cell exercises the representative non-RD object — the node — end
# to end through the real `linstor` CLI → REST → store:
#
#   $ linstor node set-property <node> Aux/cc-i1-marker present
#   # → node list-properties shows the marker
#   $ linstor node set-property <node> Aux/cc-i1-marker      # empty = delete
#   # → node list-properties NO LONGER shows the marker
#
# Pre-fix (node handler used maps.Copy of override_props): an empty
# value would either be ignored by the CLI's delete routing OR, on the
# override path, stored as an empty string — the marker key would still
# be present. Post-fix the key is gone.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

NODE=${WORKER_1:?no worker node discovered}
MARKER_KEY="Aux/cc-i1-marker"

cleanup() {
    # Best-effort: ensure the marker is gone even if a step bailed.
    "${LCTL[@]}" node set-property "$NODE" "$MARKER_KEY" >/dev/null 2>&1 || true
    linstor_cli_teardown
}
trap cleanup EXIT

# marker_present — echo "yes" when MARKER_KEY is listed on the node's
# properties, "no" otherwise. Uses the machine-readable listing so the
# parse is column-agnostic across client versions.
marker_present() {
    local raw
    raw=$("${LCTL[@]}" --machine-readable node list-properties "$NODE" 2>/dev/null || echo "[]")
    if jq -e --arg k "$MARKER_KEY" '
        [.. | objects | select(.key == $k)] | length > 0
    ' <<<"$raw" >/dev/null 2>&1; then
        echo yes
    else
        echo no
    fi
}

echo ">> set Aux marker on node $NODE"
"${LCTL[@]}" node set-property "$NODE" "$MARKER_KEY" present

if [[ "$(marker_present)" != "yes" ]]; then
    echo "FAIL: marker $MARKER_KEY not set after set-property with a value"
    exit 1
fi
echo "   marker present: ok"

echo ">> clear the marker with an EMPTY value (I1 empty-value=delete)"
"${LCTL[@]}" node set-property "$NODE" "$MARKER_KEY"

if [[ "$(marker_present)" != "no" ]]; then
    echo "FAIL: marker $MARKER_KEY still present after empty-value set-property"
    echo "      (empty override must DELETE the key, not store an empty string)"
    "${LCTL[@]}" --machine-readable node list-properties "$NODE" 2>/dev/null \
        | jq --arg k "$MARKER_KEY" '[.. | objects | select(.key==$k)]' >&2 || true
    exit 1
fi

echo "PASS: node empty-value set-property deleted $MARKER_KEY (corner I1)"
