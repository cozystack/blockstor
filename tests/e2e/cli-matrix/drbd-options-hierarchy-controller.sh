#!/usr/bin/env bash
#
# usage: drbd-options-hierarchy-controller.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case C1/C2 (DRBD options hierarchy).
#
# Closes two questions the upstream docs leave open
# (linstor-administration.adoc ~3301-3326 documents only syntax):
#
#   C1 — does a `controller drbd-options` value land in the .res of a
#        resource created AFTER the set?  (inheritance / "retroactive")
#   C2 — "closer to the resource wins": when the SAME knob is set at the
#        controller AND at the resource-definition, the RD value must win.
#
# Reproduction shape:
#
#   $ linstor controller drbd-options --max-buffers 36864
#   $ linstor rd c <rd> ; vd c <rd> 1G
#   $ linstor r c --auto-place=2 -s <pool> <rd>
#   # → drbdsetup show <rd> shows max-buffers 36864 (controller inherited, C1)
#
#   $ linstor rd drbd-options --max-buffers 8192 <rd>
#   # → drbdsetup show <rd> now shows max-buffers 8192 (RD wins, C2)
#
# The bug this guards (pre-fix pkg/effectiveprops): the controller-level
# ExtraProps value was merged AFTER the RD typed value, so the
# cluster-wide controller default wrongly clobbered the closer RD
# override — drbdsetup would keep showing 36864 even after the RD set.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-cc-drbdhier
POOL=${POOL:-lvm-thin}
CTRL_MB=36864
RD_MB=8192

cleanup() {
    # Reset the controller-level knob so we don't leak a cluster-wide
    # max-buffers override onto sibling cells / the live stand.
    "${LCTL[@]}" controller drbd-options --unset-max-buffers >/dev/null 2>&1 || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on 2 nodes (got $ok_nodes) — C1/C2 fixture unavailable"
    exit 0
fi

# show_max_buffers <node> — echo the effective max-buffers for $RD as
# the running kernel sees it. `drbdsetup show` dumps the live config in
# the same `net { max-buffers <n>; }` shape the .res carries.
#
# Capture drbdsetup's output into a variable FIRST, then awk the string.
# Piping `drbdsetup show | awk '... exit'` closed the pipe on the first
# match while drbdsetup was still writing → SIGPIPE on the writer, which
# under `set -o pipefail` surfaced as exit 141 from the command
# substitution in wait_max_buffers and aborted the cell. The here-string
# has no writer to signal, so the early awk `exit` is harmless.
show_max_buffers() {
    local node=$1 out
    out=$(on_node "$node" drbdsetup show "$RD" 2>/dev/null) || out=""
    awk '/max-buffers/ { gsub(/[;]/,""); print $2; exit }' <<<"$out"
}

# wait_max_buffers <node> <want> — poll the kernel config until the
# expected max-buffers shows up (drbdadm adjust is async after the CRD
# write). Fails the cell on timeout.
wait_max_buffers() {
    local node=$1 want=$2 deadline got
    deadline=$(( $(date +%s) + 90 ))
    while (( $(date +%s) < deadline )); do
        got=$(show_max_buffers "$node")
        if [[ "$got" == "$want" ]]; then
            return 0
        fi
        sleep 3
    done
    echo "FAIL: ${RD} on ${node}: max-buffers=${got:-<unset>}, want ${want}" >&2
    on_node "$node" drbdsetup show "$RD" 2>&1 | sed 's/^/    /' >&2
    return 1
}

echo ">> [C1] controller drbd-options --max-buffers $CTRL_MB"
"${LCTL[@]}" controller drbd-options --max-buffers "$CTRL_MB" >/dev/null

echo ">> rd c + vd c + r c (--auto-place=2 -s $POOL)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

echo ">> wait for 2 diskful replicas (auto-tiebreaker may add a 3rd, diskless row)"
# Use wait_diskful_count (DISKLESS/TIE_BREAKER rows excluded) — same
# helper the lifecycle catchers use. The previous `linstor_diskful_nodes
# | grep -c .` / `| head -1` forms raced SIGPIPE under `set -o pipefail`:
# `head -1` closes the pipe after the first node while the helper's
# internal `while read` loop is still emitting the rest, so the pipeline
# exited 141 and `set -e` aborted the cell before C1 ever ran. Consume
# the helper via mapfile/process-substitution (no pipe) — the project
# convention every other cell already follows.
if ! wait_diskful_count "$RD" 2 60; then
    die "never reached 2 diskful replicas"
fi

# Pick a diskful node to probe the kernel config on. mapfile over a
# process substitution — no pipe, so no SIGPIPE under pipefail.
mapfile -t diskful_nodes < <(linstor_diskful_nodes "$RD")
node=${diskful_nodes[0]:-}
[[ -n "$node" ]] || die "no diskful node for $RD"

echo ">> [C1] assert controller max-buffers ($CTRL_MB) inherited into $RD on $node"
wait_max_buffers "$node" "$CTRL_MB"

echo ">> [C2] rd drbd-options --max-buffers $RD_MB $RD (closer scope override)"
"${LCTL[@]}" resource-definition drbd-options --max-buffers "$RD_MB" "$RD" >/dev/null

echo ">> [C2] assert RD max-buffers ($RD_MB) now wins over controller ($CTRL_MB) on $node"
wait_max_buffers "$node" "$RD_MB"

echo ">> drbd-options-hierarchy-controller OK (C1 inherited, C2 closer-wins pinned)"
