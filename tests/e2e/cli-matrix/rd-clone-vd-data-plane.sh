#!/usr/bin/env bash
#
# usage: rd-clone-vd-data-plane.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 020 (fix: accept use_zfs_clone and
# materialise VD-bearing RD clones).
#
# Audit gap: before the fix, POST /v1/resource-definitions/{rd}/clone
# rejected golinstor v0.58+'s `use_zfs_clone` field with 400
# (DisallowUnknownFields), breaking linstor-csi clone-from-source, and
# a VD-bearing source answered an explicit 501 (Bug 114 gate) instead
# of producing a clone. Post-fix the handler routes VD-bearing sources
# through the snapshot-restore machinery: internal snapshot
# `clone-<target>` of the source + restore-marker materialisation, so
# the target RD comes back with hydrated VDs, replicas on the
# snapshot-holding nodes, and the REAL source bytes (delta row 82 in
# docs/cli-parity-known-deltas.md).
#
# This cell pins the DATA PLANE, not just the envelope: a clone that
# converges UpToDate but reads back zeros is the Bug 114 silent-empty-
# shell failure mode resurfacing. Both wire variants are driven:
#
#   A. plain python CLI — `linstor resource-definition clone <src> <dst>`.
#      linstor-client 1.27.1 declares `--use-zfs-clone` with
#      action=store_true, default=None, and python-linstor only
#      serialises non-None kwargs, so the bare verb POSTs a body
#      WITHOUT `use_zfs_clone` (the `use_zfs_clone=false/absent`
#      branch of delta row 82). The CLI then polls
#      GET /v1/resource-definitions/{src}/clone/{dst} until COMPLETE.
#   B. raw REST with `use_zfs_clone: true` — the exact body
#      linstor-csi sends on CSI clone-from-source (golinstor v0.58+
#      sets UseZfsClone on every CreateVolume with a volume content
#      source). Driven via curl because the CLI flag's presence varies
#      across client builds while the wire shape is the contract.
#
# Contract per variant:
#   1. clone verb/POST accepted (CLI exit 0 / HTTP 201, CloneStarted
#      envelope — never 400 on use_zfs_clone, never 501).
#   2. clone status answers COMPLETE.
#   3. target RD materialises 2 diskful replicas that converge
#      UpToDate (observer-stamped Status).
#   4. EVERY diskful replica of the clone holds the deterministic
#      marker seeded on the source (promote each in turn — a silently
#      empty replica reports UpToDate but reads back zeros).
#   5. the internal snapshot `clone-<target>` is visible on the source
#      (it must outlive the clone — `zfs clone` targets stay dependent
#      on their origin snapshot; delta row 82).
#
# Pool: `stand` (FILE_THIN) — snapshot-capable, present on every stand
# worker; same pool the Bug 397 snapshot-restore data-integrity cell
# uses for its byte-level asserts. Override with POOL=zfs-thin to
# exercise the literal `zfs clone` data plane.
#
# Unit pin: pkg/rest/clone_use_zfs_clone_bug020_test.go. This cell is
# the stand-side companion (real python-linstor + satellite + kernel).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

SRC=cli-matrix-020-clsrc
DST_CLI=cli-matrix-020-cla
DST_ZFS=cli-matrix-020-clb
POOL=${POOL:-stand}
MARKER='BLOCKSTOR-BUG020-CLONE-MARKER'

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    # Clones first (their backing storage may depend on the source's
    # internal snapshot), then the source. delete_rd reaps the
    # clone-<target> Snapshot CRDs together with the source RD.
    delete_rd "$DST_CLI"
    delete_rd "$DST_ZFS"
    delete_rd "$SRC"
    assert_no_orphans "$DST_CLI"
    assert_no_orphans "$DST_ZFS"
    assert_no_orphans "$SRC"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 020] source RD: 2 diskful replicas on $POOL"
"${LCTL[@]}" resource-definition create "$SRC" >/dev/null
"${LCTL[@]}" volume-definition create "$SRC" 64M >/dev/null
_out=$("${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1 $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$SRC" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N2 $SRC: $_out" >&2; exit 1; }
wait_uptodate "$SRC" "$N1" "$N2"

echo ">> [Bug 020] seed deterministic marker on $N1 $SRC"
on_node "$N1" drbdadm primary --force "$SRC" 2>/dev/null || true
dev=$(resolve_drbd_device "$N1" "$SRC" 0) || {
    echo "ABORT: could not resolve /dev/drbd for $SRC on $N1" >&2
    exit 2
}
on_node "$N1" bash -c \
    "printf '$MARKER' | dd of='$dev' bs=1 count=${#MARKER} conv=fsync status=none"
on_node "$N1" drbdadm secondary "$SRC" 2>/dev/null || true
wait_uptodate "$SRC" "$N1" "$N2"

# wait_clone_replicas <rd> [timeout] — the clone materialises its
# replicas asynchronously on the snapshot-holding nodes; poll until 2
# diskful Resource CRDs exist. Echoes the node list one per line.
wait_clone_replicas() {
    local rd=$1 timeout=${2:-120}
    local deadline=$(( $(date +%s) + timeout ))
    local nodes=()
    while (( $(date +%s) < deadline )); do
        mapfile -t nodes < <(linstor_diskful_nodes "$rd")
        if (( ${#nodes[@]} == 2 )); then
            printf '%s\n' "${nodes[@]}"
            return 0
        fi
        sleep 2
    done
    echo "wait_clone_replicas: $rd never materialised 2 diskful replicas (got ${#nodes[@]})" >&2
    return 1
}

# assert_clone_marker <rd> <node...> — promote each diskful replica of
# the clone in turn and read the marker region back. Catches the Bug
# 114 silent-empty-shell mode: UpToDate by DRBD, zeros on disk.
assert_clone_marker() {
    local rd=$1
    shift
    local nodes=("$@")
    local node other dev marker_read
    for node in "${nodes[@]}"; do
        for other in "${nodes[@]}"; do
            [[ "$other" == "$node" ]] && continue
            on_node "$other" drbdadm secondary "$rd" 2>/dev/null || true
        done
        dev=$(resolve_drbd_device "$node" "$rd" 0 2>/dev/null) || dev=""
        marker_read=$(on_node "$node" bash -c "
            drbdadm primary --force $rd 2>/dev/null || true
            if [ -n '$dev' ]; then
                head -c ${#MARKER} '$dev' 2>/dev/null
            fi
        " 2>/dev/null || echo "")
        on_node "$node" drbdadm secondary "$rd" 2>/dev/null || true
        if [[ "$marker_read" != "$MARKER" ]]; then
            echo "FAIL (Bug 020): replica $node of $rd does NOT hold the source bytes" >&2
            echo "  expected marker '$MARKER', read back '$marker_read'" >&2
            return 1
        fi
        echo "   $node: marker present"
    done
}

# assert_internal_snapshot <dst> — delta row 82: the clone's internal
# snapshot `clone-<dst>` lives on the SOURCE and must outlive the
# clone (zfs targets stay dependent on their origin snapshot).
assert_internal_snapshot() {
    local dst=$1
    if ! kubectl get "snapshots.blockstor.cozystack.io/${SRC}.clone-${dst}" \
            >/dev/null 2>&1; then
        echo "FAIL (Bug 020): internal snapshot ${SRC}.clone-${dst} not found" >&2
        kubectl get snapshots.blockstor.cozystack.io --no-headers 2>/dev/null >&2 || true
        return 1
    fi
}

# ---- variant A: plain CLI clone (no use_zfs_clone on the wire) ------------

echo ">> [Bug 020 / A] linstor resource-definition clone $SRC $DST_CLI"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource-definition clone "$SRC" "$DST_CLI" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 020): rd clone exited $rc (pre-fix: 501 on VD-bearing source)" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> [Bug 020 / A] clone replicas materialise + converge UpToDate"
mapfile -t cli_nodes < <(wait_clone_replicas "$DST_CLI" 120)
wait_uptodate "$DST_CLI" "${cli_nodes[0]}" "${cli_nodes[1]}"

echo ">> [Bug 020 / A] marker bytes present on EVERY clone replica"
assert_clone_marker "$DST_CLI" "${cli_nodes[@]}"
assert_internal_snapshot "$DST_CLI"

# ---- variant B: raw REST with use_zfs_clone=true (linstor-csi shape) ------

echo ">> [Bug 020 / B] POST /v1/resource-definitions/$SRC/clone use_zfs_clone=true"
http_code=$(curl -sS -m 30 -o /tmp/cli-matrix-020-clone.json -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"name\":\"${DST_ZFS}\",\"use_zfs_clone\":true}" \
    "http://127.0.0.1:${LCTL_PORT}/v1/resource-definitions/${SRC}/clone" \
    2>/dev/null || echo "000")
if [[ "$http_code" != "201" ]]; then
    echo "FAIL (Bug 020): use_zfs_clone=true POST answered HTTP $http_code, want 201" >&2
    echo "  (pre-fix: 400 DisallowUnknownFields on use_zfs_clone)" >&2
    cat /tmp/cli-matrix-020-clone.json >&2 2>/dev/null || true
    exit 1
fi

echo ">> [Bug 020 / B] GET clone status reaches COMPLETE"
deadline=$(( $(date +%s) + 60 ))
clone_status=""
while (( $(date +%s) < deadline )); do
    clone_status=$(curl -fsS -m 5 \
        "http://127.0.0.1:${LCTL_PORT}/v1/resource-definitions/${SRC}/clone/${DST_ZFS}" \
        2>/dev/null | jq -r '.status // empty' 2>/dev/null || echo "")
    if [[ "$clone_status" == "COMPLETE" ]]; then
        break
    fi
    sleep 2
done
if [[ "$clone_status" != "COMPLETE" ]]; then
    echo "FAIL (Bug 020): clone status for $DST_ZFS never reached COMPLETE (last='$clone_status')" >&2
    exit 1
fi

echo ">> [Bug 020 / B] clone replicas materialise + converge UpToDate"
mapfile -t zfs_nodes < <(wait_clone_replicas "$DST_ZFS" 120)
wait_uptodate "$DST_ZFS" "${zfs_nodes[0]}" "${zfs_nodes[1]}"

echo ">> [Bug 020 / B] marker bytes present on EVERY clone replica"
assert_clone_marker "$DST_ZFS" "${zfs_nodes[@]}"
assert_internal_snapshot "$DST_ZFS"

echo ">> rd-clone-vd-data-plane OK (Bug 020: both wire variants materialise a real data-plane clone)"
