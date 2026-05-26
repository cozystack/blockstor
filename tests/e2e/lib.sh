#!/usr/bin/env bash
# Shared helpers for tests/e2e/*.sh — keeps each scenario script
# focused on the scenario itself, not on boilerplate. Sourced from the
# scenario, never executed directly.
#
# Conventions:
#   - All scripts take WORK_DIR as $1 (matches stand/Makefile).
#   - $KUBECONFIG is set from WORK_DIR/kubeconfig.
#   - Per-test timeout knobs live at the top of each script.
#   - Use on_node() to reach a satellite pod; never hard-code pod names.

set -euo pipefail

NS=${NS:-blockstor-system}

# Discover the cluster's worker node names so scripts can reference
# them as $WORKER_1, $WORKER_2, $WORKER_3 instead of hardcoding a
# specific cluster prefix (parallel stands name workers `<NAME>-worker-N`).
# Sorted alphabetically so $WORKER_1 == worker-1, etc.
mapfile -t _BS_WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort
)
WORKER_1="${_BS_WORKERS[0]:-}"
WORKER_2="${_BS_WORKERS[1]:-}"
WORKER_3="${_BS_WORKERS[2]:-}"
export WORKER_1 WORKER_2 WORKER_3

# on_node runs CMD inside the satellite pod scheduled on NODE.
# Wraps the jsonpath dance; quote args carefully.
on_node() {
    local node=$1
    shift
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")

    if [[ -z "$pod" ]]; then
        echo "no satellite pod on node $node" >&2
        return 1
    fi

    kubectl -n "$NS" exec "$pod" -- "$@"
}

# ---- k8s-native readers (preferred over drbdsetup-status grep) ----
#
# Replaces `kubectl exec satellite -- drbdsetup status ... | grep ...`
# bypass patterns with reads of Resource.Status, which the satellite-
# side events2 observer already populates from the same kernel state.
# See docs/test-status-cheatsheet.md for the full mapping table.

# status_disk_state <rd> <node> [volNum=0] — local kernel disk state for
# the named volume on the named node, as observed by the satellite and
# reflected on Resource.Status. Returns "UpToDate"/"Inconsistent"/
# "Outdated"/"Diskless"/"Failed"/"Negotiating"/"Attaching"/"Detaching",
# or empty string if the Resource is missing or the volume not yet
# observed. Prefer this over parsing `drbdsetup status | grep disk:`.
status_disk_state() {
    local rd=$1 node=$2 vol=${3:-0}
    kubectl get resource "${rd}.${node}" -o json 2>/dev/null \
        | jq -r --argjson v "${vol}" \
            '.status.volumes[]? | select(.volumeNumber==$v) | .diskState // ""'
}

# wait_disk_state <rd> <node> <expected> [timeout=60] [volNum=0] — poll
# Resource.Status until the given volume reaches the expected diskState
# or timeout. Non-zero exit on timeout.
wait_disk_state() {
    local rd=$1 node=$2 expected=$3 timeout=${4:-60} vol=${5:-0}
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if [[ "$(status_disk_state "$rd" "$node" "$vol")" == "$expected" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "wait_disk_state: $rd on $node vol $vol never reached $expected within ${timeout}s" >&2
    return 1
}

# status_role <rd> <node> — local DRBD-9 role on this replica. Returns
# "Primary" / "Secondary" / "Unknown" / "" (empty when the Resource is
# missing or the observer has not yet stamped a value). Status.Role
# shipped in commit a077afcf2 (Phase 11.5.b P0). Prefer this over
# `on_node "$node" drbdsetup status "$rd" | grep role:` — same kernel
# truth, no satellite-pod coupling.
status_role() {
    local rd=$1 node=$2
    kubectl get resource "${rd}.${node}" -o jsonpath='{.status.role}' 2>/dev/null
}

# status_suspended <rd> <node> — DRBD-9 I/O-suspension reason on this
# replica. Returns "" (= No, normal I/O), "Quorum", "User", "NoData",
# or "Fencing". Status.Suspended shipped in commit a077afcf2
# (Phase 11.5.b P0). Pair with status_volume_quorum() to distinguish
# "kernel lost quorum on this volume" from "operator manually
# suspended I/O". The pre-conversion bypass conflated
# `quorum:no | suspended:* | may_promote:no` into one grep — after
# conversion, pick the precise field the assertion needs.
status_suspended() {
    local rd=$1 node=$2
    kubectl get resource "${rd}.${node}" -o jsonpath='{.status.suspended}' 2>/dev/null
}

# status_volume_quorum <rd> <node> [volNum=0] — per-volume kernel
# quorum bool from events2 device frames. Returns "true" (has quorum)
# / "false" / empty. Status.Volumes[].Quorum shipped in commit
# 0cca4a942 (Phase 11.4.b P0). Per-volume, in contrast to the
# coarser node-wide `drbd.linbit.com/lost-quorum` k8s taint.
status_volume_quorum() {
    local rd=$1 node=$2 vol=${3:-0}
    kubectl get resource "${rd}.${node}" \
        -o jsonpath="{.status.volumes[?(@.volumeNumber==${vol})].quorum}" 2>/dev/null
}

# wait_role <rd> <node> <expected> [timeout=30] — poll Resource.Status
# until the local role reaches the expected value ("Primary" or
# "Secondary") or timeout. Non-zero exit on timeout. Useful when the
# test has just issued `drbdadm primary --force` and needs to wait for
# the observer to stamp the new role before sampling it.
wait_role() {
    local rd=$1 node=$2 expected=$3 timeout=${4:-30}
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if [[ "$(status_role "$rd" "$node")" == "$expected" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "wait_role: $rd on $node never reached role=$expected within ${timeout}s" >&2
    return 1
}

# wait_uptodate POD waits up to 180s for both replicas of $RD to reach
# disk:UpToDate. Caller defines $RD and the two node names $PRIMARY,
# $PEER before calling. Optional 4th arg picks a non-default volume
# number (defaults to 0 for back-compat with single-volume RDs). Exits
# non-zero on timeout. Initial sync on a fresh DRBD resource on a busy
# QEMU stand can take 60-120s; 180s is the safety margin.
wait_uptodate() {
    local rd=$1 primary=$2 peer=$3 vol=${4:-0}
    local deadline=$(( $(date +%s) + 180 ))

    while (( $(date +%s) < deadline )); do
        local p1 p2
        p1=$(status_disk_state "$rd" "$primary" "$vol")
        p2=$(status_disk_state "$rd" "$peer" "$vol")

        if [[ "$p1" == "UpToDate" && "$p2" == "UpToDate" ]]; then
            return 0
        fi

        sleep 2
    done

    echo "FAIL: $rd vol $vol never reached UpToDate on both peers" >&2
    return 1
}

# status_connection_state <rd> <node> <peer> — full kernel connection
# state string as observed FROM `node` TOWARD `peer`: Connected /
# Connecting / StandAlone / BrokenPipe / NetworkFailure / Timeout /
# Established / Unconnected / Disconnecting / ProtocolError / TearDown /
# WFConnection. Returns "" if the connection row hasn't been observed
# yet (Resource missing or pre-events2). Prefer this over parsing
# `drbdsetup status --verbose | grep -oE 'connection:[A-Za-z]+'`.
status_connection_state() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .message // ""'
}

# status_connected <rd> <node> <peer> — derived bool ("true"/"false")
# from the observer snapshot: true iff the (node,peer) connection is
# Connected/Established at the kernel level. Useful when the test only
# cares about "are they talking" rather than the exact state.
status_connected() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .connected // false'
}

# status_replication_state <rd> <node> <peer> — per-peer DRBD-9
# replication state machine: Established / SyncSource / SyncTarget /
# PausedSyncS / PausedSyncT / VerifyS / VerifyT / Ahead / Behind / Off /
# WFBitMapS / WFBitMapT / WFSyncUUID / StartingSync[ST]. Prefer this
# over parsing `drbdsetup status --verbose | grep replication:`.
status_replication_state() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .replicationState // ""'
}

# wait_connection_state <rd> <node> <peer> <want> [timeout=60] — poll
# Resource.Status.connections until the (node,peer) connection's
# `message` matches WANT, or timeout elapses. WANT may be a literal
# ("Connected") or an alternation ("Connected|Established"). Non-zero
# exit on timeout.
wait_connection_state() {
    local rd=$1 node=$2 peer=$3 want=$4 timeout=${5:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=""
    while (( $(date +%s) < deadline )); do
        cur=$(status_connection_state "$rd" "$node" "$peer")
        if [[ "$cur" =~ ^(${want})$ ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_connection_state: ${rd}.${node}<->${peer} never reached '${want}' (last='${cur}') within ${timeout}s" >&2
    return 1
}

# wait_replication_state <rd> <node> <peer> <want> [timeout=60] — poll
# Resource.Status.connections until replicationState matches WANT, or
# timeout elapses. WANT supports alternation, e.g. "SyncTarget|PausedSyncT".
wait_replication_state() {
    local rd=$1 node=$2 peer=$3 want=$4 timeout=${5:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local cur=""
    while (( $(date +%s) < deadline )); do
        cur=$(status_replication_state "$rd" "$node" "$peer")
        if [[ "$cur" =~ ^(${want})$ ]]; then
            return 0
        fi
        sleep 2
    done
    echo "wait_replication_state: ${rd}.${node}<->${peer} never reached '${want}' (last='${cur}') within ${timeout}s" >&2
    return 1
}

# device_for_rd resolves the local /dev/drbdN minor for an RD.
device_for_rd() {
    local rd=$1 node=$2
    on_node "$node" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${rd}.res | head -1"
}

# write_random NODE DEV BYTES — write urandom to the device, return md5.
# BYTES is rounded up to a 4096-byte block (direct I/O alignment).
#
# Talos guard: on no-udev systems an `open(2)` against a missing
# `/dev/drbd<N>` silently creates a regular file in tmpfs and dd
# writes into THAT instead of the kernel's DRBD device. Tests then
# observe a false PASS because the read-back hash matches what was
# just written to tmpfs. The `test -b` guard ABORTs the test instead
# so a regression in EnsureDeviceNode (pkg/drbd/devnode.go) surfaces
# loudly. Returns exit 2 (distinct from dd's exit 1) so test-runner
# logs make the failure mode unambiguous.
write_random() {
    local node=$1 dev=$2 bytes=$3
    local blocks=$(( (bytes + 4095) / 4096 ))
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        test -b ${dev} || { echo \"ABORT: ${dev} is not a block device — \$(stat -c '%F' ${dev} 2>/dev/null || echo missing)\" >&2; exit 2; }
        dd if=/dev/urandom of=${dev} bs=4096 count=${blocks} status=none oflag=direct
        dd if=${dev} bs=4096 count=${blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
    "
}

# read_md5 NODE DEV BYTES — read first BYTES of DEV, return md5.
# Same alignment rules as write_random.
#
# Talos guard: same rationale as write_random above. A missing
# `/dev/drbd<N>` would let dd read all-zeros from a tmpfs regular file
# and return a deterministic-but-wrong hash. Abort instead.
read_md5() {
    local node=$1 dev=$2 bytes=$3
    local blocks=$(( (bytes + 4095) / 4096 ))
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        test -b ${dev} || { echo \"ABORT: ${dev} is not a block device — \$(stat -c '%F' ${dev} 2>/dev/null || echo missing)\" >&2; exit 2; }
        dd if=${dev} bs=4096 count=${blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
    "
}

# delete_rd cleans up an RD + every Resource named after it + every
# Snapshot of the RD. Trapped from each scenario so partial runs
# don't leave orphans that trip the next test's wait_uptodate with
# stale kernel / .res / marker / snapshot state. Belt-and-suspenders
# at every layer:
#
#   - delete Snapshot CRDs (otherwise the satellite-side reconciler
#     re-asserts kernel state for "still-needed-for-snapshot" devices)
#   - delete Resource CRDs (waits on finalizers; the satellite-side
#     teardown chain runs drbdadm down + provider.DeleteVolume)
#   - delete the RD CRD
#   - on every satellite: drbdsetup down + remove .res + remove the
#     .md-created marker (otherwise re-create with the same name
#     skips drbdadm create-md and trips 'No valid meta data found')
delete_rd() {
    local rd=$1

    kubectl get snapshots.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' \
        | xargs -r kubectl delete --wait=true --timeout=30s snapshots.blockstor.cozystack.io 2>/dev/null || true
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' \
        | xargs -r kubectl delete --wait=true --timeout=30s resources.blockstor.cozystack.io 2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resourcedefinitions.blockstor.cozystack.io/${rd}" 2>/dev/null || true

    # Force-kill any lingering kernel-level state for this RD. The
    # marker-file cleanup is essential — leaving .md-created behind
    # makes the next re-create with the same RD name silently skip
    # drbdadm create-md, so drbdadm adjust then fails with 'No valid
    # meta data found' on the freshly-allocated lower disk.
    #
    # Outer + inner timeouts: `drbdsetup down` can hang forever if
    # the kernel module has a half-open connection to a force-deleted
    # peer (DRBD-9 keeps trying to gracefully tear). Without these
    # the test's EXIT trap blocks the next scenario indefinitely.
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        timeout 15 kubectl -n "$NS" exec "$pod" -- bash -c "
            timeout 5 drbdsetup down ${rd} 2>/dev/null || true
            rm -f /etc/drbd.d/${rd}.res /etc/drbd.d/${rd}.md-created
            rm -f /var/lib/blockstor-pool/${rd}_*.partial 2>/dev/null || true
        " 2>/dev/null || true
    done
}

# delete_all_rds wipes EVERY ResourceDefinition / Resource / Snapshot
# CRD on the cluster, then waits up to `timeout` seconds for the stand
# to converge to idle. Pre-test self-defence helper: scenarios that
# assert post-state invariants (`linstor r l` empty, no orphan ZVOLs,
# zero dmesg warnings) MUST call this at the top so leakage from
# previously-run scenarios (cc-autoplace.* from client-compat.sh,
# e2e-clone-src from clone.sh, e2e-affinity-controller from
# affinity-controller.sh, …) doesn't get blamed on the current run.
#
# Unlike per-test `delete_rd <name>` traps — which fire only on EXIT
# and skip resources whose owning RD was already torn down by an
# earlier failure — this enumerates the live CRDs and feeds each into
# `delete_rd` so the satellite-side kernel teardown (drbdsetup down +
# .res / .md-created / .owned removal) runs for every leaked slot.
#
# Honours the same "NEVER force-strip finalizers" pin as the cascade
# tests: each `delete_rd` waits for cascade with a 30s timeout. If
# anything is still present after `timeout`, logs to stderr and
# returns non-zero so the caller can dump_diag + exit rather than
# silently masking the prior-test bug.
delete_all_rds() {
    local timeout=${1:-90}

    # Enumerate first; iterate by name so an empty list is a no-op.
    local rds
    rds=$(kubectl get resourcedefinitions.blockstor.cozystack.io \
        --no-headers 2>/dev/null | awk '{print $1}')
    if [[ -n "$rds" ]]; then
        echo ">> delete_all_rds: pre-test wipe of: $(echo "$rds" | tr '\n' ' ')"
        local rd
        for rd in $rds; do
            delete_rd "$rd" 2>/dev/null || true
        done
    fi

    # Mop up any stray Resource / Snapshot CRDs whose owning RD was
    # already gone (so the loop above didn't touch them). delete_rd
    # only matches by RD prefix, so a hand-leaked `foo.node` without
    # a `foo` RD slips past it.
    local res
    res=$(kubectl get resources.blockstor.cozystack.io \
        --no-headers 2>/dev/null | awk '{print $1}')
    if [[ -n "$res" ]]; then
        echo ">> delete_all_rds: orphan Resource sweep: $(echo "$res" | tr '\n' ' ')"
        echo "$res" | xargs -r kubectl delete --wait=true --timeout=30s \
            resources.blockstor.cozystack.io 2>/dev/null || true
    fi
    local snaps
    snaps=$(kubectl get snapshots.blockstor.cozystack.io \
        --no-headers 2>/dev/null | awk '{print $1}')
    if [[ -n "$snaps" ]]; then
        echo "$snaps" | xargs -r kubectl delete --wait=true --timeout=30s \
            snapshots.blockstor.cozystack.io 2>/dev/null || true
    fi

    # Wait until the kernel + CRD state converges.
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        local crd_count
        crd_count=$( {
            kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null
            kubectl get resourcedefinitions.blockstor.cozystack.io --no-headers 2>/dev/null
            kubectl get snapshots.blockstor.cozystack.io --no-headers 2>/dev/null
        } | grep -cv '^$' || true )

        if [[ "$crd_count" == "0" ]]; then
            return 0
        fi

        sleep 2
    done

    echo "delete_all_rds: cluster still has CRDs after ${timeout}s:" >&2
    kubectl get resourcedefinitions.blockstor.cozystack.io --no-headers 2>/dev/null >&2 || true
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null >&2 || true
    return 1
}

# wait_cluster_idle waits until the stand is back to a clean slate
# between back-to-back e2e scenarios on the same cluster — no
# blockstor CRDs for resources / RDs / snapshots, and no kernel-side
# DRBD configuration. Returns success once both layers are empty or
# after the deadline expires (best-effort; logs to stderr but doesn't
# fail). The batch driver should call this before launching the next
# scenario so resize-luks / linstor-cli / cross-node don't observe
# the previous test's residue.
wait_cluster_idle() {
    local deadline=$(( $(date +%s) + 30 ))

    while (( $(date +%s) < deadline )); do
        local crd_count drbd_busy=0
        crd_count=$( {
            kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null
            kubectl get resourcedefinitions.blockstor.cozystack.io --no-headers 2>/dev/null
            kubectl get snapshots.blockstor.cozystack.io --no-headers 2>/dev/null
        } | grep -cv '^$' || true )

        for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
            local out
            out=$(kubectl -n "$NS" exec "$pod" -- drbdsetup status 2>/dev/null || true)
            if [[ "$out" != "" && "$out" != *"No currently configured DRBD found"* ]]; then
                drbd_busy=1

                break
            fi
        done

        if [[ "$crd_count" == "0" && "$drbd_busy" == "0" ]]; then
            return 0
        fi

        sleep 2
    done

    echo "wait_cluster_idle: timed out, stand may still have residue" >&2

    return 0
}

# reset_cluster_state forces the cluster back to a clean slate between
# back-to-back e2e scenarios run by stand/run-scenarios-only.sh.
#
# Unlike wait_cluster_idle (which only *waits* and never mutates), this
# actively tears down leftover state the way stand/iter.sh does between
# dev-loop iterations: it deletes every RD, force-strips stuck
# satellite-resource finalizers, runs `drbdsetup down` for any kernel-
# resident DRBD resource on every satellite, wipes per-resource
# .res / .md-created markers, and removes orphan regular-file
# /dev/drbd<N> entries.
#
# Why this is needed: the batch dispatcher (run-scenarios-only.sh)
# previously ran scenarios back-to-back with NO inter-scenario cleanup.
# A test that crashed mid-flight (or force-deleted satellites) left
# orphan kernel slots, stale .res files, and stuck finalizers behind —
# wedging every subsequent scenario into a cascade of false FAILs
# (Run 54: 7+ cascade victims). Calling this between scenarios gives
# each one the same clean cluster that `make iter` does.
#
# The .owned markers are deliberately left in place — they record which
# StoragePool owns a backing device and are not per-RD scratch state.
# Best-effort throughout: every step swallows its own errors so a
# partially-wedged cluster still gets maximally cleaned. Returns the
# exit status of the final delete_all_rds convergence wait so the
# caller can log (but not necessarily fail on) a stand that won't drain.
reset_cluster_state() {
    local timeout=${1:-90}

    echo ">> reset_cluster_state: begin $(date -Iseconds)"

    # 0. Force-sweep satellite pods stuck Terminating.
    #    rolling-upgrade restarts the satellite DaemonSet; under back-to-
    #    back execution a satellite pod can wedge in Terminating for tens
    #    of minutes — a kubelet/containerd stuck-stop, NOT a finalizer
    #    block (the pod carries no finalizer), so the finalizer-strip in
    #    step 1 cannot clear it. A dead-but-present pod makes the next
    #    scenario (e.g. satellite-utils-smoke) exec into it and fail every
    #    probe. Scope strictly to app=blockstor-satellite so we never nuke
    #    healthy workloads, and only force-delete pods whose
    #    deletionTimestamp is older than 30s (genuine graceful shutdowns
    #    finish well within that). Idempotent: a no-op when nothing is
    #    Terminating.
    local now_epoch sweep_pod sweep_dt sweep_age
    now_epoch=$(date +%s)
    while read -r sweep_pod sweep_dt; do
        [ -n "$sweep_pod" ] || continue
        [ -n "$sweep_dt" ] && [ "$sweep_dt" != "<none>" ] || continue
        sweep_age=$(( now_epoch - $(date -d "$sweep_dt" +%s 2>/dev/null || echo "$now_epoch") ))
        if (( sweep_age >= 30 )); then
            echo ">> reset_cluster_state: force-deleting stuck-Terminating satellite pod $sweep_pod (Terminating ${sweep_age}s)"
            kubectl -n "$NS" delete pod "$sweep_pod" \
                --grace-period=0 --force >/dev/null 2>&1 || true
        fi
    done < <(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o 'jsonpath={range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}' 2>/dev/null)

    # 1. Strip stuck satellite-resource finalizers BEFORE the RD wipe.
    #    If a previous scenario force-deleted its satellite pods, nothing
    #    is left to clear the finalizer and `kubectl delete` would hang.
    #    Patching finalizers=[] makes the subsequent delete_all_rds
    #    actually complete instead of blocking on the apiserver.
    kubectl get resources.blockstor.cozystack.io -o name 2>/dev/null \
        | xargs -r -I{} kubectl patch {} --type=merge \
            -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
    kubectl get resourcedefinitions.blockstor.cozystack.io -o name 2>/dev/null \
        | xargs -r -I{} kubectl patch {} --type=merge \
            -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true

    # 2. Tear down RDs / Resources / Snapshots and wait for the CRD layer
    #    + per-RD kernel teardown (delete_rd runs drbdsetup down + .res /
    #    .md-created removal for each enumerated RD) to converge.
    local rc=0
    delete_all_rds "$timeout" || rc=$?

    # 3. Sweep any DRBD resource the kernel still holds that delete_all_rds
    #    couldn't enumerate (its owning RD was already gone, so delete_rd
    #    never named it). `drbdsetup down` operates directly on kernel
    #    state and doesn't need a .res file. Wipe per-resource .res /
    #    .md-created markers too; leave .owned markers alone.
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        timeout 30 kubectl -n "$NS" exec "$pod" -- bash -c '
            for r in $(drbdsetup status --json 2>/dev/null \
                | python3 -c "import json,sys; print(\" \".join(r[\"name\"] for r in json.load(sys.stdin)))" 2>/dev/null); do
                timeout 5 drbdsetup down "$r" 2>/dev/null || true
            done
            rm -f /etc/drbd.d/*.res /etc/drbd.d/*.md-created 2>/dev/null || true
        ' 2>/dev/null || true
    done

    # 4. Wipe residual /dev/drbd<N> entries that are REGULAR FILES (not
    #    block devices) — a previous scenario's `dd of=/dev/drbdN` that
    #    raced ahead of the kernel uevent leaves a regular file that then
    #    blocks devtmpfs from auto-creating the real block node. Preserve
    #    genuine block-device entries (the kernel owns those).
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        timeout 15 kubectl -n "$NS" exec "$pod" -- bash -c '
            for f in /dev/drbd*; do
                [ -e "$f" ] && [ -f "$f" ] && [ ! -b "$f" ] && rm -f "$f"
            done
            true
        ' 2>/dev/null || true
    done

    # 5. Reassert the lvm-thin backing VG on any worker where it went
    #    missing. lifecycle-toggle-retry.sh deliberately destroys the
    #    `blockstor-lvm` VG to provoke an lvcreate failure and restores
    #    it in its own EXIT trap — but if that scenario is killed
    #    mid-flight (timeout, force-deleted satellite, dispatcher crash)
    #    the trap never runs, leaving the VG gone (and stale dm nodes /
    #    a dangling /dev/blockstor-lvm dir) on that worker. Every later
    #    lvm-thin scenario then fails with poolMissing. Centralising the
    #    recreation here makes recovery independent of any one trap.
    #    Mirrors stand/install-pools.sh create_lvm (same dm/dir/PV scrub
    #    and udev-less activation flags); the backing device is the
    #    spare disk that is free or already lvm-owned (never the zfs
    #    disk). No-op when the VG already exists.
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        timeout 60 kubectl -n "$NS" exec "$pod" -- bash -c '
            vgs blockstor-lvm >/dev/null 2>&1 && exit 0
            # pick the lvm backing disk: a whole "disk" (not loop/cdrom,
            # not the OS disk identified by a mounted/vfat partition)
            # that is free or already LVM2_member, never zfs_member.
            os_disks=$(lsblk -nro NAME,FSTYPE,MOUNTPOINT,PKNAME \
                | awk "(\$3!=\"\" || \$2==\"vfat\") && \$4!=\"\" {print \$4}" | sort -u)
            dev=""
            for d in $(lsblk -nrdo NAME,TYPE | awk -v os="$os_disks" "
                    BEGIN { n=split(os,a,\"\n\"); for(i=1;i<=n;i++) skip[a[i]]=1 }
                    \$2==\"disk\" && \$1!~/^loop/ && \$1!~/^sr/ && !(\$1 in skip) {print \$1}"); do
                own=$(lsblk -nro FSTYPE /dev/$d 2>/dev/null \
                    | awk "/zfs_member/{z=1} /LVM2_member/{l=1} END{print (z?\"zfs\":(l?\"lvm\":\"free\"))}")
                if [ "$own" = "lvm" ]; then dev=/dev/$d; break; fi
                if [ "$own" = "free" ] && [ -z "$dev" ]; then dev=/dev/$d; fi
            done
            [ -n "$dev" ] || { echo "reset_cluster_state: no lvm spare disk on $(hostname)" >&2; exit 0; }
            echo "reset_cluster_state: recreating blockstor-lvm on $dev"
            for dm in $(dmsetup ls 2>/dev/null | awk "/^blockstor--lvm/{print \$1}"); do
                dmsetup remove "$dm" 2>/dev/null || true
            done
            rm -rf /dev/blockstor-lvm 2>/dev/null || true
            pvremove -ff -y "$dev" 2>/dev/null || true
            wipefs -af "$dev" 2>/dev/null || true
            vgcreate -y blockstor-lvm "$dev" || exit 0
            CFG="activation{udev_sync=0 udev_rules=0}"
            lvcreate --config "$CFG" -y -Wn -Zn -L 1G  blockstor-lvm -n thin_meta || exit 0
            lvcreate --config "$CFG" -y -Wn -Zn -L 13G blockstor-lvm -n thin      || exit 0
            lvconvert --config "$CFG" -y -Wn -Zn --type thin-pool \
                --poolmetadata blockstor-lvm/thin_meta blockstor-lvm/thin || exit 0
            echo "reset_cluster_state: blockstor-lvm/thin recreated"
        ' 2>/dev/null || true
    done

    echo ">> reset_cluster_state: done $(date -Iseconds) (delete_all_rds rc=$rc)"
    return $rc
}

# skip emits the e2e SKIP sentinel and exits 0. `make e2e` collapses any
# non-zero script exit into make's own exit 2, so an exit-code SKIP
# convention cannot survive the make wrapper — instead we print a stdout
# sentinel that run-scenarios-only.sh greps for in the per-scenario log
# and reclassifies as SKIP (for the env-gated allowlist) or FAIL (any
# other scenario that opts out is a regression — a mandatory test must
# not silently disappear). Exit 0 so make itself reports success.
skip() {
    echo "__E2E_SKIP__: $*"
    exit 0
}

# require_workers enforces that the cluster has at least N satellite
# nodes Ready AND at least N satellite pods Ready (Bug 298). The pod-
# readiness check guards against the previous-test-cascade pattern:
# rolling-upgrade leaves a satellite pod stuck Terminating with DRBD
# kernel state in a half-open Connecting slot. The Node row stays
# Ready (kubelet/Talos is fine), so the bare `kubectl get nodes` check
# would let the next test race ahead and silently observe only N-1
# usable satellites — typically surfacing as "only 2/3 reached
# UpToDate" or "destination never reached UpToDate" failures that
# falsely blame the next test. Wait briefly for residual Terminating
# pods to clear; bail with SKIP (not FAIL) if they don't, so the
# test result reflects the actual cascade rather than masking it.
require_workers() {
    local want=$1
    local got
    got=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' --no-headers 2>/dev/null \
        | awk '$2 == "Ready"' | wc -l)

    if (( got < want )); then
        skip "scenario needs $want satellite workers, found $got"
    fi

    # Bug 298: wait up to 30s for residual Terminating satellite pods
    # from a prior scenario's cascade. A Terminating pod on a worker
    # means DRBD on that node is unreachable to subsequent tests; the
    # heartbeat watchdog will eventually flip the Node row OFFLINE and
    # the test will start observing fewer healthy replicas than it
    # placed. Catch this early with a SKIP so the failure attribution
    # is correct.
    local deadline=$(( $(date +%s) + 30 ))
    local ready_pods=0
    while (( $(date +%s) < deadline )); do
        ready_pods=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o 'jsonpath={range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name} {end}' 2>/dev/null \
            | wc -w)
        local total_pods
        total_pods=$(kubectl -n "$NS" get pods -l app=blockstor-satellite --no-headers 2>/dev/null | wc -l)
        if (( ready_pods >= want )) && (( ready_pods == total_pods )); then
            return 0
        fi
        sleep 2
    done

    if (( ready_pods < want )); then
        skip "scenario needs $want Ready satellite pods, found $ready_pods (previous-test cascade — check for Terminating pods)"
    fi
}

# rest_post POSTs JSON BODY to PATH on the in-cluster controller.
# Uses kubectl-port-forward + a host-side curl/wget so we don't need
# curl in the distroless controller image. Path starts with /v1.
rest_post() {
    local path=$1 body=$2

    # Random ephemeral port so back-to-back rest_post / rest_put
    # calls don't collide on TIME_WAIT remnants from the previous
    # port-forward — observed on clone.sh where the second
    # rest_post would bind a stale socket and curl would error 22.
    local lport
    lport=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    kubectl -n "$NS" port-forward svc/blockstor-controller "${lport}:3370" >/dev/null 2>&1 &
    local pf=$!

    _wait_port_forward "$lport" "$pf"

    local out
    out=$(curl -fsS -XPOST -H'Content-Type: application/json' \
        "http://127.0.0.1:${lport}${path}" -d "$body")

    kill "$pf" 2>/dev/null || true
    wait "$pf" 2>/dev/null || true

    echo "$out"
}

# rest_put is the PUT variant of rest_post — same port-forward dance.
rest_put() {
    local path=$1 body=$2

    local lport
    lport=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    kubectl -n "$NS" port-forward svc/blockstor-controller "${lport}:3370" >/dev/null 2>&1 &
    local pf=$!

    _wait_port_forward "$lport" "$pf"

    local out
    out=$(curl -fsS -XPUT -H'Content-Type: application/json' \
        "http://127.0.0.1:${lport}${path}" -d "$body")

    kill "$pf" 2>/dev/null || true
    wait "$pf" 2>/dev/null || true

    echo "$out"
}

# _wait_port_forward blocks until the forwarded socket actually
# answers (probed via /v1/healthz which is a no-store, no-cache
# 204 from the controller). The flat `sleep 1` it replaces lost
# races under 17-stand parallel-iter load — kubectl port-forward
# can take >1 s to bind to a free local port when the apiserver
# is busy, and curl then fails with `(7) Failed to connect`.
_wait_port_forward() {
    local lport=$1 pf=$2 attempt

    for attempt in $(seq 1 30); do
        if curl -fsS -m 1 "http://127.0.0.1:${lport}/v1/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done

    echo "rest_post/put: port-forward to :${lport} never bound" >&2
    kill "$pf" 2>/dev/null || true
    return 1
}

# rd_apply applies a 2-replica RD with given size onto the named pair
# of workers. Used by scenarios that don't need the full apply boilerplate.
#
# Default pool is `stand` (FILE_THIN, sparse .img + losetup) because the
# majority of scenarios exercise control-plane / DRBD behaviour and do
# not care about the backing-storage byte-perfect contract. Tests that
# write payload bytes and read them back on a different replica MUST
# opt into ZFS_THIN (set STORPOOL=zfs-thin or pass POOL as 5th arg) —
# DRBD-on-loopfile has a known kernel write-path interaction that, while
# mitigated by --direct-io=on on fresh attach (commit f06830296),
# still bites on residue loop attachments (DIO=0 surviving satellite
# restart) and on snapshot-restore / clone paths whose fresh-attach
# timing differs from the live-write path. ZFS-backed zvols are the
# architecturally correct substrate for byte-integrity scenarios.
#
# Override via STORPOOL env var or rd_apply RD P1 P2 SIZE POOL.
rd_apply() {
    local rd=$1 primary=$2 peer=$3 size=${4:-65536} pool=${5:-${STORPOOL:-stand}}
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${rd}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${size}}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${primary}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${primary}
  props: {StorPoolName: ${pool}}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${peer}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${peer}
  props: {StorPoolName: ${pool}}
EOF
}
