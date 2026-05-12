#!/usr/bin/env bash
#
# usage: iter.sh <stand-name> <scenario>
#
# One iteration of the dev loop on ONE stand: roll the
# controller+satellite to the latest image already in the local
# registry, clean any leftover blockstor RDs, and run a single
# e2e scenario.
#
# IMPORTANT: iter does NOT rebuild — it expects `make build-images`
# to have been run once by the operator after their `git push`.
# That keeps multiple parallel iters on different stands from
# racing on the same `docker build`. Workflow:
#
#   # one-shot, after editing+pushing a fix:
#   git pull && make build-images
#   # then fan out to as many stands as needed:
#   make iter NAME=e2e1 SCENARIO=auto-diskful &
#   make iter NAME=e2e2 SCENARIO=two-volume-rd &
#   make iter NAME=e2e3 SCENARIO=tiebreaker &
#   …
#
# Each stand's result lands in /tmp/iter-<stand>.{log,result};
# `grep PASS /tmp/iter-*.result` gives the current matrix.

set -u

NAME="${1:?stand name required (e.g. e2e1)}"
SCENARIO="${2:?scenario name required (e.g. auto-diskful)}"

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

LOG="/tmp/iter-${NAME}.log"
RESULT="/tmp/iter-${NAME}.result"
WORK_DIR="$REPO_ROOT/.work/$NAME"

if [[ ! -d "$WORK_DIR" ]]; then
    echo "FAIL: $WORK_DIR does not exist — run \`make up NAME=$NAME\` first" >&2
    exit 2
fi

: > "$LOG"
: > "$RESULT"

export KUBECONFIG="$WORK_DIR/kubeconfig"

step() {
    echo ">>> $(date +%H:%M:%S) $NAME: $1" | tee -a "$LOG"
    bash -c "$2" >> "$LOG" 2>&1
    local rc=$?
    echo "<<< $(date +%H:%M:%S) $NAME: $1 rc=$rc" | tee -a "$LOG"
    return $rc
}

# Reapply CRDs first — schema additions in this branch (Resource.
# Status.Connections etc.) only land when the apiserver knows about
# them, otherwise the satellite observer's SSA fails with `field not
# declared in schema` and the resource reconciler stalls.
step "apply CRDs" "kubectl apply -f $REPO_ROOT/config/crd/bases" \
    || { echo "$NAME apply-crds FAIL" > "$RESULT"; exit 1; }

# Force-delete pods (not just rollout restart) — observed on the
# 17-stand matrix: a plain rollout-restart with imagePullPolicy:Always
# still uses the previously-pulled image layer for ~half the stands
# even after a `--no-cache` rebuild + push. Force-delete makes
# kubelet do a fresh `containerd images pull` on pod recreate, which
# resolves the `:dev` tag to the current registry manifest digest.
step "force-delete pods" "kubectl -n blockstor-system delete pod -l app=blockstor-controller --grace-period=0 --force --ignore-not-found 2>&1 | tail -2; kubectl -n blockstor-system delete pod -l app=blockstor-satellite --grace-period=0 --force --ignore-not-found 2>&1 | tail -3" \
    || { echo "$NAME rollout FAIL" > "$RESULT"; exit 1; }

step "rollout-status (controller)" "kubectl -n blockstor-system rollout status deploy/blockstor-controller --timeout=120s" \
    || { echo "$NAME rollout-controller FAIL" > "$RESULT"; exit 1; }

step "rollout-status (satellite)" "kubectl -n blockstor-system rollout status ds/blockstor-satellite --timeout=120s" \
    || { echo "$NAME rollout-satellite FAIL" > "$RESULT"; exit 1; }

# Clean any leftover blockstor Resources / RDs from the previous
# iteration. Force + grace-period=0 because a hung finalizer
# from a satellite-side bug we're trying to fix shouldn't block
# the next attempt.
step "cleanup leftover" \
    "kubectl delete resource --all --force --grace-period=0 --ignore-not-found 2>&1 | tail -3; kubectl delete resourcedefinition --all --ignore-not-found 2>&1 | tail -3"

# Tear down any DRBD resources the kernel modules still hold on the
# satellite pods. `kubectl delete --force --grace-period=0` above
# bypasses the satellite finalizer that would normally run `drbdadm
# down`, so stale resources keep their DRBD minors (1000+) and the
# next scenario's create-md hits "Device '1000' is configured".
# Wipe per-resource .res + .md-created markers too — the satellite
# wipes /etc/drbd.d on startup but a no-restart iter (controller
# image unchanged) skips that path.
step "drbdadm down stale resources on satellites" \
    "kubectl -n blockstor-system get pod -l app=blockstor-satellite -o name | while read p; do
        kubectl -n blockstor-system exec \$p -- bash -c '
            for r in \$(drbdsetup status --json 2>/dev/null | python3 -c \"import json,sys; print(\\\" \\\".join(r[\\\"name\\\"] for r in json.load(sys.stdin)))\" 2>/dev/null); do
                drbdsetup down \$r 2>&1 || drbdsetup del-resource \$r 2>&1 || true;
            done;
            rm -f /etc/drbd.d/*.res /etc/drbd.d/*.md-created 2>/dev/null || true
        ' 2>&1 | sed \"s|^|\$p: |\";
    done"

step "e2e:$SCENARIO" "make e2e NAME=$NAME SCENARIO=$SCENARIO"
rc=$?

if [[ $rc -eq 0 ]]; then
    echo "$NAME $SCENARIO PASS" > "$RESULT"
else
    echo "$NAME $SCENARIO FAIL" > "$RESULT"
fi

echo ">>> $(date +%H:%M:%S) $NAME: done — $(cat "$RESULT")" | tee -a "$LOG"
