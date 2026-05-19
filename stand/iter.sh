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

# Re-apply Deployment / DaemonSet manifests with the freshly-pushed
# image digests from `make build-images`. The manifests carry
# `image: __REGISTRY__/<name>:dev` placeholders;
# `stand/render-manifest.sh` rewrites them to
# `image: <reg>/<name>@sha256:<digest>` reading digests from
# .work/_factory/digest-<name>.txt — written by build-images.sh
# after each `docker push`. Digest pinning makes every rebuild a
# spec change, so `kubectl apply` triggers a real rolling update;
# without it, Runs 26-35 silently ran against stale apiserver code
# because the floating `:dev` tag never changed the Deployment
# spec and containerd cache served up an old digest under
# `imagePullPolicy: Always`.
#
# We don't call install-blockstor.sh here — its node-CR bootstrap
# and per-component waits are redundant on a stand that's already
# been installed; the rollout-status steps further down handle
# waiting. We only need the manifest re-apply step.
step "re-apply blockstor manifests (digest-pinned)" \
    "$REPO_ROOT/stand/render-manifest.sh $WORK_DIR $REPO_ROOT/stand/blockstor-deploy.yaml | kubectl apply -f - 2>&1 | tail -3 &&
     $REPO_ROOT/stand/render-manifest.sh $WORK_DIR $REPO_ROOT/stand/blockstor-apiserver-deploy.yaml | kubectl apply -f - 2>&1 | tail -3 &&
     $REPO_ROOT/stand/render-manifest.sh $WORK_DIR $REPO_ROOT/stand/blockstor-satellite-daemonset.yaml | kubectl apply -f - 2>&1 | tail -3" \
    || { echo "$NAME re-apply FAIL" > "$RESULT"; exit 1; }

# Graceful pod delete: controller goes force (no DRBD state to drain),
# satellites get the full terminationGracePeriodSeconds so the PreStop
# hook can run `drbdadm down --all` and release every DRBD connection
# before SIGTERM. Without the graceful drain, the kernel module (which
# is host-level on the Talos node — shared across pod restarts!) keeps
# half-open `Connecting` peer states forever, and the next iter's
# `drbdsetup down` blocks waiting for the gone peer to ack. PreStop is
# only invoked on normal termination, not on `--force --grace-period=0`.
step "force-delete controller pod" "kubectl -n blockstor-system delete pod -l app=blockstor-controller --grace-period=0 --force --ignore-not-found 2>&1 | tail -2" \
    || { echo "$NAME rollout FAIL" > "$RESULT"; exit 1; }

step "force-delete apiserver pods" "kubectl -n blockstor-system delete pod -l app=blockstor-apiserver --grace-period=0 --force --ignore-not-found 2>&1 | tail -3" \
    || { echo "$NAME rollout FAIL" > "$RESULT"; exit 1; }

step "graceful-delete satellite pods" "kubectl -n blockstor-system delete pod -l app=blockstor-satellite --ignore-not-found --timeout=60s 2>&1 | tail -3" \
    || { echo "$NAME rollout FAIL" > "$RESULT"; exit 1; }

step "rollout-status (controller)" "kubectl -n blockstor-system rollout status deploy/blockstor-controller --timeout=120s" \
    || { echo "$NAME rollout-controller FAIL" > "$RESULT"; exit 1; }

step "rollout-status (apiserver)" "kubectl -n blockstor-system rollout status deploy/blockstor-apiserver --timeout=120s" \
    || { echo "$NAME rollout-apiserver FAIL" > "$RESULT"; exit 1; }

step "rollout-status (satellite)" "kubectl -n blockstor-system rollout status ds/blockstor-satellite --timeout=120s" \
    || { echo "$NAME rollout-satellite FAIL" > "$RESULT"; exit 1; }

# Clean any leftover blockstor Resources / RDs from the previous
# iteration. We strip finalizers first because `kubectl delete
# --force --grace-period=0` only removes the grace period — it
# leaves finalizers in place. With the satellite already gone
# (force-deleted above before its PreStop could strip), nothing
# is going to clear those finalizers, and `kubectl delete` then
# hangs forever waiting for the apiserver to remove the object.
# Patching finalizers=[] makes the next delete actually succeed.
#
# Bug 285 cleanup: clear stale Node.Spec.Flags (most importantly
# EVICTED, which is sticky and disqualifies a node from being
# picked as a tiebreaker witness candidate; an auto-evict tick that
# fired during a controller restart while LastHeartbeatTime was nil
# would otherwise leave the next iter without a 3rd-node witness
# candidate — confirmed root cause of 5 of Run 5's e2e scenarios on
# stand e2e7). Strips the spec.flags array entirely on every Node;
# the auto-evict / heartbeat loops re-stamp it within seconds when
# the underlying condition is genuinely still true.
step "cleanup leftover" \
    "kubectl get resource -o name 2>/dev/null | xargs -r -I{} kubectl patch {} --type=merge -p '{\"metadata\":{\"finalizers\":[]}}' >/dev/null 2>&1 || true;
     kubectl delete resource --all --ignore-not-found --timeout=30s 2>&1 | tail -3;
     kubectl get resourcedefinitions -o name 2>/dev/null | xargs -r -I{} kubectl patch {} --type=merge -p '{\"metadata\":{\"finalizers\":[]}}' >/dev/null 2>&1 || true;
     kubectl delete resourcedefinition --all --ignore-not-found --timeout=30s 2>&1 | tail -3;
     kubectl get nodes.blockstor.io.blockstor.io -o name 2>/dev/null | xargs -r -I{} kubectl patch {} --type=json -p='[{\"op\":\"remove\",\"path\":\"/spec/flags\"}]' >/dev/null 2>&1 || true"

# Tear down any DRBD resources the kernel modules still hold on the
# satellite pods. `kubectl delete --force --grace-period=0` above
# bypasses the satellite finalizer that would normally run `drbdadm
# down`, so stale resources keep their DRBD minors (1000+) and the
# next scenario's create-md hits "Device '1000' is configured".
# Wipe per-resource .res + .md-created markers too — the satellite
# wipes /etc/drbd.d on startup but a no-restart iter (controller
# image unchanged) skips that path.
#
# Bug 285: use `drbdsetup down` not `drbdadm down` because the
# satellite's startup-side cleanStateDir() wipes /etc/drbd.d/*.res
# BEFORE this cleanup gets to run (the satellite cycle may have
# completed an extra restart between the previous iter and this
# one), and `drbdadm down <name>` then fails with `no resources
# defined!` because drbdadm can't enumerate without a .res file.
# `drbdsetup` operates directly on kernel state and doesn't need
# the .res file at all.
step "drbdsetup down stale resources on satellites" \
    "kubectl -n blockstor-system get pod -l app=blockstor-satellite -o name | while read p; do
        timeout 30 kubectl -n blockstor-system exec \$p -- bash -c 'for r in \$(drbdsetup status --json 2>/dev/null | python3 -c \"import json,sys; print(\\\" \\\".join(r[\\\"name\\\"] for r in json.load(sys.stdin)))\" 2>/dev/null); do timeout 5 drbdsetup down \$r 2>/dev/null || true; done; rm -f /etc/drbd.d/*.res /etc/drbd.d/*.md-created 2>/dev/null || true' 2>&1 | sed \"s|^|\$p: |\" || true;
    done"

step "e2e:$SCENARIO" "make e2e NAME=$NAME SCENARIO=$SCENARIO"
rc=$?

if [[ $rc -eq 0 ]]; then
    echo "$NAME $SCENARIO PASS" > "$RESULT"
else
    echo "$NAME $SCENARIO FAIL" > "$RESULT"
fi

echo ">>> $(date +%H:%M:%S) $NAME: done — $(cat "$RESULT")" | tee -a "$LOG"
