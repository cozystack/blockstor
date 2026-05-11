#!/usr/bin/env bash
#
# usage: iter.sh <stand-name> <scenario>
#
# One iteration of the dev loop: pull HEAD, rebuild images (cached
# layers when code is unchanged), roll the controller+satellite on
# the named stand, clean any leftover blockstor RDs, and run a
# single e2e scenario.
#
# Idiomatic flow:
#   - operator edits + git pushes a fix
#   - on the stand: `make iter NAME=eXX SCENARIO=foo`
#   - while the e2e is running, operator reads other stands' logs,
#     spots the next bug, pushes another fix
#   - `make iter NAME=eYY SCENARIO=bar` updates a different stand
#     to the new image while eXX is still running its scenario
#
# Result lands in /tmp/iter-<stand>.{log,result}.

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

step "git pull" "git pull --ff-only origin main 2>&1 | tail -5" \
    || { echo "$NAME pull FAIL" > "$RESULT"; exit 1; }

step "build-images" "make build-images" \
    || { echo "$NAME build FAIL" > "$RESULT"; exit 1; }

step "rollout-restart" "kubectl -n blockstor-system rollout restart deploy/blockstor-controller ds/blockstor-satellite" \
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

step "e2e:$SCENARIO" "make e2e NAME=$NAME SCENARIO=$SCENARIO"
rc=$?

if [[ $rc -eq 0 ]]; then
    echo "$NAME $SCENARIO PASS" > "$RESULT"
else
    echo "$NAME $SCENARIO FAIL" > "$RESULT"
fi

echo ">>> $(date +%H:%M:%S) $NAME: done — $(cat "$RESULT")" | tee -a "$LOG"
