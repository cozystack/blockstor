#!/usr/bin/env bash
# U3 snapshot-robustness stand validation — runs ENTIRELY under the
# stand lock (invoked via flock by the caller). Non-disruptive: it does
# NOT switch the shared checkout's branch; it fetches the U3 branch and
# copies ONLY the new (additive) test files into place, runs them, then
# removes the copies so the other agent's checkout is left untouched.
set -uo pipefail

export KUBECONFIG=/home/ubuntu/blockstor/.work/dev/kubeconfig
REPO=/home/ubuntu/blockstor
BRANCH=issues/u3-snapshot-robustness
LP=4399
RESULT=0

cd "$REPO" || { echo "FATAL: no repo"; exit 1; }

echo "=== U3 stand-validate: fetch $BRANCH ==="
git fetch origin "$BRANCH" 2>&1 | tail -2 || { echo "FATAL: fetch failed"; exit 1; }

# Files this validation introduces (all NEW — safe to drop in / remove).
REPLAYS=(
  tests/operator-harness/replay/snap-single-replica-zfs-restore-u464.yaml
  tests/operator-harness/replay/snap-suspend-resume-isolation-u138-u52.yaml
)
CELLS=(
  tests/e2e/cli-matrix/snap-single-replica-zfs-restore-u464.sh
  tests/e2e/cli-matrix/snap-suspend-resume-isolation-u138-u52.sh
)

CLEAN_FILES=()
for f in "${REPLAYS[@]}" "${CELLS[@]}"; do
    if [ ! -e "$f" ]; then CLEAN_FILES+=("$f"); fi
    git checkout "origin/$BRANCH" -- "$f" 2>&1 | tail -1 || { echo "FATAL: checkout $f"; exit 1; }
done
chmod +x "${CELLS[@]}" 2>/dev/null || true

cleanup_files() {
    echo "=== removing copied-in validation files ==="
    for f in "${CLEAN_FILES[@]:-}"; do
        [ -n "$f" ] && rm -f "$f" 2>/dev/null || true
    done
    # Unstage any index entries our checkout created without touching
    # the other agent's working state.
    git reset -q -- "${REPLAYS[@]}" "${CELLS[@]}" 2>/dev/null || true
}
trap cleanup_files EXIT

# --- BS apiserver port-forward for the replay runner -------------------
echo "=== start BS apiserver port-forward on $LP ==="
PFPID=""
for attempt in 1 2 3; do
    setsid nohup kubectl -n blockstor-system port-forward --address 127.0.0.1 \
        deploy/blockstor-apiserver "$LP":3370 </dev/null >/tmp/u3-bs-pf.log 2>&1 &
    PFPID=$!
    sleep 6
    if curl -fsS --max-time 5 "http://127.0.0.1:$LP/v1/controller/version" >/dev/null 2>&1; then
        echo "  BS apiserver reachable on $LP (pf pid $PFPID)"
        break
    fi
    echo "  attempt $attempt: not reachable yet"; cat /tmp/u3-bs-pf.log 2>/dev/null | tail -3
    PFPID=""
done

kill_pf() { [ -n "${PFPID:-}" ] && kill "$PFPID" 2>/dev/null || true; }

if [ -z "$PFPID" ]; then
    echo "FATAL: BS apiserver port-forward never came up"; RESULT=1
fi

# --- L7 replays --------------------------------------------------------
if [ -n "$PFPID" ]; then
    for y in "${REPLAYS[@]}"; do
        echo ""; echo "=== L7 replay: $y ==="
        if BS_URL="http://127.0.0.1:$LP" \
            timeout 1200 bash tests/operator-harness/replay-runner.sh u3-stand "$y" 2>&1 | tail -40; then
            echo "REPLAY-RESULT $y: PASS"
        else
            echo "REPLAY-RESULT $y: FAIL"; RESULT=1
        fi
    done
fi
kill_pf

# --- L6 cli-matrix cells (manage their own port-forward via lib.sh) ----
WORK=/home/ubuntu/blockstor/.work/dev
for c in "${CELLS[@]}"; do
    echo ""; echo "=== L6 cli-matrix: $c ==="
    if timeout 1500 bash "$c" "$WORK" 2>&1 | tail -45; then
        echo "CELL-RESULT $c: PASS"
    else
        echo "CELL-RESULT $c: FAIL"; RESULT=1
    fi
done

# --- safety: make sure no ccu3-* resource was left suspended -----------
echo ""; echo "=== post-run suspend safety sweep (ccu3-* only) ==="
for n in dev-worker-1 dev-worker-2 dev-worker-3; do
    pod=$(kubectl -n blockstor-system get pod -l app.kubernetes.io/component=satellite \
        --field-selector spec.nodeName="$n" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -z "$pod" ] && continue
    for rd in $(kubectl get resources.blockstor.cozystack.io -o jsonpath='{.items[*].spec.resourceDefinitionName}' 2>/dev/null | tr ' ' '\n' | grep '^ccu3-' | sort -u); do
        kubectl -n blockstor-system exec "$pod" -- drbdadm resume-io "$rd" 2>/dev/null && echo "  resumed $rd on $n" || true
    done
done

echo ""; echo "=== U3 stand-validate done (RESULT=$RESULT) ==="
exit $RESULT
