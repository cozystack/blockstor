# DRBD9 troubleshooting test scenarios

Companion to `tests/linstor-cli-scenarios.md`. Where that file covers
the operator-facing CLI surface (definitional + provisioning), this
one covers the **failure modes** Andrei Kvapil documented in
[Troubleshooting DRBD9 in LINSTOR](https://dev.to/kvaps/troubleshooting-drbd9-in-linstor-40fn).

The article is a field guide built from production incidents — 14
cases of DRBD9 misbehaviour and how to recover. Most are kernel-side
DRBD problems, but they each surface in `linstor r l` output and
should be:

- **Detected**: blockstor's observer must report the right state in
  `Resource.Status` (and therefore in `linstor r l` Conns/State
  columns)
- **Survived**: blockstor's reconciler must not make things worse by
  fighting the operator's recovery commands
- **Recoverable**: the operator-side recovery flow must work against
  blockstor's REST shim the same way it does against upstream LINSTOR

Tests below assume the dev stand (`make up NAME=... && make piraeus
NAME=... && make blockstor NAME=...`) with at least 3 workers and a
real-disk storage pool (ZFS_THIN or LVM_THIN — FILE_THIN's
copy-on-reflink doesn't exercise all DRBD edge cases).

---

## State-reporting tests

These pin that blockstor's observer translates DRBD `events2` frames
into the right `Resource.Status` shape, and therefore the right
`linstor r l` State / Conns columns. Each test induces a known DRBD
state with raw kernel commands on the satellite host, then asserts
the wire-side reflection.

### 1. UpToDate ↔ Outdated transition

**Why:** Cases 1–2. After a node restart, the replica that was down
should come back as Outdated until it resyncs to UpToDate.

**Setup:** 2-replica RD on worker-1 + worker-2, Primary on worker-1
(via mount). Write a 64 MiB random payload.

**Steps:**
```bash
# Simulate worker-2 going down for a moment:
kubectl drain <worker-2> --ignore-daemonsets --force
sleep 10
kubectl uncordon <worker-2>
# Wait for satellite restart, observer to re-establish:
linstor r l -r test
```

**Expected timeline:**
- During drain: worker-2 row → State `Outdated` or Conns
  `Connecting`/`NetworkFailure`
- After uncordon: State transitions Outdated → SyncTarget → UpToDate
  within ~30s for a 64 MiB write
- Final: both rows `UpToDate`

**Assertion:** `linstor r l -r test --output-version=v0` parses
cleanly at every poll; State column never lands in `Unknown`.

---

### 2. Diskless tiebreaker reports `TieBreaker`, not `Diskless`

**Why:** Slide 16 of the talk + the per-resource `Flags=[DISKLESS]`
on Spec.Flags. blockstor must distinguish operator-requested
diskless (`linstor r c --diskless`) from autoplacer-stamped
tiebreaker (`ensureTiebreaker` reconciler).

**Setup:** RD with `PlaceCount: 2` → autoplacer adds tiebreaker.

**Steps:**
```bash
linstor r l -r test
```

**Expected:**
- 2 rows State = `UpToDate`
- 1 row State = `TieBreaker` (NOT `Diskless`)

A separate test should add a manual diskless attach:
```bash
linstor r c <worker-N> test --diskless
linstor r l -r test
```
**Expected:** New row with State = `Diskless`, distinct from the
tiebreaker row.

---

### 3. StandAlone / Connecting / BrokenPipe surface in Conns column

**Why:** Cases 1, 4, 7. The non-Connected DRBD-9 connection states
must reach `Resource.Status.Connections[i].Message` so
`linstor r l --faulty` flags them red.

**Setup:** RD with 2 diskful replicas, both Connected.

**Steps:** Drop tcp/<port> on worker-2 in+out via iptables.
```bash
linstor r l -r test
# Expected: worker-1 Conns column shows StandAlone(worker-2) or
# Connecting(worker-2) within 30s
```

Then heal:
```bash
# Restore iptables
sleep 30
linstor r l -r test
# Expected: Conns back to Ok
```

**Assertion:** Every non-Connected DRBD state the kernel emits
appears in `view/resources` `connections[peer].message`. Audit list:
`Connected`, `StandAlone`, `Connecting`, `Disconnecting`,
`BrokenPipe`, `NetworkFailure`, `Timeout`, `Unconnected`.

---

### 4. Inconsistent state reaches Status.Volumes[i].diskState

**Why:** Cases 3–6 (stuck sync). When the kernel reports a volume
as `Inconsistent`, `linstor r l` must show it — operator decision
point: re-place or recover.

**Setup:** Force an Inconsistent state by interrupting an initial
sync mid-flight (write 1 GiB on Primary, then kill the secondary
satellite pod before sync completes).

**Steps:**
```bash
linstor r l -r test
linstor v l -r test
```

**Expected:**
- One row State = `Inconsistent` (peer-disk state in
  `linstor r l`)
- `linstor v l` per-volume State column = `Inconsistent`

**Failure mode caught:** observer drops device-kind frames when
disk state isn't `UpToDate` → State stays `Unknown` and operator
flies blind.

---

## Reconciler-survival tests

The article's recovery flows assume the operator can run raw
`drbdadm disconnect` / `connect` / `down` / `up` without the
controller immediately undoing them. blockstor's reconciler is
designed to apply intent (Spec) onto the kernel state — these tests
assert the **window** an operator has to do manual recovery.

### 5. `drbdadm disconnect <res>` from operator shell survives ≥30s

**Why:** Case 1 recovery flow. Operator needs to disconnect a
single peer to break a stuck connection retry loop.

**Setup:** RD with 3 replicas, all Connected.

**Steps:**
```bash
# On worker-1's satellite container:
drbdadm disconnect test
# Observe the state for 30s:
for i in {1..6}; do
    drbdadm status test
    sleep 5
done
```

**Expected:**
- Connection state stays `StandAlone` for the full 30s
- Reconciler does NOT auto-reconnect (it would defeat the recovery
  attempt)

After 30s, operator runs `drbdadm connect test` and expects
`Connected` to restore within 5s.

**Assertion:** blockstor's reconciler must distinguish "operator-
initiated disconnect" from "missing peer config" — only the latter
should trigger reconnection.

**Open design question:** how does blockstor know not to re-apply
the .res / re-run `drbdadm adjust` for a manually-disconnected
resource? Possibly via a per-resource `Aux/operator-managed=true`
prop that pauses the satellite reconciler. Test should exercise
that prop.

---

### 6. `drbdadm down <res>` reverses on next reconcile cycle

**Why:** Case 3 + recovery practice. Operator stops a resource on
one node to fix a config issue, then expects the reconciler to
bring it back automatically.

**Setup:** RD with 3 replicas.

**Steps:**
```bash
# Operator shell:
drbdadm down test
# State right after:
drbdadm status test    # expect: "No currently configured DRBD found"
# Wait for reconciler to react:
sleep 10
drbdadm status test    # expect: resource back, connecting to peers
```

**Expected:** Reconciler re-renders .res, runs `drbdadm up`, peer
reconnects. Total downtime ≤ 10s.

---

## Recovery-flow tests

These walk the operator-side recovery from each article case,
asserting the linstor-CLI path works against blockstor.

### 7. Replace a failed replica via `r d` + `rd ap`

**Why:** Cases 3–6 recipe. When `drbdadm disconnect` + `connect
--discard-my-data` doesn't unstick a sync, deletion + auto-place is
the escape hatch.

**Setup:** 2-replica RD with one stuck Inconsistent replica.

**Steps:**
```bash
linstor r d <bad-worker> test
# Wait for finalizer-driven cleanup:
linstor r l -r test
# Expected: only the good replica left (+ TieBreaker if 2→1)
linstor rd ap test
# Expected: auto-placer adds a fresh replica on a different worker
linstor r l -r test
# Expected: 3 rows, all converging to UpToDate
```

**Assertion:**
- DRBD port from the deleted replica is **freed** (allocator
  reuses it or picks the next free port)
- The new replica's `.res` doesn't include the dead peer's `on
  <node>` block
- Initial sync to the new replica completes via either: full
  replication, OR `SeedFromGi` skip-sync (the Phase 8.1 feature) if
  the new replica was previously a member of this RD

**Failure mode this session uncovered:** force-stripping
finalizers leaves DRBD ports occupied at kernel level → port
collision on next placement. The test must use proper `linstor r d`
(finalizer-driven), NEVER `kubectl patch --type=json -p='[{"op":
"remove","path":"/metadata/finalizers"}]'`.

---

### 8. `linstor node lost <node>` evicts a permanently-failed node

**Why:** Article mentions for unrecoverable nodes. blockstor's Lost
handler was wired this session to actually delete the Node CRD
(matches upstream's "drop-node" semantic).

**Setup:** Cluster with worker-2 powered off.

**Steps:**
```bash
linstor node lost <worker-2>
linstor n l
# Expected: worker-2 row gone from node list
linstor r l
# Expected: worker-2's resources gone (controller auto-deleted them
# when the Node CRD disappeared via finalizer-cascade)
linstor rd ap test
# Expected: replicas re-placed on remaining workers (worker-1,
# worker-3) without operator intervention
```

**Assertion:** Lost is idempotent — `linstor node lost <worker-2>`
twice does NOT error on the second call (this session's fix:
NotFound on delete folds into success).

---

### 9. Stuck SyncTarget recovers via down+up cycle

**Why:** Case 8. Direct DRBD-side recovery: `drbdadm down test`
followed by `drbdadm up test`.

**Setup:** RD with 2 replicas, one stuck SyncTarget showing
0%/min progress for >2 minutes.

**Steps:**
```bash
# On the stuck satellite's container:
drbdadm down test
drbdadm up test
# Wait for sync:
for i in {1..12}; do
    drbdadm status test
    sleep 10
done
```

**Expected:** Sync resumes after ~10s of reattach. Total time to
UpToDate depends on the volume size but progress should advance.

**Reconciler-side assertion:** the satellite's
`tearDownRemovedPeers` + Apply doesn't fight the manual down/up
sequence (the reconciler sees the state and converges without
running its own `drbdadm down`).

---

### 10. Split-brain detection: blockstor does NOT auto-reconcile

**Why:** Case 7 from the article + transcript 29:00–32:00. Two
Primary replicas with diverged data is a data-loss situation.
LINSTOR's job is to **detect and stop**, not auto-merge.

**Setup:** RD with 2 replicas, both UpToDate. Then:
```bash
# On worker-1:
drbdadm disconnect test
drbdsetup primary test --force
echo "data-A" | dd of=/dev/drbd<N> oflag=direct
# Simultaneously on worker-2:
drbdadm disconnect test
drbdsetup primary test --force
echo "data-B" | dd of=/dev/drbd<N> oflag=direct
# Re-establish connection:
drbdadm connect test    # on worker-1
drbdadm connect test    # on worker-2
```

**Expected:**
- Both replicas land in `StandAlone` (DRBD-9 auto-detects the
  split-brain on reconnect)
- `linstor r l -r test` Conns column shows `StandAlone` for both
- `linstor r l -r test --faulty` flags the RD as in trouble
- blockstor's reconciler does **NOT** invoke `connect
  --discard-my-data` on either side (operator decision point)

**Recovery (operator-side):**
```bash
# Operator decides worker-1's data is the truth:
ssh worker-2 'drbdadm disconnect test'
ssh worker-2 'drbdadm secondary test'
ssh worker-2 'drbdadm connect --discard-my-data test'
# worker-2's replica resyncs from worker-1 → UpToDate
```

**Assertion:** After recovery `linstor r l` shows both UpToDate.
blockstor's auto-quorum (`quorum: majority` in `.res`) prevents
split-brain in 3-node configurations — this test should also have
a 3-node variant where reaching split-brain is harder.

---

### 11. Node-id mismatch surfaces in dmesg, operator escapes via r d

**Why:** Cases 10–11: `Peer presented a node_id of X instead of Y`.
The Phase 8.1 work made `Status.DRBDNodeID` stable; this test
proves the invariant.

**Setup:** Heavy churn — create + delete + recreate the same RD 5
times in quick succession, with replicas landing on the same nodes
each time.

**Steps:**
```bash
for i in 1 2 3 4 5; do
    linstor rd c test
    linstor vd c test 1G
    linstor r c <worker-1> test -s lvm-thin
    linstor r c <worker-2> test -s lvm-thin
    sleep 5
    linstor rd d test
done
linstor rd c test
linstor vd c test 1G
linstor r c <worker-1> test -s lvm-thin
linstor r c <worker-2> test -s lvm-thin
# Check dmesg on both workers for node-id mismatch errors:
kubectl exec -n blockstor-system <satellite-w1> -- dmesg | grep -i 'node_id' | tail
kubectl exec -n blockstor-system <satellite-w2> -- dmesg | grep -i 'node_id' | tail
```

**Expected:**
- Each iteration's `r c` assigns the **same** `Status.DRBDNodeID`
  for the same worker (Phase 8.1 invariant)
- dmesg shows zero `node_id of X instead of Y` errors

**Failure mode this catches:** the port-collision class of bugs
where allocator + reconciler race + node-id reassignment turn a
benign churn into a data-loss event.

---

### 12. Orphaned diskless resource cleanup

**Why:** Case 13. Resource deleted from LINSTOR but still alive at
kernel level — typically after a force-strip incident (the one
this session reproduced).

**Setup:** Create a 2-replica RD + 1 diskless on worker-3. Then
force-strip the diskless replica's finalizer (simulating a
recovery scenario, NOT the recommended path):
```bash
kubectl patch resource.blockstor.io.blockstor.io test.<worker-3> \
    --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'
linstor r l -r test
# Expected: worker-3 row gone from CLI
# But DRBD kernel state on worker-3 still has the resource:
kubectl exec -n blockstor-system <satellite-w3> -- drbdsetup status test
# Expected: resource still up, no peer config
```

**Recovery:**
```bash
kubectl exec -n blockstor-system <satellite-w3> -- drbdsetup down test
# Verify clean:
kubectl exec -n blockstor-system <satellite-w3> -- drbdsetup status test
# Expected: "No currently configured DRBD found"
```

**Assertion + open question:** should blockstor's satellite have a
periodic sweeper that runs `drbdsetup down <res>` for every kernel-
resident DRBD resource that has no matching Resource CRD on the
local node? Article suggests yes; would close the
force-strip-recovery loop without operator intervention. Test the
sweeper if landed.

---

### 13. Bitmap corruption: `drbdadm verify` + invalidate path

**Why:** Case 14. Rare but data-loss-adjacent. Two replicas both
report UpToDate but `Allocated` size differs in `linstor v l` —
the changelog bitmap is corrupt.

**Setup:** Hard to induce naturally — fault-inject by overwriting
a few bytes of DRBD metadata on one replica via `dd` (offline).
Then bring DRBD back up.

**Steps:**
```bash
linstor v l -r test
# Expected: row 1 Allocated = 3.07 MiB, row 2 Allocated = 5.21 MiB
# (mismatched despite both UpToDate → bitmap divergence)

# Operator-side verify:
drbdadm verify test:<peer>
# Then re-connect to trigger resync:
drbdadm disconnect test && drbdadm connect test
# Or for stubborn cases:
drbdadm invalidate test
```

**Expected:** After verify + reconnect, Allocated converges.

**Assertion:** blockstor's observer surfaces the Allocated value
through `Resource.Status.Volumes[i]` (or via per-volume layer-data)
so the operator can detect the divergence in the CLI without
shelling into a node.

---

## Layer-stack tests (DRBD-9 layered features)

The article mentions LUKS, VDO, DM-Cache as layers between LVM/ZFS
and DRBD. blockstor's Phase 9 work added LayerStack support.

### 14. LUKS-encrypted resource survives the same recovery flows

**Why:** Adds the encryption layer between storage and DRBD. Every
recovery test above (#5–13) should pass with LayerStack = `["DRBD",
"LUKS", "STORAGE"]`.

**Setup:** RG with `LayerStack: ['DRBD', 'LUKS', 'STORAGE']`,
encryption passphrase set via `linstor rd modify ... DrbdOptions/Net/shared-secret`.

**Steps:** Repeat #5 (operator disconnect window) and #7 (replica
replace via r d + rd ap) with this RG.

**Expected:** Same outcomes as plain DRBD. LUKS keyfile setup
happens before `drbdadm up` so the replacement replica is decryptable.

---

### 15. No-DRBD storage class (single-replica local) round-trips

**Why:** Phase 9: blockstor supports `LayerStack: ['STORAGE']` —
pure local volumes for app-level-replicated workloads (Postgres,
etc.). LINSTOR-CLI surface should still work.

**Setup:** RG with `PlaceCount: 1`, `LayerStack: ['STORAGE']`.

**Steps:**
```bash
linstor rg spawn local test-local 1G
linstor r l -r test-local
# Expected: 1 row, Layers column = "STORAGE" (no DRBD), State = "ok"
linstor v l -r test-local
# Expected: DeviceName = /dev/<vg>/<lv>, no /dev/drbd*
```

**Recovery:** When the local-replica node goes down, there's no
replication to fall back to. blockstor's `linstor node lost`
flow should refuse / warn for a single-replica RD (data loss).

---

## Diagnostic command tests

### 16. `drbdadm status <res>` from a satellite reflects kernel state

Sanity check that operators can shell into a satellite container
and run `drbdadm status` to see live state. Not a blockstor bug per
se, but the satellite container must include `drbd-utils`.

```bash
kubectl exec -n blockstor-system <satellite-w1> -- drbdadm status test
```

**Expected:** Output matches the article's example shape
(per-resource role + per-peer connection + per-volume disk-state).

---

### 17. `drbdsetup status --verbose` shows node IDs

Per Cases 10–11, node IDs are the key to diagnosing peer-presented-
wrong-id incidents.

```bash
kubectl exec -n blockstor-system <satellite-w1> -- drbdsetup status test --verbose
```

**Expected:** Output includes `node-id:<N>` for every peer block.
The N must match `Resource.Status.DRBDNodeID` for that peer (Phase
8.1 invariant).

---

## Out of scope

These are mentioned in the article but **not** appropriate as
blockstor tests:

- **Manual `dd if=/dev/drbdN of=/dev/drbdN` re-write to fix
  bitmaps** (Case 14 deep recovery): operator escalation path,
  not a contract test. Document in `docs/drbd-recovery.md`.
- **`linstor controller drbd-options` mass-tuning**: covered by
  the controller-props tests in `linstor-cli-scenarios.md`.
- **Multi-cluster federation** (article doesn't cover, but Q&A
  asked): out of cozystack scope.

---

## Test harness pattern

Same as `linstor-cli-scenarios.md` — `tests/e2e/drbd-tshoot-<case>.sh`
with port-forward setup/teardown.

Two helpers worth landing in `tests/e2e/lib.sh`:

```bash
# Inject DRBD state by exec-ing into a satellite container.
satellite_exec() {
    local node=$1; shift
    local pod
    pod=$(kubectl get pod -n blockstor-system -l app=blockstor-satellite \
        -o jsonpath="{.items[?(@.spec.nodeName=='$node')].metadata.name}")
    kubectl exec -n blockstor-system "$pod" -- "$@"
}

# Wait for a Resource CRD to reach a target DRBD state, with timeout.
wait_drbd_state() {
    local rd=$1 node=$2 want=$3 deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        got=$(kubectl get resource.blockstor.io.blockstor.io "$rd.$node" \
            -o jsonpath='{.status.drbdState}' 2>/dev/null)
        [[ "$got" == "$want" ]] && return 0
        sleep 2
    done
    echo "timeout: $rd@$node never reached $want (last: $got)" >&2
    return 1
}
```

---

## Priority for landing

| # | Test                                  | Priority | Reason                                                     |
|---|---------------------------------------|----------|------------------------------------------------------------|
| 1 | UpToDate ↔ Outdated transition        | High     | Covers the common drain/uncordon flow                      |
| 2 | TieBreaker vs Diskless distinction    | High     | Wire-shape regression risk                                 |
| 3 | StandAlone/Connecting in Conns column | High     | Just fixed observer cleanup this session                   |
| 4 | Inconsistent reaches diskState        | High     | Status.Volumes[i].diskState bug we hit this session        |
| 5 | Operator disconnect survives 30s      | Medium   | Needs reconciler pause-mode design — open question         |
| 7 | r d + rd ap port-release flow         | High     | Closes the force-strip aftermath loop                      |
| 8 | linstor node lost is idempotent       | Medium   | This session's Lost-delete change                          |
| 10| Split-brain detection (no auto-merge) | High     | Data-loss prevention invariant                             |
| 11| Node-id stability under churn         | High     | Phase 8.1 invariant — easy to regress                      |
| 12| Orphaned diskless cleanup (sweeper)   | Medium   | Open: implement sweeper? would close manual-recovery loop  |
| 15| No-DRBD storage class                 | Medium   | Phase 9 coverage                                           |
| 17| node-id shows in drbdsetup --verbose  | Low      | Smoke check                                                |

Others (6, 9, 13, 14, 16) are good-to-have but lower-leverage —
either they exercise raw DRBD that blockstor doesn't directly own,
or they require kernel-level fault injection that's hard to script
reliably in CI.
