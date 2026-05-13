# LINSTOR CLI compatibility test scenarios

This document enumerates the `linstor` CLI operations demonstrated in
Andrei Kvapil's "LINSTOR: Kubernetes-Like Open-Source Storage" talk
and maps each one to a concrete blockstor test we should land.

Every scenario below describes:

- **Setup**: what the cluster must look like before the test runs
- **Steps**: the exact `linstor` CLI commands to run, in order
- **Expected**: what blockstor's REST + CRD state must look like
  after, and what the CLI should render
- **Why**: which presentation slide / transcript section the
  scenario backs

The tests are end-to-end against a running blockstor stand
(`tests/e2e/linstor-cli-*.sh`). They assume `linstor` is pointed at
the blockstor REST endpoint via port-forward (`kubectl port-forward
svc/blockstor-controller 3370:3370`) — *not* at piraeus's Java
LINSTOR. Each one is one-shot: setup → run → teardown.

`make iter NAME=<cluster> SCENARIO=linstor-cli-<name>` is the dev
loop. Failures dump `linstor` stderr + the relevant CRD YAML for
diagnosis.

---

## Discovery and inspection

These pin the read-only surface the operator hits before doing
anything else. They're the cheapest tests — no state mutation, just
assertion that the wire shape renders correctly in the CLI's table
formatter.

### 1. `linstor node list` renders satellite-backed nodes

**Why:** Slide 1. The operator's first command. Wire shape: node
name, node type, addresses, state.

**Setup:** Fresh 3-worker blockstor stand. All satellites Running
and heartbeating (`Status.Conditions[Ready]=True`).

**Steps:**
```bash
linstor node list
```

**Expected:**
- Exit 0
- Table shows 3 rows, one per worker
- NodeType column = `SATELLITE` for every row
- Addresses column non-empty (`<IP>:3367` or similar)
- State column = `Online` (green) for every row

**Failure modes to catch:** State column blank → `connectionStatus`
not flowing from Node.Status; empty Addresses → satellite endpoint
discovery broken.

---

### 2. `linstor storage-pool list` enumerates per-node pools

**Why:** Slide 2. Operator's view of available pools. Wire shape:
pool name, node, driver, pool-name, free/total, snapshots, state.

**Setup:** Stand with `make piraeus` + `make blockstor` + 9
storagepools (3 × {stand, lvm-thin, zfs-thin}).

**Steps:**
```bash
linstor storage-pool list
```

**Expected:**
- 12 rows: 9 real pools + 3 synthesised DfltDisklessStorPool
  (one per node)
- Driver column reflects LVM_THIN / ZFS_THIN / FILE_THIN / DISKLESS
- PoolName column non-empty for non-diskless rows
- FreeCapacity + TotalCapacity show TiB/GiB values for non-diskless
  (DISKLESS rows show empty FreeCapacity)
- CanSnapshots = `True` for thin / ZFS pools, `False` for plain LVM
- State = `Ok` for all

**Failure modes:** Worker placeholder `__WORKER_1__` (StoragePool
yaml not run through sed) → use install-pools.sh, not raw apply;
DfltDisklessStorPool missing → blockstor must synthesise per-node
diskless pool entries.

---

### 3. `linstor resource-definition list` shows RDs with port + RG

**Why:** Slide 4. Wire shape: name, port, resource-group, state.

**Setup:** Two RDs created via `linstor rd c test1 --resource-group
default` and `linstor rd c test2 --resource-group default`.

**Steps:**
```bash
linstor resource-definition list
```

**Expected:**
- 2 rows
- ResourceGroup column = `default` (or whatever RG was passed)
- Port column non-empty (DRBD TCP port allocated by blockstor's
  allocator)
- State column = `ok`

---

### 4. `linstor volume-definition list -r <rd>` shows inline VDs

**Why:** Slide 5. Wire shape: rd name, volume-nr, minor, size,
gross, state.

**Setup:** RD `test1` with one VD created via `linstor vd c test1
10G`.

**Steps:**
```bash
linstor volume-definition list -r test1
```

**Expected:**
- 1 row
- VolumeNr = 0
- VolumeMinor = a stable integer (e.g. 1005) allocated by blockstor
- Size = `10 GiB`
- State = `ok`

**Failure mode caught this session:** flat `[]VolumeDefinition`
response → empty table. CLI iterates
`lstmsg.resource_definitions[i].volume_definitions[j]`, so the
endpoint must return `[ResourceDefinitionWithVolumeDefinition]`
shape.

---

### 5. `linstor resource list -r <rd>` shows per-node replicas

**Why:** Slide 7. Wire shape: rd, node, port, usage, conns, state,
created-on.

**Setup:** RD with autoplace=2 → 2 UpToDate replicas + 1 TieBreaker.

**Steps:**
```bash
linstor resource list -r test1
```

**Expected:**
- 3 rows (2 diskful + 1 diskless tiebreaker)
- Port column shows the same TCP port for all 3 rows
- Usage = `Unused` (no Primary)
- Conns = `Ok` for connected, blank for not-yet-established
- State = `UpToDate` for diskful, `TieBreaker` for the diskless one
- CreatedOn populated with a timestamp

**Failure mode caught this session:** Status.Connections accumulates
StandAlone entries for already-removed peers → ghost rows in the
Conns column. Need observer to handle `destroy connection` events.

---

### 6. `linstor volume list -r <rd>` shows materialised block devices

**Why:** Slide 8. Wire shape: node, resource, storage-pool, vol-nr,
minor, device-name, allocated, in-use, state.

**Setup:** Same as #5.

**Steps:**
```bash
linstor volume list -r test1
```

**Expected:**
- 2 rows (one per diskful replica; tiebreaker excluded)
- DeviceName = `/dev/drbd<N>` matching the VolumeMinor from VD list
- StoragePool = the pool the placer chose
- Allocated = small value (~few MiB on a fresh thin pool)
- InUse = `Unused`
- State = `UpToDate`

**Failure modes:** Tiebreaker row appearing → wrong filter;
DeviceName blank → satellite hasn't reported `device` events
through observer yet; Status.Volumes[i].diskState empty after 30s →
observer is dropping `exists device disk:UpToDate` frames.

---

### 7. `linstor resource-group list` renders SelectFilter fields

**Why:** Slide 14 (final RG view). Wire shape: name, SelectFilter
(multiline: PlaceCount, StoragePool(s), DisklessOnRemaining,
LayerStack), VlmNrs, Description.

**Setup:** Default `DfltRscGrp` (auto-created at first RD without
explicit RG) + one custom RG.

**Steps:**
```bash
linstor resource-group list
linstor resource-group list -r dfltrscgrp
```

**Expected:**
- `DfltRscGrp` row (rendered as `dfltrscgrp` by blockstor, since
  LINSTOR is case-insensitive — the CLI accepts either form)
- SelectFilter cell shows `PlaceCount: 2` for the default RG, plus
  any storage-pool / layer-stack constraints set
- `linstor rg modify` round-trips the SelectFilter changes

---

### 8. `linstor volume-group list <rg>` for per-volume defaults

**Why:** Slide 13. RG's volume-group entries carry per-volume-number
default props (encryption, layer-data, etc).

**Setup:** Custom RG with one volume-group at vlm-nr 0.

**Steps:**
```bash
linstor volume-group list default
```

**Expected:**
- 1 row with VolumeNr=0, Flags column shows any flags set on the VG

---

## Provisioning workflows (three levels of detail)

### 9. High-level: `resource-group spawn` does the full chain

**Why:** Slide 15. The CSI/operator-facing one-shot. Creates RD +
VDs + replicas + tiebreaker in one call. Matches the spawn fix
landed this session.

**Setup:** Default RG with `PlaceCount: 2`.

**Steps:**
```bash
linstor resource-group spawn default test-spawn 10G
linstor resource-definition list -r test-spawn
linstor resource list -r test-spawn
```

**Expected after spawn:**
- `rd list` shows test-spawn with state `ok`
- `r list` shows 3 rows: 2 UpToDate + 1 TieBreaker
- All under the same DRBD port
- Storage pool selection respects the RG's StoragePool constraint
  (`stand`, `lvm-thin`, or `zfs-thin` per the SelectFilter)

**Cleanup:** `linstor rd d test-spawn` → all 3 resources gone, port
freed.

**Failure mode caught:** spawn used to be definitional-only — the
operator had to run `r c --auto-place=N` separately. Now spawn
autoplaces inline per SelectFilter.PlaceCount.

---

### 10. Mid-level: `--auto-place` does the placement step only

**Why:** Slide 17–18. The operator-driven split: manually build the
RD + VD, then ask the autoplacer to land replicas.

**Setup:** Empty cluster, default RG.

**Steps:**
```bash
linstor resource-definition create test-auto --resource-group default
linstor volume-definition create test-auto 10G
linstor resource list -r test-auto    # expect empty
linstor create test-auto --auto-place 2
linstor resource list -r test-auto    # expect 3 rows
```

**Expected:**
- After `rd c`: rd list shows 1 row, resource list empty
- After `vd c`: vd list shows the new VD, resource list still empty
- After `create --auto-place 2`: 3 resources placed (2 UpToDate +
  1 TieBreaker)

---

### 11. Manual: per-node `resource create` with explicit storage-pool

**Why:** Slides 19–21. The lowest-level workflow — useful for
debugging when autoplacer picks wrong nodes.

**Setup:** Empty cluster.

**Steps:**
```bash
linstor resource-definition create test-manual --resource-group default
linstor volume-definition create test-manual 10G
linstor resource create <worker-1> test-manual -s lvm-thin
linstor resource create <worker-2> test-manual -s lvm-thin
linstor resource list -r test-manual
```

**Expected:**
- After first `r c`: 1 resource on worker-1, state UpToDate (or
  Inconsistent + auto-sync to UpToDate)
- After second `r c`: 2 diskful replicas + 1 auto-added TieBreaker
- TieBreaker landed on the remaining worker (worker-3 in a 3-worker
  cluster) automatically — same `ensureTiebreaker` reconciler as #9

---

### 12. Tiebreaker disabled by `AutoAddQuorumTiebreaker=False`

**Why:** Operator wants exactly 2 replicas, no tiebreaker (e.g.
when running a 2-node DRBD pair with external quorum service).

**Setup:** Cluster as in #11.

**Steps:**
```bash
linstor rg modify default DrbdOptions/AutoAddQuorumTiebreaker False
linstor resource-group spawn default test-no-tb 10G
linstor resource list -r test-no-tb
```

**Expected:**
- 2 rows only — no TieBreaker
- Both UpToDate
- Subsequent `linstor r d <node> test-no-tb` doesn't trigger
  tiebreaker auto-recreation

---

## Resource lifecycle

### 13. `linstor resource delete <node> <rd>` removes one replica

**Why:** Transcript ~24:00–25:30. The atomic peer-remove. Was the
ghost-peer bug source earlier this session.

**Setup:** RD `test` with 2 UpToDate + 1 TieBreaker.

**Steps:**
```bash
linstor resource delete <worker-3> test
linstor resource list -r test
```

**Expected:**
- TieBreaker replica gone OR auto-recreated on a different node
  (depending on whether AutoAddQuorumTiebreaker is on/off)
- Remaining diskful replicas' `linstor r l` Conns column does NOT
  show `StandAlone(<worker-3>)` — the deleted peer is fully gone
  from the connection list
- blockstor's `view/resources` no longer reports the removed peer

**Failure mode caught:** observer's `destroy connection` event
unhandled → ghost StandAlone entries linger forever.

---

### 14. `linstor resource-definition delete <rd>` cascades to all replicas

**Why:** Transcript ~27:00 implicit. RD delete must finalise the
satellite-side cleanup before the CRD goes away.

**Setup:** Any RD with replicas placed.

**Steps:**
```bash
linstor rd d test
linstor resource list      # expect empty for test
linstor rd l               # expect no test row
```

**Expected:**
- All Resource CRDs of the RD enter Terminating, satellite
  finalizer runs `drbdadm down` + `rm .res` + provider's
  `DeleteVolume`, finalizer strips, CRDs gone
- DRBD ports freed (no `Local address(port) already in use` errors
  on the next RD that lands on the same node)
- No stuck Terminating Resources after 30s

**Failure mode:** Operator-induced force-strip of finalizers — this
session's pitfall. Test should assert finalizers are stripped via
the satellite reconciler, not via manual `kubectl patch`.

---

## DRBD admin operations (operator-side debugging)

These exercise blockstor's `view/resources` Status reporting
under DRBD state changes that the satellite observer surfaces.
They don't invoke `drbdadm` directly — blockstor's reconciler
owns the kernel; the test merely asserts the CLI displays the
right state.

### 15. Replica role surfaces in `r l` Usage column

**Why:** Transcript ~28:00. Primary vs Secondary visibility.

**Setup:** RD with replicas placed. Mount + write on one worker to
auto-promote.

**Steps:**
```bash
# On worker-1:
mkfs.ext4 /dev/drbd<N> && mount /dev/drbd<N> /mnt/test
# From admin shell:
linstor resource list -r test
```

**Expected:**
- worker-1 row Usage = `InUse` (auto-promoted to Primary)
- Other rows Usage = `Unused`

After unmount:
- All rows Usage = `Unused` again

**Failure mode:** `Status.InUse` not flowing → observer's
resource-kind frame translation broken; or `InUse` stuck `true`
after demote → role-change-on-secondary frame ignored.

---

### 16. Disconnected peer surfaces as Conns column state change

**Why:** Transcript 25:30–27:00. Network partition simulation.

**Setup:** RD with 2 diskful replicas.

**Steps:**
```bash
# Simulate iptables drop tcp/<port> in+out on worker-2
linstor resource list -r test
# Expected: worker-1's Conns shows StandAlone(worker-2) OR Connecting
# Restore iptables
linstor resource list -r test
# Expected: Conns back to Ok
```

**Expected:**
- Conns column reflects DRBD-9 connection states (`Connected`,
  `StandAlone`, `Connecting`, `NetworkFailure`, `BrokenPipe`)
- Recovery is automatic once network restores

---

### 17. DISKLESS attachment for pod on a non-replica node

**Why:** Transcript ~21:30. The auto-diskful mechanism: pod
schedules onto a node that doesn't host a replica → CSI publishes
via DRBD diskless.

**Setup:** 3-worker cluster, RD with 2 diskful on worker-1 +
worker-2. Schedule a Pod that mounts the PVC onto worker-3.

**Steps:**
```bash
kubectl apply -f testdata/pod-pinned-worker-3.yaml
sleep 30  # let CSI publish
linstor resource list -r test
```

**Expected:**
- worker-3 row appears with State = `Diskless` (or `UpToDate` with
  DISKLESS layer marker)
- Layers column shows just `DRBD` (no `STORAGE`)
- Pod reads/writes succeed (data round-trips via DRBD-9 replication)

After pod delete:
- worker-3 row drops (or stays DISKLESS until next reconcile —
  blockstor's auto-detach is operator-tunable)

---

## Snapshot and clone operations

The session's csi-sanity work pinned the REST contract; these
add the operator-visible CLI surface.

### 18. `linstor snapshot create` materialises a snapshot per node

**Why:** Transcript ~42:00 (snapshot-to-S3 mention). The
controller-side snapshot definition.

**Setup:** RD with 2 diskful replicas.

**Steps:**
```bash
linstor snapshot create test snap1
linstor snapshot list -r test
linstor snapshot list   # cluster-wide
```

**Expected:**
- Snapshot list shows snap1 with Nodes column = both diskful
  worker names
- Per-node SnapshotNode entries materialised from Spec.Nodes
  (this session's fix — without it linstor-csi says "missing
  snapshots")

---

### 19. `linstor snapshot delete` is idempotent

**Steps:**
```bash
linstor snapshot delete test snap1
linstor snapshot delete test snap1   # repeat
```

**Expected:** Both calls return success; second is a no-op (matches
the `DeleteSnapshot folds NotFound into success` fix landed this
session).

---

### 20. `linstor resource-definition clone` creates a derived RD

**Why:** csi-sanity's `CreateVolume from source volume` exercises
this. Wire shape was the `ResourceDefinitionCloneStarted` envelope
fix landed this session.

**Steps:**
```bash
linstor rd clone test test-clone
linstor rd l
```

**Expected:**
- test-clone appears in rd list
- clone-status endpoint returns Complete within ~30s
- test-clone has its own VDs + replicas (autoplaced)

---

## Storage pool management

### 21. `linstor storage-pool create` adds a new pool on a node

**Why:** Operator adding a new disk to a running cluster.

**Steps:**
```bash
linstor sp create <worker-1> new-pool LVM_THIN \
    --pool-name=blockstor-lvm/thin
linstor sp list
```

**Expected:** new-pool row appears for worker-1 with the right
driver and pool-name.

---

### 22. `linstor storage-pool delete` removes the pool

```bash
linstor sp delete <worker-1> new-pool
linstor sp list   # new-pool row gone
```

---

## Case-insensitive name handling

### 23. Mixed-case RG name normalises to lowercase

**Why:** Upstream LINSTOR is case-insensitive (`DfltRscGrp` and
`dfltrscgrp` address the same object). blockstor stores lowercase
everywhere — verify the CLI's case-permissive lookup still works.

**Steps:**
```bash
linstor rg create MyMixedCaseRG --place-count 2 --storage-pool stand
linstor rg list                 # row shows `mymixedcaserg`
linstor rg list MyMixedCaseRG   # finds it (case-insensitive lookup)
linstor rg modify MYMIXEDCASERG <prop> <val>   # finds it again
```

**Expected:** All three lookups hit the same CRD. The stored name
is lowercase; the CLI tolerates any case on input.

**Failure mode caught:** Without `Name()` lowercasing, blockstor
slugified `MyMixedCaseRG` to `<8hex>-mymixedcaserg` and case-only
lookups missed.

---

## Tests we should explicitly NOT add

These are out of scope for the blockstor REST contract:

- **`drbdadm`-direct manipulation** (transcript 25:30–32:00):
  disconnect/connect/up/down/primary/secondary directly on a
  satellite. blockstor's satellite reconciler owns the kernel; an
  operator running these manually fights the reconciler. Document
  the recovery story in `docs/drbd-recovery.md`, not as a CLI test.
- **Split-brain recovery via `--discard-my-data`** (transcript
  30:00–32:00): same reasoning — blockstor's auto-quorum +
  DrbdOptions defaults are supposed to prevent split-brain
  reaching the operator. If it does, the recovery is satellite-
  level, not CLI-level.
- **External LINSTOR cluster connection** (Q&A 41:46–43:00):
  multi-cluster federation isn't in the cozystack scope.
- **`linstor backup`** (S3 export, Q&A 42:30): out of scope for
  Phase 11.x; revisit when shipping the snapshot-to-S3 path.

---

## Test harness skeleton

```bash
#!/usr/bin/env bash
# tests/e2e/linstor-cli-<scenario>.sh
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"
source "$(dirname "$0")/lib.sh"

# Port-forward blockstor-controller in the background, kill on exit.
kubectl port-forward -n blockstor-system svc/blockstor-controller \
    3370:3370 >/tmp/pf-blockstor.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; cleanup' EXIT
sleep 2

# linstor CLI talks to the forwarded port via default config.
LINSTOR="linstor --controllers=127.0.0.1:3370"

# … per-scenario commands + asserts …
```

Each scenario script:
- Lives in `tests/e2e/linstor-cli-<name>.sh`
- Returns non-zero on any assertion failure
- Dumps CRD YAML + linstor stderr into the log on failure
- Cleans up its fixtures on exit (trap)
- Is independently re-runnable (idempotent setup, full teardown)

Run a single scenario:
```bash
make e2e NAME=e2e6 SCENARIO=linstor-cli-spawn
```

Run the matrix:
```bash
for s in tests/e2e/linstor-cli-*.sh; do
    n=$(basename "$s" .sh | sed s/linstor-cli-//)
    make iter NAME=e2e6 SCENARIO=linstor-cli-$n
done
grep PASS /tmp/iter-*.result
```
