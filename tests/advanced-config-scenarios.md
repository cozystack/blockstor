# Advanced configuration test scenarios

Fourth companion doc to:

- `tests/linstor-cli-scenarios.md` — operator-facing happy path
- `tests/drbd-troubleshooting-scenarios.md` — DRBD9 failure modes
- `tests/observability-cheat-sheet-scenarios.md` — three-level narrowing flow

This file covers the **configuration knobs** an operator turns to
adapt LINSTOR to their environment: separate replication networks,
placement constraints, quorum tuning for VM workloads, drive
replacement, port range expansion, performance tuning, and fault
injection. Each section comes from a LINBIT KB article and maps to
one or more blockstor test scenarios.

---

## Network configuration

### 1. Separate replication network via NetInterface + PrefNic

**Source:** "Configure separate networks for DRBD replication"
([kb.linbit.com](https://kb.linbit.com/linstor/kubernetes/configure-separate-networks-for-drbd-replication-linstor-op-v1-kubernetes/)).

**Why it matters:** Production clusters typically separate
Kubernetes control-plane traffic from DRBD replication for
predictable bandwidth. blockstor's `Node.Spec.NetInterfaces[]`
already supports multiple interfaces per node (covered by
linstor-cli-scenarios.md #4 net-interfaces phase). This test pins
the end-to-end flow: configure a second NIC, ask the StorageClass
to use it, verify DRBD's `.res` reflects the right addresses.

**Setup:** Stand workers each have a second NIC on a separate
subnet (e.g. `10.10.10.0/24`).

**Steps:**

```bash
# Add the replication interface on each satellite-backed Node:
for n in worker-1 worker-2 worker-3; do
    linstor node interface create $n repl-net 10.10.10.${ip##*-}
done

# StorageClass pins the interface:
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: pref-nic-sc}
provisioner: linstor.csi.linbit.com
parameters:
  property.linstor.csi.linbit.com/PrefNic: repl-net
  ...
EOF

# Provision + verify
kubectl apply -f testdata/pvc-pref-nic.yaml
linstor r l -r <pvc-vol-id>
satellite_exec worker-1 cat /etc/drbd.d/<pvc-vol-id>.res
```

**Expected:**
- `linstor node interface list <worker-1>` shows two interfaces
  (default + repl-net)
- The provisioned resource's `.res` file uses `10.10.10.x`
  addresses in its `address` blocks, NOT the default
  control-plane IP
- DRBD connection establishes over the secondary network (verify
  with `ss -tn dst 10.10.10.0/24` on the satellite host)

**Failure modes:**
- StorageClass `PrefNic` parameter ignored → satellite uses default
  interface → control plane saturates under heavy replication
- Interface registered but `repl-net` IP not in `.res` → satellite-
  side rendering doesn't honour PrefNic

---

## Placement constraints

### 2. Topology-aware replicasOnDifferent via Aux/topology props

**Source:** "Controlling replicas using Kubernetes node labels and
LINSTOR auxiliary properties"
([kb.linbit.com](https://kb.linbit.com/linstor/kubernetes/controlling-replicas-using-kubernetes-node-labels-and-linstor-auxiliary-properties/)).

**Why it matters:** Multi-rack / multi-AZ clusters need replicas
spread across failure domains. The upstream operator syncs k8s
node labels (`topology.kubernetes.io/zone`, `.../region`,
`kubernetes.io/hostname`) to LINSTOR Aux/topology props; the
StorageClass `replicasOnDifferent` parameter then constrains
placement.

**Setup:** 6-worker stand, labelled into 3 zones:

```bash
kubectl label node worker-1 worker-2 topology.kubernetes.io/zone=a
kubectl label node worker-3 worker-4 topology.kubernetes.io/zone=b
kubectl label node worker-5 worker-6 topology.kubernetes.io/zone=c
```

**Steps:**

```bash
# Verify the operator picked up labels into Aux/topology:
linstor node list-properties worker-1
# Expected: Aux/topology/topology.kubernetes.io/zone=a

# StorageClass that spreads:
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: spread-zones}
provisioner: linstor.csi.linbit.com
parameters:
  replicasOnDifferent: "topology.kubernetes.io/zone"
  placementCount: "3"
  storagePool: stand
EOF

# Provision + verify spread
kubectl apply -f testdata/pvc-spread.yaml
linstor r l -r <pvc-vol-id>
```

**Expected:**
- 3 replicas, each on a worker from a different zone (one per zone)
- `replicasOnSame` variant (`zone=a`) clusters replicas in the
  same zone

**Failure modes:**
- Operator side: blockstor doesn't sync k8s node labels to
  `Node.Props["Aux/topology/..."]` → constraints have nothing to
  filter on. Whichever component owns this sync (controller-side
  NodeReconciler watching k8s `Node`) needs explicit coverage.
- Placer side: `replicasOnDifferent` parameter ignored → 3 replicas
  land on workers from the same zone defeating HA.

**Open question:** does blockstor's placer honour
`replicasOnDifferent` / `replicasOnSame`? Check `pkg/placer/` for
the constraint pass.

---

### 3. Per-node AutoplaceTarget=false excludes a node

**Source:** "Preventing LINSTOR resource placement on a node"
([kb.linbit.com](https://kb.linbit.com/linstor/preventing-linstor-resource-placement-on-a-node/)).

**Why it matters:** Operator needs to drain a node for maintenance
without evicting existing replicas — just stop the placer from
choosing it for new ones.

**Setup:** 3-worker stand, no existing RDs.

**Steps:**

```bash
linstor node set-property worker-2 AutoplaceTarget false

# Try autoplace 10 RDs:
for i in {1..10}; do
    linstor rd c test-$i
    linstor vd c test-$i 100M
    linstor rd ap test-$i --place-count 2
done

# Check distribution:
linstor r l --output-version=v0 | grep -v worker-2
```

**Expected:**
- All 20 resources land on worker-1 + worker-3 (none on worker-2)
- Existing replicas on worker-2 (if any) are untouched
- After `linstor node set-property worker-2 AutoplaceTarget true`,
  new RDs start landing on worker-2 again

**Failure modes:** blockstor's placer ignores `AutoplaceTarget`
prop → maintenance node receives new placements unexpectedly.

---

### 4. Custom Aux property for deploy-zone constraint

**Why:** Same as #2 but using a non-topology Aux property that
blockstor doesn't sync automatically. Tests the manual-Aux flow.

**Steps:**

```bash
linstor node set-property --aux worker-1 deploy-zone production
linstor node set-property --aux worker-2 deploy-zone production
linstor node set-property --aux worker-3 deploy-zone staging

linstor rg create prod-only \
    --replicas-on-same Aux/deploy-zone=production \
    --place-count 2

linstor rg spawn prod-only test-prod 1G
linstor r l -r test-prod
```

**Expected:** Both replicas land on worker-1 + worker-2 (production
zone); worker-3 (staging) never selected.

---

## TCP port range

### 5. TcpPortAutoRange expansion unblocks placement at scale

**Source:** "Change TCP port range for DRBD resources"
([kb.linbit.com](https://kb.linbit.com/linstor/linstor-change-tcp-port-range-for-drbd-resources/)).

**Why it matters:** Default range 7000–7999 = 1000 ports = 1000
RDs max. Production clusters with thousands of PVCs exhaust this.
This session's DRBD-port-collision incident on e2e6 was a related
class of bug (collision after force-strip), but the underlying
"port pool exhausted" is also a real failure.

**Setup:** Stand with 9 storage pools (more than enough for stress
test).

**Steps:**

```bash
# Read current default
linstor controller list-properties | grep TcpPortAutoRange
# Expected: 7000-7999 (or blockstor's default)

# Stress: create 1001 RDs to overflow the default range:
for i in $(seq 1 1001); do
    linstor rd c stress-$i
    linstor vd c stress-$i 10M
    linstor rd ap stress-$i --place-count 1
done

# Expect: 1001st create fails with port-exhaustion error
# (or whatever code blockstor returns for this case)

# Expand the range:
linstor controller set-property TcpPortAutoRange 7000-9999

# Retry the failed RD:
linstor rd ap stress-1001 --place-count 1
```

**Expected:**
- Pre-expansion: 1001st autoplace returns an error citing port
  exhaustion (not generic "internal error" — operator-actionable
  message)
- Post-expansion: same autoplace succeeds with a port from
  8000–9999 range

**Implementation pointer for blockstor:** the port allocator in
`internal/controller/resource_controller.go:allocatePort` already
reads from `Spec.DRBDPortRange` on the Node and falls back to
controller defaults. Test that `TcpPortAutoRange` on
ControllerConfig.Spec.ExtraProps overrides the compiled-in
default.

---

## Quorum policies for VM workloads

### 6. `auto-quorum=suspend-io` keeps VMs alive through quorum loss

**Source:** "LINSTOR quorum policies and VM environments"
([kb.linbit.com](https://kb.linbit.com/linstor/linstor-quorum-policies-and-vm-environments/)).

**Why it matters:** Default DRBD behaviour on quorum loss is
`io-error`, which kicks VM filesystems read-only. For
virtualisation workloads, `suspend-io` is preferred: I/O blocks
until quorum returns, VM stays running.

**Setup:** 3-worker stand. RG with VM-friendly quorum:

```bash
linstor rg modify default \
    DrbdOptions/Resource/auto-quorum suspend-io
linstor rg modify default \
    DrbdOptions/Resource/on-no-data-accessible suspend-io
```

**Steps:** Spawn an RD from this RG, mount it on a worker, write
in a loop. Then break quorum by partitioning two workers from the
third.

```bash
# On the surviving minority partition, `dd` writes hang (don't
# error out). After re-partitioning to restore quorum, dd resumes.
```

**Expected:**
- Spawn-time `.res` includes `options { quorum majority;
  on-no-quorum suspend-io; }`
- During quorum loss, writes block (verify with `ps` showing the
  dd process in `D` state)
- After heal, writes resume without filesystem corruption
- Without the override, the default `io-error` would have made
  `mount /dev/drbdN` switch to read-only

**Implementation pointer:** these are pure DrbdOptions props on
RG/RD. blockstor's `.res` renderer
(`pkg/drbd/conffile.go`) needs to surface them into the `options
{}` block.

---

### 7. `AutoAddQuorumTiebreaker=False` for 2-replica RDs

Already covered in linstor-cli-scenarios.md #12 — same DrbdOptions
key as #6 above. Worth pinning that blockstor's RG-level prop
inherits down to RDs and prevents the `ensureTiebreaker`
reconciler from over-stamping.

---

## Drive replacement

### 8. Failed disk replacement recovers via satellite reconnect

**Source:** "Replacing a failed drive in LINSTOR"
([kb.linbit.com](https://kb.linbit.com/linstor/replacing-a-failed-drive-in-linstor/)).

**Why it matters:** The cheapest physical-recovery path. When a
disk dies, replace it, recreate the LVM/ZFS pool with the same
name, and let the satellite rediscover it. No data-plane operator
commands needed — the controller-side reconciler converges.

**Setup:** RD with 2 diskful replicas on LVM-thin pool. Power down
worker-2, swap disk, power back up.

**Steps:**

```bash
# On worker-2 host post-swap (via talosctl/console):
pvcreate /dev/sdb
vgcreate blockstor-lvm /dev/sdb   # same VG name as before
lvcreate -T -L 14G blockstor-lvm/thin

# Trigger satellite re-discovery:
linstor node reconnect worker-2

# Wait + verify
linstor sp list | grep worker-2
linstor r l -r <rd>
```

**Expected:**
- StoragePool reappears under worker-2 with capacity reflecting
  the new disk
- worker-2's replica re-syncs (Inconsistent → SyncTarget →
  UpToDate)
- Existing replicas on worker-1 / worker-3 are unaffected
- DRBD-side: `drbdadm status` on worker-2 shows the disk state
  transition

**Failure modes:**
- StoragePool stays missing → satellite reconcile didn't pick up
  the new VG (image missing `lvm2-pvscan` or the
  `StoragePoolReconciler` filter is too strict)
- Replica stuck Inconsistent → DRBD metadata zone reused from old
  disk → bitmap mismatch; recovery is `linstor r d worker-2 <rd>`
  + `rd ap` (covered in drbd-troubleshooting-scenarios.md #7)

---

## Performance-tuning properties

### 9. Write-perf DrbdOptions surface through controller-props

**Source:** "Using LINSTOR to tune DRBD for write performance"
([kb.linbit.com](https://kb.linbit.com/linstor/linstor-using-linstor-to-tune-drbd-for-write-performance/)).

**Why it matters:** Operators tune DRBD via `linstor controller
drbd-options --max-buffers=N` etc. These all map to
`DrbdOptions/{Net,Disk,…}/<key>` props on the controller. Test
that blockstor's `.res` renderer surfaces them.

**Setup:** Fresh stand.

**Steps:**

```bash
# Tune writes-heavy:
linstor controller drbd-options --max-buffers=10000
linstor controller drbd-options --max-epoch-size=10000
linstor controller drbd-options --al-extents=65534
linstor controller drbd-options --disk-flushes=no
linstor controller drbd-options --md-flushes=no

# Verify props persisted:
linstor controller list-properties | grep -E 'max-buffers|al-extents|flushes'

# Spawn a new RD, check .res honours the props:
linstor rd c perf-test
linstor vd c perf-test 1G
linstor rd ap perf-test --place-count 2

satellite_exec worker-1 cat /etc/drbd.d/perf-test.res
# Expected: `disk { al-extents 65534; disk-flushes no; md-flushes no; }`
#           `net { max-buffers 10000; max-epoch-size 10000; }`
```

**Expected:** Every prop set via `controller drbd-options` appears
in the rendered `.res` for new RDs. Existing RDs receive the props
on next reconcile (`linstor rd adjust` flow).

**Failure modes:**
- Props persisted but `.res` renderer doesn't include them →
  `pkg/drbd/conffile.go` missing the option in its template
- Renderer includes them but kernel rejects → typo in option name
  vs DRBD-9 schema

---

### 10. External DRBD metadata pool on a separate device

**Source:** "Optimizing write performance with DRBD and RAID"
([kb.linbit.com](https://kb.linbit.com/linstor/linstor-optimizing-write-performance-with-drbd-and-raid/)).

**Why it matters:** DRBD metadata writes are small + frequent
(activity log). Co-locating them with bulk data on a RAID5
triggers read-modify-write penalties. Solution: store metadata
on a separate pool (typically NVMe), let the data pool be the
RAID-backed bulk store.

**Setup:** Worker with two storage pools: `storage` (LVM-thin on
RAID5) + `meta` (LVM-thin on NVMe).

**Steps:**

```bash
linstor rg create striped-rg --storage-pool storage --place-count 2
linstor rg set-property striped-rg StorPoolNameDrbdMeta meta
linstor rg spawn striped-rg test-perf 10G

satellite_exec worker-1 cat /etc/drbd.d/test-perf.res
# Expected: `meta-disk` block points to /dev/<meta-pool>/...,
# NOT internal metadata in the data LV
```

**Expected:** `.res` `meta-disk` directive references the meta
pool's device, not `internal`. DRBD's small metadata writes go to
the NVMe; bulk data writes go to the RAID.

**Implementation pointer:** blockstor's
`pkg/dispatcher/conffile-render.go` (wherever the meta-disk
directive lives) needs to honour `StorPoolNameDrbdMeta` from RG/RD
props. Today blockstor likely defaults to `internal` always.

---

## Backend storage capabilities matrix

### 11. Each backend supports its declared feature set

**Source:** "Comparing LINSTOR back-end storage types"
([kb.linbit.com](https://kb.linbit.com/linstor/comparing-linstor-back-end-storage-types/)).

**Why it matters:** Pin which storage backends support which
features. blockstor advertises feature flags through
`StoragePool.Status.SupportsSnapshots`, `Static_traits`, etc.
Operators rely on these for RG placement decisions.

**Test matrix:**

| Backend          | Snapshots | Thin | Encryption (LUKS) | Compression |
|------------------|-----------|------|-------------------|-------------|
| LVM (thick)      | ✗ ✗       | ✗    | ✓ (via layer)     | ✗           |
| LVM_THIN         | ✓         | ✓    | ✓                 | ✗           |
| ZFS              | ✓         | ✗ ✓  | ✓                 | ✓ (zfs)     |
| ZFS_THIN         | ✓         | ✓    | ✓                 | ✓           |
| FILE             | ✗         | ✗    | ✓                 | ✗           |
| FILE_THIN        | ✓ (reflink)| ✓    | ✓                 | ✗           |

**Steps per backend:** create storage pool, run snapshot CRUD,
verify `Status.SupportsSnapshots` matches actual capability, try
clone-from-snapshot, expect success or `MethodNotSupported`
matching the matrix.

```bash
# Example: LVM thick should reject snapshot
linstor sp create worker-1 thick-pool LVM --pool-name=vg-thick
linstor rd c lvm-thick-test
linstor vd c lvm-thick-test 100M
linstor r c worker-1 lvm-thick-test -s thick-pool

linstor snapshot create lvm-thick-test snap1
# Expected: error "storage backend does not support snapshots"
```

**Failure modes:** blockstor returns success on unsupported
operations (silent no-op) → test catches this. Or capability flag
in `Status` is wrong → operator's UI/CSI driver thinks snapshots
work and fails at runtime.

---

## Fault injection

### 12. dmsetup error target simulates per-sector I/O failures

**Source:** "How to simulate disk I/O error" (linux-tips KB).

**Why it matters:** Real-disk failures are rare in CI, so we need
synthetic injection. `dmsetup` error targets let us turn specific
byte ranges of a block device into permanent I/O errors at the
device-mapper layer.

**Use-case for blockstor:** test the satellite observer's
`disk:Failed` event handling
(`tests/drbd-troubleshooting-scenarios.md` #4 Inconsistent state).
Today the observer's auto-detach path is unit-tested in
`pkg/satellite/observer_test.go` because the e2e flow needs
writable `/sys` which Talos PSA forbids. With `dmsetup` injection
on a non-Talos worker (or a privileged debug pod) we get real
kernel events.

**Setup:** Privileged debug pod on a worker with `dmsetup`
available.

**Steps:**

```bash
# Wrap the backing LV with a failing dm device:
size=$(blockdev --getsz /dev/blockstor-lvm/some-lv)
# Build a table with a corrupt 1MB region at offset 100MB:
cat <<EOF > /tmp/fail.table
0 204800 linear /dev/blockstor-lvm/some-lv 0
204800 2048 error
206848 $((size-206848)) linear /dev/blockstor-lvm/some-lv 206848
EOF
dmsetup create faildev < /tmp/fail.table

# Reroute the satellite to use the faildev (would require pointing
# blockstor's StoragePool at /dev/mapper/faildev instead of the
# raw LV — design TBD)

# Force a read of the bad region:
dd if=/dev/drbd<N> bs=4096 skip=25000 count=1 of=/dev/null
# Expected: I/O error, DRBD events2 emits `change device disk:Failed`,
# observer auto-detaches per the on-io-error=detach policy

linstor r l -r <rd>
# Expected: this node's replica shows State=Failed or Diskless
# (after auto-detach)
```

**Expected:** Detach happens within ~10s of the I/O error. The
replica transitions to Diskless. After replacing the underlying
device (#8), the operator runs `linstor r c <node> <rd> -s <pool>`
to re-add a diskful replica.

**Open question:** does blockstor's satellite expose a config
knob to point a StoragePool at a dm device rather than a raw LV
for testing? Probably needs a small `StoragePool.Spec.DeviceOverride`
or similar — out-of-scope for now, document as future work.

---

## Out of scope

These were on the original article list but don't fit blockstor's
current scope:

- **S3 remote backup** (Storj guide) — Phase 11.x doesn't include
  the LINSTOR `backup create`/`backup ship` paths. The endpoints
  return 501 from blockstor today (see `pkg/rest/remotes.go`'s
  empty-list stubs). Revisit when shipping the snapshot-to-S3 leg
  per the operational follow-up in PLAN.md.
- **RWX volumes** — Already covered by `tests/e2e/rwx-ganesha.sh`
  (technically piraeus-stack smoke since RWX rides on
  linstor-csi's NFS-Ganesha sidecar, not blockstor's own REST
  surface).

---

## Priority

| # | Test                                       | Priority | Notes |
|---|--------------------------------------------|----------|-------|
| 1 | Separate replication network (PrefNic)     | Medium   | Production-relevant, requires multi-NIC stand |
| 2 | replicasOnDifferent via Aux/topology       | High     | HA correctness; needs node-label sync |
| 3 | AutoplaceTarget=false drains placer        | High     | Common maintenance op |
| 4 | Custom Aux property constraints            | Medium   | Variant of #2 |
| 5 | TcpPortAutoRange expansion                 | High     | This session hit the port-collision class |
| 6 | auto-quorum=suspend-io for VMs             | High     | Data-protection invariant for VM workloads |
| 8 | Drive replacement recovery                 | Medium   | Operator runbook coverage |
| 9 | DrbdOptions surface in .res renderer       | High     | Foundational — any tuning depends on this |
| 10| External DRBD metadata pool                | Low      | Niche perf optimisation, may not be in scope |
| 11| Backend capabilities matrix                | Medium   | Catches silent no-ops on unsupported ops |
| 12| dmsetup fault-injection harness            | Medium   | Future work — design needed first |

---

## Cross-document index

| Topic                          | linstor-cli | drbd-tshoot | observability | advanced-config |
|--------------------------------|:-----------:|:-----------:|:-------------:|:---------------:|
| Node CRUD                      | #1, #11     |             | #1            |                 |
| Storage-pool CRUD              | #2, #21–22  |             | #1, #9        | #8, #10, #11    |
| Net-interfaces                 | #4 (recorder)|            |               | #1              |
| RG/RD/Resource provisioning    | #9, #10, #11|             |               |                 |
| Resource delete + recovery     | #13, #14    | #7          | #5            |                 |
| Snapshot/clone                 | #18–20      | (data plane out of scope) | | #11 (snapshot capability) |
| DRBD states / Conns column     |             | #1–4        | #4, #8        |                 |
| Split-brain                    |             | #10         |               |                 |
| Quorum / tiebreaker            | #12         |             |               | #6, #7          |
| Port allocation                |             | #11 (node-id stability) |  | #5              |
| Topology / placement constraints |           |             |               | #2, #3, #4      |
| DRBD perf tuning               |             |             |               | #9, #10         |
| Fault injection                |             | #4, #11     |               | #12             |

The four docs are intentionally non-overlapping. If you're adding
a test, find your topic in this matrix to identify which doc owns
it.
