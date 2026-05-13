# LINSTOR UG9 feature coverage scenarios

Final companion file in the series; the other four:
- `tests/linstor-cli-scenarios.md` — CLI surface (happy path)
- `tests/drbd-troubleshooting-scenarios.md` — DRBD failure modes
- `tests/observability-cheat-sheet-scenarios.md` — three-level narrowing
- `tests/advanced-config-scenarios.md` — networks, placement, quorum
- `tests/recovery-skill-scenarios.md` — recovery decision tree

This file is the contract-style read of the upstream
[**LINSTOR User Guide 9**](https://github.com/LINBIT/linstor-documentation/tree/master/UG9/en),
chapters `linstor-administration.adoc` and `linstor-kubernetes.adoc`.
The UG enumerates LINSTOR features chronologically as they were
added; this file translates them to blockstor's reality:

- **Supported (S)** — implemented in blockstor; test exists already
  in one of the other 4 docs OR scenario added below
- **Partial (P)** — implemented but with caveats (e.g., property
  accepted but reconciler doesn't act on it, or feature works for
  one storage backend only)
- **Should support (T)** — not implemented but in-scope for Cozystack
  use cases; a test scenario is included so we know what "done"
  means
- **Out of scope (O)** — explicitly not implementing (NVMe-oF on
  RDMA, DRBD Proxy on cozystack flat-L2 clusters, MariaDB/Postgres
  backend — we use CRDs)

Tests use the dev stand (`make up && make piraeus && make blockstor
&& make pools`) on at least 3 workers. The PrefNic and multi-path
tests need a node with two NICs in different subnets — provisioned
via `stand/setup-host.sh --extra-nic` (operator step on the OCI
host).

---

## Group 1 — Network interface management

### 1.1 PrefNic on node (S, missing test)

**UG ref:** §"Managing network interface cards" (linstor-administration.adoc:2120-2185)

**Setup:** Each worker has two NICs:
- `default` — 10.244.0.0/24 (control / cluster network)
- `repl` — 10.245.0.0/24 (DRBD replication network)

Create both interfaces:
```bash
for n in worker-1 worker-2 worker-3; do
  IP=10.245.0.$(( $(echo $n | tr -dc 0-9) + 10 ))
  linstor node interface create $n repl $IP
done
```

**Steps:**
```bash
# Set PrefNic at node level (recommended by UG: simpler than pool-level)
for n in worker-1 worker-2 worker-3; do
  linstor node set-property $n PrefNic repl
done

# Create a new resource and verify .res points at 10.245.x:
linstor rd c prefnic-test
linstor vd c prefnic-test 1G
linstor r c worker-{1,2,3} prefnic-test --storage-pool zfs-thin
sleep 5
ssh worker-1 'cat /var/lib/linstor.d/prefnic-test.res | grep address'
```

**Expected:**
- Each peer's `address` line shows the 10.245.x interface, not 10.244.x
- DRBD connection comes up on the repl network (`ss -tnp 'sport = :7000'` on worker-1 shows peer = 10.245.x)
- Bytes flow over `repl` NIC under load (verify with `iftraf` or `nload` during a 1 GiB write)

**Blockstor implementation reference:** `pkg/dispatcher/dispatcher.go:peerAddressWithPrefNic` already resolves PrefNic via the storage pool's prop. This test pins that the node-level PrefNic (which UG calls "the safer way") also flows through. If the dispatcher today only honours pool-level PrefNic, that's a P, not S.

### 1.2 PrefNic on storage pool with Diskless caveat (P)

**UG ref:** §"Managing network interface cards" (lines 2152-2160)

The UG warns: setting PrefNic on storage pool means **Diskless / Tiebreaker resources still use `default`** unless you also PrefNic the diskless pool. Test pins blockstor either:
- Behaves consistently with UG (Diskless goes via default) — document this and recommend node-level PrefNic
- OR auto-inherits PrefNic from the diskful pools' peers (better UX) — document that too

**Setup:** PrefNic `repl` only on `zfs-thin` pool (NOT on node). Create a 3-replica resource that gets a Tiebreaker on worker-3 (Diskless).

**Expected:** Audit the .res file on worker-3. Document which interface it uses. If `default` — match upstream and add an operator-facing note in `docs/`. If `repl` — verify the auto-inheritance code path is intentional.

### 1.3 Controller-satellite traffic on dedicated NIC (T)

**UG ref:** §"Managing network interface cards" (lines 2161-2173)

`linstor node interface modify <node> <name> --active` sets `StltCon` flag for controller-satellite traffic. blockstor uses gRPC from satellite to apiserver (port 7000), not LINSTOR's controller-satellite stream. The `StltCon` concept maps to "which IP does the satellite use to dial the apiserver."

**Test:** Verify `linstor node interface list worker-1` shows `StltConn/0/Active=true` on the dialing interface. The contract-test normaliser already includes `StltConn/0/*` keys (`tests/contract/normalize.go`), so the wire surface exists — pin that we actually emit the right values, not just the empty keys.

### 1.4 Multiple DRBD paths (T)

**UG ref:** §"Creating multiple DRBD paths with LINSTOR" (lines 2187-2256)

The UG syntax:
```bash
linstor resource-connection path create alpha bravo myResource path1 nic1 nic1
linstor resource-connection path create alpha bravo myResource path2 nic2 nic2
```

**blockstor status:** Not implemented. `grep -r ResourceConnectionPath` returns zero matches. This is a real gap for HA networking on Cozystack clusters with bonded / redundant uplinks.

**Test (when implemented):**
- Setup 2 NICs per node, in different subnets.
- Create 2 paths per resource-connection.
- Block traffic on path1 via iptables.
- Verify DRBD switches to path2 within `ping-timeout` (default ~5s).

**Implementation footprint:** REST endpoint `/v1/resource-definitions/{rd}/resource-connections/{nodeA}/{nodeB}/paths`, plus `ConfFileBuilder` extension to emit multiple `path { ... }` blocks per connection in the .res file. The blockstor `pkg/drbd/conffile.go` currently emits a single path; adding multi-path is mechanical.

**Priority:** P1. Cozystack HCI deployments typically use a single bond on top of redundant uplinks (Linux handles the redundancy below DRBD), so multi-path is **nice-to-have, not blocking**. Document that explicitly so operators know the workaround.

---

## Group 2 — Resource groups and auto-placement

### 2.1 BalanceResourcesEnabled / Interval / GracePeriod (T)

**UG ref:** §"Automatically maintaining resource group placement count" (lines 887-908)

Three controller properties: `BalanceResourcesEnabled`,
`BalanceResourcesInterval` (default 3600s), `BalanceResourcesGracePeriod` (default 3600s).
LINSTOR 1.26+ runs a recurring task that re-balances resources to
match the resource-group's place-count (e.g., after a satellite is
evicted, new resources get auto-placed to restore the count).

**blockstor status:** Not implemented. `grep BalanceResources` zero
matches. Cozystack's `linstor-affinity-controller` arguably covers
adjacent ground (per-PVC affinity), but the auto-rebalance to fill
place-count gap isn't there.

**Test (when implemented):**
- 3 nodes, 1 RG with place-count=3, 5 resources spawned.
- Stop worker-3 satellite for > `BalanceResourcesGracePeriod` + `BalanceResourcesInterval`.
- Expect: blockstor's controller picks a 4th node (must exist) and auto-places the missing replica.
- Side effect: resources end up balanced (≈ same count per node).

**Priority:** P1 for production-grade clusters; P2 if Cozystack guarantees `kube-scheduler`-style spreading at PVC creation time and tolerates degraded replica counts under node loss.

### 2.2 replicas-on-same / replicas-on-different (S, missing test)

**UG ref:** §"Constraining automatic resource placement by using auxiliary node properties" (lines 1006-1095)

blockstor honours Aux/ node props in the autoplacer. Verify
end-to-end: set `Aux/site=dc1` on workers 1/2, `Aux/site=dc2` on worker-3, RG with `--replicas-on-same site` and `--place-count 2`.

**Expected:**
- All 2 replicas land on the same site (either both dc1 OR both dc2)
- Test inverse: `--replicas-on-different site --place-count 2` → one on dc1, one on dc2

**Failure modes:**
- Aux/ props not synced from k8s node labels (blockstor's k8s store has a "sync labels to Aux/" feature; test that it's actually wired)
- Autoplacer ignores the constraint and places freely

### 2.3 x-replicas-on-different (T)

**UG ref:** §"Ensuring automatic resource placement on different nodes for disaster recovery" (lines 1097-1199)

Form: `--x-replicas-on-different <prop> <max-per-value>`. Example: `--x-replicas-on-different site 2 --place-count 3` → 2 replicas on one site, 1 on another. Used for stretched clusters.

**blockstor status:** No mention in code. P1 for cross-DC Cozystack deployments.

**Test (when implemented):**
- 6 nodes: 2 in dc1, 2 in dc2, 2 with no `site` aux
- RG `--x-replicas-on-different site 2 --place-count 3`
- Verify: 2 replicas land in one of {dc1, dc2, unset}, 1 in a different group

### 2.4 Storage pool selection strategies (P)

**UG ref:** §"Storage pool placement" (lines 933-993)

Four strategies (`MaxFreeSpace`, `MinReservedSpace`, `MinRscCount`,
`MaxThroughput`) weighted via `Autoplacer/Weights/*` props.

**blockstor status:** Autoplacer is "greatest-free-first, deterministic ties on NodeName" (per PLAN.md line 367). That's effectively `MaxFreeSpace` with no weighting. The other three strategies + weighting are **not implemented**.

**Test (when implemented):**
- Set `Autoplacer/Weights/MinRscCount=1` on controller, `Autoplacer/Weights/MaxFreeSpace=0`
- Expect: placement minimises per-pool resource count (round-robin) rather than free-space-first

**Priority:** P2 — `MaxFreeSpace` is sane default for Cozystack's homogeneous clusters.

### 2.5 do-not-place-with / do-not-place-with-regex (T)

**UG ref:** §"Constraining automatic resource placement..." (line 993)

Useful for: "don't co-locate replicas of the user's stateful set with each other on the same node."

**blockstor status:** Not implemented.

**Test (when implemented):** Create 3 resources tagged with the same StatefulSet hash; verify autoplace honours the constraint and doesn't pile them onto one node.

### 2.6 layer-list / providers constraint (S, test partial)

**UG ref:** §"Constraining automatic resource placement by LINSTOR layers or storage pool providers" (lines 1201-1232)

`--providers ZFS_THIN` → only place on ZFS_THIN pools.
`--layer-list drbd,storage` → only use pools that can layer DRBD over Storage.

**blockstor status:** RG schema accepts these; autoplacer must filter. Verify via test:
```bash
linstor rg c zfs-only --place-count 2 --providers ZFS_THIN
linstor rg sp zfs-only zfs-test 1G
linstor r l -r zfs-test   # all replicas on ZFS_THIN pools
```

---

## Group 3 — DRBD options and automatisms

### 3.1 Auto-quorum policies (S, missing explicit test)

**UG ref:** §"Auto-quorum policies" (lines 4234-4279)

`DrbdOptions/auto-quorum` ∈ {disabled, suspend-io, io-error}. Default
behaviour: when ≥ 2 diskful + 1 diskless OR ≥ 3 diskful, quorum is
auto-configured with `quorum majority + on-no-quorum io-error`.

**blockstor status:** `storetest.go` already pins
`DrbdOptions/auto-quorum: io-error`. Verify on the stand:
- 3-replica RD → `linstor rd lp <rsc> | grep quorum` shows `majority`
- Set `auto-quorum=disabled` on RG, spawn new RD → no auto-config
- Test in `tests/advanced-config-scenarios.md` §Quorum policies (existing)

### 3.2 Auto-evict (T)

**UG ref:** §"Auto-evict" (lines 4282-4348)

Four properties:
- `DrbdOptions/AutoEvictMinReplicaCount`
- `DrbdOptions/AutoEvictAfterTime` (default 60 min)
- `DrbdOptions/AutoEvictMaxDisconnectedNodes` (default 34%)
- `DrbdOptions/AutoEvictAllowEviction` (default true)

When satellite is offline > `AutoEvictAfterTime` AND fewer than `MaxDisconnectedNodes`% are offline AND `AllowEviction=true` → node marked EVICTED, resources auto-placed elsewhere.

**blockstor status:** `pkg/satellite/reconciler.go` mentions
`DrbdOptions/AutoEvictAllowEviction` is consumed but the actual
eviction logic isn't there yet. P1 — without auto-evict the cluster
relies on manual `node evacuate` after hardware loss.

**Test (when implemented):**
- 4-node cluster, RG place-count=3, 5 resources
- Stop worker-3 satellite + drop its kubelet
- Wait `AutoEvictAfterTime + 1m`
- Verify: worker-3 → EVICTED, 5 resources re-placed on worker-4

### 3.3 Auto-diskful and auto-diskful-allow-cleanup (S, missing failover test)

**UG ref:** §"Auto-diskful and related options" (lines 4350-4426)

`DrbdOptions/auto-diskful = <minutes>` — if a Diskless node holds Primary for > N minutes, LINSTOR auto-toggles it to Diskful. `auto-diskful-allow-cleanup` — after toggle, demote / remove from secondary that no longer fits replica count.

**blockstor status:** auto-diskful **is implemented** (`internal/controller/resource_controller.go:auto-diskful path`, `internal/controller/auto_diskful_test.go`). But UG-style end-to-end test missing.

**Test:**
- 3-replica RD with `place-count=2` (1 diskful, 1 diskful, 1 diskless tiebreaker)
- Set `DrbdOptions/auto-diskful=1` on RD
- Mount the diskless replica via a Pod (forces it to Primary)
- Wait > 60s
- Verify: tiebreaker → Diskful via `r td --storage-pool ...`; secondary diskful replica is removed by `auto-diskful-allow-cleanup` to maintain place-count=2

### 3.4 SkipDisk on I/O error (T)

**UG ref:** §"SkipDisk" (lines 4428-4460)

DRBD detects I/O errors → state goes UpToDate → Failed → Diskless → LINSTOR auto-sets `DrbdOptions/SkipDisk=True` on the resource → satellite passes `--skip-disk` to subsequent `drbdadm adjust`.

**blockstor status:** Observer needs to detect the events2 transition and write the prop. Reconciler needs to honour the prop and pass `--skip-disk`. Verify with grep — likely not implemented.

**Test:** Inject I/O errors via `dmsetup` (replace the backing device with a `dm-error` target), observe:
- Observer reports the state transition in `Resource.Status.DiskState`
- `DrbdOptions/SkipDisk` flips to `True` automatically
- `linstor r l` shows the `(R)` marker as UG describes
- After cleanup (remove dm-error), `r sp <node> <rsc> DrbdOptions/SkipDisk` (no value) clears the prop and DRBD re-syncs

**Priority:** P1. Without SkipDisk, a single failed disk can wedge `drbdadm adjust` cluster-wide.

### 3.5 DRBD options at all levels (S)

**UG ref:** §"Setting DRBD options for LINSTOR objects" (lines 3301-3434)

LINSTOR supports `drbd-options` (RD / RG / controller level) and `drbd-peer-options` (per resource-connection / node-connection).

**blockstor status:** RD-level props supported via `rd sp`. **Resource-connection-level and node-connection-level peer-options** — need to verify. The UG shows `linstor resource drbd-peer-options --max-buffers 8192 node-0 node-1 backups` writes to `/v1/resource-definitions/backups/resource-connections/node-0/node-1`. Check if blockstor's REST has that endpoint.

**Test:**
- Set `--max-buffers 8192` via resource-connection peer-options
- Verify the prop appears in the .res file on both peers' connection block
- Verify rebuild of .res file doesn't drop the override on reconcile

---

## Group 4 — Over-provisioning

### 4.1 MaxFreeCapacityOversubscriptionRatio (P)

**UG ref:** §"Configuring a maximum free capacity over provisioning ratio" (lines 3454-3499)

Default 20. Caps `MaxVolumeSize` returned by `rg query-size-info` to `free_space × ratio`.

**blockstor status:** OpenAPI generated types include `MaxFreeCapacityOversubscriptionRatio` etc.; presence of an actual gate (rg query-size-info or autoplace blocks based on this) needs to be verified.

**Test:**
- ZFS_THIN pool with 10 GiB free
- Set `MaxFreeCapacityOversubscriptionRatio=2`
- `linstor rg query-size-info <rg>` → MaxVolumeSize = 20 GiB
- Attempt `linstor rg spawn rg too-big 25G` → expect: rejected with "exceeds oversubscription"
- Default ratio (20): MaxVolumeSize = 200 GiB

### 4.2 MaxTotalCapacityOversubscriptionRatio (T)

**UG ref:** §"Configuring a maximum total capacity over provisioning ratio" (lines 3501-3530)

Caps total over-subscription based on total pool capacity (not free). Combined with the free-capacity ratio, the more restrictive wins.

**Test:** Same as 4.1 but against `total_capacity`.

### 4.3 MaxOversubscriptionRatio (T)

**UG ref:** §"Configuring a maximum over subscription ratio for over provisioning" (lines 3531-3550)

Caps the sum of provisioned sizes / actual usable capacity.

**Test:** Provision 100 × 1 GiB volumes on a 50 GiB pool (oversub = 2×); set `MaxOversubscriptionRatio=1.5` and verify the 76th volume creation fails. (Or run on smaller numbers.)

**Priority for all three:** P1. Cozystack tenants over-allocate aggressively; without these gates a single tenant can OOM the pool.

---

## Group 5 — Toggling and migration

### 5.1 toggle-disk diskful ↔ diskless (S)

**UG ref:** §"Toggling a resource between diskful and diskless" (lines 3609-3640)

Implemented in `pkg/rest/resource_toggle_disk.go`. Existing tests cover the happy path.

### 5.2 toggle-disk retry / cancel (T)

**UG ref:** §"Recovering stuck resources from failed toggle disk operations" (lines 3631-3641, LINSTOR ≥ 1.34.0)

If toggle fails mid-flight (backing storage issue, satellite disconnect), re-issuing the same command retries, issuing the opposite cancels.

**Test:**
- Trigger a toggle-disk → diskful with a deliberately broken pool (e.g., zpool degraded)
- Confirm it's stuck (FlagDiskAdding doesn't clear)
- Cancel with `r td --diskless`
- Verify: resource ends up Diskless, no orphan ZVOL

### 5.3 resource toggle-disk --migrate-from (S, missing test)

**UG ref:** §"Migrating a resource to another node" (lines 3642-3656)

Two-step pattern:
```bash
linstor r c bravo backups --drbd-diskless
linstor r td bravo backups --storage-pool pool_ssd --migrate-from alpha
```

Server waits for sync from alpha to bravo, then removes alpha's diskful copy.

**Test:**
- 3-replica RD on workers 1/2/3
- Migrate worker-3's copy → worker-4 (4-node cluster)
- Verify: at no point is replica count < 3 (no redundancy loss during migration)
- Final: worker-3 has no copy, worker-4 has UpToDate, workers 1/2 untouched

**blockstor status:** Worth verifying — UG says the LINSTOR client orchestrates the wait. blockstor's REST should expose the `migrate_from` field in the toggle-disk request body and the controller should handle the wait + cleanup.

---

## Group 6 — Storage layout

### 6.1 DRBD consistency groups (multi-volume RD) (S)

**UG ref:** §"DRBD consistency groups" (lines 1701-1719)

Two `volume-definition create` calls on the same RD give you a multi-volume resource where writes across volumes are replicated in chronological order.

**blockstor status:** OpenAPI schema supports multiple `VolumeDefinitions` per RD. Test:
- `linstor vd c multi 500M` (vol 0)
- `linstor vd c multi 100M` (vol 1)
- `linstor r c worker-{1,2,3} multi`
- Write to /dev/drbd<X> and /dev/drbd<Y> on Primary, verify peer reads both

### 6.2 Per-volume storage pool routing (S, missing test)

**UG ref:** §"Placing volumes of one resource in different storage pools" (lines 1722-1755)

`linstor volume-definition set-property <rd> <vol> StorPoolName <pool>` routes that volume to a specific pool. Useful for: data on SSD, metadata on HDD.

**Test:**
- RD with two volumes
- Set `StorPoolName=ssd-pool` on vol 0, `StorPoolName=hdd-pool` on vol 1
- Verify `drbd-overview` / `linstor v l` shows volumes on different backing devices

### 6.3 External DRBD metadata (StorPoolNameDrbdMeta) (T)

**UG ref:** §"Using external DRBD metadata" (lines 4463-4534)

`StorPoolNameDrbdMeta` (settable on node/RG/RD/resource/VG/VD) routes metadata to a separate pool — useful for putting metadata on faster storage than data, or sharing a metadata SSD across many data HDDs.

**blockstor status:** Not implemented. `grep StorPoolNameDrbdMeta` returns 0 matches.

**Test (when implemented):**
- Two pools per node: `ssd-meta` and `hdd-data`
- Set `StorPoolNameDrbdMeta=ssd-meta` on RD
- Verify `pkg/storage/lvm` / `pkg/storage/zfs` creates two LVs/ZVOLs per replica: one for data on hdd, one for meta on ssd
- DRBD `.res` file uses `meta-disk /dev/...` pointing at the ssd LV
- `dd` 1 GiB → data flows to hdd, metadata updates to ssd (verify via `iostat -dx 1`)

**Priority:** P2. Cozystack's typical deployment is homogeneous (one tier of storage); external metadata is more useful in stretched / hybrid storage setups.

### 6.4 Layer stack rules (S)

**UG ref:** §"Using LINSTOR without DRBD" (lines 1758-1837)

The layer table:
| Layer | Child layer |
|-------|-------------|
| DRBD | CACHE, WRITECACHE, NVME, LUKS, STORAGE |
| CACHE | WRITECACHE, NVME, LUKS, STORAGE |
| WRITECACHE | CACHE, NVME, LUKS, STORAGE |
| NVME | CACHE, WRITECACHE, LUKS, STORAGE |
| LUKS | STORAGE |
| STORAGE | - |

**blockstor status:** Supports DRBD, LUKS, STORAGE layers (per `pkg/satellite/reconciler.go`). NVME, CACHE, WRITECACHE not implemented.

**Test (enforcement contract):** `linstor rd c x --layer-list cache,storage` should be accepted; `--layer-list cache,drbd,storage` should be rejected (cache can't be parent of drbd). Verify blockstor enforces ordering on RD create.

---

## Group 7 — Encryption

### 7.1 Master passphrase / cluster encryption (S)

**UG ref:** §"Encryption commands" (lines 2288-2324)

`linstor encryption create-passphrase`, `enter-passphrase`, `modify-passphrase`. blockstor has these in `pkg/rest/encryption.go`.

### 7.2 Automatic passphrase from env / linstor.toml (O)

**UG ref:** §"Automatic passphrase" (lines 2326-2351)

`MASTER_PASSPHRASE` env var OR `[encrypt] passphrase=...` in linstor.toml — auto-creates / auto-enters passphrase on startup.

**blockstor status:** **Orchestrated by piraeus-operator, not by
blockstor.** piraeus reads the passphrase Secret and calls the
standard `linstor encryption create-passphrase` /
`enter-passphrase` REST endpoints (covered in 7.1) against
blockstor. blockstor's job is to honour those REST calls
correctly — the "auto" part (Secret → REST call → controller
restart loop) belongs one layer up.

**What blockstor must guarantee:**
- `create-passphrase` is idempotent: re-issuing against an already-seeded passphrase returns success (not "already exists" error that would break piraeus's reconciler)
- `enter-passphrase` after apiserver pod restart re-unlocks any prior LUKS state without losing data
- Both endpoints are tested in `pkg/rest/encryption_test.go`

If piraeus's auto-passphrase orchestration ever breaks against
blockstor, the failure is in REST-contract divergence — covered by
the contract-replay tests against the LINSTOR oracle.

### 7.3 LUKS layer + DRBD shared-secret (S)

**UG ref:** §"Encrypted volumes" + PLAN.md line 385-386

LUKS at data-at-rest, `DrbdOptions/Net/shared-secret` for in-transit. Both implemented. Existing tests in `tests/advanced-config-scenarios.md`.

---

## Group 8 — Snapshots and backups

### 8.1 Local snapshots (S)

**UG ref:** §"Working with resource snapshots and backups" (lines 2445-2530)

Create, restore, rollback, delete. blockstor: `pkg/rest/snapshots.go`. Existing tests.

### 8.2 Snapshot shipping to S3 (O)

**UG ref:** §"Shipping snapshots to an S3 remote" (lines 2549-2650)

**blockstor status:** **Out of scope by design.** Cozystack-side
backup tools (Velero, k8s-native VolumeSnapshot-driven object-store
exporters) cover the cross-cluster DR use case at a higher layer
where they can coordinate application-consistent backups across
multiple PVCs. Pushing S3 shipping into the storage control plane
duplicates that surface and forces blockstor to own S3 credentials,
retries, and lifecycle.

The `/v1/remotes` REST stubs in `pkg/rest/remotes.go` return
deterministic 501-style responses so `linstor remote create s3 ...`
gives a clear error rather than appearing to succeed silently.

### 8.3 Snapshot shipping to LINSTOR remote (T)

**UG ref:** §"Shipping snapshots to a LINSTOR remote" (lines 2652-2724)

Direct cluster-to-cluster snapshot replication, no object store
involved — `zfs send | ssh peer-cluster zfs recv`-style on the wire.

**blockstor status:** REST stubs only.

**Test (when implemented):**
- Two stand clusters (`make up NAME=src && make up NAME=dst`)
- Create LINSTOR remote on `src` pointing at `dst`'s apiserver
- Snapshot a resource on `src`, ship to `dst`
- Verify the resource appears on `dst` with matching data

**Priority:** P2 — useful for in-cluster DR between Cozystack
clusters under common ops control; not needed if all DR rides on
application-layer tooling.

### 8.4 Scheduled backup shipping (O)

**UG ref:** §"Scheduled backup shipping" (lines 2946-3300)

**blockstor status:** **Out of scope by design.** Scheduling +
retention belongs in k8s-side tooling (Velero, snapshotter CronJobs,
external orchestrators); the storage control plane stays focused on
the snapshot primitive itself.

---

## Group 9 — Node lifecycle

### 9.1 Node evacuate (S, missing real-world test)

**UG ref:** §"Evacuating a node" (lines 2365-2425)

`linstor node evacuate <node>` flags satellite for cordon, autoplacer re-distributes its resources elsewhere.

**blockstor status:** `pkg/rest/node_lifecycle.go:handleNodeEvacuate` implemented (sets EVICTED flag). The auto-placement of evicted resources to other nodes — verify it actually happens.

**Test:**
- 4-node cluster, 6 resources spread across all
- `linstor node evacuate worker-3`
- Wait < 5 min
- Expect: worker-3's resources re-placed on worker-4; resources stay UpToDate throughout
- `linstor n l | grep worker-3` shows EVICTED status

### 9.2 Multi-node evacuate (T)

**UG ref:** §"Evacuating multiple nodes" (lines 2411-2424)

`linstor node evacuate worker-3 worker-4` (variadic).

**Test:** Variadic argument flows through REST → controller picks an order that doesn't lose redundancy at any point.

### 9.3 Restore evacuating node (S, missing test)

**UG ref:** §"Restoring an evacuating node" (lines 2425-2444)

`linstor node restore <node>` clears EVICTED flag.

**Test:** Evacuate, then restore — node returns to Online + can be a target for new resources. Existing resources that were already moved off stay where they are (no auto-balance back).

---

## Group 10 — QoS

### 10.1 sysfs blkio throttle (T)

**UG ref:** §"QoS settings" (lines 4537-4609)

Four properties:
- `sys/fs/blkio_throttle_read` (bytes/s)
- `sys/fs/blkio_throttle_write`
- `sys/fs/blkio_throttle_read_iops`
- `sys/fs/blkio_throttle_write_iops`

Settable on volume / SP / resource / controller / node / RG / VG / RD / VD.

**blockstor status:** Not implemented. Cozystack uses container-level cgroup limits (kubelet enforces via Pod resources), so block-device level QoS is **probably out of scope**. But: the `MaxThroughput` autoplacer strategy (Group 2.4) depends on this prop — if you implement that strategy, the props need to be readable.

**Test (when implemented):**
- Set `sys/fs/blkio_throttle_write=1048576` on VD
- Verify `/sys/fs/cgroup/blkio/blkio.throttle.write_bps_device` on the satellite has the value
- `fio` write hits the throttle

**Priority:** P3. Container-level QoS is the cozystack-native answer.

---

## Group 11 — k8s-specific

### 11.1 StorageClass parameters (S)

**UG ref:** linstor-kubernetes.adoc §"Available parameters in a storage class" (1770+)

linstor-csi parameter passthrough. blockstor's REST surface must accept the CSI-shaped requests these parameters produce (autoPlace, placementCount, storagePool, replicasOnSame, replicasOnDifferent, encryption, layerList, etc.).

**Coverage:** csi-sanity tests cover the wire path. The behaviour is pinned by the existing CLI scenarios doc and Group 2 above.

### 11.2 property.linstor.csi.linbit.com/* passthrough (S, missing test)

**UG ref:** lines 2186-2199

`property.linstor.csi.linbit.com/DrbdOptions/auto-quorum: disabled` in StorageClass parameters → linstor-csi sets the prop on the RG → propagates to all RDs spawned from it.

**Test:**
- StorageClass with `property.linstor.csi.linbit.com/DrbdOptions/auto-quorum=disabled`
- Create PVC, verify `linstor rd lp <rsc> | grep auto-quorum` = `disabled`

### 11.3 K8s node labels → Aux/ props sync (P)

**UG ref:** linstor-kubernetes.adoc §replicasOnSame (lines 2098+) — "The operator periodically synchronizes all labels from Kubernetes Nodes."

**blockstor status:** verify this label-sync exists. If not, document as gap — without it `replicasOnSame: "topology.kubernetes.io/zone=z1"` won't match anything.

**Test:**
- Label k8s node with `topology.kubernetes.io/zone=z1`
- Wait <60s
- `linstor n lp worker-1 | grep Aux/topology` shows the synced prop

### 11.4 Affinity controller (S)

**UG ref:** linstor-kubernetes.adoc §"LINSTOR affinity controller" (2698+)

Piraeus deploys `linstor-affinity-controller` separately. blockstor inherits this — verify it works against blockstor's REST (the affinity controller reads from `/v1/view/resources` and writes PV affinity annotations).

### 11.5 Fast workload failover via HA controller (S)

**UG ref:** linstor-kubernetes.adoc §"Fast workload failover using the high availability controller" (1384+)

Piraeus `ha-controller` evicts pods from a failed node faster than k8s' default. Already deployed in our stand (`piraeus-datastore` namespace has `ha-controller-*`). Just verify it works against blockstor-backed PVCs.

**Test:**
- Pod with PVC on worker-1
- Hard-stop worker-1 (kubelet + node)
- Expect: ha-controller evicts the pod within ~10s (not the default 5min toleration)
- Pod reschedules to worker-2; PVC reattaches; data intact

---

## Priority summary

| Group | Item | Status | Priority |
|-------|------|--------|----------|
| 1.1 PrefNic node-level | S | P0 test |
| 1.2 PrefNic pool-level + Diskless | P | P1 doc + test |
| 1.4 Multiple DRBD paths | T | P1 implement |
| 2.1 BalanceResources | T | P1 implement |
| 2.3 x-replicas-on-different | T | P1 implement |
| 2.4 Autoplacer weights | P | P2 implement |
| 3.2 Auto-evict | T | P1 implement |
| 3.4 SkipDisk | T | P1 implement |
| 4.1-4.3 Over-subscription ratios | P/T | P1 implement+test |
| 5.2 toggle-disk retry/cancel | T | P2 implement |
| 6.3 External DRBD metadata | T | P2 implement |
| 7.2 Auto-passphrase orchestration | O | — (piraeus owns it) |
| 8.2 S3 snapshot shipping | O | — (k8s-side tooling) |
| 8.3 LINSTOR remote shipping | T | P2 implement |
| 8.4 Scheduled backup shipping | O | — (k8s-side tooling) |
| 9.1 Node evacuate | S | P0 test |
| 10.1 sysfs blkio | T | P3 (likely OOS) |
| 11.3 K8s label → Aux/ sync | P | P1 verify |

P0 = test gap, no new code. P1 = blocks GA for cozystack
production-grade deployments. P2 = stretch goal. P3 = out-of-scope
unless customer asks.

---

## Cross-document index — final consolidated map

| Topic | Source doc | This doc |
|-------|-----------|----------|
| CLI happy path | linstor-cli-scenarios.md | Group 7, 8 |
| DRBD9 failure modes (the article) | drbd-troubleshooting-scenarios.md | Group 3.4 (SkipDisk) |
| Three-level narrowing | observability-cheat-sheet-scenarios.md | (referenced) |
| Networks / placement / quorum | advanced-config-scenarios.md | Group 1, 2, 3.1 |
| Recovery decision tree | recovery-skill-scenarios.md | Group 9 |
| **Official UG9 feature parity** | **this doc** | — |

The five docs together form a closed loop: any feature LINSTOR
documents and Cozystack expects to work has a test scenario *somewhere*
across these files. New features should land their tests in whichever
doc fits the theme; this UG9 doc is the index of record.
