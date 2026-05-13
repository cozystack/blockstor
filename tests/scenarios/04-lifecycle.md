# Group 4 — Resource & node lifecycle

CRUD on Resource / RD / RG, `toggle-disk` (incl. `--migrate-from`
and retry/cancel), snapshot CRUD + clone + restore, node lifecycle
(evacuate / restore / lost, auto-evict), `auto-diskful` +
`auto-diskful-allow-cleanup`.

This is the operator-facing churn surface. Unit tests cover the
REST contract; e2e covers the satellite-finalizer cleanup and
multi-stage operations that need real DRBD.

[Group index in README.md](README.md).

---

## Resource / RD / RG CRUD

### 4.1 RD create + delete cascades to all replicas — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #14

**Unit:** `pkg/rest/resource_definitions_test.go` — DELETE marks RD + all child Resources with FlagDelete; satellite finalizer-driven cleanup runs and CRDs disappear.
**E2E:** `linstor rd d test` → no stuck Terminating after 30s; ports freed; subsequent `rd c test` on same name works.

**Failure mode caught this session:** force-strip finalizers leaves DRBD kernel state alive holding ports 7000-7002. Test MUST use `linstor rd d`, NEVER `kubectl patch --type=json -p='[...remove finalizers]'`.

### 4.2 RD create with existing name returns Conflict — S

- **Priority:** P1  **Target:** unit  **Complexity:** L

`pkg/rest/resource_definitions_test.go` — POST same name twice → 409 with operator-actionable text.

### 4.3 `linstor resource delete <node> <rd>` removes one replica — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #13; drbd-troubleshooting #7

**E2E:** `linstor r d worker-3 test` → TieBreaker stamps on another node OR stays missing per `AutoAddQuorumTiebreaker`; **Conns column on remaining peers does NOT show ghost StandAlone**. Cross-listed with 1.17.

### 4.4 RG modify round-trips SelectFilter changes — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #7

`linstor rg modify default --place-count 3 --storage-pool stand` → subsequent `rg list` reflects new values; next `rg spawn` honours them.

### 4.5 RG delete is refused if RDs exist — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Deleting a resource group" (lines 1404-1438)

Returns 4xx + actionable text "cannot delete: 3 resource-definitions exist". Force-delete only via explicit flag.

### 4.6 `linstor volume-definition set-size` grows/shrinks — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating and deploying resources and volumes" (lines 844-853); PLAN.md volume-resize e2e

**Unit:** PATCH VD size → satellite Reconciler runs `lvextend`/`zfs set volsize` + `drbdadm resize`.
**E2E:** `tests/e2e/volume-resize-pvc.sh` already exists per PLAN.md. Write checksum, grow via REST, verify checksum + filesystem sees new size.

**Note:** UG warns about shrinking with data → blockstor must propagate the warning (or refuse if no `--force` flag).

### 4.7 Multi-volume RD (consistency group) — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"DRBD consistency groups" (lines 1701-1719); ug9-features 6.1

Two `vd c` on same RD → 2 volumes on each replica. Writes across volumes replicate in chronological order.

### 4.8 Per-volume storage-pool routing — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Placing volumes of one resource in different storage pools" (lines 1722-1755); ug9-features 6.2

`linstor vd set-property <rd> <vol> StorPoolName <pool>` routes vol-N to a specific pool. Test: vol 0 on ssd-pool, vol 1 on hdd-pool → `linstor v l` shows different StoragePool per row.

---

## Toggle-disk

### 4.9 `toggle-disk --diskless` ↔ `--storage-pool` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Toggling a resource between diskful and diskless" (lines 3609-3629); existing tests in `pkg/rest/resource_toggle_disk*`

**Unit:** PATCH `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk` flips `Flags[DISKLESS]`.
**E2E:** `linstor r td worker-3 test --diskless` → diskful replica becomes diskless (or vice versa); `linstor r l` reflects.

### 4.10 `toggle-disk --migrate-from <node>` — S, missing test

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Migrating a resource to another node" (lines 3642-3656); ug9-features 5.3

**Why:** Move replica without losing redundancy. Two-step:
```bash
linstor r c bravo backups --drbd-diskless
linstor r td bravo backups --storage-pool pool_ssd --migrate-from alpha
```

**E2E:** 3-replica RD on workers 1/2/3; migrate worker-3 → worker-4 (4-node cluster); replica count is **never** < 3 during migration. Final: worker-3 has no copy, worker-4 has UpToDate.

### 4.11 `toggle-disk` retry on failure — T

- **Priority:** P2  **Target:** e2e  **Complexity:** M (LINSTOR 1.34.0+)
- **Source:** UG9 §"Recovering stuck resources..." (lines 3631-3640); ug9-features 5.2

Re-issuing same `r td` retries; issuing opposite cancels.

**E2E:** trigger toggle-disk with broken pool (`zpool` degraded) → stuck; issue `--diskless` → cancels cleanly, no orphan ZVOL.

---

## Snapshots

### 4.12 Snapshot create + list + delete CRUD — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #18, #19; PLAN.md snapshot reconciler

**Unit:** `pkg/rest/snapshots_test.go` — CRUD idempotent (delete twice is success); `Spec.Nodes` synthesised at create time (see 1.12).
**E2E:** `linstor s c test snap1` → `linstor s l -r test` shows Nodes column = both diskful workers.

### 4.13 Snapshot rollback restores data — S, missing test

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Rolling back to a snapshot" (lines 2500-2522)

Write A → snapshot → write B → rollback → read A. Backend dependent (ZFS_THIN: `zfs rollback`; LVM_THIN: `lvconvert --merge`).

### 4.14 Snapshot restore creates new RD — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Restoring a snapshot" (lines 2474-2498); PLAN.md `POST /v1/resource-definitions/{rd}/snapshot-restore-resource`

`POST snapshot-restore-resource` seeds new RD from snapshot metadata, returns 201. Satellite clones the per-volume data on next reconcile (via `ShipSnapshot` machinery for cross-node).

### 4.15 RD clone — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #20; csi-sanity `CreateVolume from source volume`

`linstor rd clone test test-clone` → ResourceDefinitionCloneStarted envelope (see 1.11). Clone status polls to Complete within ~30s.

### 4.16 Cross-node snapshot ship (within cluster) — S

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** M
- **Source:** PLAN.md `Reconciler.ShipSnapshot`; UG9 §"Snapshot shipping within a single LINSTOR cluster"

ZFS: `zfs send | ssh peer zfs recv`. LVM_THIN: `thin-send-recv`. Existing 3 contract tests.

**Integration:** `pkg/satellite` with `FakeShipExec` — assert command lines.
**E2E:** snapshot a RD on worker-1, restore on worker-2 (no diskful replica there before).

### 4.17 LINSTOR-remote snapshot ship (cross-cluster) — T

- **Priority:** P2  **Target:** e2e  **Complexity:** H (implement)
- **Source:** UG9 §"Shipping snapshots to a LINSTOR remote" (lines 2652-2724); ug9-features 8.3

Direct cluster-to-cluster snapshot replication. P2 — useful for in-cluster DR, not needed if all DR rides on application-layer tooling.

### 4.18 S3 / scheduled backup shipping — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Shipping snapshots to an S3 remote" + §"Scheduled backup shipping"; ug9-features 8.2, 8.4

**Out of scope by design.** Cozystack-side tooling (Velero, etc.) covers cross-cluster DR. Pin: `pkg/rest/remotes.go` stubs return deterministic 501-style responses so operator gets clear error rather than silent success.

---

## Node lifecycle

### 4.19 Node register via Hello — S

- **Priority:** P0  **Target:** integration  **Complexity:** L
- **Source:** PLAN.md satellite-controller gRPC

Satellite dials controller → Hello idempotently upserts Node CRD → returns ClusterID. Test: envtest with mocked satellite client → Node CRD created with NetInterfaces, Aux/, StoragePools[]. Repeating Hello with same node name updates instead of duplicating.

### 4.20 `linstor node evacuate <node>` — S, missing real-world test

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Evacuating a node" (lines 2365-2410); ug9-features 9.1

**E2E:** 4-node cluster, 6 resources; `linstor node evacuate worker-3`; wait <5min; worker-3's resources re-placed on worker-4; resources stay UpToDate throughout; `linstor n l | grep worker-3` shows EVICTED.

**Refuses if InUse:** UG line 2383-2386 — if any resource on the node is InUse (Primary), evacuate fails. Test the refusal path.

### 4.21 Multi-node evacuate — T

- **Priority:** P1  **Target:** e2e  **Complexity:** M (after 4.20)
- **Source:** UG9 §"Evacuating multiple nodes" (lines 2411-2424); ug9-features 9.2

`linstor node evacuate worker-3 worker-4` (variadic). Controller picks an order that doesn't lose redundancy at any point.

### 4.22 `linstor node restore` un-evicts — S, missing test

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** UG9 §"Restoring an evacuating node" (lines 2425-2444); ug9-features 9.3

Evicted node → restore → Online + can be a target for new resources. Existing resources that were already moved off stay (no auto-balance back).

### 4.23 `linstor node lost <node>` deletes Node CRD — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #8; PLAN.md `handleNodeLost`

**Unit:** Lost is idempotent — second call after delete returns success (NotFound folds in).
**E2E:** power off worker-2; `linstor node lost worker-2`; `linstor n l` shows no worker-2 row; existing resources' worker-2 replicas marked DELETING by controller-side cascade; `rd ap` re-places on remaining nodes.

### 4.24 Auto-evict on prolonged satellite offline — T

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** H (implement)
- **Source:** UG9 §"Auto-evict" (lines 4282-4348); ug9-features 3.2

Four properties: `DrbdOptions/AutoEvict{MinReplicaCount,AfterTime,MaxDisconnectedNodes,AllowEviction}`. Satellite offline > `AutoEvictAfterTime` AND `<MaxDisconnectedNodes`% offline AND `AllowEviction=true` → node EVICTED + resources auto-placed elsewhere.

**Status:** `pkg/satellite/reconciler.go` mentions `AutoEvictAllowEviction` but enforcement isn't wired. P1 — without this, node loss requires manual `node evacuate`.

**Integration:** envtest with mocked clock → satellite offline-time elapses → reconciler triggers eviction.
**E2E:** 4-node cluster, RG place-count=3, 5 resources; stop worker-3 satellite + drop kubelet; wait `AutoEvictAfterTime + 1m`; worker-3 → EVICTED; 5 resources re-placed on worker-4.

---

## Auto-diskful

### 4.25 Auto-diskful: Diskless held Primary > N minutes → toggle Diskful — S

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** M
- **Source:** UG9 §"Auto-diskful" (lines 4350-4400); ug9-features 3.3; PLAN.md `internal/controller/auto_diskful_test.go`

**Status:** Implemented; UG-style end-to-end missing.

**Integration:** envtest sets `DrbdOptions/auto-diskful=1` on RD, observer reports Primary InUse on Diskless replica for >1 min → `r td --storage-pool ...` triggers.
**E2E:** 3-replica RD with place-count=2 (1 diskful + 1 diskful + 1 diskless tiebreaker); mount the diskless replica's PV via a Pod that gets scheduled on the tiebreaker node → forces it to Primary; wait > 60s; tiebreaker → Diskful via `r td`; secondary diskful replica is removed via `auto-diskful-allow-cleanup` to maintain place-count=2.

### 4.26 `auto-diskful-allow-cleanup` removes excess after promotion — S

- **Priority:** P1  **Target:** integration  **Complexity:** L
- **Source:** UG9 §"Setting the auto-diskful-allow-cleanup option" (lines 4410-4426)

Test inside 4.25's flow. Default `True` — after Diskless → Diskful, the now-excess Secondary gets removed to fit place-count.

---

## Implementation-order recommendation

1. 4.1, 4.3, 4.4, 4.5 — RD/Resource/RG CRUD foundation (existing)
2. 4.2, 4.6, 4.7, 4.8 — multi-volume + size + per-vol pool routing
3. 4.9, 4.10 — toggle-disk (incl. migrate)
4. 4.12, 4.14, 4.15 — snapshot/clone/restore CRUD
5. 4.13, 4.16 — snapshot rollback + ship
6. 4.19, 4.20, 4.22, 4.23 — node lifecycle
7. 4.24 — auto-evict (P1, implement first)
8. 4.25, 4.26 — auto-diskful e2e
9. 4.21 — multi-node evacuate
10. 4.11, 4.17 — toggle-disk retry, LINSTOR-remote ship (P2 stretch)

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 7 |
| P0 e2e | 7 |
| P1 unit | 3 |
| P1 e2e | 4 |
| P2 e2e | 2 |
| T (implement first) | 3 |
| O (out of scope) | 1 |
