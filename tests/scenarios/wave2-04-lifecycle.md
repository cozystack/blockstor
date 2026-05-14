# Wave 2 — Group 4 — Resource & node lifecycle (Day2 ops)

Full Day2 CRUD churn surface: Node CRUD (add / modify-type / evacuate
× variants / restore / lost / `node info` providers table), RD CRUD
(create / delete with snapshot-block / resize grow+shrink / reassign
to other RG / set-default-storpool), Resource CRUD (manual /
autoplace +1 / drbd-diskless / delete-single-replica / deactivate /
migrate), toggle-disk (add/remove/stuck), multi-volume RDs (consistency
group + per-volume pool routing), and controller-side ops (HA failover,
rolling upgrade).

Pairs with wave1's `04-lifecycle.md` — Day2 scenarios that flesh out
the CRUD churn surface beyond wave1's foundational tests.

[Group index in README.md](README.md).

---

## Node CRUD

### 4.W01 `node create <name> <ip>` registers a satellite — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Adding nodes to your cluster" (lines 465-541) via tests/scenarios/day2-node-add.md

Cross-listed with wave1 4.19. POST `/v1/nodes` upserts Node CRD; default NetInterface name `default` with the supplied IP. Omitting IP → DNS resolve attempt → actionable error if it fails.

**Unit:** envtest Hello + REST create idempotent; second call updates rather than dup-creates.
**E2E:** create → `node list` shows `Online` + `<ip>:3366 (PLAIN)` within 10s.

### 4.W02 Arbitrary node name vs `uname --nodename` — P

- **Priority:** P2  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Naming LINSTOR nodes" (lines 504-518) via tests/scenarios/day2-node-add-with-arbitrary-name.md

LINSTOR node name decouples from hostname; controller logs INFO on mismatch; DRBD `.res` uses real hostname in `on <hostname>` block. Test pins both: REST allows the mismatch, generated `.res` uses kernel hostname.

### 4.W03 `node modify --node-type combined` — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Specifying LINSTOR node types" (lines 542-559) via tests/scenarios/day2-node-modify-type.md

**Out of scope.** blockstor only supports satellite-typed nodes — the controller is a separate Deployment, not a host-co-located process. See `out-of-scope.md` → "Node type CRUD". `NodeType` field stays on the wire (Bug 59) but PUT that flips it returns 501.

### 4.W04 `node lost <node>` destroys cluster state — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Auto-evict" (lines 4281-4348) via tests/scenarios/day2-node-lost.md

Cross-listed with wave1 4.23. Idempotent (second call after NotFound returns success). Distinct from `node delete` which refuses when resources remain. Aggressive — never run on a recoverable node.

### 4.W05 `node evacuate <node>` (single) — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Evacuating a node" (lines 2364-2409) via tests/scenarios/day2-node-evacuate.md

Cross-listed with wave1 4.20. Refuses if any resource is `InUse` (Primary). Refuses if no candidate exists (place-count == node count).

**E2E:** ensure all resources Unused first; `node evacuate worker-3`; wait for SyncTarget rows to settle; `r l --nodes worker-3` empty; final `node delete worker-3` succeeds.

### 4.W06 `node evacuate <node1> <node2>` (multiple) — T

- **Priority:** P1  **Target:** e2e  **Complexity:** M (after 4.W05)
- **Source:** UG9 §"Evacuating multiple nodes" (lines 2410-2423) via tests/scenarios/day2-node-evacuate-multiple.md

Cross-listed with wave1 4.21. Sequence: pre-set `AutoplaceTarget=false` on each target node so no in-flight replica lands back on another evacuating node. Test pins the prop-then-evacuate ordering.

### 4.W07 `node restore` un-evicts (before `node delete`) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** UG9 §"Restoring an evacuating node" (lines 2424-2443) via tests/scenarios/day2-node-evacuate-restore.md

Cross-listed with wave1 4.22. Restored node returns to `Online`; storage pools / properties preserved; already-moved replicas stay on their new hosts (no auto-balance back). Re-enable autoplace target post-restore.

### 4.W08 `node info` reports per-node provider/layer support — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Listing supported storage providers and storage layers" (lines 560-602) via tests/scenarios/day2-node-list-supported-providers.md

Two tables: providers (Diskless / LVM / LVMThin / ZFS{,Thin} / FILE{,Thin}) and layers (DRBD / LUKS / STORAGE — wave1 6.11 narrows out CACHE/WRITECACHE/NVME). Operator's fastest "why didn't autoplace pick this node" diagnostic.

**Unit:** REST handler synthesises both tables from observed capability + installed binaries.
**E2E:** fresh stand — `node info` for each worker shows `Diskless=+`, `LVM=+` or `ZFS=+`, `DRBD=+`.

## RD CRUD

### 4.W09 `rd create <name>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating and deploying resources and volumes" (lines 823-862) via tests/scenarios/day2-rd-create.md

Bare RD reserves a name + port; storage allocated only on `r c` / `rg spawn`. RD defaults to `DfltRscGrp` unless `--resource-group` passed.

### 4.W10 `rd delete <name>` cascades to all replicas — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Deleting a resource definition" (lines 1350-1366) via tests/scenarios/day2-rd-delete.md

Cross-listed with wave1 4.1. **Failure mode pinned this session:** force-strip finalizers leaves DRBD kernel state alive holding ports — test MUST use `linstor rd d`, never `kubectl patch --type=json -p='[remove finalizers]'`.

### 4.W11 RD delete refused while snapshots exist — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Deleting a resource definition" (lines 1364-1366 WARNING) via tests/scenarios/day2-rd-with-snapshot-delete-blocked.md

Returns clear `snapshot` error; operator deletes snapshots first, then RD delete succeeds. Cross-listed with wave2-08.

### 4.W12 `vd set-size` grows online — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating and deploying resources and volumes" (lines 844-853) via tests/scenarios/day2-rd-resize.md

Cross-listed with wave1 4.6. `lvextend` / `zfs set volsize` on each replica + `drbdadm resize` + consumer `resize2fs` / `xfs_growfs`. VD index mandatory even when single-vol.

### 4.W13 `vd set-size` shrink warning — P

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating and deploying resources and volumes" (line 851 WARNING) via tests/scenarios/day2-rd-resize-shrink-warning.md

LINSTOR does NOT enforce FS-shrink-first; data loss if operator shrinks block-device before FS. Test pins: either UG-style warning surfaced in REST response, or `--force` flag required.

### 4.W14 `rd modify --resource-group <other>` re-assigns RD — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Deleting a resource group" (lines 1421-1429) via tests/scenarios/day2-rd-reassign-to-other-rg.md

PATCH `/v1/resource-definitions/{rd}` updates RG ref; existing replicas not moved; only future autoplace / balance uses the new defaults. RD-level props still override new RG.

### 4.W15 RD-level `StorPoolName` default — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 lines 1742-1755 (priority order) via tests/scenarios/day2-rd-set-storpool-default.md

Cross-listed with wave1 4.8. Priority (high → low): VD > resource > RD > node. `r c` without `--storage-pool` resolves via this chain.

## Resource CRUD

### 4.W16 `r c <node> <rd> --storage-pool <pool>` manual placement — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Manually placing resources" (lines 864-874) via tests/scenarios/day2-resource-create-manual.md

Cross-listed with wave1 2.4. Bypasses autoplacer; `BalanceResources` leaves manually-placed resources alone (controller prop). Tiebreaker reconciler still stamps the 3rd diskless.

### 4.W17 `r c --auto-place +1 <rd>` adds one replica — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Using auto-place to extend existing resource deployments" (lines 1246-1278) via tests/scenarios/day2-resource-create-autoplace-plus-one.md

`+1` is only valid on `r c`, not `rg c`. RG's `--place-count` NOT updated; subsequent spawns still use old count. Returns `Not enough available nodes` when constraint-impossible.

### 4.W18 `r c --drbd-diskless` permanent diskless client — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"DRBD clients" (lines 1686-1699) via tests/scenarios/day2-resource-create-drbd-diskless.md

Cross-listed with wave1 5.7. Distinct from auto-stamped TieBreaker — operator-requested = State `Diskless`. Consumes TCP port but no on-disk storage.

### 4.W19 `r d <node> <rd>` removes one replica — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Deleting a resource" (lines 1368-1401) via tests/scenarios/day2-resource-delete-single-replica.md

Cross-listed with wave1 4.3 and 1.17 (Connections cleanup). UG quirk: dropping below 3 diskful triggers controller-side unset of `DrbdOptions/Resource/quorum` and emits INFO; test asserts no spurious 500.

### 4.W20 `r deactivate` for snapshot-ship target (DRBD = permanent) — P

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Shipping a snapshot in the same cluster" (lines 2883-2918) via tests/scenarios/day2-resource-deactivate.md

DRBD-layered resource cannot be re-activated after deactivate — only RESTORE into a new RD (see wave2-08). Test pins: REST `r deactivate` returns 200 + WARNING text about the permanence for DRBD-layered resources.

### 4.W21 `r td --migrate-from <source>` online migration — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Migrating a resource to another node" (lines 3642-3656) via tests/scenarios/day2-resource-migrate.md

Cross-listed with wave1 4.10. Two-step: `r c bravo backups --drbd-diskless` then `r td bravo backups --storage-pool ... --migrate-from alpha`. Replica count never drops during migration.

### 4.W22 `r td <node> <rd> --storage-pool <pool>` diskless → diskful — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Toggling a resource between diskful and diskless" (lines 3608-3629) via tests/scenarios/day2-resource-toggle-disk-add.md

Cross-listed with wave1 4.9 inverse direction. Allocates backing LV/ZVOL; DRBD syncs from peer through Inconsistent → SyncTarget → UpToDate.

### 4.W23 `r td <node> <rd> --diskless` diskful → diskless — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Toggling a resource between diskful and diskless" (lines 3608-3629) via tests/scenarios/day2-resource-toggle-disk-remove.md

Inverse of 4.W22. Refuses if `<node>` is the last diskful replica (no peer to read from).

### 4.W24 `toggle-disk` retry / cancel on stuck — T

- **Priority:** P2  **Target:** e2e  **Complexity:** M (LINSTOR 1.34.0+)
- **Source:** UG9 §"Recovering stuck resources from failed toggle disk operations" (lines 3631-3641) via tests/scenarios/day2-resource-toggle-disk-stuck.md

Cross-listed with wave1 4.11. Re-issuing same command retries; inverse cancels. **Root cause must be fixed first** — retry against unhealthy backend just re-stuck.

## Multi-volume RDs

### 4.W25 Multi-volume RD = DRBD consistency group — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"DRBD consistency groups (multiple volumes within a resource)" (lines 1700-1719) via tests/scenarios/day2-multi-volume-rd.md

Cross-listed with wave1 4.7. Two `vd c` on same RD → 2 backing LVs per replica (`<rd>_00000`, `<rd>_00001`). Cross-volume write order preserved by DRBD; multiple separate RDs do NOT share a consistency group.

### 4.W26 Per-volume storage-pool routing via VD prop — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Placing volumes of one resource in different storage pools" (lines 1721-1755) via tests/scenarios/day2-multi-volume-rd-per-pool.md

Cross-listed with wave1 4.8. `vd set-property <rd> 0 StorPoolName pool_hdd` + `vd set-property <rd> 1 StorPoolName pool_ssd` → vol 0 on HDD, vol 1 on SSD on every replica. Property lookup order in `pkg/store` honours VD > RD > node.

## Controller-side ops

### 4.W27 Controller HA failover preserves cluster state — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Creating a highly available LINSTOR cluster" (lines 1521+) and linstor-kubernetes §"High-availability deployment in Operator v1" (lines 1375-1421) via tests/scenarios/day2-controller-ha-failover.md

**Out of scope.** K8s Deployment + Lease-based leader election already covers the controller-HA contract for blockstor's apiserver (cozystack runs N≥3 replicas behind a Service). See `out-of-scope.md` → "Controller HA failover orchestration". The Java-pacemaker / DRBD-replicated DB orchestration upstream uses doesn't apply.

### 4.W28 Rolling upgrade controller + satellites preserves I/O — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Upgrading LINSTOR" (lines 173-260) via tests/scenarios/day2-upgrade-linstor-controller.md

Controller upgrade BEFORE satellites (backward-compat one-way). For blockstor: `helm upgrade blockstor` → 3-replica Deployment surge update → satellites detect new controller image → rolling DaemonSet restart, one satellite at a time. DRBD survives satellite restarts; Pods stay running.

**E2E:** existing PVC + Pod loop writing 1 KiB/s; helm upgrade with new image tag; assert: no Pod restart; no write error; `node list` shows all Online after each satellite finishes; DRBD `r l` settles UpToDate after each satellite-side resync of accumulated bitmap delta.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 8 |
| P0 e2e | 8 |
| P1 unit | 3 |
| P1 e2e | 5 |
| P2 unit | 2 |
| P2 e2e | 1 |
| T (implement first) | 1 |
