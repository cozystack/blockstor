# Test strategy

A 3-tier test architecture that closes the gap between unit-level `go test ./...` (which keeps shipping bugs to operator hands) and slow real-cluster e2e (which is too expensive to gate every PR on). Drafted after the 2026-05-14 hand-found bug spree (Bugs 78–83) — every one of them passed unit tests and was caught in seconds the moment a real `linstor` CLI hit the apiserver.

## Execution tracker

See `docs/agent-playbook.md` for the per-agent contract. Status values: `pending`, `in_progress`, `done`. Each row owned by exactly one agent at a time.

| Phase | Group | Tests | Status | Branch / PR |
|---|---|---|---|---|
| 0 | Harness scaffold + smoke | 1 | done | landed in 2a8e532a5 |
| 1 | A — Node | 10 | done | landed in ed96c13bf |
| 1 | B — Storage Pool | 9 | pending | — |
| 1 | C — Resource Group | 7 | done | landed via cherry-pick |
| 1 | D — Resource Definition | 9 | done | landed via cherry-pick |
| 1 | E — Volume Definition | 7 | pending | — |
| 1 | F — Resource | 14 | pending | — |
| 1 | G — Snapshot | 10 | pending | — |
| 1 | H — Controller / Error Reports / KV | 6 | pending | — |
| 1 | I — Node/Resource connections | 6 | pending | — |
| 1 | J — CSI | 12 | pending | — |
| 1 | K — Workflows | 12 | done | landed via cherry-pick |
| 1 | L — Concurrency / cache trail | 6 | done | landed via cherry-pick |
| 2 | Tier 3 — drbd-utils contract | 5 | pending | — |
| 2 | CI wiring (`.github/workflows/integration.yml`) | — | done | committed with this doc |
| 2 | E2E cleanup — drop duplicates folded into Tier 2 | — | pending | — |

Total Tier 2 tests: 108. Tier 3: 5. Tier 4 retained: 8 scripts.

## Tier architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Tier 2: tests/integration/      envtest + native linstor CLI │
│   What:    REST/CRD/reconciler contracts, operator workflows │
│   Where:   GitHub Actions, every PR, < 5 minutes             │
│   Covers:  ~70% of wave1+wave2 scenarios                     │
├──────────────────────────────────────────────────────────────┤
│ Tier 3: tests/contract/         drbd-utils in Docker         │
│   What:    drbdmeta / drbdadm-parser byte-shape              │
│   Where:   GitHub Actions Linux runner, every PR, < 1 minute │
│   Covers:  Bug 81-class                                      │
├──────────────────────────────────────────────────────────────┤
│ Tier 4: tests/e2e/              Talos VM with real DRBD      │
│   What:    kernel-state (suspended:quorum, SplitBrain, sync) │
│   Where:   nightly or pre-release manual, 30 minutes         │
│   Covers:  what Tier 2/3 physically cannot                   │
└──────────────────────────────────────────────────────────────┘
```

---

## Tier 2: scaffold (`tests/integration/`)

### Harness components

| File | Responsibility |
|---|---|
| `harness/envtest.go` | Boots `envtest.Environment{}` (in-process apiserver+etcd), applies CRDs from `config/crd/bases/` |
| `harness/manager.go` | Starts the controller-runtime Manager with ALL reconcilers (Node, Resource, RD, RG, SP, Snapshot, AutoSnapshot, AutoDiskful, AutoEvict, RGRebalance) + REST on a free `127.0.0.1:N` |
| `harness/satellite.go` | In-process satellite reconciler with per-node `FakeExec` — mocks drbdadm/drbdsetup/lvm/zfs while writing Status (DiskState, ConnectionState, FreeCapacity, PoolMissing) like a real satellite |
| `harness/linstor.go` | `exec.Command("linstor", "--controllers", url, "--machine-readable", args...)` + JSON response parsing + stderr scan for python traceback (pattern lifted from `client-compat.sh`) |
| `harness/csi.go` | gRPC client to our CSI plugin started in-process against the same apiserver |
| `harness/fixtures.go` | Pre-seeded `Node` × 3 + `StoragePool` × 9 (lvm/zfs/file × 3 worker), `controller` SP, default `RG` |
| `harness/asserts.go` | `Eventually(t, 30s, func() bool {...})`, `MustList`, `MustGet`, `WaitForDRBDState` (via satellite mock) |
| `harness/concurrent.go` | Helpers for goroutine-storm tests: `RunParallel(t, N, fn)` |

### CI requirements

- `setup-envtest` action caches `kube-apiserver` + `etcd`
- `apt install -y linstor-client python3-linstor` in the job
- Build constraint `//go:build integration` so `go test ./...` does not pull integration tests by default

---

## Tier 2: tests (per group)

### Group A — Node (`node_test.go`, 10 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestNodeListEmpty` | `linstor n l` with no nodes → empty envelope, not a traceback | wire-shape |
| `TestNodeListAfterCreate` | create 3 nodes → `linstor n l` shows 3 | basic |
| `TestNodeCreatePopulatesNetIf` | `linstor n c worker-1 10.0.0.1` → CRD NetInterfaces correct | basic |
| `TestNodeRestorePUT` | `linstor n restore` — PUT route | **Bug 78** |
| `TestNodeEvacuatePUT` | `linstor n evacuate` — PUT + InUse refusal + force | **Bug 78**, Bug 18 |
| `TestNodeEvictPUT` | `linstor n evict` alias | **Bug 78** |
| `TestNodeLostCascadesOrphans` | `linstor n lost X` → all resources on X removed | Bug 28 |
| `TestNodeReconnect` | PUT /reconnect → 200 no-op | wire-shape |
| `TestNodeAuxLabelSync` | k8s label X on node → visible in `linstor n l --aux` | Bug 13, F7 |
| `TestNodeWireShapeFields` | all JSON envelope fields from openapi.json are present | Bug 59 |

### Group B — Storage Pool (`storagepool_test.go`, 9 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestSPListAfterFixtures` | 9 SPs from fixtures are visible | basic |
| `TestSPCreatePerProvider` | LVM_THIN/ZFS_THIN/FILE/FILE_THIN/DISKLESS → ProviderKind correct in CRD and `sp l` | Bug 63, 73 |
| `TestSPDeleteEmpty` | delete without resources → 200 | basic |
| `TestSPDeleteRefusesIfInUse` | RD on the SP → delete → 409 conflict | Bug 52 |
| `TestSPPoolMissingReportsFaulty` | satellite stamps PoolMissing=true → `linstor sp l` renders Faulty + reports[] | **Bug 83**, Bug 74 |
| `TestSPCapacityFlow` | satellite writes FreeCapacity → REST returns it | basic |
| `TestSPSetProperty` | `linstor sp sp <prop> <val>` → CRD Props updated | basic |
| `TestPhysicalStorageList` | `linstor ps l` → non-empty, doesn't crash on zero devices | Bug 51, 70, 72 |
| `TestPhysicalStorageCreateDevicePool` | `linstor ps cdp zfs --pool-name X /dev/Y` → end-to-end up to SP | Bug 68, 73 |

### Group C — Resource Group (`resourcegroup_test.go`, 7 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestRGListCreateDelete` | basic CRUD | wire-shape |
| `TestRGSpawnResources` | `rg sr <rg> <name> --auto-place=2` → RD + 2 R created | basic |
| `TestRGDeleteRefusesIfHasRDs` | `rg d <rg>` with RD children → 409 | Bug 11 |
| `TestRGModifyReAutoplaces` | rg-modify placementCount=2→3 → dependent RDs gain +1 replica | Bug 60 |
| `TestRGEffectivePropsChain` | RG.Prop X → RD inherits → R inherits | Bug 54 |
| `TestRGSetPropertyDrbdNet` | `rg sp <rg> DrbdOptions/Net/ping-timeout 500` → R-rendered .res has it | 5.W03 |
| `TestRGListPropertiesContract` | envelope shape (Bug 60) | wire-shape |

### Group D — RD (`resourcedefinition_test.go`, 9 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestRDCreateListDelete` | basic CRUD | wire-shape |
| `TestRDInheritsLayerStackFromRG` | rg layer=DRBD,LUKS,STORAGE → rd auto-inherits | Bug 54 |
| `TestRDDeleteCascadesResourcesSnapshots` | rd d → R+S removed | Bug 1, 4 |
| `TestRDCloneFromSource` | `rd clone <src> <dst>` → backing storage clone + DRBD metadata | Bug 15, 21 |
| `TestRDListWithVolumeDefinitions` | `?with_volume_definitions=true` → VDs included | Bug 53 |
| `TestRDFilterByRscDfns` | `?rsc_dfns=a,b` → only a,b in response | Bug 61 |
| `TestRDListLayerData` | layer_data[] populated | Bug 58 |
| `TestRDSetPropertyEffective` | `rd sp <rd> X Y` → effective-props endpoint sees it | Bug 203 |
| `TestDfltRscGrpCanonical` | spawn without rg → DfltRscGrp (CamelCase) | Bug 57 |

### Group E — VD (`volumedefinition_test.go`, 7 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestVDCreateListDelete` | basic | wire-shape |
| `TestVDModifyGrowSize` | size 100M→200M → R gets Resize | Bug 36 |
| `TestVDModifyMergeProps` | modify does not drop existing props | Bug 36, 37 |
| `TestVDLateAddTriggersReconcile` | rd→r (no VD) → r=Unknown → vd c → r=UpToDate (NOT Diskless) | **Bug 79** |
| `TestVDCreateWithoutRD` | vd c without rd → 404 | wire-shape |
| `TestVDSetProperty` | `vd sp <rd> 0 X Y` | basic |
| `TestVDFreeSpaceFromBackingPool` | VD propagates FreeCapacity hints | Bug 35 |

### Group F — Resource (`resource_test.go`, 14 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestRCreateExplicit` | `r c <node> <rd>` | basic |
| `TestRAutoPlace2ReachesUpToDate` | `r c <rd> --auto-place 2` → exactly one auto-primary, both UpToDate, tiebreaker on the 3rd node | **Bug 80** |
| `TestRAutoPlace3WithTieBreaker` | placementCount=3 on 3 nodes → 3 diskful, no tiebreaker | Bug 28 |
| `TestRToggleDiskful2Diskless` | flag flip + drbdadm detach captured | basic |
| `TestRToggleDiskless2Diskful` | reverse | basic |
| `TestRToggleCancel` | `r td --cancel` aborts | Bug 40 |
| `TestRMigrateDisk` | `r migrate-disk from to` → add-before-drop | Bug 34 |
| `TestRActivateDeactivate` | wire-shape OK envelope | Bug 45, 46 |
| `TestRDeleteIdempotent` | delete 404 → 200 (matches upstream) | Bug 56, 66 |
| `TestRDeleteCascadesSnapshots` | r d → child Snapshots swept | Bug 1 |
| `TestRListFaultyFilter` | `--faulty` | F5 |
| `TestREffectivePropsEndpoint` | `/v1/resources/<r>/effective-properties` | Bug 203 |
| `TestRListVolumePoolField` | StoragePool in volume entries is not None | Bug 75 |
| `TestRSetPropertyDrbdNet` | per-R DrbdOptions/Net/* → satellite emits | basic |

### Group G — Snapshot (`snapshot_test.go`, 10 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestSnapCreateListDelete` | basic | wire-shape |
| `TestSnapCreateEmptyNameFails` | `snap c <rd> ""` → 400 | Bug 200 |
| `TestSnapListPagination` | `?offset=N&limit=M` | Bug 201 |
| `TestSnapDeleteIdempotent` | repeat delete → 200 | Bug 199 |
| `TestSnapRestoreCreatesNewRD` | `snap restore` → new RD with cloned VD | F1 |
| `TestSnapRollbackOnExistingRD` | `snap rollback` → existing RD reverts | F1, Bug 21 |
| `TestSnapShipCrossNode` | satellite FakeExec captures send-recv | F8 |
| `TestSnapOrphanCleanup` | finalizer-strip → orphan ZFS snap detected by sweeper | Bug 64, Bug 43 |
| `TestSnapDeleteBlockedByLater` | snap ordering: delete of older blocked by newer | wave2 |
| `TestAutoSnapshotPeriodicTick` | RG `AutoSnapshot/Run` prop → snapshots created | basic |

### Group H — Controller / Error Reports / KV (`controller_test.go`, 6 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestControllerVersion` | `linstor c v` | wire-shape |
| `TestControllerListProperties` | `linstor c lp` | basic |
| `TestControllerSetProperty` | `linstor c sp DrbdOptions/Net/X Y` → effective on all RDs | basic |
| `TestErrorReportsList` | envelope + pagination | F6, Bug 62 |
| `TestErrorReportsFilterByNode` | `--nodes=X` | F6 |
| `TestKVStoreCRUD` | linstor kv list/show/modify | basic |

### Group I — Node-connection / Resource-connection (`connections_test.go`, 6 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestNodeConnectionSetProperty` | `linstor node-connection sp A B X Y` | basic |
| `TestResourceConnectionPathCreate` | multi-path DRBD .res | 3.7 |
| `TestPingTimeoutPropagation` | controller>RG>RD>R precedence | 5.W03 |
| `TestNetOptionsSplit` | `connect-int`, `max-buffers`, `protocol` | 5.W04 |
| `TestEffectivePropsAtAllLevels` | walk levels via the effective-props endpoint | Bug 203 |
| `TestDrbdProxyEndpoint` | `linstor drbd-proxy ...` envelope | wave2 |

### Group J — CSI (`csi_test.go`, 12 tests)

Bring up the CSI gRPC server in-process against the same apiserver+REST.

| Test | Scenario | Bug guard |
|---|---|---|
| `TestCSIIdentityServer` | GetPluginInfo, Capabilities | wire-shape |
| `TestCSICreateVolumeFromEmpty` | CreateVolume → RD+VD+R created, autoplace=N | basic |
| `TestCSICreateVolumeIdempotent` | repeat CreateVolume same name → 200 same volume | wire-shape |
| `TestCSIDeleteVolume` | DeleteVolume → RD removed + cascade | basic |
| `TestCSIControllerPublish` | Publish on diskful node → 200 | basic |
| `TestCSIControllerPublishDiskless` | Publish on node without replica → creates diskless + publishes | Bug 30, F4, 7.12 |
| `TestCSICreateSnapshot` | wire-shape | csi-sanity |
| `TestCSIDeleteSnapshotIdempotent` | repeat delete → 200 | Bug 199 |
| `TestCSIListSnapshotsPagination` | envelope + nextToken | Bug 201 |
| `TestCSICreateVolumeFromSnapshot` | clone-via-snapshot pipeline | F1 |
| `TestCSICreateVolumeFromClone` | direct clone | Bug 15 |
| `TestCSIValidateCapabilities` | rejects unsupported access modes | wire-shape |

### Group K — Workflows (`workflow_test.go`, 12 tests) — "operator-day"

These tests stitch several groups into realistic sequences.

| Test | Scenario | Bug guard |
|---|---|---|
| `TestWFHappyPath` | rd→vd→r×2→snap→delete | smoke |
| `TestWFLateVD` | rd→r×2 (Unknown)→vd→UpToDate | **Bug 79** |
| `TestWFAutoPlace2Concurrent` | rd→vd→`r c --auto-place=2` | **Bug 80** |
| `TestWFNodeEvacuateReplaceRestore` | evacuate→migration→add new node→restore old | Bug 19, 5.9 |
| `TestWFNodeLostCascade` | node lost → orphans + port/minor recycled | Bug 28 |
| `TestWFPoolDestroyedDropsFromPlacer` | satellite signals PoolMissing → placer skips → reports faulty | **Bug 83**, Bug 35 |
| `TestWFReplicasOnSame` | placer respects `RscGrp/replicas-on-same` | wave1 2.7 |
| `TestWFReplicasOnDifferent` | wave1 2.7 | basic |
| `TestWFLUKSStackEndToEnd` | encryption passphrase → LUKS layer in .res | 6.W12 |
| `TestWFSpawnAndDependentReAutoplace` | rg spawn → modify rg → dependents re-autoplace | Bug 60 |
| `TestWFBalanceResourcesTick` | RGRebalance fires exactly on prop interval | 2.15, 2.20 |
| `TestWFToggleDiskUnderSync` | toggle-disk during SyncTarget defers | Bug 8 |

### Group L — Concurrency / cache trail (`concurrent_test.go`, 6 tests)

| Test | Scenario | Bug guard |
|---|---|---|
| `TestConcurrentRDCreateSameName` | 10 parallel creates → exactly one succeeds | basic |
| `TestConcurrentAutoplaceSameRG` | 10 parallel spawn → no duplicate placements | wave1 2.x |
| `TestConcurrentAutoPrimaryElection` | auto-place=2, n satellites reconcile in parallel → exactly one auto-primary | **Bug 80** |
| `TestConcurrentRDDeleteAndRCreate` | r c during rd d → no orphan resource | Bug 1 |
| `TestConcurrentSnapDeleteAndRDDelete` | overlap → consistent end-state | Bug 1, 65 |
| `TestConcurrentSPModify` | sp modify race vs satellite capacity update | wave1 |

---

## Tier 3: `tests/contract/` (drbd-utils in Docker)

3–5 files, all Linux-only with `t.Skip` on macOS.

| Test | What it checks |
|---|---|
| `TestDrbdmetaCreateMDSucceeds` | exec `drbdmeta /dev/loop0 v09 internal create-md --max-peers=15` on a loop file → exit 0 |
| `TestDrbdmetaSetGiRequiresNodeId` | `set-gi <gi>` without `--node-id` → exit 10 with expected message (**Bug 81** pin) |
| `TestDrbdmetaSetGiPerPeer` | our exact call `set-gi --node-id 1 <gi>` → exit 0 |
| `TestDrbdmetaDumpMdParses` | `dump-md` → parsed by our Status reader |
| `TestDrbdadmConfigValidate` | `drbdadm dump <rsc>` against the .res from `pkg/drbd/conffile.go` → exit 0 |

`Dockerfile` (alpine + drbd-utils package, ~20 MB), built once, `docker run` per-test ~200ms.

---

## Tier 4: `tests/e2e/` (the parts that physically need a kernel)

**Keep** (audit + cleanup of existing scripts):

| Script | Why Tier 4 only |
|---|---|
| `tests/e2e/recovery-standalone.sh` | SplitBrain → StandAlone, discard-my-data |
| `tests/e2e/recovery-synctarget.sh` | replication:SyncTarget, partial bitmap |
| `tests/e2e/recovery-suspended-quorum.sh` | **Bug 82** real kernel state |
| `tests/e2e/recovery-disk-replace.sh` | external metadata |
| `tests/e2e/csi-with-kubelet.sh` | mount, formatfs, pod restart |
| `tests/e2e/iptables-partition.sh` | network partition wave1 5.10, 7.11 |
| `tests/e2e/burnin-zfs-thin.sh` | 24h sustained I/O (task #73) |
| `tests/e2e/client-compat.sh` | keep as "smoke" against real DRBD |

**Remove after migration to Tier 2**:

- Every `tests/e2e/*.sh` that does not actually exercise kernel-state (e.g. `linstor-cli.sh`, `cheat-sheet-cli-*.sh`) — folded into `tests/integration/cli_smoke_test.go`

---

## Roadmap (1.5–2 weeks of work, 1 person)

### Week 1

- **Day 1**: scaffold harness (envtest, manager, linstor wrapper, fixtures). PR #A
- **Day 2**: Group A + B (node, sp) — 19 tests. PR #B
- **Day 3**: Group C + D + E (rg, rd, vd) — 23 tests. PR #C
- **Day 4**: Group F + G (r, snap) — 24 tests. PR #D
- **Day 5**: Group H + I + J (controller, connections, **CSI**) — 24 tests. PR #E

### Week 2

- **Day 6**: Group K + L (workflows + concurrency) — 18 tests. PR #F
- **Day 7**: Tier 3 (drbdmeta-in-Docker) — 5 tests. PR #G
- **Day 8**: CI wiring (.github/workflows/integration.yaml + Tier 3 job). PR #H
- **Day 9**: Audit existing `tests/e2e/*.sh`, drop duplicates, keep kernel-only. PR #I
- **Day 10**: docs at `tests/README.md` with the tier strategy + DoD update in CONTRIBUTING.md

### In parallel

- DoD change: a new PR requires a Tier 2 test if it changes REST/CRD/reconciler. If it changes drbd-utils integration → Tier 3 required. If it changes kernel-state machine → Tier 4 required.
- Wave2 scenario marker convention: `✓ (Tier 2)` / `✓ (Tier 4)` / `— (out-of-scope)`

---

## What stops landing on the operator's terminal

From the 6 hand-found bugs of 2026-05-14: **78, 79, 80, 83** — Tier 2 (Groups F K J K), **81** — Tier 3, **82** — Tier 4. The whole class of "404/405 with empty body", "handler returned 200 but the client crashes", "reconcile race on cache trail", "late VD does not trigger provisioning", "sp Faulty not surfaced" — **gone**.
