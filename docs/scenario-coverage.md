# Scenario coverage audit

Audit of `tests/scenarios/*.md` (7 files, 172 scenarios) against the
existing test surface (unit, integration, e2e). Cross-referenced
through (a) explicit `Scenario N.M` markers in the test source, (b)
test names matching the scenario topic, and (c) feature-presence
greps for `T` (to-implement) scenarios.

**Last refreshed:** post-Wave-4 (`master @ e51cad3`, 2026-05-13). The
previous audit (commit `6735278`) was authored against an older base;
between then and now Wave-4 closed gaps across Groups 4, 5, 6, and 7
(see "Wave-4 deltas" in the summary).

## Status legend

- `covered-unit` — a Go unit test in `pkg/.../*_test.go` or
  `internal/controller/*_test.go` exercises this scenario's logic.
- `covered-integration` — covered by a Go integration test
  (`pkg/storage/zfs/zfs_integration_test.go`, envtest in
  `pkg/store/k8s/k8s_test.go` / `internal/controller/suite_test.go`,
  or `tests/contract/*_test.go`).
- `covered-e2e` — covered by a script in `tests/e2e/*.sh`
  (live Talos+QEMU stand).
- `spec-skip` — a test exists but contains `t.Skip(...)` until the
  feature lands (e.g. `pkg/satellite/controllers/storagepool_replacement_test.go`
  for 6.19, `pkg/satellite/ship_dispatch_test.go` cross-provider
  pin).
- `gap` — no test found at any level; feature exists in code but
  pinning is missing.
- `out-of-scope` — explicitly marked `O` in the scenario doc; not
  implementing.

Priority taken from each scenario's `Priority:` line in the doc.

## Group 1 — API & CLI contract (28 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 1.1 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/nodes_test.go (TestNodes*), tests/e2e/linstor-cli.sh | P0 hybrid |
| 1.2 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/storage_pools_test.go (TestViewStoragePools*), tests/e2e/linstor-cli.sh | P0 |
| 1.3 | 01-api-contract.md | covered-unit | pkg/rest/resource_definitions_test.go (TestResourceDefinitionsListEmpty, TestResourceDefinitionsCreateRoundTrip) | P0 unit |
| 1.4 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/volume_definitions_test.go, tests/e2e/linstor-cli.sh | P0 |
| 1.5 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/resources_test.go (TestResourcesView*), pkg/rest/autoplace_test.go (TestResourceListAndGet), tests/e2e/linstor-cli.sh | P0; connections cleanup verified in 1.17 |
| 1.6 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/resources_test.go, tests/e2e/linstor-cli.sh | P0 |
| 1.7 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/resource_groups_test.go (TestResourceGroups*), tests/e2e/linstor-cli.sh | P0 |
| 1.8 | 01-api-contract.md | covered-unit | pkg/rest/resource_groups_test.go | P1 unit; volume-group surface |
| 1.9 | 01-api-contract.md | covered-unit | pkg/rest/kv_store_test.go:TestKVGetReturnsSingleElementArray | P0 |
| 1.10 | 01-api-contract.md | covered-unit | pkg/rest/kv_store_test.go:TestKVPutDeletePersistInProcessLocalBag | P0 |
| 1.11 | 01-api-contract.md | covered-unit | pkg/rest/rd_clone_test.go (TestRDClone*) | P0 |
| 1.12 | 01-api-contract.md | covered-unit | pkg/rest/snapshots_test.go (TestSnapshotsCreateRoundTrip) | P0 |
| 1.13 | 01-api-contract.md | covered-unit | pkg/rest/snapshots_test.go (TestSnapshotsDeleteMissing, TestSnapshotsDeleteThenGet) | P0 |
| 1.14 | 01-api-contract.md | covered-unit | pkg/rest/snapshots_test.go (TestSnapshotsViewFilters), pkg/rest/resources_test.go | P1 |
| 1.15 | 01-api-contract.md | covered-unit | pkg/rest/remotes_test.go (TestRemotesTypedEndpointsBareArray) | P1 |
| 1.16 | 01-api-contract.md | covered-unit | pkg/rest/nodes_test.go:TestNodesUpdate | P0 |
| 1.17 | 01-api-contract.md | covered-unit + covered-e2e | pkg/satellite/controllers/observer_internal_test.go (TestTranslateEventConnection, TestMergeConnections*), tests/e2e/lc-connection-cleanup.sh | P0 |
| 1.18 | 01-api-contract.md | covered-unit | pkg/store/k8s/crdname_test.go (TestK8sName_*, TestSetOriginalName_CaseOnlyDifference) | P0 |
| 1.19 | 01-api-contract.md | covered-unit | pkg/store/k8s/crdname_test.go (TestK8sName_SlugifiesInvalidNames, TestSetAndOriginalName_RoundTrip) | P1 |
| 1.20 | 01-api-contract.md | covered-unit | pkg/rest/api_call_rc_envelope_test.go:TestAPICallRcEnvelopeShape | P0 |
| 1.21 | 01-api-contract.md | covered-unit | pkg/rest/spawn_test.go (TestCopyVolumeGroupProps + override-props paths) | P0 |
| 1.22 | 01-api-contract.md | covered-unit | pkg/api/v1/lax_int_test.go (lenient decoder) | P0 |
| 1.23 | 01-api-contract.md | covered-e2e | tests/e2e/cheat-sheet-csi-level1.sh | P1 e2e |
| 1.24 | 01-api-contract.md | covered-e2e | tests/e2e/cheat-sheet-cli-level2.sh | P1 e2e |
| 1.25 | 01-api-contract.md | covered-e2e | tests/e2e/satellite-utils-smoke.sh | P1 e2e |
| 1.26 | 01-api-contract.md | covered-e2e | tests/e2e/cheat-sheet-naming-deltas.sh | P2 |
| 1.27 | 01-api-contract.md | covered-unit | pkg/rest/resources_test.go + snapshots_test.go (offset/limit boundaries) | P1 |
| 1.28 | 01-api-contract.md | covered-unit + covered-e2e | pkg/rest/spawn_test.go (TestSpawnCreatesRDAndVDs, TestSpawnRollsBackOnVDFailure), tests/e2e/linstor-cli.sh | P0 |

## Group 2 — Placement (20 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 2.1 | 02-placement.md | covered-unit | internal/controller/first_available_pool_test.go, pkg/placer/placer_test.go (TestPlaceCreatesNUpToPlaceCount) | P0 |
| 2.2 | 02-placement.md | covered-unit | pkg/placer/placer_test.go (TestPlaceCandidatePools*, FreeCapacity ordering) | P0 |
| 2.3 | 02-placement.md | covered-unit + covered-e2e | pkg/rest/spawn_test.go, cross-listed with 1.28 | P0 |
| 2.4 | 02-placement.md | covered-e2e | tests/e2e/linstor-cli.sh, linstor-cli-replica-move.sh | P1 |
| 2.5 | 02-placement.md | covered-unit | pkg/placer/placer_test.go:TestPlaceReplicasOnSamePicksLargestGroup | P0; doc says "missing test" but unit exists |
| 2.6 | 02-placement.md | covered-unit | pkg/placer/placer_test.go:TestPlaceReplicasOnDifferentFallsBackToExcludedNode, pkg/rest/autoplace_test.go:TestAutoplaceReplicasOnDifferent* | P0; doc says "missing test" — e2e still gap |
| 2.7 | 02-placement.md | covered-unit | pkg/placer/placer_test.go:TestPlaceReplicasOnDifferentExcludeMode | P1 |
| 2.8 | 02-placement.md | gap | — | P1 T — `xReplicasOnDifferent` not implemented (grep clean) |
| 2.9 | 02-placement.md | gap | — | P0 P — `AutoplaceTarget` exclusion path no specific test (verify code surface) |
| 2.10 | 02-placement.md | gap | — | P1 T — `doNotPlaceWith` not implemented |
| 2.11 | 02-placement.md | gap | — | P2 T — `doNotPlaceWithRegex` not implemented |
| 2.12 | 02-placement.md | covered-unit | pkg/rest/layer_validation_test.go, pkg/placer/placer_test.go (provider filter) | P1 |
| 2.13 | 02-placement.md | covered-unit | internal/controller/node_label_sync_test.go:TestNodeLabelSyncToAuxProps, pkg/store/k8s/topology_labels_test.go | P0 partial — envtest/e2e missing |
| 2.14 | 02-placement.md | covered-unit | pkg/store/k8s/topology_labels_test.go (Aux/ prop sync), internal/controller/node_label_sync_test.go | P1 |
| 2.15 | 02-placement.md | gap | — | P1 T — `BalanceResources*` not implemented (grep clean) |
| 2.16 | 02-placement.md | covered-unit | pkg/placer/placer_test.go (greatest-free-first paths) | P0 |
| 2.17 | 02-placement.md | gap | — | P2 T — `Autoplacer/Weights/*` not implemented |
| 2.18 | 02-placement.md | covered-unit + covered-e2e | internal/controller/ensure_tiebreaker_test.go, pkg/rest/autoplace_test.go:TestAutoplaceDisklessOnRemaining, tests/e2e/tiebreaker.sh | P0 |
| 2.19 | 02-placement.md | covered-unit | pkg/rest/autoplace_test.go:TestAutoplaceConflictWhenInsufficient, pkg/rest/autoplace_test.go:TestAutoplaceReplicasOnDifferentExhausted | P1 |
| 2.20 | 02-placement.md | gap | — | P1 T — partial-fail re-place needs 2.15 to land first |

## Group 3 — Networking (12 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 3.1 | 03-networking.md | covered-unit | pkg/rest/net_interface_test.go (TestNetInterface*) | P0 |
| 3.2 | 03-networking.md | covered-unit | pkg/rest/net_interface_test.go:TestNetInterfaceCreateMissingName + name validation paths | P1 |
| 3.3 | 03-networking.md | covered-integration | pkg/satellite/controllers/heartbeat_test.go, pkg/satellite/controllers/heartbeat_internal_test.go (Hello upsert) | P0 |
| 3.4 | 03-networking.md | gap | — | P0 S — `PrefNic` exists in pkg/dispatcher/dispatcher.go but no dedicated test; e2e missing (needs multi-NIC stand) |
| 3.5 | 03-networking.md | gap | — | P1 P — Diskless caveat unverified |
| 3.6 | 03-networking.md | gap | — | P1 — StorageClass `PrefNic` propagation has no e2e |
| 3.7 | 03-networking.md | gap | — | P1 T — `ResourceConnectionPath` not implemented (grep clean) |
| 3.8 | 03-networking.md | gap | — | P2 T — depends on 3.7 |
| 3.9 | 03-networking.md | covered-unit | pkg/rest/net_interface_test.go (StltConn key surface), tests/contract/normalize.go | P1 partial |
| 3.10 | 03-networking.md | gap | — | P2 T — satellite re-dial on interface modify unimplemented |
| 3.11 | 03-networking.md | gap | — | P1 — multi-NIC stand harness not in tests/e2e |
| 3.12 | 03-networking.md | gap | — | P0 — "missing test" per doc; replication-traffic iftop e2e missing |

## Group 4 — Lifecycle (26 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 4.1 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/resource_definitions_test.go:TestResourceDefinitionsDeleteCascadesChildren, tests/e2e/lc-rd-delete-cascade.sh | P0 |
| 4.2 | 04-lifecycle.md | covered-unit | pkg/rest/resource_definitions_test.go:TestResourceDefinitionsCreateConflict | P1 |
| 4.3 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/autoplace_test.go:TestResourceCreateAndDelete + TestResourceDeleteTieBreakerStampsSuppression, tests/e2e/lc-connection-cleanup.sh | P0 |
| 4.4 | 04-lifecycle.md | covered-unit | pkg/rest/resource_groups_test.go:TestResourceGroupsUpdate | P1 |
| 4.5 | 04-lifecycle.md | covered-unit | pkg/rest/resource_groups_test.go:TestResourceGroupsDelete + DeleteMissingRG | P1 |
| 4.6 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/volume_definitions_test.go:TestVolumeDefinitionsUpdate, tests/e2e/resize-pvc.sh, resize-luks.sh, resize-plain.sh, resize-no-drbd.sh | P0 |
| 4.7 | 04-lifecycle.md | covered-e2e | tests/e2e/two-volume-rd.sh | P1 |
| 4.8 | 04-lifecycle.md | gap | — | P1 — per-volume StorPoolName routing has no dedicated test |
| 4.9 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/resource_toggle_disk_test.go (TestToggleDisk*), tests/e2e/toggle-disk.sh | P0 |
| 4.10 | 04-lifecycle.md | covered-e2e | tests/e2e/lifecycle-toggle-migrate.sh | P1 |
| 4.11 | 04-lifecycle.md | covered-e2e | tests/e2e/lifecycle-toggle-retry.sh | P2 — SPEC-style e2e against UG9 contract |
| 4.12 | 04-lifecycle.md | covered-unit | pkg/rest/snapshots_test.go (CRUD + idempotent + missing) | P0 |
| 4.13 | 04-lifecycle.md | gap | — | P0 — snapshot rollback returns 501 (TestSnapshotRollback501WithActionableText pins gap) |
| 4.14 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/snapshot_restore_test.go (TestSnapshotRestore*), tests/e2e/snapshot-restore-cross-node.sh | P0 |
| 4.15 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/rd_clone_test.go, tests/e2e/clone.sh | P0 |
| 4.16 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/satellite/ship_dispatch_test.go (TestCrossNodeClone*, TestZFSSendSnapshotPreflight), tests/e2e/snap-ship-cross-node.sh | P1 — landed |
| 4.17 | 04-lifecycle.md | spec-skip | pkg/rest/remotes_test.go:TestLinstorRemoteShipReturns501WithText | P2 T — LINSTOR-remote ship not implemented |
| 4.18 | 04-lifecycle.md | out-of-scope | pkg/rest/remotes_test.go (501 stub) | S3 / scheduled backup explicitly O |
| 4.19 | 04-lifecycle.md | covered-integration | pkg/satellite/controllers/heartbeat_test.go, pkg/satellite/controllers/heartbeat_internal_test.go, pkg/store/k8s/k8s_test.go:TestK8sNodeStore | P0 |
| 4.20 | 04-lifecycle.md | covered-e2e | tests/e2e/node-evacuate.sh, pkg/rest/node_lifecycle_test.go (TestNodeEvacuate*) | P0 — unit + e2e both present; doc note outdated |
| 4.21 | 04-lifecycle.md | covered-e2e | tests/e2e/node-multi-evacuate.sh | P1 — landed |
| 4.22 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/node_lifecycle_test.go:TestNodeRestoreClearsFlag, tests/e2e/node-restore.sh | P1 |
| 4.23 | 04-lifecycle.md | covered-unit + covered-e2e | pkg/rest/node_lifecycle_test.go:TestNodeLost*, tests/e2e/node-lost.sh | P0 |
| 4.24 | 04-lifecycle.md | gap | — | P1 T — `AutoEvict*` enforcement not wired |
| 4.25 | 04-lifecycle.md | covered-unit | internal/controller/auto_diskful_test.go (TestAutoDiskless*), tests/e2e/auto-diskful.sh | P1 |
| 4.26 | 04-lifecycle.md | covered-unit | internal/controller/auto_diskful_test.go (cleanup branch) | P1 |

## Group 5 — DRBD state & recovery (38 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 5.1 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/observer_internal_test.go:TestTranslateResourceEventHasResource + TestMergeResourceCachesInUseAcrossNonResourceEvents | P0 |
| 5.2 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/observer_internal_test.go:TestMergeResourceCachesDrbdStateAcrossNonResourceEvents, pkg/drbd/events2_test.go | P0 |
| 5.3 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/observer_internal_test.go:TestTranslateEventConnection, TestBuildObserverConnectionStatus | P0 |
| 5.4 | 05-drbd-state-recovery.md | covered-unit + covered-e2e | pkg/satellite/controllers/observer_internal_test.go:TestMergeConnectionsSnapshotsAllPeers, tests/e2e/lc-connection-cleanup.sh | P0 |
| 5.5 | 05-drbd-state-recovery.md | covered-unit | internal/controller/node_heartbeat_controller_test.go:TestNodeHeartbeat_StaleFlipsToUnknown + node_reconcile_branches_test.go | P0 — unit landed; e2e gap |
| 5.6 | 05-drbd-state-recovery.md | gap | — | P0 — SyncTarget no-interfere observation pinned in observer test, but e2e "do not adjust mid-sync" assertion missing |
| 5.7 | 05-drbd-state-recovery.md | covered-unit | internal/controller/disk_class_test.go:TestSplitByDiskless, TestFilterTieBreaker | P0 |
| 5.8 | 05-drbd-state-recovery.md | gap | — | P0 — drain/uncordon e2e (UpToDate↔Outdated) missing |
| 5.9 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/state-inconsistent-mid-sync.sh | P0 |
| 5.10 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/state-standalone-partition.sh, network-partition.sh | P0 |
| 5.11 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/observer_internal_test.go:TestObserverWritesSkipDiskOnFailed, pkg/satellite/reconciler_drbd_test.go:TestReconcilerPassesSkipDiskFlag | P1 — observer auto-sets `DrbdOptions/SkipDisk=True` on `disk:Failed`; reconciler gates `drbdadm adjust --skip-disk`. CLI render `Skip-Disk (R)` not surfaced — needs effective-props REST + python-linstor-client (out of repo). |
| 5.12 | 05-drbd-state-recovery.md | gap | — | P0 — Unknown-branch full e2e walkthrough missing; partial via 5.5 |
| 5.13 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/recovery-deleting-convert.sh | P0 |
| 5.14 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/recovery-discard-my-data.sh | P0 |
| 5.15 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/state-auto-resync.sh | P0 |
| 5.16 | 05-drbd-state-recovery.md | gap | — | P0 — cross-listed with 5.6; reconciler-quiet-during-sync e2e missing |
| 5.17 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/recovery-false-diskless.sh, pkg/rest/autoplace_test.go (TestMakeAvailable*) | P1 — make-available landed |
| 5.18 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/recovery-inconsistent-blocking.sh | P0 |
| 5.19 | 05-drbd-state-recovery.md | covered-unit | pkg/rest/resource_adjust_test.go:TestResourceDeactivate, TestResourceActivateUnknown | P1 — unit landed; e2e gap |
| 5.20 | 05-drbd-state-recovery.md | covered-unit | internal/controller/set_quorum_test.go (persistence + retry), internal/controller/quorum_policy_test.go | P0 — unit landed; e2e gap |
| 5.21 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/recovery-stuck-synctarget.sh | P1 |
| 5.22 | 05-drbd-state-recovery.md | covered-e2e | tests/e2e/two-primaries-live-migration.sh | P0 |
| 5.23 | 05-drbd-state-recovery.md | gap | — | P2 P — bitmap drop (xfail on 9.2.17+) — no test |
| 5.24 | 05-drbd-state-recovery.md | covered-unit | internal/controller/drbd_ids_test.go:TestDRBDNodeIDStableAcrossPeerChurn | P1 — invariant pinned; e2e churn loop missing |
| 5.25 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/observer_internal_test.go:TestObserverReportsPausedSyncS, pkg/satellite/reconciler_drbd_test.go:TestApplyDefersAdjustDuringPausedSyncS | P2 — observer + reconciler-defer landed |
| 5.26 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/reconciler_drbd_test.go (no down on Primary path), pkg/satellite/controllers/sweeper_test.go | P0 |
| 5.27 | 05-drbd-state-recovery.md | covered-unit | tests/safety/safety_rails_test.go:TestNoForceStripFinalizers | P0 |
| 5.28 | 05-drbd-state-recovery.md | covered-unit | tests/safety/safety_rails_test.go:TestNoControllerSideNodeLost | P0 |
| 5.29 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/reconciler_drbd_test.go:TestReconcilerRespectsOperatorDisconnect | P1 — unit landed; live-stand 30s e2e still gap |
| 5.30 | 05-drbd-state-recovery.md | gap | — | P1 — `drbdadm primary --force` not auto-undone — no test |
| 5.31 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/reconciler_drbd_test.go:TestReconcilerDoesNotPropagateDiscardMyData | P1 — landed |
| 5.32 | 05-drbd-state-recovery.md | gap | — | P1 — reconciler reverses drbdadm down (e2e gap; sweeper covers orphan path 5.34) |
| 5.33 | 05-drbd-state-recovery.md | gap | — | P2 — stuck SyncTarget down+up cycle not tested |
| 5.34 | 05-drbd-state-recovery.md | covered-unit | pkg/satellite/controllers/sweeper_test.go (TestSweeperDownsOrphan, TestSweeperRespectsRateLimit, TestSweeperSkipAnnotationDisablesSweep) | P1 |
| 5.35 | 05-drbd-state-recovery.md | gap | — | P1 — mass-incident SOP nightly harness still missing (cross-listed 7.24) |
| 5.36 | 05-drbd-state-recovery.md | covered-unit | pkg/rest/resources_test.go:TestFaultyFilterPrioritizesZeroUpToDate | P1 — landed |
| 5.37 | 05-drbd-state-recovery.md | covered-unit | pkg/rest/resources_test.go:TestResourceShapeIncludesReversibilityHint + TestFaultyFilter* | P1 partial — `recovery_metadata` hint engine still spec-skip |
| 5.38 | 05-drbd-state-recovery.md | covered-unit | pkg/rest/error_reports_test.go (TestErrorReportsListEmpty, TestErrorReportGetMissing) | P1 — surface present; filter assertions partial |

## Group 6 — Storage backends (24 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 6.1 | 06-storage-backends.md | covered-unit | pkg/storage/lvm/lvm_thin_test.go (TestThin* full CRUD) | P0 |
| 6.2 | 06-storage-backends.md | covered-unit + covered-integration | pkg/storage/zfs/zfs_test.go + zfs_integration_test.go (TestZFSAgainstRealPool) | P0 |
| 6.3 | 06-storage-backends.md | covered-unit | pkg/storage/file/file_test.go (TestCreateVolume*, TestSnapshotsCpReflink) | P1 |
| 6.4 | 06-storage-backends.md | covered-unit | pkg/storage/lvm/lvm_thick_test.go (TestThickCreateSnapshot rejection paths) | P1 |
| 6.5 | 06-storage-backends.md | covered-integration | pkg/satellite/controllers/heartbeat_test.go + storagepool_test.go + pkg/store/k8s/k8s_test.go:TestK8sStoragePoolStore | P0 |
| 6.6 | 06-storage-backends.md | covered-unit | pkg/rest/physical_storage_test.go (TestPhysicalStorageList*) | P2 |
| 6.7 | 06-storage-backends.md | covered-unit | pkg/storage/contract_test.go (TestNamingContract_*), per-provider Capabilities() | P1 |
| 6.8 | 06-storage-backends.md | covered-unit | pkg/placer/placer_test.go (mixed-provider RG scenario 6.8 cases) | P1 — landed |
| 6.9 | 06-storage-backends.md | covered-unit | pkg/rest/layer_validation_test.go (TestValidateLayerStack_*, TestRGCreateRejectsBadLayerStack), pkg/api/v1/layer_stack_test.go, internal/controller/layer_stack_test.go | P0 |
| 6.10 | 06-storage-backends.md | covered-unit + covered-e2e | internal/controller/layer_stack_test.go, tests/e2e/no-drbd.sh | P1 |
| 6.11 | 06-storage-backends.md | out-of-scope | pkg/rest/layer_validation_test.go:TestValidateLayerStack_RejectsUnsupportedLayers | CACHE/WRITECACHE/NVME explicitly O |
| 6.13 | 06-storage-backends.md | covered-unit + covered-e2e | pkg/luks/luks_test.go (TestFormat/Open/Close/Resize), tests/e2e/luks-layer.sh + drbd-luks-stack.sh | P0 |
| 6.14 | 06-storage-backends.md | covered-unit | pkg/rest/drbd_passphrase_test.go (TestDRBDPassphrase*), pkg/drbd/conffile_test.go:TestBuildIncludesNetSecret | P0 |
| 6.15 | 06-storage-backends.md | covered-unit | pkg/rest/encryption_test.go (TestPassphrase* incl Create/Modify/Enter + Secret*) | P0 |
| 6.16 | 06-storage-backends.md | covered-integration | tests/contract/replay_test.go (TestReplayMatchingTrace, TestReplayStatusDiverges), tests/contract/oracle_test.go | P1 |
| 6.17 | 06-storage-backends.md | out-of-scope | — | piraeus owns auto-passphrase orchestration |
| 6.18 | 06-storage-backends.md | gap | — | P2 T — `StorPoolNameDrbdMeta` external metadata not implemented |
| 6.19 | 06-storage-backends.md | spec-skip | pkg/satellite/controllers/storagepool_replacement_test.go:TestStoragePoolDriveReplacement6_19 (t.Skip pending Faulted condition) | P1 — SPEC test scaffolded; production assertions awaiting Faulted condition |
| 6.20 | 06-storage-backends.md | gap | — | P2 T — dmsetup error-target injection design TBD |
| 6.21 | 06-storage-backends.md | covered-unit + covered-integration | pkg/storage/zfs/zfs_test.go:TestCreateSnapshotIssuesZfsSnap, zfs_integration_test.go | P0 |
| 6.22 | 06-storage-backends.md | covered-unit | pkg/storage/lvm/lvm_thin_test.go:TestThinCreateSnapshot | P0 |
| 6.23 | 06-storage-backends.md | covered-unit | pkg/storage/file/file_test.go:TestSnapshotsCpReflink | P1 |
| 6.24 | 06-storage-backends.md | covered-unit + covered-e2e | pkg/satellite/ship_dispatch_test.go (TestCrossNodeCloneDispatchesTo*), tests/e2e/snap-ship-cross-node.sh | P1 |

## Group 7 — Quorum & observability (24 scenarios)

| Scenario | Doc location | Status | Test file | Notes |
|---|---|---|---|---|
| 7.1 | 07-quorum-observability.md | covered-unit | internal/controller/quorum_policy_test.go:TestQuorumPolicy, set_quorum_test.go | P0 |
| 7.2 | 07-quorum-observability.md | covered-unit | internal/controller/quorum_policy_test.go (suspend-io branch), pkg/drbd/conffile_test.go (resource options) | P0 — unit landed; e2e VM-style hang test gap |
| 7.3 | 07-quorum-observability.md | covered-unit | internal/controller/set_quorum_test.go (TestSetQuorumWritesValue, ReplacesExistingValue) | P1 |
| 7.4 | 07-quorum-observability.md | covered-unit | pkg/drbd/options_test.go, pkg/drbd/conffile_test.go:TestBuildEmitsResourceOptions | P1 |
| 7.5 | 07-quorum-observability.md | covered-unit | internal/controller/set_quorum_test.go (persistence via store), pkg/satellite/reconciler_drbd_test.go | P0 — unit landed; e2e bounce-satellite persistence missing |
| 7.6 | 07-quorum-observability.md | covered-unit + covered-e2e | internal/controller/ensure_tiebreaker_test.go, tiebreaker_test.go, pick_tiebreaker_test.go, tests/e2e/tiebreaker.sh | P0 |
| 7.7 | 07-quorum-observability.md | covered-unit | internal/controller/auto_tiebreaker_test.go (TestIsAutoTieBreakerEnabled*), ensure_tiebreaker_test.go:TestEnsureTiebreakerHonoursSuppressionAnnotation | P0 |
| 7.8 | 07-quorum-observability.md | covered-unit | internal/controller/remove_witnesses_test.go, apply_witness_decision_test.go | P1 |
| 7.9 | 07-quorum-observability.md | covered-e2e | tests/e2e/observability-three-way.sh | P0 |
| 7.10 | 07-quorum-observability.md | gap | — | P0 — K8s-only narrowing e2e (pool-not-found PVC error chain) missing |
| 7.11 | 07-quorum-observability.md | covered-e2e | tests/e2e/observability-linstor-node-bridge.sh | P0 |
| 7.12 | 07-quorum-observability.md | gap | — | P1 — CSI ↔ DRBD permission-denied chain (diskless attach) e2e missing |
| 7.13 | 07-quorum-observability.md | covered-e2e | tests/e2e/observability-destructive-walk.sh | P1 — landed |
| 7.14 | 07-quorum-observability.md | gap | — | P0 — drbdadm down recovered by reconciler — e2e gap (cross-listed with 5.32) |
| 7.15 | 07-quorum-observability.md | covered-e2e | tests/e2e/observability-capacity-correlation.sh | P2 — landed |
| 7.16 | 07-quorum-observability.md | covered-unit | pkg/rest/resources_test.go:TestFaultyFilterPrioritizesZeroUpToDate + TestResourceShapeIncludesReversibilityHint | P1 partial; cross-listed with 5.37 |
| 7.17 | 07-quorum-observability.md | covered-unit | pkg/rest/error_reports_test.go | P1 partial — filter assertions need expansion |
| 7.18 | 07-quorum-observability.md | gap | — | P2 P — copilot approval-metadata cross-project with ccp |
| 7.19 | 07-quorum-observability.md | covered-unit | pkg/rest/query_size_info_test.go:TestQuerySizeInfoFreeCapacityRatioGate + TestSpawnRejectsExceedingFreeCapacityRatio | P1 — landed (recent commit) |
| 7.20 | 07-quorum-observability.md | covered-unit | pkg/rest/query_size_info_test.go:TestQuerySizeInfoTotalCapacityRatioGate + TestSpawnRejectsExceedingTotalCapacityRatio | P1 — landed |
| 7.21 | 07-quorum-observability.md | covered-unit | pkg/rest/query_size_info_test.go:TestQuerySizeInfoOverallRatioFallback + TestSpawnRejectsExceedingOversubscriptionRatio | P1 — landed |
| 7.22 | 07-quorum-observability.md | out-of-scope | pkg/rest/properties_info_test.go (props readable) | P3 — sysfs blkio_throttle stays O |
| 7.23 | 07-quorum-observability.md | out-of-scope | — | depends on 7.22 + 6.11 |
| 7.24 | 07-quorum-observability.md | gap | — | P1 — mass-incident SOP nightly harness missing (cross-listed with 5.35) |
| 7.25 | 07-quorum-observability.md | covered-unit | pkg/rest/resources_test.go:TestFaultyFilterPrioritizesZeroUpToDate | P1 — cross-listed with 5.36 |

---

## Summary

**Total scenarios audited: 172** across 7 groups. Post-Wave-4
refresh: many Group 5 / 7 e2e scripts and observer/reconciler unit
pins landed since commit `6735278`; the new numbers below reflect
the current `master` test surface walked from `tests/scenarios/*.md`.

### Status counts

| Status | Count |
|--------|------:|
| covered-unit (incl. hybrid unit+e2e/integration) | 103 |
| covered-e2e (e2e-only or e2e-leading) | 23 |
| covered-integration (integration-only) | 4 |
| spec-skip (t.Skip pending feature) | 2 (4.17, 6.19) |
| out-of-scope (`O`) | 5 (4.18, 6.11, 6.17, 7.22, 7.23) |
| gap (no test surface) | 35 |

### Gap count by priority

- **P0 still gapped: 10** items
  - 2.9 AutoplaceTarget exclusion
  - 3.4 PrefNic on node (unit/e2e gap)
  - 3.12 Replication-only traffic on `repl` NIC
  - 4.13 Snapshot rollback (501 today)
  - 5.6 SyncTarget reconciler no-interfere (e2e gap)
  - 5.8 UpToDate ↔ Outdated under drain/uncordon
  - 5.12 Unknown-branch e2e walkthrough
  - 5.16 Branch SyncTarget — do not interfere (e2e)
  - 7.10 K8s-side error narrows without descending
  - 7.14 `drbdadm down` recovered by reconciler

  Note: 5.24 and 7.5 each have unit-level pins landed (drbd-id stability
  invariant and quorum-policy persistence respectively) — the remaining
  e2e portions are tracked as in-flight rather than P0 gaps.

- **P1 still gapped: 15 distinct work items** (16 table rows; 5.35
  and 7.24 are the same mass-incident SOP harness counted twice
  across cross-listed groups)
  - 2.8 x-replicas-on-different (T — needs implementation)
  - 2.10 do-not-place-with (T — needs implementation)
  - 2.15 BalanceResources* (T — needs implementation)
  - 2.20 Spawn partial-fail re-place (T, depends on 2.15)
  - 3.5 PrefNic on storage-pool Diskless caveat
  - 3.6 StorageClass PrefNic propagation
  - 3.7 ResourceConnectionPath multi-path (T)
  - 3.11 Multi-NIC stand harness
  - 4.8 Per-volume storage-pool routing
  - 4.24 Auto-evict (T — needs implementation)
  - 5.11 `SkipDisk` (T — needs implementation)
  - 5.30 `drbdadm primary --force` not auto-undone
  - 5.32 `drbdadm down` reverses on reconcile (e2e; cross-listed 7.14)
  - 5.35 ↔ 7.24 Mass-incident SOP nightly harness (single work item)
  - 7.12 CSI ↔ DRBD permission-denied chain

- **P2/P3 gapped: 9** items (mostly T-status / out-of-Wave-4 scope:
  2.11, 2.17, 3.8, 3.10, 5.23, 5.33, 6.18, 6.20, 7.18)

### Wave-4 deltas (closed since `6735278`)

- **Group 4:** 4.11 SPEC e2e for toggle-disk retry/cancel; 4.16, 4.21
  already covered but reclassified from "missing" → "landed".
- **Group 5:** 5.25 PausedSyncS observer + reconciler-defer unit
  tests; 5.29 operator-disconnect unit pin; 5.31 already covered
  (priority adjusted from P0→P1 in source doc); 5.36 faulty-filter
  unit pin.
- **Group 6:** 6.8 mixed-provider RG covered by `placer_test.go`
  cases.
- **Group 7:** 7.13 destructive-walk e2e; 7.15 capacity-correlation
  e2e; 7.19/7.20/7.21 oversubscription-ratio gates unit tests.

### Biggest cluster of gaps

**Group 5 (DRBD state & recovery) — 9 gaps**, still the dominant
bucket but down from 13 pre-Wave-4. Remaining items split into:

- **P0 e2e branches** (5.6, 5.8, 5.12, 5.16) — same harness pattern
  as `state-*.sh` / `recovery-*.sh` scripts that already exist; the
  unit-level translations are pinned.
- **P1 reconciler-survival** (5.30, 5.32, 5.35) — overlap with
  Group 7 (5.32 ↔ 7.14, 5.35 ↔ 7.24).

Group 3 (networking) is second with 7 gaps; nearly all are blocked
by the missing multi-NIC stand harness (3.11). Group 2 (placement)
has 7 gaps that map 1:1 to **unimplemented features**
(`xReplicasOnDifferent`, `doNotPlaceWith`, `BalanceResources`,
weighted scoring) — feature gaps, not test gaps.

**Recommended next-wave landing order:**

1. **Group 5 P0 e2e** (5.6, 5.8, 5.12, 5.16) — reuse the
   `state-inconsistent-mid-sync.sh` / `recovery-*.sh` skeletons.
2. **Group 7 P0 cross-cutting** (7.10 K8s-only narrowing, 7.14
   drbdadm-down recovery) — observability stand already exists
   (`observability-three-way.sh`, `observability-destructive-walk.sh`).
3. **Group 3 multi-NIC stand** (3.11) — unblocks 3.4, 3.5, 3.6, 3.12
   in one shot.
4. **Group 4 4.13 snapshot rollback** — REST currently returns 501
   pinned by `TestSnapshotRollback501WithActionableText`; gap is the
   implementation, not the test.
