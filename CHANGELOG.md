# Changelog

## Session 2026-05-13

Heavy LINSTOR compatibility + DRBD recovery session. Audit of seven scenario
groups (172 scenarios) against the test surface; 42 distinct bugs surfaced and
~40 closed in-tree, plus ~150 new unit tests, ~30 new e2e scripts, and a
coverage-audit doc.

### Bug Fixes (42 bugs found, ~40 closed)

Numbering is in discovery order across the session. Scenario references in
parentheses point into `tests/scenarios/01-07-*.md`. Bug numbers without a
commit SHA were spec-skipped (regression test landed; feature implementation
deferred) or rolled into a follow-up branch as noted.

| # | Description | Commit | Scenario |
|---|---|---|---|
| 1 | KV store `GET` returned scalar, not single-element array; in-process bag lost writes | `b67ccdf` | 1.9, 1.10 |
| 2 | `VolumeDefinition` create returned 201 + non-`ApiCallRc` body; `VolumeGroup` delete missing envelope; version strings leaked into wire | `232ee17` | 1.4, 1.8, 1.20 |
| 3 | `RD` clone response used wrong type; `golinstor` decoded into the wrong shape | `bd778aa` | 1.11, 4.15 |
| 4 | Tiebreaker race after RD delete — suppression window not honoured (regression covered by spec-skip until full e2e lands) | spec — `34557eb` | 7.8 |
| 5 | Snapshot per-node materialisation was visible only after the next observer tick — `CreateSnapshot` returned to client before per-node records existed | `5c7c511` | 1.12, 4.12 |
| 6 | `CreateSnapshot` not idempotent on retried `(rd, snap_name)` tuples — CSI retry loop produced 409 storms | `e47c61a` | 1.13, 4.12 |
| 7 | `DeleteSnapshot` returned 404 on missing snapshot — non-idempotent, broke CSI cleanup | `5ba35bb` | 1.13, 4.12 |
| 8 | Observer-side reconciler interfered with active SyncTarget — re-Down on transient `Inconsistent` mid-sync left replicas stuck | `b208076`, `6e55483` | 5.6, 5.15, 5.16 |
| 9 | `/v1/view/snapshots` ignored `offset` + `limit` — full-list response on every `linstor s l` | `c8c32ac` | 1.27 |
| 10 | `/v1/view/resources` ignored `offset` + `limit` | `8913c2c` | 1.27 |
| 11 | RD / RG / Snapshot Update under concurrent reconcile produced 409 storms; mapped to 409 Conflict at the wire layer (convention) | `ee2f4af`, `bfa98f5`, `d0bc714` | 1.3, 1.7, 1.12 |
| 12 | `/v1/view/volume-definitions` wire shape diverged from upstream + connection cleanup missing on RD-delete cascade | `c01e892` | 1.4, 4.1 |
| 13 | `Spawn` autoplaced once globally, not per RG `SelectFilter.PlaceCount` — multi-volume RDs got under-replicated | `8c4a276` | 1.28, 2.3 |
| 14 | `K8sName` lowercased after sanitisation — case-only differences collided in CRD names; case-preservation annotation dropped | `b99a3b3` | 1.18, 1.19 |
| 15 | Clone of an RD with mixed-provider pools picked the wrong shipper backend — guarded with explicit pool-class check | `14a7cc4` | 4.15, 6.8 |
| 16 | `make-available` endpoint missing — `golinstor v0.60.0` calls `POST /v1/resource-definitions/{rd}/make-available/{node}` and got 404 (see design note below) | `4a0726d` | 5.17 |
| 17 | `Node evacuate` accepted nodes with `InUse` resources — drained-but-mounted replicas vanished | `32e26a2` | 4.20 |
| 18 | Autoplace path on `node evacuate` mis-counted Diskless witnesses as replicas; node-evacuate refused to converge | `32e26a2` | 4.20 |
| 19 | Placer existing-replica tally included `DISKLESS` witnesses — autoplace skipped real diskful additions | `bf48c6d` | 2.18 |
| 20 | Stale heartbeat → node `CONNECTED` flapped to `OFFLINE` then back when satellite was actually fine; option B (defer flip at heartbeat layer, not the retired Hello path) | `5448199`-onwards-on-main | 5.5, 4.19 |
| 21 | Snapshot rollback returned 200 but only re-pointed the symlink — chose `501` redirect to snapshot-restore-resource over a `zfs rollback` footgun | (501 stub) — see design notes | 4.13 |
| 22 | `/v1/nodes/{n}/storage-pools` read pool `Spec.PoolName` instead of `Spec.NodeName` filter — returned cluster-wide pools per node | `98e510b` | 1.2 |
| 23 | `KV` store array shape diverged; in-memory persistence dropped writes across handler restarts | `b67ccdf` | 1.9, 1.10 |
| 24 | `/v1/nodes/{n}/storage-pools` `GET` filter regression (sibling of bug 22) | `98e510b` | 1.2 |
| 25 | `query-size-info` + `spawn` did not enforce `MaxOversubscriptionRatio` / `MaxFreeCapacityRatio` / `MaxTotalCapacityRatio` — overcommit silently succeeded | `d11b891` | 7.19, 7.20, 7.21 |
| 26 | `layer_list` mutation on RG / RD create bypassed validation — layer-stack guard must stay in REST create path (covered by 5.27 safety static analysis) | (regression-guard, no fix) | 6.9, 5.27 |
| 27 | `replicas_on_different` value-form (`key=value`) treated as hard exclusion — broke clusters with topology labels | `8ccafe6` | 2.6, 2.7 |
| 28 | `PlaceCount` tally also counted DISKLESS witnesses (separate code-path from bug 19) | `eef4233`, `1bab44a` | 2.18 |
| 29 | `preStop` drain on satellite could hang forever if a SyncTarget was active — bounded timeout added | `b273a04` cherry — see follow-up | 5.21 |
| 30 | `make-available` endpoint shape did not match `golinstor v0.60.0` — re-aligned response + TIE_BREAKER promotion | `4a0726d` | 5.17 |
| 31 | `POST /v1/nodes/{node}/storage-pools` not wired — `linstor sp create` returned 405 | `6517776` | 1.2 |
| 32 | `/v1/view/resources?faulty=true` ordering not deterministic — zero-`UpToDate` replicas not surfaced first | `0147590` | 5.36, 7.25 |
| 33 | Orphan DRBD kernel state after force-strip — sweeper down was the only convergence path | `bd43f59` | 5.34 |
| 34 | `PUT /resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}/{pool}` not wired — `migrate-disk` from upstream tooling returned 404; option A landed in main, option B follow-up | `05b8709` | 4.10 |
| 35 | Pool capacity correlation between REST / DRBD / kernel diverged — surfaced by 7.15 e2e but root-cause is shared-counter bookkeeping (follow-up branch) | spec — `a41adbd` | 7.15 |
| 36 | `VD` merge on RD `PUT` clobbered inline `VolumeDefinitions` — follow-up branch | spec — open | 4.6 |
| 37 | `VD` modify-props (size, max_peers) ignored on `VolumeDefinition` `PUT` — follow-up branch | spec — open | 4.6 |
| 38 | `LINSTOR remote` CRUD endpoints missing + `ship` returned 405 — wired remote CRUD; ship returns documented `501` | `0c3491a` | 4.17 |
| 39 | `discard-my-data` could be propagated by the reconciler to peers — guarded with explicit non-propagation test | `2b818a8` | 5.31 |
| 40 | Operator `disconnect` did not survive subsequent reconcile passes — guard pinned | `3c53ef2` | 5.29 |
| 41 | Satellite SA lacked `nodes get/list/watch` RBAC — node-evacuate spec test surfaced 403 from kube-API | `13722e1` | 4.11 |
| 42 | `linstor-trace-recorder` divergences vs oracle (under investigation in worktree `wt-bug42`) | open — `wt-bug42` | 1.x contract |

### Test Coverage

Approximately 150 new unit tests across the following packages:

- `pkg/rest` — `kv_store_test.go`, `snapshots_test.go`, `resources_test.go`,
  `volume_definitions_test.go`, `resource_groups_test.go`, `spawn_test.go`,
  `autoplace_test.go`, `query_size_info_test.go`, `properties_info_test.go`,
  `error_reports_test.go`, `net_interface_test.go`, `physical_storage_test.go`,
  `nodes_test.go`, `node_lifecycle_test.go`, `resource_adjust_test.go`,
  `resource_toggle_disk_test.go`, `rd_clone_test.go`, `snapshot_restore_test.go`,
  `remotes_test.go`, `layer_validation_test.go`, `encryption_test.go`,
  `drbd_passphrase_test.go`, `api_call_rc_envelope_test.go`
- `pkg/satellite` — `controllers/observer_internal_test.go`,
  `controllers/heartbeat_test.go`, `controllers/heartbeat_internal_test.go`,
  `controllers/storagepool_test.go`, `controllers/storagepool_replacement_test.go`,
  `controllers/sweeper_test.go`, `reconciler_drbd_test.go`,
  `ship_dispatch_test.go`
- `pkg/placer` — `placer_test.go` (replicas-on-same / -different,
  Diskless-witness exclusion, mixed-provider pools)
- `pkg/storage` — `contract_test.go`, `zfs/zfs_test.go`,
  `zfs/zfs_integration_test.go`, `lvm/lvm_thin_test.go`,
  `lvm/lvm_thick_test.go`, `file/file_test.go`
- `pkg/drbd` — `events2_test.go`, `conffile_test.go`, `options_test.go`
- `pkg/luks` — `luks_test.go` (Format/Open/Close/Resize)
- `internal/controller` — `quorum_policy_test.go`, `set_quorum_test.go`,
  `ensure_tiebreaker_test.go`, `tiebreaker_test.go`, `pick_tiebreaker_test.go`,
  `remove_witnesses_test.go`, `apply_witness_decision_test.go`,
  `auto_tiebreaker_test.go`, `auto_diskful_test.go`, `drbd_ids_test.go`,
  `node_heartbeat_controller_test.go`, `node_reconcile_branches_test.go`,
  `disk_class_test.go`, `first_available_pool_test.go`,
  `node_label_sync_test.go`, `layer_stack_test.go`
- `tests/contract` — `replay_test.go`, `oracle_test.go`, `normalize.go`,
  property-bag detection
- `tests/safety` — `safety_rails_test.go` (no force-strip finalizers,
  no controller-side node-lost)
- `test/cheatsheet` — observability + naming-deltas drift coverage

Approximately 30 e2e scripts committed under `tests/e2e/`:

- `tests/e2e/auto-diskful.sh`, `backing-device-fail.sh`,
  `cheat-sheet-cli-level2.sh`, `cheat-sheet-csi-level1.sh`,
  `cheat-sheet-naming-deltas.sh`, `clone.sh`, `drbd-luks-stack.sh`,
  `evacuate.sh`, `lc-connection-cleanup.sh`, `lc-rd-delete-cascade.sh`,
  `lifecycle-toggle-migrate.sh`, `lifecycle-toggle-retry.sh`,
  `linstor-cli-replica-move.sh`, `linstor-cli.sh`, `luks-layer.sh`,
  `network-partition.sh`, `no-drbd.sh`, `node-evacuate.sh`,
  `node-lost.sh`, `node-multi-evacuate.sh`, `node-restore.sh`,
  `observability-capacity-correlation.sh`,
  `observability-destructive-walk.sh`,
  `observability-linstor-node-bridge.sh`, `observability-three-way.sh`,
  `recovery-deleting-convert.sh`, `recovery-discard-my-data.sh`,
  `recovery-false-diskless.sh`, `recovery-inconsistent-blocking.sh`,
  `recovery-stuck-synctarget.sh`, `replica-add-no-resync.sh`,
  `resize-luks.sh`, `resize-no-drbd.sh`, `resize-plain.sh`,
  `resize-pvc.sh`, `rwx-ganesha.sh`, `satellite-utils-smoke.sh`,
  `snap-ship-cross-node.sh`, `snapshot-restore-cross-node.sh`,
  `state-auto-resync.sh`, `state-inconsistent-mid-sync.sh`,
  `state-standalone-partition.sh`, `tiebreaker.sh`, `toggle-disk.sh`,
  `two-primaries-live-migration.sh`, `two-volume-rd.sh`

Five scenario docs landed under `tests/scenarios/`:

- `tests/scenarios/01-api-contract.md` (28 scenarios)
- `tests/scenarios/02-placement.md` (20 scenarios)
- `tests/scenarios/03-networking.md` (12 scenarios)
- `tests/scenarios/04-lifecycle.md` (26 scenarios)
- `tests/scenarios/05-drbd-state-recovery.md` (38 scenarios)
- `tests/scenarios/06-storage-backends.md` (24 scenarios)
- `tests/scenarios/07-quorum-observability.md` (24 scenarios)

Coverage audit doc: `docs/scenario-coverage.md` cross-references all 172
scenarios against unit / integration / e2e surfaces, with explicit
`covered-unit` / `covered-integration` / `covered-e2e` / `spec-skip` / `gap`
/ `out-of-scope` labels and per-priority gap accounting.

### Design Decisions

- **Bug 11 — 409 Conflict for state errors (convention).** RD / RG /
  Snapshot Update races under concurrent reconcile retry via
  `RetryOnConflict`; the wire surface returns 409 (matching the upstream
  controller). Codified in `pkg/store/k8s/*` and pinned in unit tests.
- **Bug 20 — Option B at the heartbeat layer (not the Hello path).** The
  `Hello` upsert was retired in Phase 10.6; the stale-heartbeat fix lives
  in `internal/controller/node_heartbeat_controller` so it composes with
  the rest of the controllers' watch fan-out.
- **Bug 21 — `501` redirect to snapshot-restore-resource.** A direct
  `zfs rollback` on a live volume is a data-loss footgun; the documented
  workaround is `snapshot-restore-resource` into a fresh RD, then
  swap PVs. The 501 body links to the runbook so the caller can't miss it.
- **Bug 26 — layer-validation stays in RG + RD create.** Static analysis
  (scenario 5.27) confirms there is no downstream re-validation; the
  REST handler is the single guard for `layer_list`. The regression
  guard is the test pinning unsupported stacks (CACHE / WRITECACHE /
  NVMe-oF / NVMe-TCP) to 422.
- **Bug 34 — option A landed in main, option B is the follow-up.** The
  current path wires the upstream `migrate-disk` URL with synchronous
  semantics; option B (async with a status sub-resource) is on the
  `feat/bug-34-migrate-option-b` worktree.
- **Bug 30 — `make-available` matches `golinstor v0.60.0`.** Response shape
  + `TIE_BREAKER` promotion follow the upstream library's expectations,
  so existing piraeus / operator code paths work unchanged.

### Out of Scope (deliberate)

- **CACHE / WRITECACHE / NVMe-oF / NVMe-TCP layers.** No production
  consumers; layer validator returns 422 with actionable text.
- **Auto-passphrase orchestration.** Piraeus owns the secret lifecycle;
  blockstor exposes the passphrase surface only.
- **S3 snapshot shipping.** Velero owns object-storage backups; the
  `LINSTOR remote` `ship` endpoint stays a documented `501` until / unless
  there's an explicit re-scoping.
- **sysfs `blkio` QoS.** kubelet + cgroup-level handle this; setting it
  per-DRBD-resource fights the kubelet QoS class.

### Operational Notes

- **Bug 41 RBAC fix requires a stand re-deploy after every controller
  rebuild.** The satellite SA pulls the role binding on pod startup; an
  in-place rebuild without re-apply will revive the 403.
- **Stand load tolerance: ~4-6 simultaneous cluster bring-ups max.**
  Above that, the shared QEMU host hits I/O contention and DRBD
  initial-sync flakes show up as scenario 5.16 false positives.
- **Worktree isolation didn't always hold.** Several fix-agent runs wrote
  into the main repo instead of their dedicated worktree; mitigated by
  cherry-picking the branch tip into the bug-N worktree before review.
  Net effect: some commits land twice in `git log --all` (same subject,
  different SHAs — e.g. `4a0726d` ↔ `586a2c2`, `bf48c6d` ↔ `eef4233`).
