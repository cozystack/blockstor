# Changelog

All notable changes to blockstor are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project follows
[Semantic Versioning](https://semver.org/).

## v0.1.10 — 2026-06-06

Corner-case parity campaign release. Every behavior in this release was validated on a live Talos+QEMU stand, with the ⚖️-ambiguous cases compared against an upstream LINSTOR 1.33.2 oracle cluster (controller + 3 satellites) running side-by-side on the same DRBD kernel. 61 corner-case plan items closed; 15 new rows added to the known-deltas whitelist.

### Fixed

- **`DrbdOptions/auto-quorum disabled` was silently ignored (#97, #105)** — the opt-out gate read the camelCase `DrbdOptions/AutoQuorum` key that no production path writes (#97), and after the key fix it still read `Spec.Props` while the store transcoder routes the kebab key into `Spec.ExtraProps` (#105) — so the reconciler kept re-stamping `quorum=majority` over an operator's manual `quorum off`. Both layers fixed; the gate now honors the canonical key in both property bags. Caught at operator-CLI level by the L7 replay — twice — after unit fixtures passed.
- **Empty `set-property` value now deletes the key everywhere (#97, #107)** — upstream semantics ("empty value = delete the property") were implemented for RD/RG modify first (#97) and then rolled out to all remaining CLI-reachable handlers: node, storage-pool, controller, resource, volume-definition, volume-group, storage-pool-definition (#107). KV-store and log-level handlers are deliberately exempt (empty is real data there).
- **Controller-tier DRBD options no longer beat closer scopes (#98)** — `linstor controller drbd-options` values used to override RD-level overrides; the effective-properties resolver now applies the upstream precedence (controller < resource-group < resource-definition < resource) uniformly. The rewrite initially dropped non-DRBD upper-scope properties (`FileSystem/Type` never reached the satellite, breaking mkfs seeding) — caught by 3× CI failures and an isolated-stand A/B repro, fixed in the same PR.
- **Autoplace now upgrades a tiebreaker witness in place (#111)** — `resource create --auto-place +1` (and the absolute form) on a 2-diskful + witness shape failed with "Not enough nodes": the placer counted the witness-holding node as taken. It is now an upgrade candidate, promoted via the same flag transition the explicit `r c --storage-pool` path uses; wire result matches upstream (in-place witness upgrade, no fourth node).
- **`node delete` on an EVICTED node is rejected; `AutoplaceTarget=false` excludes a node from autoplace (#102)** — both upstream-documented guards were missing.
- **Deprecated `--diskless` wire alias accepted (#103)** — `DRBD_DISKLESS` is normalized to `DISKLESS` at the resource-create boundary, so older clients and scripts behave identically to upstream.
- **Snapshot edge guards (#100)** — in-place `snapshot rollback` is rejected with an actionable pointer to the safe `snapshot resource restore` path (upstream's rollback both destroys newer snapshots and, on ≥1.31.2, silently resurrects deleted replicas — verified live against the oracle); restore into an RD that already has volume definitions returns the upstream-typed `FAIL_EXISTS_VLM_DFN` envelope; `AutoSnapshot/Keep ≤ 0` falls back to 10; snapshots on thick-LVM pools are rejected with the upstream envelope.
- **Finalizer-blocked deletions surface the `DELETE` flag (#94)** — a resource-definition held by node teardown now shows `DELETING` in `rd l`, matching upstream's two-phase deletion visibility.
- **`--layer-list` duplicate-layer rejection reports the real fault (#108)** — duplicate detection now runs before the position check, so `drbd,drbd,storage` says "appears more than once" instead of a misleading ordering error.

- **Resync transfers only written data on real-block thin pools (#112)** — the rendered `disk {}` section now includes `rs-discard-granularity` on LVM-thin/ZFS pools (matching upstream), so DRBD discards provably-zero ranges during resync instead of byte-copying them — measured ~2x faster recovery of partially-written volumes. FILE_THIN (loop-backed) pools deliberately omit the option: a full-device mkfs discard on loop backing dirties the bitmap against the day0-seeded peers and wedges fresh-create convergence (found by CI, isolated on the stand, recorded as a known delta).

### Parity pins and recorded divergences

- Volume-number allocation (smallest-free reuse after `vd d`, explicit `--vlmnr` gap fill) pinned oracle-identical; multi-volume RDs render as one DRBD resource with nested `volume {}` blocks (#96).
- Deletion semantics pinned byte-identical to upstream: `rd d` blocked by snapshots while `r d` is not; `rg delete` with live RDs rejected with the upstream envelope (#94).
- Placement contracts pinned: unsatisfiable place-count accepted at `rg c` and failing only at spawn (FAIL_NOT_ENOUGH_NODES); `--x-replicas-on-different` empty-bucket semantics; bare-flag autoplace property reset; `--providers` order-independence (#99). The plan's assumption that `rg c --place-count +1` is rejected upstream was disproven by the oracle and documented.
- BalanceResources: blockstor's `RGRebalanceReconciler` already provides the equivalent of upstream's periodic balancer and honors `BalanceResourcesEnabled=false`; residual divergences recorded (#108).
- Quorum behavior pinned at the DRBD-kernel level: a diskless tiebreaker can hold but never return quorum — a severed node stays UpToDate yet unpromotable (`drbdadm primary` → "No quorum"), exactly per the DRBD documentation (#106).
- New known-deltas rows for: shrink rejection (BS stricter, `force=true` escape), default `on-no-quorum=suspend-io` seed (data-safety choice vs upstream's unset), quorum-property strip-vs-stamp on `r d`, permissive property-value validation, resource-connection/node-connection peer-options surfaces, per-object option-class enforcement, StorPoolName resolution chain, autoplacer weight defaults, DRBD port/minor base ranges (20000+ to coexist with upstream on shared kernels), `rd lp` inherited-property inlining, layer-list error envelope shape.

### Testing & infrastructure

- Upstream LINSTOR 1.33.2 oracle cluster install script for the dev stand: controller + 3 satellites with disjoint DRBD port/minor ranges, enabling live A/B parity validation (#95).
- `state-standalone-partition` hardened against its dominant CI flake modes: the partition rule is verified applied before the detect wait, transient empty status reads are retried instead of sampled, and an evidence dump precedes any failure (#104).
- Replay harness: `hold_s` await option — an assertion must stay true for N consecutive seconds, catching value-flapping that a first-match await false-passes (#105); `prop_value` await extended to node/controller objects (#107); `vd_size_kib` await fixed (an environment-variable-across-pipe bug made it pass-proof since introduction) and `{{rg}}`/`{{device}}` substitutions added (#110); fixture-gated replays now SKIP cleanly when the stand lacks the fixture (#110).
- Replay assertions made auto-tiebreaker-aware: after operations that leave two diskful replicas, the witness legitimately re-occupies the vacated node — `resource_absent` assertions replaced with `replica_diskless`/`tiebreaker_present` (#103, #109, #110).
- `stand/up.sh`: backticks in heredoc comments no longer execute as command substitution on hosts that have the named binaries; respawn-wedge latch wait widened for loaded CI runners (#101).
- Slow-stand resync headroom on large-volume replay gates; L4 quorum scenario and corner-case L6/L7 coverage across all campaign groups (#99, #100, #102, #103, #106, #107, #108, #110).

## v0.1.9 — 2026-06-03

Patch release with a single operator-reported fix. Versions v0.1.6–v0.1.8 are skipped: those tags pre-exist in the repository from an inherited lineage and do not correspond to blockstor releases.

### Fixed

- **Resource flaps forever after `vd d` (Bug 399, #92)** — deleting a volume definition removed the volume from the RD and the DRBD kernel, but two add-only projections never forgot it: the controller's RD → `Resource.spec.volumes` projection kept a stale entry, and the satellite observer's volume cache only evicted on the events2 `destroy device` frame — which a DISKLESS/tiebreaker replica never receives (it has no local disk to destroy) — so the observer re-emitted a phantom `status.volumes[n]=Diskless` every kernel tick, oscillating the resource status and PATCH-storming the apiserver ~1/s indefinitely. The controller now prunes `spec.volumes` entries absent from the RD, and the observer converges its volume cache against the live RD volume set (the `blockstor.io/volume-numbers` annotation), so both diskful and diskless replicas settle to exactly the remaining volumes. The e2e `volumes_settled` flap gate was also made kine-safe (volume-set stability across polls instead of resourceVersion equality, which k3s/kine inflates with the global store revision).

## v0.1.5 — 2026-06-03

Large bug-fix release. Spans the REST API wire-validation surface (Bugs 356–383: input validation, typed `FAIL_*` envelopes, idempotency), the satellite DRBD / LUKS / metadata paths, and a multi-round operator-lifecycle bug-hunt that closed the four operator-reported DRBD lifecycle bugs the REST sweep missed plus their adjacent classes (Bugs 384–397). Every operator-facing fix lands with an L1/L2 unit/contract test, an L6 `cli-matrix` cell, and an L7 operator-replay workflow, validated on the live Talos+DRBD stand.

### Fixed

- **Late `vd c` leaves the new volume `Inconsistent` on every replica (Bug 384, data integrity, #83)** — adding a volume to an already-initialized multi-replica resource ran the seed path with `isWinner=false` unconditionally (first-activation election is gated on `!rdInitialized`), so no replica seeded the new volume `UpToDate` and it latched `Inconsistent` forever. The satellite now re-runs the lowest-node-id winner election per freshly-added volume, so exactly one replica becomes the SyncSource. Class regression of the Bug 79/332 family.
- **`node evict` demotes a healthy diskful replica to TieBreaker (Bug 385, #83)** — `ensureTiebreaker` counted a witness stranded on a just-EVICTED node as live, so the witness was never relocated and a healthy diskful drifted into the tiebreaker role. Replicas on EVICTED/LOST nodes are now excluded from the witness/quorum decision and stranded witnesses are reaped.
- **`node restore` does not recreate the auto-TieBreaker (Bug 386, #83)** — the RD reconciler did not watch `Node`, so clearing the EVICTED flag never re-ran the tiebreaker invariant, leaving two diskful UpToDate with no witness (split-brain risk). Adds a Node watch.
- **`r d` of a diskful on a 2-diskful + 1-INACTIVE resource grows a useless TieBreaker (Bug 387, #83)** — an INACTIVE (`drbdadm down`) replica is not a voting peer but was counted as a diskful, so the delete spuriously converted to a witness. INACTIVE replicas are excluded from the voting set.
- **`node evacuate` never prunes the source replica (Bug 389, #81)** — evacuate gap-filled a replacement but never deleted the source on the drained node, leaving the resource permanently at place_count+1. Now does strict add-before-drop (prune only after the replacement reaches UpToDate) and derives the redundancy target from the current diskful count, so it works on RDs that inherit `place_count=0` from `DfltRscGrp`.
- **auto-diskful ignores EVICTED/LOST nodes and INACTIVE replicas (Bug 390, #82)** — the deficit count and promotion-candidate set treated drained-node and deactivated replicas as healthy diskful, masking deficits and promoting onto draining nodes. Both are now filtered.
- **Autoplace under-places when an INACTIVE replica is present (Bug 393, #85)** — `placer.countDiskfulReplicas` counted INACTIVE replicas toward `place_count`, so a replacement active replica was never placed. INACTIVE is now excluded, mirroring the auto-diskful and tiebreaker invariants.
- **`snapshot create` fails on any resource with an INACTIVE replica (Bug 394, #86)** — snapshot node-selection and the success denominator included the INACTIVE node, whose down DRBD device cannot ack the suspend-io barrier, aborting the whole group. INACTIVE replicas are excluded from snapshot targets.
- **Thick-LVM volume resize silently diverges the replicas (Bug 395, data integrity, #87)** — `drbdadm resize --assume-clean` ran unconditionally; on a thick `LVM` pool the grown extents hold node-distinct stale content, so replicas disagreed on the grown region with no resync (out-of-sync 0) and a failover changed the bytes an application read. Resize is now provider-aware: zero-on-allocate providers (ZFS, thin, file) keep the `--assume-clean` fast path; thick LVM omits it so DRBD resyncs the grown region. Cozystack's default (ZFS) was unaffected.
- **Snapshot-restore onto a snapshot-less node (Bug 397, #89)** — the explicit `--node-name` restore path did not constrain targets to the nodes that hold the snapshot (unlike the auto-place path), so a replica could be placed on a node lacking the data. The restore handler now rejects a snapshot-less target with a typed error, and the seed path refuses the skip-init-sync fast path for a blank-fallback replica so it SyncTargets the real copy; a legitimate all-clone restore keeps the fast path.
- **Tiebreaker / toggle-disk / LUKS / metadata satellite fixes** — `r toggle-disk --diskful` no longer leaves a stale `TIE_BREAKER` flag on the promoted replica (#54); `r d --keep-tiebreaker` keeps the auto-witness instead of collapsing it (#57); `r c` retries through the tiebreaker-collapse race instead of failing (Bug 359, #61); a TB-relocate that wedged `StandAlone` on a both-disks-bitmap state now recovers (#53); `r td --diskless` closes the LUKS mapper so the backing zvol can be reclaimed (#55); and per-volume `drbdadm create-md` stops `vd c` on an existing RD from EBUSY-looping against vol-0's attached minor (Bug 332, #58).

### Fixed — REST API wire validation & idempotency

Closes Bugs 356–383: the REST surface now validates operator input at the wire boundary (before any partial state lands) and returns upstream-matching `FAIL_*` ApiCallRc envelopes instead of bare 200s or generic 500s.

- **Name & volume-number validation** — RD/RG/Node names are capped at the 48-char k8s-label limit (Bug 360, #59) and invalid names are rejected on `s r rst` / `rg spawn` before partial state lands (#56); `volume_number` is validated in `[0, 65535]` at create (#60) and on `vd d` / `vd l` / `vd m` (Bug 365, #62); a non-numeric volume-number in the URL path returns an operator-grade envelope (Bug 380, #73).
- **Size, type & placement validation** — non-positive `volume_sizes` in spawn (Bug 381, #74) and non-positive `size_kib` on a `vd` PUT regardless of `--force` (Bug 383, #75) are rejected; `select_filter.place_count` is validated at RG create + modify (Bug 367 / 361, #64); node `Type` is validated, defaulting empty to `SATELLITE`, at `POST /v1/nodes` (Bug 370, #65); a `PUT` resource-definition validates its `resource_group` (Bug 372, #66); net-interface `PUT` validates address + port at the wire (Bug 371 / 368 / 369, #63); the fresh-create pool resolver walks the RG `StoragePoolList` (Bug 364, #67).
- **Immutability & idempotency** — `StorDriver/*` mutation is rejected on `PUT` storage-pools (Bug 373, #68) and storage-pool-definitions (Bug 375, #70); five bare-200 write endpoints now emit an ApiCallRc envelope (Bug 374, #69); `drop-property` (Bug 378, #71) and net-interface `DELETE` (Bug 379, #72) are idempotent on a missing parent.

### Test infrastructure

- **L7 replay convergence assertions were silent no-ops (Bug 388, #83 / #84)** — `all_uptodate` / `wait_settle` filtered replicas on `spec.resourceName`, but the CRD field is `spec.resourceDefinitionName`, so the most-used "did the cluster converge" check passed vacuously across every replay. Fixed the field, tolerate Diskless/TieBreaker rows, and gave `no_orphans` a settle window. (This immediately caught a real drop-without-add defect in the first Bug-389 fix.)
- **e2e flake hardening (Bugs 392 / 396 / 398 — #84 / #88 / #90)** — `state-standalone-partition` and siblings flaked under CI on three substrate-level read/scan races, none of them blockstor data bugs (DRBD partition recovery is forensically correct — the writer stays SyncSource, ZFS checksums clean). The connection-state waits now read kernel ground truth instead of the lagging CRD projection; the marker round-trip distinguishes a real (stable) on-disk corruption from a non-deterministic nested-QEMU read-path glitch; and the stand's Talos config narrows the LVM `global_filter` so the node-side `pvscan` no longer races the satellite for DRBD/dm/zvol/loop backing devices (`open(/dev/loopN): Device or resource busy`).

## v0.1.4 — 2026-05-31

Bug-hunt + REST safety release. Closes issue #45 (autoplace capacity gate on the real linstor-csi single-node create path) and re-enables the corresponding e2e scenario.

### Fixed

- **Autoplace / spawn / single-node-create capacity gate (issue #45)** — when a StorageClass set `placementCount: 1` + `nodeList`, linstor-csi's `manual` scheduler bypassed the existing autoplace gate and accepted placement on a 100%-full pool, leaving the PVC `Bound` with a broken backing volume. The capacity check now lives inside `createOneResource` (shared by both the bulk `/v1/resource-definitions/{rd}/resources` and the single-node alias `/v1/resource-definitions/{rd}/resources/{node}`), reads `pool.FreeCapacity` directly, and rejects with 409 + `FAIL_NOT_ENOUGH_NODES`. A separate gate also runs on `/v1/resource-definitions/{rd}/autoplace` and `/v1/resource-groups/{rg}/spawn`. The 4-tier pool-name resolver honours `RG.SelectFilter.StoragePoolList`, which is the field linstor-csi posts.
- **Recovery after operator `drbdadm down` no longer reverts** — the satellite's `shouldSkipNetOnAdjust` gate was too broad: it skipped the net section of `drbdadm adjust` whenever any peer device was `StandAlone`, which then masked the operator-driven `down` and let the reconciler re-up the resource into the wrong topology. The gate is now narrowed to `StandAlone AND peer-devices-present`, matching the actual respawn case it was meant to cover.
- **Resource DELETE / SP DELETE / Node DELETE typed envelopes** — these endpoints used to return generic 500s or unparseable error bodies. They now return upstream-matching `FAIL_*` envelopes (`FAIL_EXISTS_RSC`, `FAIL_EXISTS_STOR_POOL`, `FAIL_IN_USE`, `FAIL_NOT_FOUND_RSC_DFN`, etc.), and the CSI driver treats `FAIL_EXISTS_SNAPSHOT_DFN` as idempotent success.
- **Duplicate storage-pool POST is refused, not silently mutated** — `POST /v1/nodes/{n}/storage-pools` against an existing SP used to overwrite the existing object. It now returns 409 + `FAIL_EXISTS_STOR_POOL`.
- **Internal annotations are stripped from REST reads** — `blockstor.io/*` and `*.blockstor.cozystack.io/*` annotations are now stripped on the wire boundary for `r l`, `rd l`, `rg l`, `snap l`, autoplace GETs, and the single-node alias.
- **Recovery-down-reverses regression** — narrowed satellite skip-net gate so the StandAlone-after-respawn path doesn't revert operator-driven `drbdadm down`.

### Added

- **Missing REST routes wired** — `/v1/storage-pool-definitions`, `/v1/migrate-disk`, and the `properties/info` family are now wired with real handlers and OpenAPI shapes.
- **DRBD promotion + node event streams** — `GET /v1/events/drbd/promotion` and `GET /v1/events/nodes` now serve newline-delimited JSON events.

### Test infrastructure

- **`observability-capacity-correlation` and `csi-pvc-local` restored to the piraeus-interop lane** — both e2e scenarios now PASS after the fixes above; `csi-pvc-local` was held back by `nodeName` (immune to taints) and is now driven by `nodeSelector`. `node-replace-hardware` remains excluded pending a separate fix in the node-replacement satellite path.
- **Flaky scenarios hardened** — `state-offline-unknown` (pod-name race), `state-auto-resync` (timeout 240s → 300s + grace re-check), `recovery-down-reverses` (timeout 60s → 120s), `recovery-deleting-convert` (CRD-status fallback to kernel pair). Origins are documented inline per scenario.
- **Bare-blockstor 6-lane matrix excludes piraeus-only scenarios** — `observability-capacity-correlation` is excluded from the 6-lane matrix because it requires piraeus's `LinstorCluster` CRD, which is installed only in the e2e-piraeus job.
- **`stand/up.sh` ported to talosctl 1.13's `cluster create qemu` subcommand** + skip-list for `/24` slots inside Talos's default `10.96.0.0/12` service CIDR (slots 96-111).

### Refactor

- **`createOneResource` extracted from the bulk and single-node REST handlers** so the capacity gate, pool resolver, and create-shape validator have one home.

## v0.1.3 — 2026-05-30

### Fixed

- **CSI storage-only auto-mkfs** — `local` StorageClass (no DRBD, single replica) now reaches Pod-ready end-to-end. The satellite was leaving storage-only resources unformatted because the mkfs path was wired only for the DRBD bring-up sequence; linstor-csi then failed `NodeStageVolume` with `wrong fs type`. The reconciler now formats the backing block device on the storage-only path before exposing it to the kubelet, and the `csi-pvc-local` e2e scenario is restored to gate the contract. Also wires the missing `POST /v1/resource-definitions/{rd}/resources/{node}` alias (linstor-csi v1.10.1 issues this single-node create — pre-fix the apiserver returned HTTP 405).
- **Orphan-witness collapse grace removed** — the controller used to keep a TieBreaker witness alive for several reconcile cycles after the last diskful peer left, on the theory that a fresh diskful might race in. In practice the grace window only ever surfaced as stuck `Off` peers after `r d` + immediate `r c` on the same node. The collapse is now instant: when no diskful peer remains and no fresh placement is pending, the witness is deleted in the same reconcile.

### Test infrastructure

- **`tests/e2e/lib.sh` DS-converge barrier** — `reset_cluster_state` now waits up to 90s for the satellite DaemonSet to converge (`desiredNumberScheduled == numberReady`) before declaring the cluster reset. Pre-fix, fast successive scenarios occasionally started against a not-yet-rolled satellite and the failure looked like a flaky test rather than a missed barrier.

## v0.1.2 — 2026-05-28

Bug-fix and test-coverage release.

### Fixed

- **TieBreaker remains after `r d`** (Bug 338 re-regression) — adds the missing e2e regression catcher (`tests/e2e/tiebreaker-r-d-cleanup.sh`). The controller-side fix landed earlier in `resourcedefinition_controller.go` (`shouldKeepExistingWitness`), but the lack of a real-DRBD test let it silently re-regress on the dev stand. Future TieBreaker changes are now gated by an e2e scenario that exercises `linstor r d` one-by-one and asserts the witness invariant on the QEMU+Talos lane.

### Test infrastructure

- **e2e cascade attribution** — when a scenario leaves the cluster dirty, the next scenario is no longer wrongly blamed. New `strict_cleanup_on_exit` helper + `register_strict_cleanup` trap demote a leaving-PASS to FAIL if the cluster cleanup fails, and a pre-flight check at the top of each scenario rewrites the previous scenario's verdict to FAIL with a `LEFTOVER` reason when satellite pods or RDs are still present from the prior run.
- **piraeus interop in CI** — the CI matrix now ships an `E2E (piraeus interop)` job that installs the upstream piraeus-operator against the blockstor apiserver and runs the linstor-csi scenarios (`rwx-ganesha`, `observability-three-way`, `observability-capacity-correlation`, `csi-pvc-replicated-rwo`) on a dedicated stand. Main lanes 1-6 keep running bare blockstor; the interop scenarios that need linstor-csi v1.10.1 + LinstorCluster CRD are isolated to the piraeus job.
- **`csi-pvc-replicated-rwo` e2e** — new test pins the linstor-csi DRBD path end-to-end against the user-facing `replicated` StorageClass shape (3 DRBD replicas, full `DrbdOptions/*` prop set, write→delete pod→read back on another node).

### Refactor

- **`FilesystemFormatted` Stamper API** — adds `StampFilesystemObserved` (`Reason=FilesystemObserved`) alongside the existing `StampFilesystemFormatted` (`Reason=MkfsSucceeded`), plus a byte-identity SSA-shape test that prevents the PR #32-class `.status.volumes:null` regression. The observe call site is intentionally not wired yet — it requires routing from the observer event path rather than the per-RD apply lane and will land in a follow-up.

## v0.1.1 — 2026-05-28

### Fixed

- **Respawn StandAlone wedge** — deleting a diskful replica and immediately recreating it on the same node no longer wedges the resource. The recreated replica was force-promoting itself and minting a DRBD Current-UUID unrelated to the surviving peer, which the kernel rejected as `unrelated-data` (the connection dropped to `StandAlone` and never recovered). Auto-primary is now gated on the resource's persisted `Initialized` latch, so only a brand-new resource ever seeds a primary.
- **ZFS clone-source deletion** — deleting a volume that still has a dependent ZFS clone no longer hot-loops on `zfs destroy ... volume has dependent clones`. Dependent clones are now `zfs promote`d before the source is destroyed; the surviving clone keeps its data.

## v0.1.0 — 2026-05-27

First public release.

### Added

- **LINSTOR-compatible REST API**, served over mTLS — drives the existing client ecosystem (`linstor` CLI, `linstor-csi`, `piraeus-operator`, `ha-controller`, `golinstor`) unchanged.
- **DRBD-replicated volumes** on LVM, LVM-thin, ZFS, ZFS-thin, and file backends, with autoplacement (zones, node properties, replicas-on-different), TieBreaker + quorum, and online resize.
- **Run without DRBD** — plain local storage, single-replica diskful or diskless.
- **LUKS encryption** layer (volume-level, at rest).
- **Snapshots** — create, restore as a new resource, roll back, and clone; intra-cluster snapshot shipping via `zfs send`/`recv` and `thin-send-recv`.
- **Device-pool creation** from physical disks (`physical-storage create-device-pool`).
- **Kubernetes-native architecture** — all state in CRDs, controller and per-node satellite as `controller-runtime` managers, no external database.
- **Multi-arch images** (`linux/amd64`, `linux/arm64`) published to GHCR: `blockstor-controller`, `blockstor-apiserver`, `blockstor-satellite`.

### Notes

- Default DRBD allocation windows are disjoint from upstream LINSTOR's — TCP ports `20000–20999`, minors `20000–65535` — so blockstor can run alongside a live LINSTOR on the same nodes. Resources adopted from LINSTOR keep their original ports and minors.
- Not yet implemented (the API returns `501 Not Implemented`): cross-cluster snapshot shipping, backup create/restore/ship, schedules, and remote backends (S3, LINSTOR remotes). See the README for the current roadmap.
