# Changelog

All notable changes to blockstor are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project follows
[Semantic Versioning](https://semver.org/).

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
