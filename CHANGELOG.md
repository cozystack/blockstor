# Changelog

All notable changes to blockstor are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project follows
[Semantic Versioning](https://semver.org/).

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
