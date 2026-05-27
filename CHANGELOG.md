# Changelog

All notable changes to blockstor are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project follows
[Semantic Versioning](https://semver.org/).

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
