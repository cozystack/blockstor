# ZFS thick (non-thin) validation on dev-kvaps stand

Date: 2026-05-14. Stand: dev-kvaps (3 Talos workers). Tested via linstor CLI against blockstor apiserver (port-forward to localhost:3370).

NOTE: stand bootstrap (stand/install-pools.sh) only provisioned the blockstor-zfs zpool on workers 1+2; worker-3 had no zpool. Tests scoped to w1+w2 accordingly. The pre-existing ZFS_THIN StoragePool CRD on w3 is stale (no underlying zpool) - this is install-pools.sh / stand state, not a blockstor bug.

## Step-by-step results

| # | Step | Result | Notes |
|---|------|--------|-------|
| 1 | Inspect state | PASS | Confirmed pre-existing zfs-thin pools on w1/w2/w3 (w3 zpool missing). |
| 2 | Create ZFS thick SP via `linstor sp c zfs` | PASS w/ BUG | Pool registers, but SP CRD got wrong prop key (`StorDriver/StorPoolName` instead of `StorDriver/ZPool`). Satellite logs `ZFS provider requires "StorDriver/ZPool" in props`. Worked around by `kubectl patch` to add `StorDriver/ZPool`. See Bug #1. |
| 3 | RG + RD + autoplace 2 on zfs-thick | PASS | After workaround, RG and RD create cleanly; autoplace puts diskful on w1+w2 and DRBD-only TieBreaker on w3. |
| 4 | Convergence | PASS | Both replicas UpToDate in ~4s (1 GiB volume, single-disk Talos VM lab). |
| 5 | Verify zvols + reservation | PASS | `blockstor-zfs/zfs-thick-test_00000`: volsize=1G, refreservation=1.02G (full), used=1.02G. ZFS_THIN baseline: refreservation=none, used=16.5K. The `-s` sparse flag in pkg/storage/zfs/zfs.go:73-75 correctly distinguishes the two. |
| 6 | I/O round-trip via DRBD | PASS | Wrote 1 MiB urandom (md5 d10606f2...) on w1 primary, switched primary to w2, re-read - md5 matched. |
| 7 | Snapshot create | PASS | `linstor s c` -> ZFS snapshot present on both nodes (`zfs-thick-test_00000@snap-1`). CRD created. `linstor s l` state shows `Incomplete` - same for ZFS_THIN baseline (pre-existing blockstor limitation, not thick-specific). |
| 8 | Snapshot delete + RD delete | FAIL - BUGS | `linstor s d` removed CRD but on-disk `@snap-1` survived on both workers - orphan. RD delete cleaned w1 zvol but left `zfs-thick-test_00000` on w2 (race: Resource CRD vanished before satellite `strip finalizer` succeeded). Same orphan also seen on ZFS_THIN baseline. Bugs #2 + #3. |
| 9 | Compare to ZFS_THIN | DONE | Wire shape identical. Only deltas: (a) `zfs create -V N -s` (thin) vs `-V N` (thick); (b) `CanSnapshots=False` shown by `linstor sp l` for thick SP BEFORE the prop-key patch, `True` after - i.e. the column is gated on the satellite successfully constructing the provider, not on the provider kind. |

## Bugs uncovered

### Bug #1 - `linstor sp c <driver>` registers SP with wrong prop key (all drivers)

- Files: `pkg/satellite/factory.go:110-138` (newZFS); `pkg/rest/storage_pools.go:200-243` (handleNodeStoragePoolCreate).
- Cause: modern python-linstor-client (>= API 1.2.0) sends `"props": {"StorDriver/StorPoolName": "<pool>"}` for EVERY driver kind (`/usr/lib/python3/dist-packages/linstor/linstorapi.py:1320`). It does NOT send the kind-specific keys (StorDriver/ZPool, StorDriver/ZPoolThin, StorDriver/LvmVg, StorDriver/FileDir). The blockstor REST handler stores props as-received. The satellite factory then looks up `StorDriver/ZPool` / `StorDriver/ZPoolThin` and fails: `ZFS provider requires "StorDriver/ZPool" in props`.
- Impact: All `linstor sp c {lvm, lvmthin, zfs, zfsthin, file, filethin}` flows are broken in the same way. Only reason `zfs-thin*` pools work today is that `stand/install-pools.sh` applies `stand/blockstor-storagepools.yaml` with the correct `StorDriver/ZPoolThin` key directly via `kubectl apply`, bypassing the REST CLI path entirely. `linstor sp c lvmthin` etc. would reproduce the bug identically.
- Fix options:
  - Apiserver-side normalization in `pkg/rest/storage_pools.go::decodeStoragePoolCreate` (or `handleNodeStoragePoolCreate`): translate `StorDriver/StorPoolName` to the kind-specific key based on `body.ProviderKind` before persisting. Mirrors upstream LINSTOR's Java controller behavior.
  - Satellite-side fallback in `pkg/satellite/factory.go::newZFS` (and newLVMThin/newLVMThick/newFile): if the kind-specific key is missing, fall back to `StorDriver/StorPoolName`. Cheaper change but spreads the translation across every driver.
- Recommendation: do the apiserver-side normalization - single choke point, matches upstream contract, keeps satellite factory simple. The CRD ends up carrying the upstream-LINSTOR shape that everyone expects.

### Bug #2 - Snapshot delete leaves orphan ZFS snapshots on satellites

- File: `pkg/satellite/controllers/snapshot.go` (entire reconciler, 176 lines).
- Cause: Snapshot reconciler has NO finalizer. Compare `pkg/satellite/controllers/resource.go:44-50` (SatelliteResourceFinalizer) and `pkg/satellite/controllers/storagepool.go:36-40` (StoragePoolFinalizer) - both stamp a finalizer in handleCreate and strip it in handleDelete. Snapshot reconciler at `snapshot.go:106-130` (handleCreate) skips the stamping step. When the apiserver deletes the Snapshot CRD, kube-apiserver removes the object immediately - the satellite may or may not see the DeletionTimestamp event before the object is GCed, so handleDelete (`snapshot.go:136-153`) often never runs.
- Impact: every snapshot delete leaks the on-disk ZFS snapshot (and presumably the LVM thin snapshot on LVM_THIN - not validated here). Reproduces 100% with ZFS thick AND ZFS_THIN.
- Fix: Add `SatelliteSnapshotFinalizer = "blockstor.io.blockstor.io/satellite-snapshot"`. In handleCreate: add+Update before issuing CreateSnapshot. In handleDelete: strip+Update only after `DeleteSnapshot` resp.Ok is true. Mirror the resource.go pattern.

### Bug #3 - Resource delete can leave orphan zvols on one satellite

- File: `pkg/satellite/controllers/resource.go:464-509` (handleDelete).
- Symptom (observed): after `linstor rd d zfs-thick-test`, w1 cleaned `zfs-thick-test_00000` but w2 did not. Logs show `strip finalizer: Operation cannot be fulfilled on resources... the object has been modified` followed shortly by `strip finalizer: resources.blockstor.io.blockstor.io "zfs-thick-test.dev-kvaps-worker-2" not found`. Between the satellite re-reading res and Updating to strip the finalizer, the Resource CRD vanished. Same orphan reproduced on ZFS_THIN.
- Probable cause: stale cached read of `res` before Update; the upstream "controller-side" finalizer (`blockstor.io.blockstor.io/resource`, mentioned at resource.go:462) stripped first and the apiserver finalized the object before handleDelete re-fetched. The current code path returns early on `strip finalizer` error without retrying DeleteResource - on the next reconcile the CRD is gone, Get returns NotFound, reconciler exits, and `DeleteVolume` (zfs destroy) on the satellite never runs.
- Fix sketch: run `DeleteResource` BEFORE the Update in handleDelete; if the object disappears mid-flight (apierrors.IsNotFound), still issue the DeleteResource intent so the satellite tears down the backing volume - the zvol is the only thing that survives the CRD. Alternatively use APIReader for the Get to bypass the stale cache (same pattern noted in MEMORY's `blockstor_tiebreaker_race.md`).

## Wire-shape deltas vs ZFS_THIN

- `zfs create -V <size>M <dataset>` (thick) vs `zfs create -V <size>M -s <dataset>` (thin). Only difference. Confirmed in `pkg/storage/zfs/zfs.go:72-78`.
- Provider.Kind() returns "ZFS" vs "ZFS_THIN" (`pkg/storage/zfs/zfs.go:55-62`). Used in REST/dispatcher for routing - no observed downstream behavioral split beyond the `-s` flag.
- snapshot, send/recv, resize, destroy: code paths identical (single Provider for both modes).
- linstor sp l columns differ in pre-fix state only: thick shows blank FreeCapacity / TotalCapacity and `CanSnapshots=False` until the satellite can construct the provider; once the StorDriver/ZPool key is in place, columns populate identically to ZFS_THIN.

## Recommendations re `pkg/satellite/factory.go::newZFS`

`newZFS` itself is fine - the dual-key fallback it implements is the right defensive read. The bug is upstream, in the apiserver REST path that persists `StorDriver/StorPoolName` instead of translating it. Fixing in the factory only papers over the same wire-shape mismatch that also breaks LVM_THIN / FILE_THIN if anyone ever uses `linstor sp c` for them. The proper fix lives in `pkg/rest/storage_pools.go`.

If a satellite-side belt-and-braces fix is desired, `newZFS` could add `StorDriver/StorPoolName` as a third fallback key (after propZPool, propZPoolThin). `newLVMThin`, `newLVMThick`, `newFile` would need the same. But this leaves the prop store containing the wrong key, which will surface again next time someone reads the CRD expecting the upstream-LINSTOR shape (install-pools.sh-generated CRDs carry the kind-specific key).
