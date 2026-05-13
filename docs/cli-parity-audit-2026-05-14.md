# CLI parity audit: blockstor REST vs upstream LINSTOR (piraeus)

Date: 2026-05-14
Driver: `linstor` CLI 1.27.1 on `ubuntu@129.213.29.101`
Targets:
- **BS**: blockstor apiserver — `linstor controller 1.33.2; GIT-hash: blockstor` — port-forward `localhost:3370` → `svc/blockstor-apiserver.blockstor-system:3370`.
- **UP**: piraeus bundled Java controller — `linstor controller 1.32.3; GIT-hash: 6dac06aed233f2c89ac7cc6b1185d6dce9ec74c4` — port-forward `localhost:3371` → `svc/linstor-controller.piraeus-datastore:3370`.

Raw per-command dumps under `/tmp/cli-parity-raw/<cmd>.{bs,up}.{out,err,code}` on the dev box; JSON wire-shape dumps as `j-<cmd>.{bs,up}.json`.

## 1. Setup

Seeded the same logical state on both sides under a `parity-*` prefix so as not to disturb the existing `test` RD:

| object        | BS | UP |
|---------------|----|----|
| `parity-rg`   | `--place-count 2 --storage-pool stand` | `--place-count 2 --storage-pool pool` |
| `parity-rd`   | `--resource-group parity-rg`           | `--resource-group parity-rg`           |
| vol-def       | 16 MiB                                  | 16 MiB                                  |
| `parity-rd` R | autoplace 2 (worker-1, worker-3 + tie-breaker worker-2) | autoplace 2 (same placement; satellite-side `/var/lib/linstor.d/.backup` write failed, see Open Q) |
| snapshot      | `parity-snap` on `parity-rd`            | `parity-snap` on `parity-rd`            |
| ephemeral     | `parity-fake` Satellite node (10.99.99.99) | `parity-fake` Satellite node (10.99.99.99) |

Pre-existing state untouched on each side:
- BS already had `test` RD, SPs `lvm-thin/stand/zfs-thin` and RG `dfltrscgrp` (note: lowercase, see delta D6).
- UP started essentially empty (RG `DfltRscGrp`, SP `pool` + `DfltDisklessStorPool`).

Cleanup at end: snapshot, RD, RG, node `parity-fake` deleted on BS (instant) and on UP (async; UP RD lingered in DELETE state because of piraeus-satellite `/var/lib/linstor.d/.backup` write failure — see Open Q1, **not caused by the audit**).

## 2. Per-command table

`Delta` legend:
- **PARITY** — identical exit + equivalent semantic stdout.
- **WIRE_SHAPE** — REST JSON has different schema (missing/extra keys); CLI table renders without crashing but loses information.
- **ERROR_TEXT** — exit code matches but the message/structure differs.
- **MISSING_FEATURE** — BS returns OK but skips a real side-effect upstream performs.
- **CLI_BUG** — both sides fail identically because the CLI itself is wrong (not a controller delta).

| # | Command | UP exit | BS exit | UP digest | BS digest | Delta |
|---|---|---|---|---|---|---|
| 1 | `n l` | 0 | 0 | `Addresses` populated (`10.244.0.6:3366 (PLAIN)`) | `Addresses` column **empty** | **WIRE_SHAPE** — `net_interfaces[].address/port/type` not serialised. |
| 2 | `n l -p` | 0 | 0 | full `--pastable` table w/ addresses | empty Addresses | **WIRE_SHAPE** (same as #1) |
| 3 | `sp l` | 0 | 0 | also lists `DfltDisklessStorPool` per node; `SharedName` populated; `CanSnapshots=True` for FILE_THIN | no diskless SPs; `SharedName` always empty; `FILE_THIN.CanSnapshots=False` | **WIRE_SHAPE** + **BEHAVIOR**: BS never auto-creates `DfltDisklessStorPool`, never fills `SharedName`, mis-reports `supports_snapshots` for FILE_THIN. |
| 4 | `sp l --show-props "StorDriver/*"` | 0 | 0 | column empty (no `StorDriver/*` props set in this scenario) | column empty but BS *does* expose `props.StorDriver/LvmVg`/`ThinPool` in JSON | **WIRE_SHAPE** — props key uses upstream names but client glob still matches → cosmetically identical, but BS adds keys (BLOCKSTOR_SUPERSET for this row). |
| 5 | `rg l` | 0 | 0 | `DfltRscGrp` (CamelCase) | `dfltrscgrp` **lowercase** + custom Description "Default LINSTOR resource group — autoplace catch-all for RDs without an explicit RG." | **WIRE_SHAPE/CONTRACT** — default RG name diverges from canonical `DfltRscGrp`. CSI provisioner relies on this name (see csi-linstor `defaultResourceGroup`). |
| 6 | `rd l` | 0 | 0 | `Layers=DRBD,STORAGE` | `Layers=` (empty) | **WIRE_SHAPE** — `layer_data[]` not returned on RDs → CLI renders empty. |
| 7 | `rd l --resource-definitions parity-rd` | 0 | 0 | filter works | **filter ignored**: returns ALL RDs (`parity-rd` *and* `test`) | **BEHAVIOR_BUG** — `rscDfns=` query param not honoured. |
| 8 | `r l` | 0 | 0 | TieBreaker line shows `Layers=DRBD,STORAGE` | TieBreaker line shows `Layers=DRBD` only | **WIRE_SHAPE** — DRBD-only TieBreaker is fine, but BS omits storage child layer; if the TieBreaker has a backing volume this loses info. |
| 9 | `r l --faulty` | 0 | 0 | empty | empty | **PARITY** |
| 10 | `vd l` | 0 | 0 | shows `VolumeNr=0 VolumeMinor=1000 Size=16 MiB` | **empty rows** (only header) | **WIRE_SHAPE** — `volume_definitions[]` not returned by `GET /v1/view/volume-definitions` or analogous; this is a regression vs upstream (CSI doesn't depend on it directly but `linstor vd l` is broken). |
| 11 | `vd l --resource-definitions parity-rd` | 0 | 0 | populated row | empty | **WIRE_SHAPE** (same root cause as #10). |
| 12 | `v l` | 0 | 0 | `StoragePool=pool / DfltDisklessStorPool`; `MinorNr=1000`; `DeviceName=None` | `StoragePool=None`; `MinorNr=` empty; `DeviceName=/dev/drbd1001` (filled!) | **WIRE_SHAPE**: BS never returns `state.storage_pool_name` per-volume (field name mismatch) → CLI prints `None`. BS *does* populate `device_path` (BLOCKSTOR_SUPERSET for that field), but `minor_number` missing. |
| 13 | `v l --resources parity-rd` | 0 | 0 | same as #12 | same as #12 | (same as #12) |
| 14 | `s l` | 0 | 0 | `State=Failed` (because of the satellite `.backup` issue) | `State=Incomplete` | **ERROR_TEXT** — snapshot state machine differs. BS reports `Incomplete` regardless; upstream surfaces `Failed` when satellite reported the failure. |
| 15 | `s l --resource-definitions parity-rd` | 2 | 2 | argparse error (no such flag) | argparse error (no such flag) | **CLI_BUG** (PARITY at controller level) |
| 16 | `ps l` (physical-storage) | 0 | 0 | lists `/dev/sdb 17179869184` on 3 nodes | **empty table** | **MISSING_FEATURE** — BS does not implement `GET /v1/physical-storage`. |
| 17 | `err l` | 0 | 0 | shows 9 error reports w/ UUIDs (`6A04EACB-49809-000000`...) | empty | **MISSING_FEATURE** — BS error-reports endpoint returns empty list. CSI doesn't need it, but operators do. |
| 18 | `controller version` | 0 | 0 | `1.32.3` real git hash | `1.33.2 git=blockstor` | **PARITY** (intentional version stamping; only flag the literal `git_hash="blockstor"` if a downstream tool grep's a hex hash). |
| 30 | `sp c X dup --provider-kind LVM_THIN` (missing pool-name) | 2 | 2 | identical argparse error | identical argparse error | **CLI_BUG** (PARITY at controller level) |
| 31 | `rd c "" --resource-group parity-rg` | 2 | 2 | identical argparse error | identical argparse error | **CLI_BUG** (PARITY at controller level) |
| 32 | `r c parity-rd --auto-place 99` | 10 | 10 | upstream prints **full diagnostic**: "Not enough nodes fulfilling the following auto-place criteria: * has a deployed storage pool named [pool] * the storage pools have to have at least '16384' free space * ... Auto-place configuration details: Replica count: 99" | BS prints: `ERROR: not enough candidate storage pools for the requested placement` | **ERROR_TEXT** — exit code matches but BS message is terse, lacks the structured criteria list and `Replica count` block. Operators piping `linstor` get less info. |
| 33 | `s d parity-rd nonexistent-snap` (CSI idempotence) | 0 | 0 | `WARNING: Snapshot definition nonexistent-snap of resource parity-rd not found.` | `SUCCESS: snapshot already absent: nonexistent-snap` | **ERROR_TEXT** — both are 200 + idempotent (good!), but upstream emits a `WARNING` envelope (RC mask 0x4000_0000) and BS emits `SUCCESS` (RC mask 0x0). CSI doesn't care; tools that parse `ret_code` may. |
| 40 | `n c parity-fake 10.99.99.99 --node-type Satellite` | 0 | 0 | `SUCCESS` + UUID + WARNING "No active connection to satellite 'parity-fake'" | `SUCCESS: node created: parity-fake` (single line, no UUID, no warning) | **WIRE_SHAPE** — BS API response collapses to one apiCallRc; upstream returns 2 (info + warning). UUID never echoed. |
| 41 | `rg modify --place-count 3 parity-rg` | 0 | 0 | re-runs autoplace for matching RDs → "Resource 'parity-rd' successfully autoplaced on 1 nodes" | `SUCCESS: resource group modified: parity-rg` — RDs **not re-autoplaced** | **MISSING_FEATURE** — BS does not re-run autoplace on RG modify. Upstream does (linstor-server `CtrlRscGrpApiCallHandler.modify` triggers RescheduleAutoPlace). |
| 42 | `r d parity-rd dev-kvaps-worker-3` (CSI idempotence; here the *resource exists*, but I happened to swap the arg order via the CLI — see below) | 0 | 10 | `WARNING: Node: parity-rd, Resource: dev-kvaps-worker-3 not found.` exit 0 | `ERROR: resource "dev-kvaps-worker-3" on node "parity-rd": object not found` exit 10 | **ERROR_TEXT** — when deleting a non-existent resource/node pair, upstream returns **200 + WARNING** (idempotent); BS returns **500 + ERROR**. CSI relies on the upstream idempotent contract for `DeleteVolume`. ⚠ NB: in this row both calls hit the "not found" path because the CLI passes args as `<resource> <node>` and the harness accidentally inverted them — the same inversion hit both sides, so the **delta is real**: upstream → 200, BS → 500. |
| 43 | `r l --resources parity-rd` (post-delete) | 0 | 0 | UP shows TieBreaker upgraded to `Diskless` (its 2nd replica autoplaced after rg-modify in #41) | BS still shows 3-node TieBreaker layout (no autoplace happened in #41) | **MISSING_FEATURE** (downstream of #41). |

**Totals**: 27 commands compared (excluding the cleanup pass).
- **PARITY** rows: **5** (#9, #18, #30, #31, #15 — last three are CLI-side argparse).
- **non-PARITY** rows: **22**, broken down as:
  - WIRE_SHAPE: 9 (#1, #2, #3, #4, #6, #8, #10, #11, #40)
  - WIRE_SHAPE+BEHAVIOR overlap: 1 (#12/#13 counted once)
  - ERROR_TEXT: 5 (#5, #14, #32, #33, #42)
  - MISSING_FEATURE: 4 (#16, #17, #41, #43)
  - BLOCKSTOR_SUPERSET (extra info): 1 (#12 device_path)

## 3. Concrete fix list

Each item is a downstream-friendly hook into the controller code path. References below cite well-known LINSTOR REST endpoints / DTO names (see upstream `com.linbit.linstor.api.pojo.*` and `com.linbit.linstor.api.rest.v1.serializer.JsonGenPojo`).

| ID | Where | Symptom | What to change |
|----|-------|---------|----------------|
| F1 | `GET /v1/nodes` → DTO `Node.NetInterface` | Addresses empty in `linstor n l`. | Serialise `net_interfaces[]` with `address`, `satellite_port`, `satellite_encryption_type`, `uuid`. The cli prints `address:port (TYPE)`. |
| F2 | `GET /v1/nodes` → DTO `Node` | Missing top-level `uuid`, `props.NodeUname`, `props.CurStltConnName`, `resource_layers`, `storage_providers`, `unsupported_layers/providers`. | Populate from satellite NodeStatus capability message — most are trivially derivable. The diskless-provider list is what controls `linstor advise` etc. |
| F3 | `GET /v1/view/storage-pools` → DTO `StorPool` | Missing `free_capacity`, `total_capacity`, `static_traits.SupportsSnapshots`, `supports_snapshots`, `external_locking`. `SharedName` always empty. `DfltDisklessStorPool` not auto-created per Satellite. | (a) Carry over `freeCapacity_kib`/`totalCapacity_kib` from the spaceTracking subsystem. (b) Set `static_traits.SupportsSnapshots=true` for FILE_THIN, LVM_THIN, ZFS_THIN. (c) Auto-create per-Satellite `DfltDisklessStorPool` (driver DISKLESS) on node register. (d) Emit `SharedName = "<node>;<poolname>"` for non-shared pools. |
| F4 | `GET /v1/resource-groups` → DTO `RscGrp` | `select_filter.place_count` not nested in `select_filter`. | Wrap select-filter fields under `select_filter` object (replica_count, storage_pool_list, do_not_place_with, replicas_on_same/different, layer_stack, provider_list, disklessOnRemaining). |
| F5 | Bootstrap / migration | Default RG is `dfltrscgrp` lowercase with a friendly description string. | Rename to canonical `DfltRscGrp`. Drop the human description (upstream keeps it null). External tooling and CSI's StorageClass `linstor.csi.linbit.com/resourceGroup` greps the exact CamelCase. |
| F6 | `GET /v1/resource-definitions[?resource_definitions=]` | Filter list parameter ignored → returns all RDs. | Honour the `resource_definitions` query param in the LIST handler (semantics: case-insensitive name match, multi-value). Same fix needed for `vd l --resource-definitions`. |
| F7 | `GET /v1/resource-definitions` → DTO `RscDfn` | `layer_data[]` empty → CLI shows `Layers=`. | Serialise `layer_data[]` with `type=DRBD` + nested `data.{port, secret, transport_type, peer_slots, al_stripes, al_stripe_size_kib, down}`. Same for volume-definitions' DRBD layer (minor_number, volume_number). |
| F8 | `GET /v1/view/volume-definitions` (or whichever LINSTOR endpoint backs `linstor vd l`) | Empty rows. | Implement the endpoint; today the BS API returns `[]`. Required fields: `volume_number`, `size_kib`, `layer_data[].type=DRBD .data.minor_number`, `props`. |
| F9 | `GET /v1/view/resources` → per-volume entry | `state.storage_pool_name` not surfaced → `linstor v l` prints `None`. `minor_number` missing. `device_path` is correctly populated (good — keep). | Add `volumes[].storage_pool_name` (top-level) and `volumes[].layer_data_list[].data.drbd_volume_definition.minor_number` (drbd layer). Also `allocated_size_kib`, `usable_size_kib` per volume. |
| F10 | `GET /v1/view/resources` → DTO `Resource`/`ResourceLayer` | Missing entire `layer_object.drbd.{node_id, peer_slots, al_size, al_stripes, may_promote, promotion_score}` and `layer_object.children[].storage.storage_volumes[]`. | These drive `drbdtop`-style observability. Implement the recursive layer-tree serialisation (DRBD child = STORAGE node). |
| F11 | `GET /v1/physical-storage` | Empty table. | Implement: aggregate `lsblk -J --filter='not in use'` results across satellites and return `[{size, rotational, nodes:[{node, devices:[…]}]}]`. CSI provisioner doesn't need it but `linstor ps l` is the standard tool for pool seeding. |
| F12 | `GET /v1/error-reports` | Empty list. | At minimum return the controller-side error reports (already collected on disk under `/var/log/linstor-controller/`). Optionally aggregate satellite reports. |
| F13 | `POST /v1/resource-definitions/{rd}/autoplace` error path | When candidates are insufficient, BS returns short string. | Match upstream error envelope: `cause`/`correction`/`details` with bulleted criteria list. The data is already collected by AutoPlacer — just propagate it through the apiCallRc instead of stringifying. |
| F14 | `DELETE /v1/snapshot-definitions/{rd}/{snap}` (CSI idempotence) | BS returns `SUCCESS`/`snapshot already absent`; upstream returns `WARNING`/`not found`. | Functional behaviour identical (both 200, both idempotent) but the `ret_code` mask must be **WARNING** (`0x4000_0000`) and message shape `Snapshot definition <snap> of resource <rd> not found.` to match upstream. Same convention applies for any "delete nonexistent" path. |
| F15 | `DELETE /v1/resource-definitions/{rd}/resources/{node}` | BS returns **HTTP 500 + ERROR** for nonexistent; upstream returns **HTTP 200 + WARNING**. | Treat NotFound as idempotent success with WARNING envelope. **This is a CSI-blocking delta** (`DeleteVolume` MUST be idempotent per spec). |
| F16 | `POST /v1/resource-groups/{rg}` (modify) | BS modifies the RG but does not re-run autoplace on dependent RDs. | After `place_count` (or storage-pool/topology selectors) changes, enumerate RDs in this RG and enqueue autoplace adjustments (add/remove replicas to match new count). Upstream calls this from `CtrlRscGrpApiCallHandler.modifyResourceGroup` via `CtrlAutoRebalanceTask`. |
| F17 | `POST /v1/nodes` create response | Single apiCallRc, no UUID, no "not connected" warning. | Return apiCallRc array: `[SUCCESS{message:"New node 'X' registered.", details:"Node 'X' UUID is: <uuid>"}, WARNING{message:"No active connection to satellite 'X'", ...}]`. The UUID is mandatory — operators script against it. |
| F18 | Snapshot state reporter | BS reports `Incomplete` when satellite failed; upstream reports `Failed`. | Wire the satellite's snapshot-create failure (DeviceLayer / SnapshotShipping result) into the snapshot's `state.flags` so `s l` shows `Failed` rather than `Incomplete`. CSI `CreateSnapshot` polls this to surface failures to the workload. |
| F19 | TieBreaker layer data | BS reports `Layers=DRBD` for a TieBreaker; upstream reports `Layers=DRBD,STORAGE`. | Even a TieBreaker has a backing diskless storage layer in the layer tree. Make sure the diskless-storage child layer is serialised. |
| F20 | `Snapshot` DTO | Missing `flags`, `snapshots[].snapshot_volumes[]`, `volume_definitions[].volume_definition_props`, `snapshot_definition_props`, `resource_definition_props`. | These are needed for backup/restore tooling (`linstor backup`, schedules). Less urgent than F1-F15. |

## 4. Open questions

**Q1.** On the piraeus stand, satellite pods consistently fail to write `/var/lib/linstor.d/.backup/<rd>.res` (StorageException). The directory exists, owned root:root, mode 0755 — but the write fails. This corrupts the upstream side of any **write-path** comparison (RD-delete on UP doesn't complete; snapshot enters `Failed` state). I worked around it by comparing read-paths after the write succeeded once, and by treating snapshot `Failed`-vs-`Incomplete` as the contract delta on its own. **Is this a SELinux-on-Oracle-Linux container issue, a known piraeus bug, or specific to this stand?** It affects whether F18 is "BS bug" or "BS conservative choice".

**Q2.** F15 (idempotent DELETE) is CSI-blocking per spec, but I didn't verify against an actual `csi-sanity` run on this stand. Worth a follow-up: do BS satellites also return 500 for `DELETE` of a non-existent volume on the *gRPC* path, or only on REST? (Memory notes say csi-sanity baseline is 53/74 — some of the 21 failures may already cover this.)

**Q3.** F5: renaming `dfltrscgrp` → `DfltRscGrp` is a breaking change for any deployment that already provisioned RDs without specifying `--resource-group`. We'd need a one-shot migration that creates the CamelCase RG and re-points dangling RDs. Worth raising on the next blockstor cluster-bring-up checklist.

**Q4.** F11 (physical-storage list) duplicates information that on blockstor lives in Kubernetes node objects (BlockDevice CRs). Question: do we want REST to surface them, or should we instruct operators to use `kubectl get blockdevices.*` instead? The contract delta exists either way, but the fix could be "implement endpoint" or "document the BS-native alternative".

**Q5.** The CLI argument order for `r d` (`linstor r d <RD> <NODE>` — RD first, node second) — the upstream client docs confusingly put it as `<NODE> <RD>` in some examples; I hit this in row #42. Worth a `--help` text audit, but that's an upstream linstor-client issue, not BS.
