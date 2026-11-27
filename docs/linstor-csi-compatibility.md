# linstor-csi compatibility

blockstor serves the LINSTOR REST API that linstor-csi drives. This page says which driver versions work against it, which REST API version it advertises and why, and how every call linstor-csi v1.13 makes is answered. The live migration calls (`make-available` with `auto_manage_dual_primary`, `unmake-available`) have their own page, [live-migration.md](live-migration.md).

## Versions

| linstor-csi | Works | Notes |
|---|---|---|
| v1.11.x (cozystack today) | yes | Manages dual Primary client-side with `allow-two-primaries` on the definition, deletes replicas one by one. Checks no REST API version. |
| v1.12.x | expected | The v1.11 calls minus a few, plus `keep_tiebreaker` on replica delete and the consistency-group calls behind `--enable-consistency-groups` (below), which blockstor serves. The consistency-group calls are replayed with v1.13's; nothing drives a v1.12 binary. |
| v1.13.x and master | yes | Refuses to start below REST API 1.29. Uses truncate, `delete_empty_resource_definition`, make-available with `auto_manage_dual_primary` and unmake-available. |

`GET /v1/controller/version` answers `rest_api_version` 1.29.0 and `version` 1.35.0. The REST API version is the highest one whose additions are all either served or refused with an explicit 501 because they mean nothing here:

| REST API | LINSTOR | Addition | blockstor |
|---|---|---|---|
| 1.28.0 | 1.34 | regular expressions in the name filters of list endpoints, `props` filters with regular expressions, `props` on a node's storage pools and a resource's volumes | served |
| 1.28.0 | 1.34 | `PUT .../resources/{node}/toggle-disk` with `operation`, `storage_pool`, `migrate_from` in the body (python-linstor sends every `r td` this way from 1.28) | served; `INTO_DRBD_CLIENT` and `layer_list` refused with 501 |
| 1.28.0 | 1.34 | `drbd_client` on resource create, `drbd_client` and `drbd_tiebreaker` on resource modify | `true` refused with 501, `drbd_client=false` accepted: blockstor renders no diskless replica without a quorum vote |
| 1.28.0 | 1.34 | `POST /v1/controller/database/export` | 501: the state lives in Kubernetes objects |
| 1.28.0 | 1.34 | `/v1/controller/auth/...` API tokens | 501: REST clients authenticate with TLS client certificates |
| 1.28.0 | 1.34 | `POST /v1/nodes/exec/drbd-reactorctl/{command}` | 501: nothing runs drbd-reactor |
| 1.28.0 | 1.34 | `GET /v1/files/{file}/status/{node}`, `alt_suffixes` on external files | no route of its own: `GET /v1/files/{path...}` catches it and answers a plain 404, as for every file, since blockstor deploys no external files |
| 1.28.0 | 1.34 | `platform` and `os_variant` on nodes, `corrupted_key` on LUKS volumes | not sent; all three are optional |
| 1.29.0 | 1.35.0-rc.1 | `DELETE /v1/resource-definitions/{rd}/resources` (truncate) with `delete_empty_resource_definition` | served |
| 1.29.0 | 1.35.0-rc.1 | `delete_empty_resource_definition` on `DELETE .../snapshots/{snap}` | served |
| none | 1.35.0 | `auto_manage_dual_primary` on make-available, `unmake-available`: in the 1.35.0 spec (info.version 1.29.1) but in no CHANGELOG-OPENAPI.md entry | served, see [live-migration.md](live-migration.md) |
| 1.29.0 | 1.35.0-rc.1 | single-object GETs and `PUT /v1/nodes/{node}/evict` written into the spec | served (they predate 1.29 in blockstor) |
| 1.29.1 | 1.35.0 | `volume_sizes` on clone | refused with 501; blockstor could serve it, so the version stays at 1.29.0 and python-linstor keeps `rd clone --volume-size` gated |
| 1.30.0 | unreleased | paginated `GET /v1/view/error-reports`, `host_name`, CBT snapshots | not served, not advertised |

python-linstor opens a feature on the advertised version alone, so a version is only raised once everything it opens is answered.

## The calls linstor-csi v1.13 makes

Every golinstor call in linstor-csi master (`pkg/client`, `pkg/linstor/highlevelclient`, `pkg/topology/scheduler`, `cmd/linstor-csi`), through the golinstor it pins (v0.65.4). "Fixed" marks what this round changed; it affected every driver version unless it says otherwise.

| Driver call | REST request | blockstor |
|---|---|---|
| `Controller.GetVersion` (start-up check) | `GET /v1/controller/version` | 1.29.0, passes the `>= 1.29` check |
| `Nodes.GetAll` with `Node` (accessible topologies) | `GET /v1/nodes?nodes=` | fixed: the filter was ignored and every node was answered, so a volume looked accessible everywhere |
| `Nodes.GetAll` with `Prop` (nodes of a topology segment) | `GET /v1/nodes?props=key=value` | fixed: same, a segment spanned the cluster |
| `Nodes.Get`, `Nodes.GetStoragePools` with `cached` | `GET /v1/nodes/{node}`, `GET /v1/nodes/{node}/storage-pools?cached=true` | served |
| `Nodes.GetStoragePoolView` with `cached`, `storage_pools` | `GET /v1/view/storage-pools` | fixed: `free_space_mgr_name` of a local pool was `node:pool`, LINSTOR's is `node;pool`; v1.13 took every pool for shared storage and accepted RWX block volumes without DRBD on local pools |
| `ResourceGroups.Get`, `Create`, `Modify`, `Delete` | `/v1/resource-groups[/{rg}]` | fixed: a duplicate create carried no `FAIL_EXISTS_RSC_GRP`, and the code was 510 instead of 522; a delete with definitions left answers `FAIL_EXISTS_RSC_DFN` |
| `ResourceDefinitions.Get`, `Create`, `Modify`, `GetAll` with `with_volume_definitions` | `/v1/resource-definitions[/{rd}]` | fixed: a duplicate create carried no `FAIL_EXISTS_RSC_DFN` |
| `ResourceDefinitions.GetVolumeDefinition(s)`, `CreateVolumeDefinition`, `ModifyVolumeDefinition` (size, `delete_props`), `DeleteVolumeDefinition` | `/v1/resource-definitions/{rd}/volume-definitions[/{n}]` | fixed: `FAIL_EXISTS_VLM_DFN` was 502, which golinstor reads as `FAIL_EXISTS_RSC` |
| `ResourceDefinitions.Clone` with `layer_list`, `resource_group`, `override_props`, `delete_props` | `POST /v1/resource-definitions/{rd}/clone` | served, the props applied right after the source's are copied, as master relies on when it sends every requested prop there; `volume_sizes` refused with 501 |
| `ResourceDefinitions.ModifyVolumeDefinition` with `delete_props` only (master, stale file system props) | `PUT .../volume-definitions/{n}` | served, the size left alone; deleting a key that is not set does nothing |
| `ResourceDefinitions.CreateVolumeDefinition` without a volume number, `GetVolumeDefinitions`, `ModifyVolumeDefinition` with override props only, `DeleteVolumeDefinition` of one member (v1.12+ consistency groups) | `/v1/resource-definitions/{rd}/volume-definitions[/{n}]` on a deployed resource | served: the next free number, the size kept on the claim, the shared resource and its other volumes kept on the member delete |
| `ResourceDefinitions.CloneStatus` | `GET /v1/resource-definitions/{rd}/clone/{target}` | served, 404 before the clone |
| `ResourceDefinitions.Truncate` with `delete_empty_resource_definition` (DeleteVolume) | `DELETE /v1/resource-definitions/{rd}/resources` | new |
| `Resources.GetAll`, `Resources.Create`, `Resources.Autoplace` | `/v1/resource-definitions/{rd}/resources[/{node}]`, `.../autoplace` | served |
| `Resources.GetResourceView` with `nodes`, `storage_pools` (ReservedCapacity) | `GET /v1/view/resources` | fixed: `storage_pools` was ignored and the STORAGE layer of a volume carried no data, so the capacity a pool had promised read as 0 and a StorageClass with `overProvision` offered its full size again for each volume. A diskful replica its satellite has not reported yet now shows its volumes in its own pool too, so a burst of CreateVolume does not overcommit the pool |
| `Resources.GetResourceView` with `resources`, and without filters (ControllerGetVolumeHealth, ControllerListVolumeHealth, the health ListVolumes carries) | `GET /v1/view/resources` | fixed: the DRBD layer carried no `promotion_score`, which the driver reads as 0, an unhealthy replica, so every volume was reported as having no healthy replica. The layer now carries the kernel's score from `drbdsetup events2` once the satellite has reported it. This affected v1.11 through VolumeCondition as well |
| `Resources.GetVolume` (Attach, right after make-available) | `GET .../resources/{node}/volumes/{n}` | fixed: 404 until the satellite reported the new replica's volume, which failed the first attach; the read now waits for the volume and its device path, up to 10 seconds. The satellite never reported a device path for a diskless replica at all, so attaching on a node without a replica failed on every retry with "could not determine device path"; it now reports `/dev/drbd<minor>` for a diskless replica as for a diskful one |
| `Resources.MakeAvailable` with `auto_manage_dual_primary` (Attach) | `POST .../resources/{node}/make-available` | served |
| `Resources.UnmakeAvailable` (Detach, `FAIL_IN_USE` retried) | `POST .../resources/{node}/unmake-available` | served |
| `Resources.CreateSnapshots` | `POST /v1/actions/snapshot/multi` | served |
| `Resources.GetSnapshotView`, `GetSnapshots` with `offset`, `limit`, `GetSnapshot` | `/v1/view/snapshots`, `.../snapshots[/{snap}]` | served, with `volume_definitions[].volume_definition_props` |
| `Resources.DeleteSnapshot` with `delete_empty_resource_definition` | `DELETE .../snapshots/{snap}` | new; `nodes` is refused with 501 instead of widened to every node |
| `Resources.RestoreVolumeDefinitionSnapshot` | `POST .../snapshot-restore-volume-definition/{snap}` | served, `FAIL_EXISTS_VLM_DFN` when the volume definitions exist |
| `Resources.RestoreSnapshot` | `POST .../snapshot-restore-resource/{snap}` | served; a repeated restore answers success where LINSTOR answers `FAIL_EXISTS_RSC`, and the driver takes both as done |
| `RESTORE_FROM_SNAPSHOT` on resources (waitSnapshotResourcesRestored) | resource `flags` | never set: the driver does not wait, as v1.11 never did |
| `KeyValueStore.Get`, `CreateOrModify` | `/v1/key-value-store/{instance}` | served |
| `Remote.*`, `Backup.*` (snapshot classes of type S3 or LINSTOR) | `/v1/remotes...` | listing served, shipping and backups refused with 501 |

The failure codes the driver tests with `errors.Is` are LINSTOR's own now: `FAIL_EXISTS_RSC_GRP`, `FAIL_EXISTS_RSC_DFN`, `FAIL_EXISTS_VLM_DFN`, `FAIL_EXISTS_RSC`, `FAIL_IN_USE`. golinstor matches a code as a bit mask, so blockstor's own numbers for the duplicate and not-found families read as other failures; every failure code is now compared with golinstor's in `pkg/rest/return_codes_linstor_test.go`.

## Behaviour behind the calls

Truncate deletes every replica of the definition, the diskless ones first, and keeps the definition and its snapshots. An unknown definition is answered 200 with `WARN_NOT_FOUND`, and a replica in use refuses the call with `FAIL_IN_USE` without deleting anything; a replica on an `OFFLINE` node is in use nowhere, as for unmake-available.

`delete_empty_resource_definition`, on truncate and on snapshot delete, then deletes the definition when no replica and no snapshot is left. As in LINSTOR it also runs after a snapshot that was already gone, so a retried DeleteSnapshot finishes what an earlier attempt left. LINSTOR checks and deletes under one lock. blockstor marks the definition `DELETE` first, which a snapshot create checks after its own write and rolls back on, and then checks again past the cache: a snapshot that races the delete either keeps the definition or undoes itself.

The scheduled rebalance leaves alone a definition with no replica that holds data, and one being deleted, as LINSTOR's BalanceResourcesTask does. Otherwise it would bring back a volume that was deleted while a snapshot kept its definition. An explicit `rg modify` or `rg adjust` still places on every definition that is not being deleted, as LINSTOR's does, so it can bring back such a volume too; linstor-csi sends one whenever a StorageClass's placement differs from its group's, for example two StorageClasses that share a group name with different placement counts.

## Not served

- RWX block volumes without DRBD on shared storage (linstor-csi v1.13.0). LINSTOR activates such a resource on both nodes; blockstor has no shared activation. A pool without `shared_space` reads as local to the driver, so it refuses such a volume at CreateVolume with "ReadWriteMany volumes require a DRBD layer or shared storage". A pool registered with `shared_space` reads as shared, as it does on LINSTOR, so CreateVolume succeeds there and every attach then fails with `FAIL_INVLD_LAYER_STACK`; see row 113 of `cli-parity-known-deltas.md`.
- Clone `volume_sizes` (REST 1.29.1), see above.
- Backups and shipping to remotes.

## How it is tested

`tests/integration/linstor_csi_v113_test.go` replays the controller-side calls of linstor-csi v1.13, in its order and with its arguments, through golinstor v0.65.4, against envtest with the real reconcilers and REST server: the start-up version check, CreateVolume and its lost create races, accessible topologies, the reserved capacity, attach and detach of an RWX block volume with a refused detach while in use, the volume health of a healthy, a degraded and a recovered volume, a consistency-group member's create, claim and delete on a deployed resource, expand, snapshot, restore and clone with their replays, the post-v1.13.2 clone props and delete-only volume definition modify, DeleteSnapshot of a live volume, and DeleteVolume with and without a snapshot keeping the definition. The mock satellite reports a new replica's volume and its device path in two steps, as the real one does, and reports a device path for diskless replicas, which the real satellite now also does (`pkg/satellite` covers that with a unit test). Nothing here has run against a real linstor-csi binary or on a cluster. The driver itself is not imported, since it brings the CSI and Kubernetes client stacks along; every request it makes goes through these golinstor calls. The linstor-csi v1.11 flows stay covered by `tests/integration/group_j_test.go`.
