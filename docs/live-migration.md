# Live migration and dual Primary

A VM live migration needs its disk writable on two nodes for a short while: the source keeps running while the target starts. DRBD allows that only when the connection between the two nodes has `allow-two-primaries` set, and only over protocol C. Since LINSTOR 1.35 the controller manages that setting itself, and linstor-csi from v1.13 relies on it. blockstor serves the same calls with the same props, so a driver sees no difference between the two controllers.

## The calls

`POST /v1/resource-definitions/{rd}/resources/{node}/make-available` with `"auto_manage_dual_primary": true` makes the resource available on `node`, as without the flag, and then prepares a live migration to `node` from the node the resource is in use on:

- the connection between the two nodes gets `DrbdOptions/Net/allow-two-primaries=yes`;
- when the protocol in effect on that connection is not C (set on the connection, on either replica, on the definition, its resource group or the controller), the connection also gets `DrbdOptions/Net/protocol=C`, for the duration of the migration only;
- when both ends are diskless, both settings go on the resource definition instead, because DRBD connects no two diskless peers.

When the resource is in use nowhere, or already in use on `node`, there is no migration and the call is a plain make-available: a driver that cannot tell a migration attach from a plain one sets the flag on every attach. A replica on a node marked `OFFLINE` counts as in use nowhere, whatever its satellite reported last, as LINSTOR forgets the state of a satellite it lost: a VM restarted after its node died gets a plain attach. When the resource is in use on two nodes, the call is accepted only as a repeat of the migration already prepared between them; any other combination is refused with `FAIL_EXISTS_LIVE_MIGRATE`, and so is a migration to a second target while one is prepared, including two attaches that race. A layer stack without DRBD is refused with `FAIL_INVLD_LAYER_STACK`. Between two diskless ends, a replica that sets a protocol other than C on itself is refused with `FAIL_INVLD_PROP`: the satellite renders a replica's own protocol over the definition's, so the override could not take effect.

The call answers once the satellites of both ends report that they applied the props, as LINSTOR answers once its satellites did: the driver starts the migration on the answer, and the target cannot become Primary before `allow-two-primaries` is in effect on both ends. Each satellite copies the token make-available writes onto the definition (`blockstor.org/live-migrate-token`) onto its Resource (`blockstor.org/live-migrate-applied`) after an apply pass that succeeded. Nodes that are not `ONLINE` are not waited for. The wait is bounded at 10 seconds, below the external-attacher's default timeout; when it runs out the call still succeeds, with a warning naming the nodes that have not reported, so a satellite older than the report does not fail every migration during an upgrade.

`POST /v1/resource-definitions/{rd}/resources/{node}/unmake-available` reverts a make-available on `node`, the migration source:

- while the replica on `node` is in use the call is refused with `FAIL_IN_USE` and nothing changes, so the driver retries once the node let go; a node marked `OFFLINE` is not in use;
- `allow-two-primaries` comes off the definition, off every connection of `node` and off the connection of the prepared migration, and the protocol goes back to what it was;
- the replica on `node` is deleted when it is a diskless attachment, unless it is the tie breaking voter of an even number of diskful replicas: then it turns into their tiebreaker in place, as LINSTOR's delete with `keepTiebreaker` does. A diskful replica and a tiebreaker stay.

An unknown resource definition (answered with `WARN_NOT_FOUND`) and a node without a replica are successful no-ops, so a driver can always issue the call after a migration.

While a migration is prepared from or to a node, the controller does not turn an in-use diskless replica there into a diskful one, which it otherwise does on a node with a storage pool: that would attach a disk and resync it while two nodes write.

## The props

The connection props live on the resource definition, one prop per setting, under `Cozystack/ResourceConnectionProps/<node-a>/<node-b>/<key>` with the two nodes in sorted order: blockstor has no resource-connection object of its own. `GET /v1/resource-definitions/{rd}/resource-connections` (`linstor resource-connection list`) lists them as connection props, the way LINSTOR shows its own, and the satellite renders the `DrbdOptions/Net/*` ones as the `net` section of that one connection in the `.res` file. `PUT /v1/resource-definitions/{rd}/resource-connections/{a}/{b}` (`linstor resource-connection set-property`, `linstor resource-connection drbd-peer-options`) changes them. Deleting a replica deletes the connection props of its node on every path (REST, CLI, the lost-node cascade, the tiebreaker reapers), as LINSTOR deletes the connections of a deleted resource.

An `allow-two-primaries` left on a connection, for instance by a migration whose source was never detached, is taken off by an `unmake-available` on either end once nothing is in use there, or by hand with `linstor resource-connection set-property <node-a> <node-b> <rd> DrbdOptions/Net/allow-two-primaries` (no value deletes the prop). The marks go with the next unmake-available of the volume on any node.

What was changed is recorded on the definition with LINSTOR's marks, so the revert puts back exactly that:

| Prop | Value |
|---|---|
| `Internal/Drbd/LiveMigrate/SourceNode` | the migration source |
| `Internal/Drbd/LiveMigrate/TargetNode` | the migration target |
| `Internal/Drbd/LiveMigrate/ProtocolSetOn` | `RSC_CONN` or `RSC_DFN`, only when the protocol was changed |
| `Internal/Drbd/LiveMigrate/PrevProtocol` | the protocol set explicitly at that scope before, if any |

A clone, a snapshot and a restore copy the definition's props, but not a migration: a copy of a volume caught mid migration runs on no VM, and nobody would unmake-available it. The connection props stay behind, the copy's replicas are its own, and what the marks say the migration wrote on the definition is reverted on the copy as unmake-available would revert it. LINSTOR copies the marks and a definition-level `allow-two-primaries` along, which leaves the copy able to run two Primaries until its first detach and refuses its first migration. An `allow-two-primaries` without marks was set by a client and is copied as it is; linstor-csi from v1.13 deletes it from a new volume's definition itself.

## linstor-csi v1.11

linstor-csi up to v1.12 manages dual Primary client-side: it sets `allow-two-primaries` on the resource definition before the second attach and deletes it on the detach that leaves one user. That path is unchanged and still works against blockstor. An unmake-available on such a definition also clears that definition-level `allow-two-primaries`.

## What linstor-csi v1.13 needs beyond this

linstor-csi v1.13 refuses to start against a controller whose `rest_api_version` (`GET /v1/controller/version`) is below 1.29 in the 1.x series. blockstor advertises 1.27.0 and keeps doing so until the rest of what 1.29 promises is served, because the version also opens python-linstor's client-side `_require_version` gates. Still missing:

- `DELETE /v1/resource-definitions/{rd}/resources` (truncate) with `delete_empty_resource_definition`, which DeleteVolume uses;
- `delete_empty_resource_definition` on `DELETE /v1/resource-definitions/{rd}/snapshots/{snap}`, which DeleteSnapshot uses; blockstor ignores it today and leaves an empty definition behind;
- the REST 1.28 additions the python client gates on (`/v1/controller/database/export`, the `/v1/controller/auth` API, `drbd_client` on resource create and modify, external-file status);
- live migration of RWX block volumes on shared storage without DRBD (linstor-csi v1.13.0), which LINSTOR serves by activating the resource on both nodes; blockstor has no shared activation and refuses `auto_manage_dual_primary` without DRBD.
