---
title: "Compatibility with LINSTOR"
linkTitle: "Compatibility with LINSTOR"
weight: 80
description: "Where blockstor behaves differently from LINSTOR, and what is not there."
---

Blockstor implements the LINSTOR REST API, so `linstor-csi`, `piraeus-operator`, `ha-controller`, `golinstor` and the `linstor` client work against it. This page lists the differences an operator can notice. The full list, kept for the project's parity tests, is in [`docs/cli-parity-known-deltas.md`](https://github.com/cozystack/blockstor/blob/main/docs/cli-parity-known-deltas.md).

## Not implemented

| Feature | What happens |
|---|---|
| Backups, backup shipping, remotes such as S3 | The API answers `501 Not Implemented`. Use snapshots and restore inside the cluster. |
| Schedules | The API answers `501 Not Implemented`. |
| `advise` | Not available. |
| In-place snapshot rollback | Refused on purpose, because it destroys every newer snapshot. Restore the snapshot into a new volume instead. |
| Per-connection DRBD peer options (`resource-connection drbd-peer-options`) | Not available. |
| External DRBD metadata (`StorPoolNameDrbdMeta`) | Not supported yet. |
| Other providers: SPDK, NVMe-oF, OpenFlex, Exos | Not supported. |

## Behaves differently

| Area | LINSTOR | Blockstor |
|---|---|---|
| Active network interface | `linstor node interface modify --active` switches the satellite's connection to another interface. | Has no effect. The first interface of a node is always the active one; to make another interface active, put it first. A change of address reaches DRBD after the node's satellite pod restarts. |
| Default action on lost quorum | `on-no-quorum io-error` | `on-no-quorum suspend-io`: I/O blocks until quorum returns and then resumes, so a replica that was cut off does not get stuck after the network heals. |
| Shrinking a volume | Allowed for stacks that support it. | Refused unless you pass `--force`. |
| DRBD ports and minors | Ports from 7000, minors from 1000. | Ports from 20000 to 20999, minors from 20000. Blockstor can run on the same nodes as LINSTOR without collisions. Change the port range per node with the `DrbdOptions/TcpPortRange` property. |
| Spawning more replicas than there are nodes | Fails and places nothing. | Places as many replicas as it can, reports the shortfall, and adds the rest when more nodes join. |
| Turning a tiebreaker into a replica with data | Can skip the initial sync. | Runs a full sync, to avoid a DRBD split where both sides refuse to connect. |
| Removing a replica below quorum | Removes the quorum options from the definition and prints a note. | Sets `quorum off` explicitly, with the same effect on DRBD. |
| DRBD option values | Validated against the DRBD schema. | Not validated: a wrong value is stored and fails later on the node. Check values before you set them. |
| Autoplacer weights | `MaxFreeSpace` is 1, the other weights 0. | All four weights default to 1. |
| `controller version` | Reports a git commit hash. | Reports `git=blockstor`. Do not parse a commit hash from it. |
| `node info` | Shows OS, CPU and memory. | These fields are empty. |

## Using the `linstor` client

The `linstor` client works against blockstor's API server. Port-forward the plain HTTP port, which is reachable only from inside the pod, and point the client at it:

```sh
kubectl -n blockstor-system port-forward deploy/blockstor-apiserver 3370:3370
linstor --controllers http://localhost:3370 node list
```

For everyday work the [`blockstor` CLI](../cli/) is simpler: it needs no port-forward and no certificates, and it accepts the same commands.
