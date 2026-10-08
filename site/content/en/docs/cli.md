---
title: "Using the CLI"
linkTitle: "Using the CLI"
weight: 60
description: "Manage nodes, pools, volumes and snapshots with the blockstor command."
---

`blockstor` is the command-line tool for blockstor. It reads your kubeconfig and works on the blockstor custom resources directly, so it needs no port-forward and no API certificate: if `kubectl` can reach the cluster, so can `blockstor`.

The commands follow the `linstor` client's noun-verb grammar, with the same short aliases, so runbooks written for LINSTOR mostly carry over by changing the command name. Tools that speak the LINSTOR REST API, such as `linstor-csi`, keep working against the blockstor API server. The examples on this page use `blockstor`.

## Install

```sh
go install github.com/cozystack/blockstor/cmd/blockstor@v<VERSION>
```

The tool uses the same kubeconfig as `kubectl`: the `KUBECONFIG` variable, then `~/.kube/config`, or the service account when it runs inside a pod.

## Getting help

```sh
blockstor --help                    # every object and action
blockstor resource create --help    # usage of one command
```

Every object and action has a short alias: `blockstor sp l` is `blockstor storage-pool list`, `blockstor rd c` is `blockstor resource-definition create`, `blockstor r l` is `blockstor resource list`.

Output options that work on list commands:

- `-m` or `--machine-readable` prints JSON.
- `--color=auto|always|never` controls colours. They are on only for a terminal, and `NO_COLOR` turns them off.

## Nodes and storage pools

```sh
blockstor node create worker-1 10.0.0.11
blockstor node list

blockstor physical-storage list
blockstor physical-storage create-device-pool zfs worker-1 /dev/sdb --pool-name data
blockstor storage-pool create lvmthin worker-2 data my_vg/my_thinpool
blockstor storage-pool list
```

Take a node out of service. `evacuate` moves its replicas to other nodes; `restore` brings it back:

```sh
blockstor node evacuate worker-2
blockstor node restore worker-2
```

## Resource groups

A resource group holds the policy, and every volume spawned from it gets the same settings:

```sh
blockstor resource-group create replicated --place-count 2 --storage-pool data
blockstor volume-group create replicated
blockstor resource-group list
```

Change the policy later with `blockstor resource-group modify`, for example `--place-count 3`, then apply it to the volumes that already exist with `blockstor resource-group adjust`.

## Volumes

Spawn a volume from a group in one step. This creates the definition, its volume and the replicas:

```sh
blockstor resource-group spawn replicated my-volume 10G
```

Or step by step, without a group policy:

```sh
blockstor resource-definition create my-volume
blockstor volume-definition create my-volume 10G
blockstor resource create my-volume --auto-place 2 --storage-pool data
```

To put replicas on named nodes instead of letting blockstor choose:

```sh
blockstor resource create worker-1 worker-2 my-volume --storage-pool data
```

Inspect the state:

```sh
blockstor resource-definition list
blockstor resource list            # replicas, nodes and DRBD state
blockstor resource list --faulty   # only replicas that are not healthy
blockstor volume list              # sizes and device paths
```

A healthy replicated volume shows `UpToDate` on every replica with data, and a `TieBreaker` when there is an even number of them.

Grow a volume online. Shrinking is refused unless you add `--force`, because it cuts off whatever data lives at the end of the device:

```sh
blockstor volume-definition set-size my-volume 0 20G
```

Turn a diskless replica into one with data, or the other way round:

```sh
blockstor resource toggle-disk worker-3 my-volume --storage-pool data
blockstor resource toggle-disk worker-3 my-volume --diskless
```

Delete a volume with all its replicas:

```sh
blockstor resource-definition delete my-volume
```

## Snapshots

```sh
blockstor snapshot create my-volume before-upgrade
blockstor snapshot list
```

Restore a snapshot as a new volume. This creates the new definition, fills its volumes from the snapshot and places the replicas. The original volume is not touched:

```sh
blockstor snapshot resource restore \
    --from-resource my-volume --from-snapshot before-upgrade --to-resource my-volume-restored
```

`blockstor snapshot resource-definition restore` is another spelling of the same command. To copy a live volume without taking a snapshot yourself, use `blockstor resource-definition clone my-volume my-volume-copy`.

In-place rollback is not available on purpose: it destroys every snapshot newer than the one you roll back to. Restore into a new volume instead.

```sh
blockstor snapshot delete my-volume before-upgrade
```

## Properties and DRBD options

Every object has `set-property`, `list-properties` and `delete-property`, and groups, definitions and the controller have `drbd-options`. Settings resolve from the controller to the group to the definition to the replica, and the narrowest one wins. See [Resources](../resources/#how-settings-are-inherited).

```sh
blockstor resource-group set-property replicated DrbdOptions/Resource/on-no-quorum suspend-io
blockstor resource-definition list-properties my-volume
```

## Encryption

```sh
blockstor encryption create-passphrase '<passphrase>'
```

See [Layer stack](../layer-stack/#encryption) before you use it.
