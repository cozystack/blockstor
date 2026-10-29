---
title: "Layer stack"
linkTitle: "Layer stack"
weight: 50
description: "Combine replication, encryption and plain local storage."
---

Each volume is built from layers. The bottom layer allocates space on the node, and each layer above adds something on top of the one below. The topmost layer is the device the pod uses.

| Layer | What it does |
|---|---|
| `STORAGE` | Allocates the block device: an LVM or LVM-thin logical volume, a ZFS zvol, or a file. Every stack ends with it. |
| `LUKS` | Encrypts the device below it with `cryptsetup`. |
| `DRBD` | Replicates the device to the other nodes over the network. |

## Supported stacks

| Stack | Use it for |
|---|---|
| `DRBD`, `STORAGE` | Replicated volumes. This is the default. |
| `DRBD`, `LUKS`, `STORAGE` | Replicated and encrypted at rest. Each replica encrypts its own copy. |
| `LUKS`, `STORAGE` | Encrypted volume with a single replica and no DRBD. |
| `STORAGE` | Plain local volume with a single replica, for caches and scratch space. |

These four are the only orders blockstor accepts. Other orders, such as LUKS on top of DRBD, are refused when the definition is created.

## Choosing the stack

A definition takes its stack from its own `layerStack`, then from its resource group's `selectFilter.layerStack`. If neither is set, it gets `DRBD`, `STORAGE`.

Set it on a group, so every volume spawned from it gets the same stack:

```sh
blockstor resource-group create local --place-count 1 --storage-pool data --layer-list storage
blockstor volume-group create local
blockstor resource-group spawn local scratch-1 20G
```

Or on one definition, in YAML:

```yaml
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata:
  name: scratch-1
spec:
  layerStack: ["STORAGE"]
  volumeDefinitions:
    - volumeNumber: 0
      sizeKib: 20971520
```

Treat the stack as fixed once a volume has replicas. Changing `layerStack` later does not encrypt or decrypt data that is already there.

## Encryption

LUKS volumes are opened with the cluster passphrase, which lives in a Secret in `blockstor-system`. Create it once, before the first encrypted volume:

```sh
blockstor encryption create-passphrase '<passphrase>'
```

Keep a copy of the passphrase somewhere safe. Without it, encrypted volumes cannot be opened. Running the command again with the same passphrase is a no-op, and blockstor refuses to replace it with a different one, because that would lock every existing encrypted volume. A definition with `LUKS` in its stack and no passphrase available is refused rather than created unencrypted.

Then create volumes with LUKS in the stack:

```sh
blockstor resource-group create secure --place-count 2 --storage-pool data --layer-list drbd,luks,storage
blockstor volume-group create secure
blockstor resource-group spawn secure secret-volume 10G
```

Growing an encrypted volume resizes every layer in order: the storage below, then LUKS, then DRBD.

Limits today:

- All volumes of a definition share one key.
- Changing the passphrase of existing volumes is not supported. To re-key a volume, create a new one and move the data.
