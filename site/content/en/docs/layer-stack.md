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

A definition's stack is decided once and then read only from the definition's own `spec.layerStack`. If neither the definition nor its resource group sets a stack, it is `DRBD`, `STORAGE`.

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

Treat the stack as fixed once a volume has replicas. Changing `layerStack` by hand later does not encrypt or decrypt data that is already there.

## How the stack is pinned

`blockstor resource-group spawn`, and a create through the REST API, copy the group's `selectFilter.layerStack` onto the new definition. A definition that ends up with no stack in a group that sets one is pinned by the controller, and the satellite builds nothing for it until then. That happens to a definition applied with `kubectl` or GitOps, created with `blockstor resource-definition create --resource-group`, or moved to another group with `blockstor resource-definition modify --resource-group`:

- A definition that holds no data yet gets the group's stack.
- A definition that already holds data keeps `DRBD`, `STORAGE`, the stack its storage was built with. It holds data once a replica has reported DRBD metadata or a device, or once `spec.initialized` is set. If the group asks for a layer this stack lacks, the controller writes the group's stack into the annotation `blockstor.io/unapplied-group-layer-stack` and emits a `LayerStackNotInherited` warning Event on the definition. Such a volume is not what its group promises: a volume with data moved into a LUKS group stays unencrypted. Copy its data to a new volume to get the group's stack.

The controller keeps checking: whenever a definition's stack lacks a layer its group asks for, whether the stack was set by hand or pinned, the annotation names the group's current stack, and a newly flagged definition gets the same warning Event. The annotation goes away once the group no longer asks for the missing layer or is deleted. Changing a group's stack later never reshapes a definition that already has one.

Clones and restores copy bytes, so the new definition takes the stack the source's data was written under. While the source is still waiting for its stack to be pinned, the API answers a clone or a restore with 503 and the CLI exits with an error; retry once the controller has pinned it.

As a last guard, the LUKS layer does not format a device that already carries a signature `blkid -p` recognises, such as DRBD metadata, a filesystem or a partition table. The replica fails with `luks: device already holds data` and names what it found. If the device was created outside blockstor and you have checked that it holds nothing you need, clear it with `wipefs -a <device>`.

List the definitions that lack a layer their group asks for:

```sh
kubectl get resourcedefinitions.blockstor.cozystack.io \
  -o custom-columns='NAME:.metadata.name,GROUP_STACK:.metadata.annotations.blockstor\.io/unapplied-group-layer-stack' \
  | awk '$2 != "<none>"'
```

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
