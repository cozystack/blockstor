---
title: "Overview"
linkTitle: "Overview"
weight: 10
description: "What blockstor is, what it does today, and when to use it."
---

Blockstor turns the local disks of your Kubernetes nodes into replicated block storage. Each volume is a logical volume (LVM) or a zvol (ZFS) on a node, and DRBD keeps copies of it on other nodes in sync, so a pod can lose its node and come back elsewhere with its data.

Blockstor speaks the LINSTOR REST API. The clients built for LINSTOR, `linstor-csi`, `piraeus-operator`, `ha-controller` and `golinstor`, work against it unchanged. What is different is how the cluster keeps its state: everything lives in Kubernetes custom resources, and controllers converge the cluster toward it. There is no separate database to back up and no in-memory state to lose when a pod restarts.

## When to use it

Blockstor fits when you want replicated block volumes for Kubernetes on your own hardware, with LVM or ZFS underneath and DRBD for replication, and you want that storage to be managed the same way as the rest of the cluster: with custom resources, `kubectl` and GitOps.

It is also a fit if you already run LINSTOR on Kubernetes. Blockstor can take over an existing LINSTOR cluster in place, without copying data. See [Migrating from LINSTOR](../migration/).

## What works today

- Replicated volumes over DRBD on LVM, LVM-thin, ZFS, ZFS-thin or file backends.
- Volumes without DRBD: plain local storage, single replica.
- LUKS encryption at rest.
- Automatic placement with constraints: zones, node properties, replicas on different nodes.
- Tiebreakers and quorum policies.
- Snapshots: create, restore as a new volume, and clone.
- Snapshot shipping between nodes of the same cluster, for clones and new replicas.
- Online volume resize.
- Creating storage pools from raw disks.
- The LINSTOR-compatible REST API over mutual TLS.

## What is not implemented

The API answers these with `501 Not Implemented`:

- Snapshot shipping to another cluster (disaster recovery).
- Backups: create, restore, ship, abort, and the backup queue.
- Schedules (cron-driven backups).
- Remote backends such as S3 and LINSTOR remotes.
- Other storage providers: SPDK, NVMe-oF, OpenFlex, Exos.

## On the roadmap

- Bring-your-own-key encryption, with Secret references in the spec.
- Shared-LUN provisioning: thick LVM plus thin qcow2 on LVM.
- A VDUSE backend through `qemu-storage-daemon`, for shared-SAN Kubernetes.

## Components

Blockstor runs as three images, published to GHCR on every release:

| Image | What it runs |
|---|---|
| `ghcr.io/cozystack/blockstor-controller` | The controllers that place replicas and allocate DRBD ports, minors and node IDs. |
| `ghcr.io/cozystack/blockstor-apiserver` | The LINSTOR-compatible REST API that CSI and other clients talk to. |
| `ghcr.io/cozystack/blockstor-satellite` | The per-node agent that creates volumes and runs DRBD. |

[Architecture](../architecture/) shows how they fit together.
