---
title: "Documentation"
linkTitle: "Docs"
weight: 20
menu:
  main:
    weight: 10
description: "Install, operate and understand blockstor."
---

Blockstor is a Kubernetes control plane for LVM and ZFS storage with DRBD replication. It speaks a LINSTOR-compatible REST API, so `linstor-csi`, `piraeus-operator`, `ha-controller` and `golinstor` keep working unchanged, and it ships its own `blockstor` command-line tool.

Where to start:

- New to blockstor: read the [Overview](overview/), then [Architecture](architecture/).
- Setting up a cluster: [Installation](installation/), then [Provisioning volumes with CSI](csi/).
- Day-to-day work: [Using the CLI](cli/) and [Resources](resources/).
- Coming from LINSTOR: [Migrating from LINSTOR](migration/) and [Compatibility with LINSTOR](compatibility/).

If you want to work on blockstor itself, see the [Developer guide](/developer-guide/).
