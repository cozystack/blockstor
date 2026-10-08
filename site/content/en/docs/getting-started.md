---
title: "Getting started"
weight: 10
description: "What blockstor needs and where to begin."
---

Blockstor installs onto an existing cluster and is driven with the standard `linstor` client and piraeus `linstor-csi`. The full walkthrough, from installing the control plane to registering nodes and pools and wiring up CSI, is in [Usage](../usage/).

## Images

The three images are published to GHCR on every release:

- `ghcr.io/cozystack/blockstor-controller`: the reconcilers
- `ghcr.io/cozystack/blockstor-apiserver`: the LINSTOR-compatible REST API, over mTLS
- `ghcr.io/cozystack/blockstor-satellite`: the per-node DRBD and storage agent

## Coming from LINSTOR

An existing LINSTOR cluster can be moved onto blockstor; see [Migrating from LINSTOR](../linstor-migration/).

## Contributing

The repository layout and the local Talos and QEMU dev stand are described in [AGENTS.md](https://github.com/cozystack/blockstor/blob/main/AGENTS.md).
