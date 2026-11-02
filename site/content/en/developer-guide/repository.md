---
title: "Repository layout"
linkTitle: "Repository layout"
weight: 30
description: "Where each part of blockstor lives."
---

```
api/v1alpha1/                 CRD types and kubebuilder markers
cmd/                          controller, apiserver, satellite, blockstor CLI, linstor-migrate
internal/controller/          controller-side reconcilers
internal/cli/                 the blockstor CLI
pkg/                          REST, stores, satellite, storage, DRBD, LUKS, placer, dispatcher
config/                       generated CRDs, RBAC and kustomize bases
stand/                        the Talos and QEMU dev stand and the reference manifests
tests/                        contract, integration, e2e, cli-matrix and replay suites
docs/                         design notes, parity deltas and audit history
site/                         this website
```

| Path | What lives there |
|---|---|
| `api/v1alpha1/` | The CRD types. `make manifests` turns their markers into `config/crd/bases/`. |
| `pkg/api/v1/` | The REST wire types, shaped like upstream LINSTOR's, and the layer-stack resolver. |
| `pkg/rest/` | The LINSTOR-compatible REST handlers. |
| `pkg/store/`, `pkg/store/k8s/` | The in-memory and CRD-backed stores behind one `store.Store` interface, and the translation between wire and CRD shapes. |
| `internal/controller/` | Controller-side reconcilers: resources, definitions, groups, pools, snapshots, nodes, plus tiebreakers, auto-diskful, eviction, rebalancing and migration. |
| `pkg/satellite/controllers/` | Satellite-side reconcilers for resources, pools, snapshots and physical devices, and the `events2` observer. |
| `pkg/satellite/` | The DRBD, LUKS and storage layer reconciler and snapshot shipping. |
| `pkg/storage/` | Storage providers: LVM, LVM-thin, ZFS, ZFS-thin, loopfile and file. |
| `pkg/drbd/` | `drbdadm` and `drbdsetup` wrappers, the `.res` renderer, the `events2` parser and the options resolver. |
| `pkg/luks/` | The `cryptsetup` wrapper. |
| `pkg/placer/` | The autoplacer: capacity-weighted, anti-affinity aware. |
| `pkg/dispatcher/` | Translates CRDs into the desired state a satellite applies: layers, options, passphrases. |
| `internal/cli/` | The `blockstor` CLI. Its design is in [`docs/cli-design.md`](https://github.com/cozystack/blockstor/blob/main/docs/cli-design.md). |
| `cmd/linstor-migrate/` | The LINSTOR database converter. |
