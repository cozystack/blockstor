---
title: "Architecture"
linkTitle: "Architecture"
weight: 30
description: "How blockstor turns a volume request into replicas on disk."
---

Blockstor is built like a Kubernetes operator. The desired state of every node, pool and volume is a custom resource, and controllers keep working until the cluster matches it. Nothing important lives only in a process's memory, so any blockstor pod can restart without losing state.

## Components

```mermaid
flowchart TB
    subgraph clients["Clients"]
        csi["linstor-csi"]
        other["piraeus-operator, ha-controller, golinstor"]
        cli["blockstor CLI"]
    end

    subgraph k8s["Kubernetes API"]
        crds[("blockstor custom resources")]
    end

    subgraph cp["blockstor-system"]
        api["blockstor-apiserver<br/>LINSTOR-compatible REST, mTLS"]
        ctrl["blockstor-controller<br/>placement and allocation"]
    end

    subgraph nodes["Storage nodes"]
        sat1["satellite on node 1"]
        sat2["satellite on node 2"]
        sat3["satellite on node 3"]
    end

    csi -->|REST| api
    other -->|REST| api
    cli -->|kubeconfig| crds
    api <--> crds
    ctrl <--> crds
    crds <--> sat1
    crds <--> sat2
    crds <--> sat3
```

- **blockstor-apiserver** serves the LINSTOR REST API over mutual TLS. It does not keep state of its own: every request becomes a read or a write of custom resources. It runs as several replicas behind one Service.
- **blockstor-controller** watches the custom resources and fills in what users do not choose: which nodes get replicas, the DRBD port, minor numbers and node IDs, and tiebreakers and quorum settings.
- **blockstor-satellite** runs on every storage node. It creates the logical volumes or zvols, sets up LUKS and DRBD, and reports what it sees back into the custom resources.
- **The `blockstor` CLI** reads and writes the custom resources directly with your kubeconfig, without going through the REST API.

There is no direct connection between the controller and the satellites. Everything goes through the Kubernetes API.

## Custom resources are the source of truth

Every object you know from LINSTOR is a cluster-scoped custom resource in the `blockstor.cozystack.io` group:

```mermaid
flowchart LR
    RG["ResourceGroup<br/>policy: place count, pool, layers"]
    RD["ResourceDefinition<br/>one volume set, its size and layers"]
    R1["Resource<br/>replica on node 1"]
    R2["Resource<br/>replica on node 2"]
    R3["Resource<br/>tiebreaker on node 3"]
    SP["StoragePool<br/>per node"]
    N["Node"]
    S["Snapshot"]

    RG --> RD
    RD --> R1
    RD --> R2
    RD --> R3
    R1 -. lives in .-> SP
    SP -. on .-> N
    RD --> S
```

Each resource has two halves:

- **spec** is what you, the controller or the API asked for.
- **status** is what the satellites observed: disk states, connection states, free capacity.

A satellite never changes a spec, and the API never writes a status. Because the state is ordinary Kubernetes objects, you can read it with `kubectl get resources`, back it up with your usual tools, and drive it from GitOps.

## How a volume is created

This is the path from a PersistentVolumeClaim to replicas on disk:

```mermaid
sequenceDiagram
    autonumber
    participant PVC as PVC
    participant CSI as linstor-csi
    participant API as blockstor-apiserver
    participant K as Kubernetes API
    participant C as blockstor-controller
    participant S as satellites

    PVC->>CSI: provision a volume
    CSI->>API: create the resource definition and place it
    API->>K: write ResourceDefinition and Resources
    K-->>C: change seen
    C->>K: pick nodes, allocate DRBD port, minors and node IDs, add a tiebreaker
    K-->>S: each satellite sees the Resources for its node
    S->>S: create LV or zvol, set up LUKS and DRBD
    S->>K: report disk and connection state
    API-->>CSI: volume is ready
    CSI-->>PVC: bound, device /dev/drbdN
```

## Each satellite watches only its own node

A satellite reacts only to the resources assigned to its node: replicas and pools whose node name is its own. Adding nodes adds satellites, and none of them has to talk to a central process.

```mermaid
flowchart LR
    subgraph K["Kubernetes API"]
        r1["Resource pvc-1 on node-1"]
        r2["Resource pvc-1 on node-2"]
        r3["Resource pvc-2 on node-1"]
    end
    s1["satellite node-1"]
    s2["satellite node-2"]
    r1 --> s1
    r3 --> s1
    r2 --> s2
```

## Layers on a node

Every replica is a stack of layers. The satellite builds it from the bottom up when it creates a replica and takes it apart from the top down when it removes one.

```mermaid
flowchart TB
    pod["Pod sees /dev/drbdN"]
    drbd["DRBD: replication to the other nodes"]
    luks["LUKS: encryption at rest, optional"]
    storage["STORAGE: LVM LV, LVM-thin LV, zvol or file"]
    pod --> drbd --> luks --> storage
```

The default stack is DRBD on top of STORAGE. See [Layer stack](../layer-stack/) for the other combinations.

## Replicas, tiebreakers and quorum

With an even number of data replicas, the controller adds a diskless tiebreaker on another node, so DRBD always has a majority to decide which side keeps writing after a network split. A replica without a disk can also serve I/O over the network to a pod on a node that holds no copy of the data.
