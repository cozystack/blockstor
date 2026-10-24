---
title: "Networking and ports"
linkTitle: "Networking and ports"
weight: 35
description: "Every listener blockstor opens, who connects to it, and what a firewall must allow."
---

Blockstor's components do not talk to each other directly. They read and write the custom resources through the Kubernetes API, and the Kubernetes API is the only thing every component needs to reach. The listeners below exist for clients, for kubelet probes, and for the data path between storage nodes.

## Listeners

| Component | Port | Protocol | Who connects | Reachable from |
|---|---|---|---|---|
| blockstor-apiserver | 3371 | HTTPS with mutual TLS | `linstor-csi`, piraeus-operator, `ha-controller`, the `linstor` client | The Services `blockstor-apiserver` and `blockstor-controller` in `blockstor-system` |
| blockstor-apiserver | 3370 | Plain HTTP, no authentication | `kubectl port-forward` for debugging | Loopback inside the pod only |
| blockstor-apiserver | 8081 | HTTP `/healthz`, `/readyz` | kubelet | The pod |
| blockstor-controller | 8081 | HTTP `/healthz`, `/readyz` | kubelet | The pod |
| blockstor-satellite | 3372 | HTTPS with mutual TLS | Other satellites, to fetch a snapshot stream | The node's own address |
| blockstor-satellite | 9101 | HTTP `/healthz`, `/readyz` | kubelet | The node's own address |
| DRBD (kernel) | 20000 to 20999 | DRBD replication, TCP | The other replicas of the same volume | The node's own address |

The satellite runs with host networking, so its ports are open on the node itself, not on a pod IP. The manifests set `hostPort` 3372 and 9101 to make this explicit. The satellite opens no metrics listener.

The controller has no REST listener. The manifests start it with `--enable-rest-api=false`, and the REST API is served by the separate `blockstor-apiserver` Deployment. Both the controller and the API server are started with `--metrics-bind-address=0`, so they open no metrics port.

Satellites do not dial the controller, and the controller does not dial satellites. Both go through the Kubernetes API.

## The API server

Port 3371 requires a client certificate and verifies it against the CA bundle given by `--tls-client-ca-file`. The manifests create that CA with cert-manager as the `blockstor-api-ca` Issuer, and issue one client certificate into the Secret `blockstor-apiserver-client-tls`. The serving certificate covers both Service names, `blockstor-apiserver` and `blockstor-controller`, with and without the `.blockstor-system.svc.cluster.local` suffix. The API server reloads certificates on rotation without a restart.

Port 3370 serves the same API without TLS and without authentication. It exists for debugging, so it listens on `127.0.0.1` inside the pod and no other pod can reach it. A port-forward connects inside the pod, so that is the way in, for example for the `linstor` client. See [Compatibility with LINSTOR](../compatibility/#using-the-linstor-client).

## DRBD replication

Each volume gets one TCP port, and all of its replicas use that port to connect to each other. The controller allocates the port from a range, per node, and the port appears in the `Port` column of `kubectl get resources` and of `blockstor resource list`.

The range is resolved in this order for every node that hosts a replica:

1. `spec.drbdPortRange` on the blockstor `Node`, with `min` and `max`.
2. The node property `DrbdOptions/TcpPortRange`, written as `min-max`, kept for LINSTOR compatibility.
3. `TcpPortAutoRange` in `spec.extraProps` of the `ControllerConfig`.
4. The built-in default, 20000 to 20999.

Minor numbers for the `/dev/drbd<N>` devices follow the same pattern with `spec.drbdMinorRange` and `DrbdOptions/MinorNrRange`, with a default of 20000 to 65535. These defaults are above what LINSTOR uses, so blockstor can share nodes with a LINSTOR installation.

A firewall between storage nodes must allow the whole port range you use, in both directions. A replica whose peer is blocked stays in `Connecting`.

## Which address replication uses

Each blockstor `Node` lists its addresses in `spec.netInterfaces`. `blockstor node create <name> <address>` writes one interface named `default`. DRBD peers connect to that address, so it has to be the node's own IP as seen by the other nodes. A pod IP does not work, because it is only routable inside the CNI.

The satellite also publishes the Kubernetes node's `InternalIP` as an interface named `k8s-internal`, and the controller prefers it when it writes peer addresses: `k8s-internal` first, then `default`, then the first interface with an address. This is what keeps replication working when another tool, such as piraeus-operator, overwrites the first interface with a pod IP. It needs the satellite's permission to read Kubernetes `nodes`, which the manifests grant.

## Snapshot stream between satellites

When a clone or a snapshot restore needs data from a snapshot on another node, the receiving satellite fetches the raw snapshot stream from the satellite that holds it, with an HTTPS `GET` on port 3372.

The stream carries raw volume data, so it runs over mutual TLS only. Every satellite presents the certificate from the Secret `blockstor-satellite-stream-tls`, issued for the name `blockstor-satellite` by its own CA, `blockstor-stream-ca`, and each side checks the other against that CA. The stream CA is kept apart from `blockstor-api-ca` on purpose: a stream certificate is also a client certificate, and the API server accepts any client certificate from its own CA. The certificate is read again on every connection, so a rotation needs no restart.

The satellite takes the files with `--stream-tls-cert-file`, `--stream-tls-key-file` and `--stream-tls-ca-file`. Started without them, it neither serves nor fetches the stream, and a cross-node clone or restore falls back to a DRBD initial sync, which is slower but works. The same happens when the port is blocked.

## What a firewall must allow

Between storage nodes, in both directions:

- The DRBD port range, 20000 to 20999 by default.
- Port 3372 for the snapshot stream.

Inside the cluster:

- From the CSI pods and other API clients to the `blockstor-apiserver` Service on port 3371.

On each node, for kubelet:

- Port 9101 on the node for the satellite's probes.
- Port 8081 inside the controller and API server pods for theirs.

Nothing needs to reach port 3370.

## Ports without authentication

Only 3370 on the API server, with full read and write access to the API, and it listens on loopback inside the pod. Port 3371 is the only API entry point meant for clients.
