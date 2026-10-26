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
| blockstor-apiserver | 3370 | Plain HTTP, no authentication | `kubectl port-forward` for debugging | Every pod that can reach the API server's pod IP, unless a NetworkPolicy blocks it. Not in any Service. |
| blockstor-apiserver | 8081 | HTTP `/healthz`, `/readyz` | kubelet | The pod |
| blockstor-controller | 8081 | HTTP `/healthz`, `/readyz` | kubelet | The pod |
| blockstor-satellite | 9100 | Plain HTTP, no authentication | Other satellites, to fetch a snapshot stream | The node's own address |
| blockstor-satellite | 9101 | HTTP `/healthz`, `/readyz` | kubelet | The node's own address |
| blockstor-satellite | 8080 | HTTP metrics (controller-runtime default) | Nothing in blockstor | The node's own address |
| DRBD (kernel) | 20000 to 20999 | DRBD replication, TCP | The other replicas of the same volume | The node's own address |

The satellite runs with host networking, so its ports are open on the node itself, not on a pod IP. The manifests set `hostPort` 9100 and 9101 to make this explicit. The satellite does not set a metrics address, so controller-runtime opens its default metrics listener on port 8080 of the node. Nothing in blockstor reads it. Block it or scrape it, but do not leave it open to networks you do not trust.

The controller has no REST listener. The manifests start it with `--enable-rest-api=false`, and the REST API is served by the separate `blockstor-apiserver` Deployment. Both the controller and the API server are started with `--metrics-bind-address=0`, so they open no metrics port.

Satellites do not dial the controller, and the controller does not dial satellites. Both go through the Kubernetes API.

## The API server

Port 3371 requires a client certificate and verifies it against the CA bundle given by `--tls-client-ca-file`. The manifests create that CA with cert-manager as the `blockstor-api-ca` Issuer, and issue one client certificate into the Secret `blockstor-apiserver-client-tls`. The serving certificate covers both Service names, `blockstor-apiserver` and `blockstor-controller`, with and without the `.blockstor-system.svc.cluster.local` suffix. The API server reloads certificates on rotation without a restart.

Port 3370 serves the same API without TLS and without authentication. It exists for debugging. The manifests start the API server with `--rest-bind-address=:3370`, which listens on every address of the pod, and they ship no NetworkPolicy. Leaving the port out of the Service only means it has no stable name: any pod that can reach an API server pod's IP gets full read and write access to the API on that port, without a certificate.

Fence it with a NetworkPolicy. This one admits only the TLS port and the probe port to the API server pods, so port 3370 is refused from other pods:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: blockstor-apiserver
  namespace: blockstor-system
spec:
  podSelector:
    matchLabels:
      app: blockstor-apiserver
  policyTypes: [Ingress]
  ingress:
    - ports:
        - {protocol: TCP, port: 3371}
        - {protocol: TCP, port: 8081}
```

A NetworkPolicy only takes effect if your CNI enforces it. `kubectl port-forward` still works with this policy in place, because it connects from inside the pod's network namespace. That is the way to reach port 3370 when you need it, for example for the `linstor` client. See [Compatibility with LINSTOR](../compatibility/#using-the-linstor-client).

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

Each blockstor `Node` lists its addresses in `spec.netInterfaces`. `blockstor node create <name> <address>` writes one interface named `default`. DRBD peers connect to that address when nothing earlier in the order below applies, so it has to be the node's own IP as seen by the other nodes. A pod IP does not work, because it is only routable inside the CNI.

Each satellite renders the DRBD configuration of its own node, peer addresses included. Before it renders, it adds the Kubernetes node's `InternalIP` to every node's interface list as an interface named `k8s-internal`. That interface exists only in the satellite's memory and never shows up on the `Node` object. It needs the satellite's permission to read Kubernetes `nodes`, which the manifests grant.

The address of a peer is picked in this order:

1. `spec.satelliteEndpoint` on the blockstor `Node`, if set.
2. The interface named `k8s-internal`.
3. The interface named `default`.
4. The first interface with an address.

This order is what keeps replication working when another tool, such as piraeus-operator, overwrites the first interface with a pod IP.

## Snapshot stream between satellites

When a clone or a snapshot restore needs data from a snapshot on another node, the receiving satellite fetches the raw snapshot stream from the satellite that holds it, with an HTTP `GET` on port 9100. The stream has no authentication and no encryption. Keep that port reachable only between storage nodes, with a NetworkPolicy or a host firewall. If the port is blocked, the clone falls back to a DRBD initial sync, which is slower but works.

## What a firewall must allow

Between storage nodes, in both directions:

- The DRBD port range, 20000 to 20999 by default.
- Port 9100 for the snapshot stream.

Inside the cluster:

- From the CSI pods and other API clients to the `blockstor-apiserver` Service on port 3371.

On each node, for kubelet:

- Port 9101 on the node for the satellite's probes.
- Port 8081 inside the controller and API server pods for theirs.

Nothing needs to reach port 3370 or the satellite's port 8080. Block port 3370 with the NetworkPolicy shown [above](#the-api-server), and port 8080 with a host firewall.

## Ports without authentication

- 3370 on the API server. Full read and write access to the API from any pod that can reach the API server's pod IP. Not exposed by a Service, but not fenced either; add the NetworkPolicy above.
- 9100 on every storage node. Reads raw snapshot data.
- 8080 on every storage node. Metrics only.

Port 3371 is the only API entry point meant for clients.
