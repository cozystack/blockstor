# Bug 42 investigation — piraeus operator rewrites blockstor Node NetInterfaces

## TL;DR

The premise in `known-issues.md` ("satellite pod IPs come from outside the pod CIDR" / "netInterfaces[0].address=<pod-CIDR>") is wrong. On both e2e-iptables and e2e-quorum the stored `Node.spec.netInterfaces[0].address` is the host InternalIP (10.27.0.x / 10.161.0.x). What actually differs between the two stands is **who owns the Node CRD's `spec` once piraeus-operator finishes reconciling**:

- **e2e-quorum** — piraeus runs against its own bundled `linstor-controller` (LinstorCluster `spec: {}`). Piraeus never touches the blockstor Node CRD. Spec stays exactly as `install-blockstor.sh` left it: one interface `default` with the k8s InternalIP, `generation: 1`, no `Aux/piraeus.io/*` props.
- **e2e-iptables** — `LinstorCluster.spec.externalController.url = http://blockstor-apiserver.blockstor-system.svc:3370` was set (by an `observability-*` e2e scenario, see `tests/e2e/observability-three-way.sh:78-84` and `tests/e2e/observability-capacity-correlation.sh:240-244`). The operator now drives blockstor-apiserver as its LINSTOR backend and rewrites the Node CRD via `PUT /v1/nodes/{node}/net-interfaces/default-ipv4`: interface name changes from `default` to `default-ipv4`, `satellitePort=3366` + `satelliteEncryptionType=PLAIN` are added, and `Aux/piraeus.io/last-applied` / `Aux/piraeus.io/configured-interfaces` props appear in `spec.props`. Generation climbs to 9.

Address itself is **still the host IP** (piraeus reads `corev1.Node.Status.Addresses[InternalIP]`, not the pod IP). The pod-CIDR claim in the bug card cannot be reproduced against the current stand state.

## Evidence

### e2e-iptables Node CRD (worker-1)

```
spec:
  netInterfaces:
  - address: 10.27.0.3
    name: default-ipv4
    satelliteEncryptionType: PLAIN
    satellitePort: 3366
  props:
    Aux/kubernetes.io/hostname: e2e-iptables-worker-1
    Aux/piraeus.io/configured-interfaces: '["default-ipv4"]'
    Aux/piraeus.io/last-applied: '["Aux/piraeus.io/configured-interfaces","Aux/topology/kubernetes.io/hostname","Aux/topology/linbit.com/hostname"]'
    Aux/topology/kubernetes.io/hostname: e2e-iptables-worker-1
    Aux/topology/linbit.com/hostname: e2e-iptables-worker-1
  type: SATELLITE
```

`last-applied-configuration` annotation shows what `install-blockstor.sh` originally applied: `{"netInterfaces":[{"address":"10.27.0.3","name":"default"}],"type":"SATELLITE"}`.

### e2e-quorum Node CRD (worker-1)

```
spec:
  netInterfaces:
  - address: 10.161.0.3
    name: default
    satelliteEncryptionType: PLAIN
    satellitePort: 3366
  type: SATELLITE
```

No props block, generation=1, interface name unchanged.

### LinstorCluster spec

- e2e-iptables: `spec.externalController.url: http://blockstor-apiserver.blockstor-system.svc:3370`, controller version `1.33.2 (Git: blockstor)`.
- e2e-quorum: `spec: {}`, controller version `1.32.3` (the bundled piraeus linstor).

### Apiserver REST trace (e2e-iptables only)

```
PUT /v1/nodes/e2e-iptables-worker-1/net-interfaces/default-ipv4  200
PUT /v1/nodes/e2e-iptables-worker-2/net-interfaces/default-ipv4  200
PUT /v1/nodes/e2e-iptables-worker-3/net-interfaces/default-ipv4  200
```

No equivalent PUTs on e2e-quorum's apiserver.

### Which component writes what

| Writer | Touches | Field-owner |
| --- | --- | --- |
| `stand/install-blockstor.sh:60-68` | creates Node CRD with `name=default`, host InternalIP | `kubectl-client-side-apply` |
| `internal/controller/node_label_sync_controller.go:189-195` | only `spec.props["Aux/<allow-listed-label>"]`, SSA with `LabelSyncFieldOwner` | label-sync owner |
| piraeus-operator (when `LinstorCluster.spec.externalController.url` is set) | full `spec.netInterfaces` rewrite via REST (`PUT /v1/nodes/{node}/net-interfaces/{name}`) and `Aux/piraeus.io/*` props | apiserver field-owner (effectively last-writer-wins; SSA isn't used for REST) |

The `mutateNetInterface` handler at `pkg/rest/nodes.go:122-143` matches by URL path's `{name}`; PUT-ing `default-ipv4` doesn't find an existing `default`, so it appends. The fact that current spec carries only `default-ipv4` (not both) means piraeus issued either a prior DELETE of `default` or `Update` replacing the whole NetInterfaces list. The current apiserver pods are 28m old so the original DELETE isn't in their tail.

## Functional impact

Address is still routable (10.27.0.x), so DRBD replication is not broken. The real downstream effects:

1. **`pkg/satellite/controllers/snapshot_fetcher.go:139`** does a strict `Name == "default"` lookup first and only falls back to "first non-empty Address" on miss. After the rename to `default-ipv4`, the strict lookup silently fails on every call. Works today only because of the fallback at line 147-153 — fragile.
2. **Field ownership churn**: every piraeus reconcile bumps `Node.generation`. We've seen `generation: 9` on iptables vs `1` on quorum after roughly the same wall time. Anything watching `generation` for change detection (e.g. heartbeat reconciler, future SSA conflicts) sees noise.
3. **Props pollution**: `Aux/piraeus.io/last-applied` and `Aux/piraeus.io/configured-interfaces` end up in `Node.spec.props`. The contract tests' normalize layer (`tests/contract/normalize.go:115-117`) already strips these, but any other consumer reading raw Props will see operator-internal bookkeeping.

## Why e2e-iptables specifically

It's not the CNI / iptables-mode kube-proxy. Both stands use Flannel with podCIDR 10.244.0.0/16 and identical kube-proxy mode. The differentiator is which e2e scenarios have been run on the stand:

- `observability-three-way.sh` and `observability-capacity-correlation.sh` patch the LinstorCluster to point piraeus at blockstor-apiserver.
- That patch is **never reverted** when the scenario finishes — it persists for the lifetime of the stand.
- e2e-iptables has these scenarios in its rotation; e2e-quorum (the quorum-scenarios stand) does not.

So Bug 42 isn't a bug in production code at all. It's a stand-state leak from an e2e scenario.

## Recommended fix

Two options, both small. Prefer (A); pair with (B) for defence in depth.

### A. Make piraeus's interface name not matter to us

`pkg/satellite/controllers/snapshot_fetcher.go:138-153` — drop the strict `Name == "default"` preference, just take the first interface with a non-empty Address. The "default" name is a LINSTOR convention that we already lose to piraeus's `default-ipv4` rename whenever the operator manages our Nodes; encoding it as a preference is dead code that masks any future regression.

```go
for i := range node.Spec.NetInterfaces {
    if node.Spec.NetInterfaces[i].Address != "" {
        return stream.PeerAddr(node.Spec.NetInterfaces[i].Address), nil
    }
}
```

### B. Have the observability e2e scenarios revert the LinstorCluster patch on exit

Both `tests/e2e/observability-three-way.sh:81-84` and `tests/e2e/observability-capacity-correlation.sh:242-245` apply the externalController patch unconditionally and never restore the prior state. Add a `trap` that captures `$CUR_URL` before patching and either un-patches (if originally empty) or patches back to the original URL on exit. That alone makes the symptom non-sticky and stops one scenario from contaminating subsequent ones on the same stand.

### Not recommended

- "Block piraeus from writing our Node CRDs" — that would defeat the whole point of `LinstorCluster.spec.externalController`. The contract is that piraeus IS allowed to drive a LINSTOR-API-shaped backend; blockstor-apiserver exposes that API by design.
- "Update the Bug 42 known-issues entry to mention pod-CIDR" — it should be rewritten or closed; the pod-CIDR framing is not supported by the data on the live stands.

## Recommended next steps

1. Apply fix (A) + (B), each as its own commit.
2. Rewrite `docs/known-issues.md` Bug 42 entry to match what's actually happening (interface-name + props churn from externalController-mode piraeus), drop the unsupported pod-CIDR claim, and adjust severity (P3 — cosmetic / fragility, not a DRBD outage).
3. Optional follow-up: add a contract test asserting that `snapshot_fetcher.peerAddr()` resolves correctly when the only NetInterface is named `default-ipv4`. That pins fix (A) and would have caught this earlier.
