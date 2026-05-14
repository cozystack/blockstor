# Wave 2 — Group 2 — Placement & auto-placement (Day2 ops)

Day2 autoplacer knobs not yet covered by wave1: weighted selection
strategies on the controller, balance-resources disable, and the
Kubernetes StorageClass locality/zone parameters that drive the
underlying RG props.

Pairs with wave1's `02-placement.md` — these scenarios feed the
autoplacer rather than rewriting it.

[Group index in README.md](README.md).

---

### 2.W01 Controller `Autoplacer/Weights/*` tune selection strategy — T

- **Priority:** P2  **Target:** unit  **Complexity:** M (implement first)
- **Source:** UG9 §"Storage pool placement" (lines 933-993) via tests/scenarios/day2-controller-set-autoplacer-weights.md

Cross-listed with wave1 2.17. Four weights: `MaxFreeSpace`, `MinReservedSpace`, `MinRscCount`, `MaxThroughput`. Defaults: `MaxFreeSpace=1`, others=0.

**Unit (after implement):** autoplacer with `Weights/MinRscCount=1, Weights/MaxFreeSpace=0` → picks the pool with fewest existing resources even if it has less free space.

### 2.W02 `BalanceResourcesEnabled=false` disables periodic rebalance — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M (after 2.15 lands)
- **Source:** UG9 §"Automatically maintaining resource group placement count" (lines 885-907) via tests/scenarios/day2-balance-resources-disable.md

Cross-listed with wave1 2.15. Disable at controller / RG / RD scope. Hierarchy: RD > RG > controller. Companion knobs: `BalanceResourcesInterval` (default 3600s), `BalanceResourcesGracePeriod` (default 3600s).

**Unit:** with prop=false, under-placement (manual `r d`) does NOT trigger re-placement on the next scan tick.
**E2E:** flip prop, delete a replica, wait > Interval+Grace, replica count stays N-1.

### 2.W03 StorageClass `allowRemoteVolumeAccess=false` + WFFC locality — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Volume locality optimization" (lines 2720-2729) + §"Single-zone homogeneous clusters" (lines 2508-2537) via tests/scenarios/day2-storage-class-locality.md

CSI parameter combo. `volumeBindingMode: WaitForFirstConsumer` + `linstor.csi.linbit.com/allowRemoteVolumeAccess: "false"` → one replica lands on the consuming Pod's node; remote nodes refuse to bind.

**E2E:** SC + PVC + Pod. First pod schedules on worker-2 → `r l` shows replica on worker-2. Second pod (anti-affinity) on worker-3 stays Pending with affinity error. Pair with wave2-11 K8s affinity controller.

### 2.W04 StorageClass `replicasOnDifferent: topology.kubernetes.io/zone` — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Multi-zonal homogeneous clusters" (lines 2538-2574) via tests/scenarios/day2-storage-class-replicas-on-different-zone.md

CSI propagates K8s node labels → `Aux/topology.kubernetes.io/zone` (depends on wave1 2.13 label-sync reconciler). SC sets `replicasOnDifferent: topology.kubernetes.io/zone` + nested `allowRemoteVolumeAccess.fromSame` restriction.

**E2E:** label 3 zones across 6 workers; PVC → replicas on distinct zone values; `kubectl get pv -o yaml | grep nodeAffinity` restricts to replica zones.

---

## Group summary

| Tag | Count |
|-----|------:|
| P1 e2e | 2 |
| P2 unit | 1 |
| T (implement first) | 2 |
