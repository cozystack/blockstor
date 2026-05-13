# Group 2 — Placement & auto-placement

The autoplacer is the brain of LINSTOR's storage scheduling. It picks
which nodes get replicas based on resource-group constraints
(`replicas-on-same`, `replicas-on-different`, `x-replicas-on-different`,
`layer-list`, `providers`, `do-not-place-with`, `AutoplaceTarget`),
storage-pool capacity + free space, and rebalancing policies
(`BalanceResources*`).

Most placement logic is pure functions over the cluster snapshot, so
**unit tests with mocked stores** cover the bulk. E2E tests pin the
operator-visible behaviour and label-sync flows.

[Group index in README.md](README.md).

---

## Autoplacer basics

### 2.1 PlaceCount + storage-pool filter — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #9, #10; PLAN.md §Autoplacer

**Why:** RG with `--place-count 2 --storage-pool stand` lands exactly 2 diskful replicas on `stand` pools. Foundation of every spawn.

**Unit:** `internal/controller/first_available_pool_test.go` — picker over a 3-node × 3-pool synthetic cluster snapshot.
**E2E:** `linstor rg spawn default test 1G` → 2 UpToDate + 1 TieBreaker in `linstor r l -r test`.

### 2.2 Greatest-free-first deterministic ordering — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** PLAN.md §Autoplacer

`pkg/dispatcher` autoplacer sorts pools by FreeCapacity descending, ties broken on NodeName. Same input → same output for reproducibility.

### 2.3 Spawn = RD + VDs + Resources in one call — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #9; PLAN.md spawn fix

Already in 1.28 — cross-listed because the spawn surface IS the autoplacer's primary entry point. Test once, reference both groups.

### 2.4 Manual create with explicit storage-pool — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** linstor-cli #11

`linstor r c <worker> <rd> -s lvm-thin` bypasses the autoplacer. Tiebreaker still gets auto-added on the remaining worker (via `ensureTiebreaker` reconciler — covered in `07-quorum-observability.md`).

---

## Constraints — replicas-on-same / replicas-on-different

### 2.5 `replicas-on-same Aux/zone=a` clusters replicas — S, missing test

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #4; UG9 §"Constraining automatic resource placement..."

**Why:** Topology-aware placement for affinity / latency-bound workloads.

**Unit:** autoplacer with mocked node-prop snapshot — workers 1+2 have `Aux/zone=a`, worker-3 has `Aux/zone=b`. RG with `replicas-on-same zone=a` and place-count 2 → picks workers 1+2.
**E2E:** label nodes, set Aux/, spawn, verify placement.

### 2.6 `replicas-on-different Aux/zone` spreads replicas — S, missing test

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #2; linstor-kubernetes.adoc §replicasOnDifferent

Inverse of 2.5. RG with `replicas-on-different zone` and place-count 2 → one replica in zone=a, one in zone=b.

### 2.7 `--replicas-on-different no-csi-volumes=true` exclusion mode — P

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §2140

When value is given, nodes with that exact value are considered **last**. Test: node-3 has `no-csi-volumes=true`, but place-count=4 forces selection → autoplacer still picks it.

### 2.8 `x-replicas-on-different site 2 --place-count 3` for stretched DR — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M (implement first)
- **Source:** UG9 §"Ensuring automatic resource placement on different nodes..."; ug9-features 2.3

**Status:** Not implemented in blockstor. The feature places at most N replicas per same-value bucket → e.g., 2 per site, 1 elsewhere.

**Unit (after implement):** autoplacer with 6 nodes (2/2/2 across sites a/b/c), `x-replicas-on-different site 2 --place-count 3` → 2 in one site + 1 in another.
**E2E:** same on the stand.

---

## Autoplacer exclusion / filtering

### 2.9 `AutoplaceTarget=false` excludes a node — P

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #3; KB:preventing-linstor-resource-placement

**Why:** Maintenance drain. Stop placing NEW replicas on a node without evicting existing ones.

**Unit:** autoplacer with `AutoplaceTarget=false` on worker-2 → never picks worker-2 even when it has the most free space.
**E2E:** set prop, spawn 10 RDs, verify zero land on worker-2.

### 2.10 `--do-not-place-with <rd>` anti-affinity — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (implement first)
- **Source:** UG9 §"Constraining automatic resource placement..."; ug9-features 2.5

Don't co-locate replicas of RD `a` with replicas of RD `b`. Useful for StatefulSet anti-affinity.

**Unit:** autoplacer with `--do-not-place-with rdA` excludes nodes already hosting `rdA`'s replicas.

### 2.11 `--do-not-place-with-regex <pattern>` — T

- **Priority:** P2  **Target:** unit  **Complexity:** L (implement first)

Pattern-based variant of 2.10.

### 2.12 `--layer-list` and `--providers` constrain pool eligibility — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §2034; ug9-features 2.6

**Unit:** autoplacer with `--providers ZFS_THIN` ignores LVM pools.
**E2E:** spawn with `--providers ZFS_THIN` → replicas all on ZFS_THIN pools.

---

## Topology / label sync

### 2.13 K8s node labels → `Aux/<label-key>` props sync — P

- **Priority:** P0  **Target:** integration + e2e  **Complexity:** M
- **Source:** advanced-config #2, ug9-features 11.3

**Why:** Without this, `replicasOnSame: "topology.kubernetes.io/zone=z1"` matches nothing. Operator-perceived "the StorageClass setting doesn't work" bug.

**Integration:** controller-runtime envtest with `NodeReconciler` watching k8s `Node` events; label a Node, assert Node CRD `Spec.Props["Aux/topology.kubernetes.io/zone"]` populated within reconcile loop.
**E2E:** label live node via `kubectl label`, wait <60s, `linstor n lp worker-1 | grep Aux/topology` shows the prop.

**Open question:** does blockstor have this reconciler today? grep `NodeLabelSync` — if not, this is a T+H combo (implement + test).

### 2.14 Custom Aux property propagation — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #4

`linstor node set-property --aux worker-1 deploy-zone production` → prop set on Node CRD → RG `--replicas-on-same Aux/deploy-zone=production` finds it. Distinct from 2.13: this is the manual operator path, not the k8s sync path.

---

## Auto-rebalance (BalanceResources*)

### 2.15 BalanceResourcesEnabled + Interval + GracePeriod — T

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Automatically maintaining resource group placement count" (1.26+); ug9-features 2.1

**Status:** Not implemented. Periodic task that re-fills the place-count gap when a satellite is evicted / a node loses a replica.

**Integration:** envtest controller with mocked clock advancing past `BalanceResourcesInterval` → re-placement triggers on a 4-node cluster where worker-3 was evicted.
**E2E:** 3 nodes + RG place-count=3, 5 resources; evict worker-3; wait > grace+interval; 4th node receives the missing replicas.

**Why P1:** without this, every node loss requires manual `rd ap` to restore redundancy. Operationally painful at scale.

---

## Storage-pool selection strategies + weights

### 2.16 MaxFreeSpace default strategy — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

`pkg/dispatcher` autoplacer uses MaxFreeSpace today (greatest-free-first). Test exists; pin it in this group's index for clarity.

### 2.17 MinReservedSpace / MinRscCount / MaxThroughput + weighted scoring — T

- **Priority:** P2  **Target:** unit  **Complexity:** M (implement first)
- **Source:** UG9 §"Storage pool placement" (lines 933-993); ug9-features 2.4

`Autoplacer/Weights/{MaxFreeSpace,MinReservedSpace,MinRscCount,MaxThroughput}` controller props weight the four strategies.

**Unit:** autoplacer with `Weights/MinRscCount=1, Weights/MaxFreeSpace=0` → picks the pool with fewest existing resources, even if it has less free space.

**Note:** `MaxThroughput` depends on per-volume `sys/fs/blkio_throttle_*` props — those are P3 unless customer asks (see 07).

---

## Place-count maintenance edge cases

### 2.18 `--auto-place 2` adds tiebreaker on remaining worker — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** linstor-cli #10, #11

After 2 diskful are placed, `ensureTiebreaker` reconciler stamps a 3rd diskless on the remaining worker (unless `AutoAddQuorumTiebreaker=false`). Cross-listed with 07.

### 2.19 Place count over node count refused at spawn — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating a resource group with impossible placement constraints"; linstor-cli #7

**Unit:** spawn with place-count=7 on a 3-worker cluster returns `Not enough available nodes` (not generic 500).
**E2E:** `linstor rg c x --place-count 7 && linstor rg spawn x test 1G` → error includes the actionable text.

### 2.20 Spawn after partial-fail re-places remainder — T

- **Priority:** P1  **Target:** integration  **Complexity:** M (after 2.15 lands)

If `rg spawn` succeeds on 2 of 3 nodes (3rd had transient pool error), the reconciler retries the missing one rather than leaving the RD half-placed. Same machinery as `BalanceResources`.

---

## Implementation-order recommendation

P0 first, then P1. Within each, unit tests before e2e:

1. 2.1, 2.2, 2.16 — autoplacer foundation (existing code, just tests)
2. 2.5, 2.6 — constraints (existing code, missing tests)
3. 2.9 — AutoplaceTarget (verify implementation; if exists → test; if not → P0 implement+test)
4. 2.13 — label sync (audit; if not implemented → P0 H complexity)
5. 2.4, 2.7, 2.12, 2.14, 2.18, 2.19, 2.20 — P1 fill-in
6. 2.8 — x-replicas-on-different (P1, implementation work)
7. 2.10, 2.11, 2.15, 2.17 — P1+ stretch (implementation work)

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 5 |
| P0 e2e | 4 |
| P1 unit | 4 |
| P1 e2e | 4 |
| P2 unit | 2 |
| T (implement first) | 5 |
