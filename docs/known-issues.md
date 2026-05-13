# Known Issues

This document tracks outstanding bugs and observations surfaced during the scenario/e2e validation session. Each entry references the scenario or test where it was first observed, plus the closing commit if any.

Severity legend:

- **P0** — data corruption, split-brain, lost volumes, or cluster-wide unavailability
- **P1** — feature unusable, retry/cancel paths missing, regression vs upstream LINSTOR
- **P2** — cosmetic, log noise, doc/RBAC drift, ergonomic gaps

## Bug 32: force-strip ZVOL cleanup race

**Status**: open (observation, not a defect per se)
**Severity**: P2
**Scenario reference**: tests/scenarios/05-finalizers.md §5.34
**Surfaced by**: scenario
**Reproduction steps**:

1. Create a Resource with a ZFS-backed VolumeDefinition on a single node.
2. Force-delete the Resource CR (`kubectl delete --grace-period=0 --force`) while the satellite is still mid-`drbdsetup down`.
3. Observe the underlying ZVOL on the host (`zfs list -t volume`).

**Expected behaviour**: ZVOL is destroyed in lock-step with the DRBD resource teardown; no stray dataset survives the CR removal.

**Actual behaviour**: A short window exists where the CR is gone but the ZVOL still resides on disk because the satellite finalizer was bypassed. The next reconcile or operator restart eventually GCs it, but for a few seconds the storage pool reports phantom capacity.

**Recommended fix**: No code change required. Document the window and rely on Bug 5.34 sweeper (see Bug 33) to handle the residual cleanup deterministically. Consider extending the sweeper to also enumerate orphan ZVOLs whose parent RD no longer exists.

**Related commits / tests**: see Bug 33 sweeper.

## Bug 33: orphan kernel DRBD-Diskless after force-strip

**Status**: closed (by Bug 5.34 sweeper)
**Severity**: P1
**Scenario reference**: tests/scenarios/05-finalizers.md §5.34
**Surfaced by**: scenario
**Reproduction steps**:

1. Create a 3-replica Resource (2 disk + 1 diskless tiebreaker).
2. Force-delete the tiebreaker Resource CR before the satellite finalizer drains.
3. Inspect `drbdsetup status` on the former diskless node.

**Expected behaviour**: Kernel DRBD device on the diskless node is fully removed; `drbdsetup status` no longer lists it.

**Actual behaviour**: Kernel resource remained in `Diskless` state with no corresponding CR, preventing subsequent recreation on the same node (port/minor collision).

**Recommended fix**: Periodic sweeper on the satellite enumerates kernel DRBD resources and reconciles against the local CR cache; any kernel resource without a matching CR is `drbdsetup down`-ed.

**Related commits / tests**: 5.34 sweeper landed in the satellite controller; e2e covers via scenario 05 force-strip path.

## Bug 34: migrate-disk endpoint missing

**Status**: closed (by 05b8709); Option B follow-up open
**Severity**: P1
**Scenario reference**: tests/scenarios/07-toggle-disk.md §7.4
**Surfaced by**: e2e test
**Reproduction steps**:

1. Create a 2-replica Resource on nodes A and B.
2. Issue `linstor resource-definition migrate-disk <rd> <fromNode> <toNode>`.

**Expected behaviour**: REST handler accepts the request, schedules a synchronous disk migration (toggle-disk on, then off on source) without losing redundancy.

**Actual behaviour** (pre-fix): Endpoint returned 404; the upstream LINSTOR client could not orchestrate disk migration through the apiserver.

**Recommended fix (landed)**: REST handler `migrate-disk` wired in 05b8709 to a two-step toggle-disk pipeline (add on target → wait UpToDate → remove on source).

**Option B follow-up (open)**: Wrap the two-step sequence in a single reconciler-driven state machine so an apiserver restart mid-migration does not strand the user with an extra diskful replica. Track at next sprint.

**Related commits / tests**: 05b8709; scenario 07 §7.4 covers the happy path.

## Bug 35: placer FreeCapacity gate missing

**Status**: open
**Severity**: P0
**Scenario reference**: tests/scenarios/08-placer.md §8.2
**Surfaced by**: scenario
**Reproduction steps**:

1. Create two storage pools with vastly different free capacity (e.g. 100 GiB and 1 GiB free).
2. Create a Resource of size 10 GiB without an explicit node selector.

**Expected behaviour**: Placer rejects the small pool because the requested size exceeds its FreeCapacity and chooses the larger pool (or fails fast with a clear error).

**Actual behaviour**: Placer ignores FreeCapacity entirely and may select the smaller pool, leading to `ENOSPC` at ZVOL/LV create time and a stuck Resource in `Creating`.

**Recommended fix**: Add a hard FreeCapacity gate inside the candidate-pool filter before scoring. Treat pools with `free < requested + reservedHeadroom` as ineligible. Add a placer unit test with two pools at different free sizes.

**Related commits / tests**: none yet; blocker for placer 1.0.

## Bug 36: handleVDUpdate wholesale-replace

**Status**: open
**Severity**: P1
**Scenario reference**: tests/scenarios/09-vd-update.md §9.1
**Surfaced by**: agent
**Reproduction steps**:

1. Create a VolumeDefinition with `props: {a: 1, b: 2}`.
2. PATCH the VD with `override_props: {a: 9}` (intent: change only `a`).

**Expected behaviour**: Final props are `{a: 9, b: 2}` — only the keys named in `override_props` are touched.

**Actual behaviour**: `handleVDUpdate` rewrites the entire props map from the request body, so `b` is silently dropped.

**Recommended fix**: Replace the wholesale assignment with a merge: apply `override_props` keys on top of the existing map, then remove keys named in `delete_props`. Mirrors the upstream LINSTOR controller semantics and is the precondition for Bug 37.

**Related commits / tests**: none; depends on Bug 37.

## Bug 37: VolumeDefinitionModify override_props/delete_props no-op

**Status**: open
**Severity**: P1
**Scenario reference**: tests/scenarios/09-vd-update.md §9.2
**Surfaced by**: e2e test
**Reproduction steps**:

1. Apply `VolumeDefinitionModify` with `override_props: {DrbdOptions/Net/protocol: C}`.
2. GET the VD and inspect `props`.

**Expected behaviour**: `DrbdOptions/Net/protocol` is set to `C`; `delete_props` removes named keys.

**Actual behaviour**: Both fields are accepted by the REST schema but ignored by the handler — the props map is unchanged.

**Recommended fix**: Wire `override_props` and `delete_props` through the same merge path introduced by Bug 36's fix. Add e2e assertions that round-trip both add and remove.

**Related commits / tests**: depends on Bug 36; scenario 09 §9.2 has the assertion stub.

## Bug 38: VD shrink accepted without STATE_INFO warning

**Status**: open
**Severity**: P2 (cosmetic)
**Scenario reference**: tests/scenarios/09-vd-update.md §9.3
**Surfaced by**: scenario
**Reproduction steps**:

1. Create a VD of size 10 GiB.
2. PATCH the VD to size 5 GiB.

**Expected behaviour**: Either the request is rejected, or it is accepted with a `STATE_INFO` / warning in the response indicating that shrinking is unsafe and won't reclaim space on the backing pool until the FS is also shrunk.

**Actual behaviour**: Request is accepted silently; no warning surfaces to the operator. Backing volume keeps its original allocation.

**Recommended fix**: Append a `STATE_INFO` entry to the API response when `newSize < currentSize`. No semantic change to storage; purely advisory.

**Related commits / tests**: none.

## Bug 39: toggle-disk retry counter missing

**Status**: open
**Severity**: P1
**Scenario reference**: tests/scenarios/07-toggle-disk.md §7.6
**Surfaced by**: agent
**Reproduction steps**:

1. Initiate `toggle-disk` on a Resource where the target node's storage pool is briefly unavailable.
2. Observe reconciler logs.

**Expected behaviour**: Reconciler retries with bounded backoff and a visible retry counter on the Resource status; after N attempts it surfaces a terminal error condition.

**Actual behaviour**: Reconciler retries forever with no visibility; if the failure is permanent, the Resource is stuck "in flight" indefinitely.

**Recommended fix**: Add `status.toggleDisk.attempts` and `status.toggleDisk.lastError`. Cap at e.g. 10 attempts before transitioning to `Failed` with a clear condition. Couple with Bug 40.

**Related commits / tests**: none; precondition for the migrate-disk Option B state machine.

## Bug 40: toggle-disk cancel state machine missing

**Status**: open
**Severity**: P1
**Scenario reference**: tests/scenarios/07-toggle-disk.md §7.7
**Surfaced by**: agent
**Reproduction steps**:

1. Start a long-running `toggle-disk` (e.g. large initial sync).
2. Issue a cancel/abort via the REST API.

**Expected behaviour**: Reconciler observes the cancel intent, rolls back the partial change (removes the half-added diskful, or re-adds the half-removed one) and returns the Resource to its pre-toggle state.

**Actual behaviour**: No cancel endpoint exists; the only escape is to delete the Resource entirely.

**Recommended fix**: Model toggle-disk as an explicit state machine (`Pending → Syncing → Promoting → Done | Cancelling → Rolled-back`). Add a cancel verb in the REST surface and have the reconciler honour it idempotently.

**Related commits / tests**: none; pairs with Bug 39.

## Bug 41: RBAC nodes get/list/watch missing in stand yaml

**Status**: closed (13722e1)
**Severity**: P2
**Scenario reference**: tests/scenarios/01-bootstrap.md §1.3
**Surfaced by**: e2e test (stand bring-up)
**Reproduction steps**:

1. Deploy blockstor on the dev stand with the previous controller-manifests yaml.
2. Watch controller logs at startup.

**Expected behaviour**: Controller starts and lists Node objects to seed its topology cache.

**Actual behaviour** (pre-fix): `nodes "" is forbidden: User "system:serviceaccount:..." cannot list resource "nodes" in API group "" at the cluster scope`; controller crashloops.

**Recommended fix (landed)**: Add `nodes` verbs `get/list/watch` to the ClusterRole shipped with the stand yaml.

**Related commits / tests**: 13722e1.

## Bug 42: piraeus pod-CIDR drift on e2e-iptables

**Status**: open (investigation pending)
**Severity**: P1
**Scenario reference**: tests/scenarios/10-e2e-iptables.md §10.1
**Surfaced by**: e2e test
**Reproduction steps**:

1. Bring up the e2e-iptables Talos+QEMU stand via `make iter`.
2. Deploy piraeus-operator with the default values.
3. Wait for the satellite DaemonSet pods to schedule and the LINSTOR controller to register them.

**Expected behaviour**: Satellite pod IPs come from the cluster's pod CIDR and are routable to/from the LINSTOR controller; resources reach `UpToDate` without DRBD net errors.

**Actual behaviour**: Some satellite pods receive addresses outside the expected pod CIDR (suspected CNI bridge / kube-proxy iptables drift on the iptables-mode stand), DRBD peer connections flap, resources oscillate between `Connecting` and `Connected`.

**Recommended fix**: Investigation pending. First steps:

- Confirm whether the drift is at the CNI layer (bridge IPAM) or kube-proxy (SNAT/MASQUERADE on the iptables stand).
- Capture `ip a`, `iptables-save`, and `kubectl get pods -o wide` on a failing run.
- Compare against the ipvs-mode stand where the issue does not reproduce.

**Related commits / tests**: scenario 10 §10.1 reproducer; no fix yet.

## Bug 49: satellite ignores runtime NetInterface changes (deferred from scenario 3.10)

**Status**: open (deferred — design gap, not a defect)
**Severity**: P2
**Scenario reference**: tests/scenarios/03-networking.md §3.10
**Surfaced by**: scenario audit (UG9 §"Managing network interface cards", lines 2167-2169)
**Reproduction steps**:

1. `kubectl edit node.blockstor.io/<worker>` and re-order the entries under `spec.netInterfaces` (or change the first entry's `address`).
2. Observe the running satellite Pod on that worker — it does not re-dial anything, does not re-render its DRBD `on <node>` block, and continues talking to the kube-apiserver via the same Service VIP.
3. Restart the satellite Pod (`kubectl delete pod -l app=blockstor-satellite,kubernetes.io/hostname=<worker>`) — only on restart does the new `Spec.NetInterfaces[0].Address` flow into `LocalAddress` (via `$POD_IP`) and from there into the next reconcile-rendered .res file.

**Expected behaviour (upstream LINSTOR contract)**: `linstor node interface modify ... --active` flips `StltConn/0/Active=true` and the satellite re-dials the controller via the freshly-active NIC without a process restart.

**Actual behaviour (blockstor)**: No re-dial happens, and there is no field to flip. Two architectural reasons:

1. Phase 10.6 retired the satellite→controller gRPC wire entirely. The satellite now talks to the kube-apiserver via the standard in-cluster config (`ctrl.GetConfig()` → `kubernetes.default.svc` Service VIP). There is no LINSTOR-style "controller endpoint" left to re-target.
2. `api/v1alpha1.NodeNetInterface` has no `Active` field. The `IsActive` flag on the REST wire is synthesised at conversion time (`i == 0` in `pkg/store/k8s/nodes.go`) — pure presentation, no behaviour. The only satellite code that reads peer NetInterfaces at runtime is `pkg/satellite/controllers/snapshot_fetcher.go` (peer-to-peer snapshot transport), and it picks by `Name == "default"` or first non-empty `Address`, not by an Active flag.

**Recommended fix**: Deferred. Implementing Outcome A (live NIC switch) would require:

- Adding an `Active` field to `NodeNetInterface` and a controller-runtime watch in the satellite that triggers reconciliation when `Spec.NetInterfaces[].Active` changes.
- A redirect plane that the satellite actually re-routes through — but the satellite's only outbound wire is the kube-apiserver REST/watch, which is owned by client-go and routes via the cluster Service VIP rather than this node's NIC selection.
- For the satellite→satellite snapshot stream the change would be marginal (peer-side already picks Address dynamically per Fetch call).

Net: implementing the upstream contract requires either resurrecting a custom controller-bind wire (rejected by the Phase 10.6 design) or accepting that "active interface" is now a pure presentation field that operators must not rely on for connection re-targeting.

The spec is pinned by `TestSatelliteFlagsLackControllerBindAddress` and `TestNodeCRDHasNoActiveField` in `pkg/satellite/stream/redial_spec_test.go`. If a future change implements Outcome A, both tests will fail and force the reviewer to replace them with a positive re-dial assertion.

**Related commits / tests**: `pkg/satellite/stream/redial_spec_test.go`; tests/scenarios/03-networking.md §3.10.

## Recommended next-fix order

1. **Bug 35** (P0, placer FreeCapacity) — only P0; blocks placer 1.0 and risks ENOSPC at create time.
2. **Bug 42** (P1, piraeus pod-CIDR drift) — blocks the iptables-mode e2e lane.
3. **Bug 36 + 37** (P1, VD props merge) — fix together; 37 depends on 36's merge plumbing.
4. **Bug 39 + 40** (P1, toggle-disk retry/cancel) — fix together; together they unlock Bug 34's Option B state machine.
5. **Bug 34 Option B** (P1 follow-up) — wrap migrate-disk in the new state machine once 39/40 land.
6. **Bug 38** (P2, cosmetic STATE_INFO on shrink) — pure UX, do last.
7. **Bug 32** (P2, observation) — document only; no code fix needed.
