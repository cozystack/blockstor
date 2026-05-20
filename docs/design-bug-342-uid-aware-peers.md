# Design — UID-aware peer tracking + adoption mode

**Status**: Draft (revision 2 — post-review)
**Closes**: Bug 342 (Phase-3 relocate zombie connection slot)
**Enables**: Disaster recovery without downtime, LINSTOR → blockstor migration

## Revision log

**rev 2** — addresses reviewer feedback:
1. Pass 2 `forget-peer` keyed on `actual.NodeID` (kernel-observed), not `expected.NodeID` — pass 1 already removes the wrong-id slot when node-id is reallocated to a new name; pass 2 must mirror that for re-incarnation under the same name.
2. Pass 3 zombie probe debounced — `connection:Connecting` AND no peer-device for >`zombieGraceSeconds` (default 30s), cross-checked against `drbdsetup events2` if available. Avoids killing in-flight handshakes.
3. Adoption-mode startup rollout jittered + opportunistic stamping to avoid thundering herd on N-RD clusters.
4. Adoption gate adds PSK equivalence check — Spec.Auth.Secret vs in-kernel secret (via `drbdsetup show -j .connections[].net.shared-secret`). If mismatch, force one no-tear-down `adjust` (re-apply connection config without del-peer) before stamping UIDs.
5. Test plan extended with race / PSK / node-id-reuse / stamp-idempotency cases.

## Problem statement

Two distinct but related issues share a single missing primitive — the satellite has no persistent notion of *which incarnation* of a peer Resource CR it last configured into the DRBD kernel.

### Bug 342 (Phase-3 relocate)

`linstor r d X` immediately followed by `linstor r c X` (sub-second window) produces this sequence:

```
t=0.00   r d X      → K8s Resource X.* marked for deletion, finalizers strip
t=0.05   gone       → K8s Resource X.* fully deleted
t=0.10   r c X      → K8s Resource X.* re-created with new metadata.uid
t=0.20   satellite informer fires once, sees X.* present in K8s with new UID
t=0.30   reconcile  → X in old .res ✓ AND X in new desired ✓ → diff empty → no del-peer
                   → drbdadm adjust sees kernel slot for X already exists → skips new-peer-device
                   → zombie slot wedges forever in Connecting
```

The kernel slot is bound to incarnation 1's identity (PSK seed, connection handshake state, internal UID). Incarnation 2's handshake against that slot never completes — DRBD-9 reports `connection:Connecting` indefinitely, with no peer-device registered for any volume.

### Disaster recovery / LINSTOR migration

`etcd` restore from backup brings back Spec but resets Status. Reconcile with empty `Status.AppliedPeerUIDs` and live kernel state must NOT trigger del-peer + adjust cycle (would tear down running connections = downtime). Same shape applies when blockstor adopts a node previously driven by upstream LINSTOR: kernel and DRBD on-disk metadata are intact, but K8s has no recorded history of which UIDs were last configured.

### Why the existing .res-file-based diff doesn't catch it

`pkg/satellite/reconciler.go::computeRemovedPeers` reads the previously-rendered `.res` and diffs against `DesiredResource.GetPeers()` (a `[]string` of peer node names). This is blind to:

1. **Same name, different identity** — Bug 342
2. **Missing .res file** — pod re-scheduled on a fresh host, .res wiped, no historical state
3. **Stale .res file** — host's /etc/drbd.d preserved across blockstor version bumps where rendering changed

`.res` is a derived render artifact, not source of truth.

## Proposed design

Two complementary primitives. Each closes a class of failures independently; together they're robust under disaster recovery and identity-change scenarios.

### 1. UID-aware peer descriptor

#### Schema change — `Resource.Spec.Peers[]` and `Status.AppliedPeerUIDs`

```go
// api/v1alpha1/resource_types.go (Status — new field)
type ResourceStatus struct {
    // existing fields preserved...

    // AppliedPeerUIDs records the metadata.uid of each peer Resource CR
    // at the time of the last successful `drbdadm adjust`. Used by the
    // satellite to detect peer re-incarnation (same node name, new UID)
    // and force `drbdadm del-peer` + `drbdmeta forget-peer` before the
    // next adjust pass — without it, the DRBD kernel zombie-slot from
    // the previous incarnation wedges the new peer in Connecting forever
    // (Bug 342).
    //
    // Stamped by the local satellite after every successful adjust.
    // Empty on first reconcile and after disaster recovery — see
    // adoption-mode below.
    AppliedPeerUIDs map[string]string `json:"appliedPeerUIDs,omitempty"`
}
```

#### Wire change — `DesiredResource.Peers []DesiredPeer`

```go
// pkg/satellite/intent/types.go
type DesiredPeer struct {
    Name        string  // peer node name (already in .res `on <name> {`)
    NodeID      int32   // DRBD node-id (already in .res `node-id N`)
    ResourceUID string  // peer Resource CR metadata.uid — NEW
}

type DesiredResource struct {
    // ...existing fields preserved...
    Peers []DesiredPeer  // was []string
}
```

Dispatcher (`pkg/dispatcher/dispatcher.go`) already enumerates sibling Resource CRDs to build the peer list — adds one field read:

```go
for _, sib := range siblings {
    peers = append(peers, intent.DesiredPeer{
        Name:        sib.Spec.NodeName,
        NodeID:      sib.Status.DRBDNodeID,   // already stamped
        ResourceUID: string(sib.UID),         // NEW — metadata.uid
    })
}
```

Call sites that need just names get a thin helper `dr.GetPeerNames() []string` for backwards compatibility.

### 2. Diff logic with kernel-state probe

Replace `computeRemovedPeers` (which reads old .res) with a stateless three-source diff:

```go
// pkg/satellite/reconciler.go (new, replaces tearDownRemovedPeers)
func (r *Reconciler) reconcilePeers(ctx context.Context, dr *intent.DesiredResource,
    localRes *v1alpha1.Resource) error {

    expected := indexByName(dr.Peers)              // K8s desired (with UIDs)
    applied  := localRes.Status.AppliedPeerUIDs    // what I last configured
    actual,  := drbdsetupShow(ctx, dr.GetName())   // kernel actual

    // Pass 1: peers in kernel that aren't in K8s desired → del-peer + forget-peer
    for slot := range actual {
        if _, want := expected[slot.Name]; !want {
            r.cfg.Adm.DelPeer(ctx, dr.GetName(), slot.Name)
            r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), volNum, devicePath, slot.NodeID)
        }
    }

    // Pass 2: UID mismatch (Bug 342) — same name, new identity in K8s.
    //
    // CRITICAL: forget-peer is keyed on the KERNEL-OBSERVED node-id
    // (slot.NodeID from `actual`), NOT on expected.NodeID. Bug 87
    // (allocator) sometimes reissues a new node-id for the new UID
    // — the kernel zombie is bound to the OLD node-id, and freeing
    // the new id would leak a GI slot occupied by the dead incarnation.
    for name, peer := range expected {
        if last := applied[name]; last == "" || last == peer.ResourceUID {
            continue
        }
        slot, ok := actual[name]
        if !ok {
            continue // kernel doesn't know this peer; adjust will add fresh
        }
        r.cfg.Adm.DelPeer(ctx, dr.GetName(), name)
        for _, vol := range dr.GetVolumes() {
            r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), vol.Number, vol.Device, slot.NodeID)
        }
    }

    // Pass 3: zombie-slot probe — DEBOUNCED to avoid killing in-flight
    // handshakes. `peer-device` absent could mean (a) zombie from prior
    // incarnation OR (b) handshake started 200ms ago and hasn't completed.
    // Debounce by:
    //   - require connection state in {Connecting, StandAlone} for >= zombieGraceSeconds
    //   - cross-check with `drbdsetup events2` last-state-change timestamp if available
    //   - default zombieGraceSeconds = 30s (configurable via env BSTOR_ZOMBIE_GRACE_S)
    // Multi-volume safe: zombie state requires NO peer-device for ANY vol
    // (a partial-handshake mid-flight may have vol 0 registered but vol 1 still pending).
    for name, slot := range actual {
        if !slot.IsConnectingOrStandalone() {
            continue
        }
        if slot.SecondsSinceLastStateChange() < zombieGraceSeconds {
            continue // handshake may still complete
        }
        if slot.HasAnyPeerDeviceConfigured(dr.GetVolumes()) {
            continue // partial registration — let DRBD finish handshake
        }
        log.Warn("zombie slot detected, forcing cleanup",
            "peer", name, "ageSeconds", slot.SecondsSinceLastStateChange())
        r.cfg.Adm.DelPeer(ctx, dr.GetName(), name)
        for _, vol := range dr.GetVolumes() {
            r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), vol.Number, vol.Device, slot.NodeID)
        }
    }

    // .res render + adjust happens after; adjust re-registers peers + peer-devices
    return nil
}

// After successful adjust, stamp Status.AppliedPeerUIDs
func (r *Reconciler) stampAppliedPeerUIDs(ctx context.Context, localRes *v1alpha1.Resource,
    expected map[string]intent.DesiredPeer) error {
    next := make(map[string]string, len(expected))
    for name, peer := range expected {
        next[name] = peer.ResourceUID
    }
    // Status subresource patch — avoid stomping observer-stamped fields
    return r.cfg.Status.Patch(ctx, localRes, func(r *v1alpha1.Resource) {
        r.Status.AppliedPeerUIDs = next
    })
}
```

### 3. Adoption mode (disaster recovery + LINSTOR migration)

Gate at the top of `reconcilePeers`:

```go
if len(localRes.Status.AppliedPeerUIDs) == 0 && actualHasConfiguredPeers(actual) {
    // Adoption path — Status is empty (fresh restore / first observation)
    // but kernel is configured. Trust kernel as baseline; stamp current
    // expected UIDs.
    //
    // Pre-condition: peer SET, NODE-IDs, PSK, and peer-device topology
    // all agree between expected and actual. Any disagreement → fall
    // through to normal diff.
    agree, reason := peerSetsAgree(expected, actual, dr.GetVolumes(), localRes.Spec.NetSecret)
    if !agree {
        log.Info("adoption refused, falling through to normal diff",
            "rd", dr.GetName(), "reason", reason)
    } else {
        // PSK equivalence check failed but everything else matched:
        // force ONE no-tear-down adjust (re-apply connection config
        // without del-peer) so kernel rotates to the K8s-Spec PSK.
        if reason == "psk_mismatch_recoverable" {
            log.Info("adoption: PSK rotation via adjust without tear-down")
            r.cfg.Adm.AdjustWithoutTearDown(ctx, dr.GetName())
        }

        // Stagger the Status patch to avoid thundering herd on N-RD
        // clusters at startup (e.g. 1000 RDs * 6 nodes = 6000 patches
        // racing apiserver). Jitter window scales with concurrent
        // reconciler workers; default = rand[0, 30s).
        if r.firstReconcileAfterBoot() {
            time.Sleep(jitterAdoption())
        }

        log.Info("adoption mode: stamping baseline UIDs from observed kernel state",
            "rd", dr.GetName(), "peers", len(actual))
        return r.stampAppliedPeerUIDs(ctx, localRes, expected)
    }
}
```

`peerSetsAgree(expected, actual, vols, specSecret)` checks:
1. **Name set equality** — same peer node names on both sides
2. **Node-id parity** — each peer's DRBD node-id matches between K8s expected and kernel actual
3. **Per-volume peer-device presence** — for every (peer, vol) pair, kernel has a registered peer-device (no zombies)
4. **PSK equivalence** — Spec.NetSecret matches `drbdsetup show -j .connections[].net.shared-secret` for every peer slot

Returns `(bool, reason)`:
- `(true, "")` — full agreement, safe to stamp Status and return
- `(true, "psk_mismatch_recoverable")` — everything matches except PSK; perform single `drbdadm adjust --skip-disk --skip-net=false` style re-apply that rotates connection config without `del-peer`, then stamp
- `(false, "<specific_field>_mismatch")` — defer to normal diff path

#### Thundering-herd mitigation

On first satellite startup post-restore, every Resource hits adoption-mode simultaneously. For N=1000 RDs and 6 satellites the apiserver sees ~6000 Status patches in seconds, racing the observer's Status writes (Volumes/Conditions/DRBDState).

Mitigation, layered:

1. **Stagger the stamp**: random sleep `[0, 30s)` per-Resource before the Status patch fires. `firstReconcileAfterBoot()` returns true only for the initial pass; subsequent reconciles skip the sleep.
2. **Opportunistic stamping**: don't trigger reconciliation eagerly for adoption — let the normal Watch event stream drive the cadence. The first time a Resource's Spec changes (or the resync interval fires) is when adoption-mode runs.
3. **Status-subresource patching with conflict-retry**: ensure the patch uses `client.MergeFrom(original)` with resource-version check, and on conflict refetch + re-stamp. This must not race with the observer's `diskState` writes — the observer's domain is `Status.Volumes[]`/`Status.Conditions[]`, the adoption stamper's domain is `Status.AppliedPeerUIDs` — disjoint fields, conflict only on resource-version churn.
4. **Cluster-wide adoption-complete marker** (optional): controller stamps a `BlockstorAdoptionComplete=True` annotation on the namespace once all satellites report adoption-pass done, so subsequent restarts don't re-jitter.

If any mismatch → fall through to normal three-pass diff (safer default than blind adoption).

### 4. Why `.res` becomes derived-only

With the above, `.res` files are output-only renders consumed by `drbdadm adjust`. They:

- Can be wiped on every reconcile without losing diff input (we don't read them)
- Can be missing on pod restart / new host (regenerated from K8s + DesiredResource)
- Can disagree with kernel state harmlessly (adjust reconciles)

Existing `extractResFilePeers` / `extractResFilePeerNodeIDs` helpers can stay as `drbdsetup show` fallback (some environments have no `/run/drbd` namespace), but they're no longer the primary signal.

## Test plan

### Unit tests (`pkg/satellite/reconciler_test.go`)

| Test | Coverage |
|------|---------|
| `TestReconcilePeers_NoChange` | applied == expected → no del-peer calls |
| `TestReconcilePeers_PeerRemoved` | peer in applied, not in expected → del-peer + forget-peer |
| `TestReconcilePeers_PeerAdded` | peer in expected, not in applied → no del-peer (adjust adds) |
| `TestReconcilePeers_UIDChanged_Bug342` | same name, new UID → del-peer + forget-peer + adjust re-registers |
| `TestReconcilePeers_ZombieSlot` | kernel slot without peer-device for vol 0 → del-peer (heals Bug 342 even without UID hint) |
| `TestReconcilePeers_AdoptionMode_EmptyStatus_KernelHealthy` | Status empty, kernel matches expected → stamp UIDs, no kernel mutations |
| `TestReconcilePeers_AdoptionMode_PartialMismatch_FallsThrough` | Status empty, kernel disagrees with expected → normal diff path (not adoption) |
| `TestReconcilePeers_StampOnSuccess` | after adjust succeeds, AppliedPeerUIDs == expected |
| `TestReconcilePeers_NoStampOnAdjustFailure` | adjust returns error → AppliedPeerUIDs unchanged |
| `TestStampAppliedPeerUIDs_PreservesObserverFields` | stamp doesn't clobber Status.Volumes/Conditions/DRBDState |
| `TestReconcilePeers_DelPeerRaceCompletedHandshake` | zombie probe must NOT tear down a peer whose handshake completed within debounce window (verifies `zombieGraceSeconds` + `HasAnyPeerDeviceConfigured` cross-check) |
| `TestReconcilePeers_AdoptionMode_PSKMismatch_ForceAdjust` | Status empty, kernel state matches names+node-ids, but PSK differs → triggers single `AdjustWithoutTearDown`, then stamps |
| `TestReconcilePeers_AdoptionMode_PSKMatch_NoMutation` | Status empty, kernel state matches FULLY (PSK incl.) → pure stamp, zero kernel mutations |
| `TestReconcilePeers_NodeIDReused_OldSlotForgotten` | Bug 87 interaction: peer A removed (node-id 2), peer B added with same node-id 2 → pass 1 forget-peers node-id 2 BEFORE pass 2 sees B; B gets clean GI slot |
| `TestReconcilePeers_StatusPatchConflict_Retried` | concurrent observer patch causes resourceVersion conflict → retry refetches and re-stamps |
| `TestReconcilePeers_StampIdempotent` | repeat reconcile with unchanged expected/applied → no-op patch (no update churn) |
| `TestReconcilePeers_MultiVol_ZombiePartialRegistration` | kernel has peer-device for vol 0 but vol 1 missing for a peer → NOT zombie (handshake in progress), don't tear down |
| `TestReconcilePeers_AdoptionMode_Thundering_Herd_Jitter` | firstReconcileAfterBoot=true → adoption stamp delayed by jitter [0, 30s); subsequent calls skip jitter |
| `TestReconcilePeers_AdoptionMode_PartialMismatch_FallsThrough` | Status empty, kernel disagrees with expected on name set → no adoption, normal diff |
| `TestPeerSetsAgree_Cases` | exhaustive table-test of agree() return values for name/nodeId/PSK/peerDevice mismatch combos |

FakeExec for `drbdsetup show`, `drbdadm del-peer`, `drbdmeta forget-peer`, `drbdadm adjust`. Inject `actual` state via test fixtures.

### Integration tests (`tests/integration/`)

| Scenario | Validates |
|----------|----------|
| `phase3_relocate_bug_342` | `r d X` + `r c X` sub-second on real DRBD → both peers reach Connected within 30s (currently wedges in Connecting forever) |
| `etcd_restore_no_downtime` | tear out Status from CRDs, restart satellite, assert kernel state unchanged + Status repopulated + no I/O interruption |
| `linstor_takeover_adoption` | spin a resource via upstream LINSTOR, swap controller to blockstor, assert no DRBD restart and writes never blocked |

### E2E catcher cells (`tests/e2e/cli-matrix/`)

Existing `r-full-lifecycle.sh` already pins Bug 342 — currently FAILs. Should PASS after fix.

New cells:

- `tests/e2e/cli-matrix/peer-uid-incarnation.sh` — explicit Bug 342 mini-repro (faster than r-full-lifecycle, isolates the relocate step)
- `tests/e2e/cli-matrix/adoption-after-status-wipe.sh` — wipe `Status.AppliedPeerUIDs` via kubectl patch, observe satellite re-stamps without touching kernel
- `tests/e2e/cli-matrix/adoption-from-linstor.sh` — manually drbdadm-up a resource (mimicking LINSTOR's render), then create matching K8s CRDs, observe adoption + no DRBD restart

### Failure mode sweep

| Failure | Reconciler response |
|---------|---------------------|
| `drbdsetup show` returns error | Skip kernel-probe pass, fall back to UID-only diff. Log warning. |
| `del-peer` fails | Bubble up (current behavior). Log includes peer name + UID. |
| `forget-peer` fails | Non-fatal (current behavior — slot leak vs reconcile wedge). |
| Status patch conflict | Retry on conflict (k8s api standard). |
| Adoption gate triggered but `peerSetsAgree` mismatch | Fall through to normal diff. Log "adoption refused: <reason>". |

## Migration plan (per-stand rollout)

This is a Status-schema change + behavioral change. Need backward compatibility during rollout:

1. **Schema bump first**: add `AppliedPeerUIDs` field, CRD regen, deploy controller with new schema (old satellites ignore the new field).
2. **Satellite read-write second**: new satellite version stamps + reads AppliedPeerUIDs. First reconcile after upgrade triggers adoption-mode (Status is empty for all existing Resources) → stamps baseline UIDs without disrupting kernel.
3. **No data plane changes** — DRBD kernel state untouched throughout. Operator-visible behavior identical to pre-fix for steady-state RDs; only the Bug 342 fast-relocate scenario changes.

Rollback: if new satellite misbehaves, revert image. Old satellite ignores `Status.AppliedPeerUIDs` field (extra-field semantics are preserved by apimachinery). On next adjust by old satellite, behavior matches pre-fix.

## Why per-Resource Status, not per-RD Status

Reviewer asked: per-RD would simplify observability ("one place to ask: what UIDs does this RD see?"). Per-Resource has a contention argument but pays for it in operator UX.

**Trade-offs:**

| Aspect | Per-Resource | Per-RD |
|--------|--------------|--------|
| Write contention | Each satellite writes its own Resource → zero cross-node racing | Multiple satellites race the single RD Status; need controller-mediated stamper |
| Locality | Naturally aligns with "what kernel slot did THIS satellite configure" — the actual measurement | Aggregate view across nodes — but each satellite's measurement is local; aggregation is reconstruction |
| Observability | Operator lists N Resource CRs and mentally aggregates | Single RD object shows the full peer-UID matrix |
| Recovery tooling | Admin restoring a single Resource Status can't reseed cluster view | Single RD Status restore reseeds the cluster's view |
| Disagreement detection | Cross-Resource Status compare detects worker-1's vs worker-2's view drift | Single object — drift is observable in one place |

**Decision rationale**: write contention dominates for steady-state reconciliation throughput (every reconcile potentially stamps). Per-Resource avoids the controller-mediated stamper hop entirely. The observability gap is mitigated by:

- A `kubectl get rd <name> -o yaml`-friendly **derived view**: small controller stamps `RD.Status.PeerUIDView[node] = corresponding-Resource.Status.AppliedPeerUIDs[]` opportunistically (read-mostly, eventually-consistent). Operators querying the RD see the aggregate; satellites still write only their local Resource.
- A `linstor r l -v --peer-uids` CLI surface that aggregates client-side without server-side state.

Trade-off acknowledged: if the aggregator controller is ever removed, observability degrades to the per-Resource hop.

## Open questions

1. **DRBD `node-id` reuse**: blockstor's allocator (Bug 87 follow-up) reuses freed node-ids. If a peer Resource is deleted and a new one is created on a different node with the same node-id, our diff catches the name change (worker-3 → worker-5) but the kernel slot may need explicit forget-peer for the OLD node-id mapping. Current `tearDownRemovedPeers` calls forget-peer with the old node-id. New design preserves this — verify in unit test `TestReconcilePeers_PeerRemoved`.

2. **Multi-volume RDs**: zombie-slot check (pass 3) reads peer-device for vol 0. If a multi-volume RD has peer-device for vol 0 present but vol 1 missing, we miss it. Loop over all volumes in `dr.GetVolumes()` — test `TestReconcilePeers_ZombieSlot_MultiVol`.

3. **Adoption-mode acceptance criteria**: `peerSetsAgree` is conservative — what if kernel has a peer NOT in K8s expected (operator manually drbdadm'd outside blockstor)? Current proposal: fall through to normal diff → del-peer. Alternative: log warning, ignore unknown peer, adopt the known subset. Operator-experience trade-off; default to the safe del-peer path.

4. **`forget-peer` permanence**: once we forget a peer, its GI slot is freed but the per-peer bitmap is gone. Next adjust re-registers and triggers a full resync (no GI history to skip-sync from). This is unavoidable — Bug 342's whole point is the peer's identity changed, so resync IS correct. Document explicitly.

## Non-goals

- Replacing peer discovery via labels or selectors (current sibling-listing via RD name works fine)
- Persisting kernel state to K8s as authoritative (kernel stays authoritative for runtime, K8s stays authoritative for config)
- Cross-cluster peer identity (each cluster's UIDs are independent — this design is per-cluster)
- Automatic LINSTOR teardown (operators control that separately; we just adopt cleanly)

## Reviewer's verdict (rev 1) — addressed

External review (separate agent, 2026-05-20) returned **ship-with-revisions** with 5 blocking concerns:

1. ✅ Pass 2 `forget-peer` keyed on `actual.NodeID`, not `expected.NodeID` — fixed in rev 2 pseudocode.
2. ✅ Pass 3 zombie probe debounced via `zombieGraceSeconds` + `HasAnyPeerDeviceConfigured` cross-check.
3. ✅ Adoption-mode thundering herd: jitter window + opportunistic stamping + per-namespace adoption-complete marker.
4. ✅ Adoption gate PSK equivalence check + `AdjustWithoutTearDown` recovery path.
5. ✅ Test plan extended with race / PSK / node-id-reuse / stamp-idempotency / multi-vol cases (10 → 19 unit tests).

Non-blocking concerns also addressed:
- Per-Resource vs per-RD trade-off section added with derived-view mitigation.
- Multi-volume zombie probe explicit in pass 3 pseudocode and test plan.

## References

- Bug 342 root cause memo: `~/.claude/projects/-Users-kvaps-git-linstor-server/memory/bug_342_phase3_relocate.md`
- Existing `tearDownRemovedPeers`: `pkg/satellite/reconciler.go:1095-1199`
- Existing peer-name discovery in dispatcher: `pkg/dispatcher/dispatcher.go` (search for `siblings`)
- DRBD `drbdsetup show -j` output format: drbd-utils source
