# Group 5 — DRBD state observation & recovery

The largest group. Covers the satellite observer's translation of
DRBD `events2` frames into `Resource.Status`, the full set of DRBD-9
states that must reach `linstor r l` output, the recovery decision
tree from the cozystack `drbd-recovery` SKILL, the named fix recipes,
the mass-incident SOP, and the forbidden-action guardrails.

Most state-translation tests are **unit** (canned events2 frames →
observer → status). Most recovery tests are **e2e** (need real DRBD
kernel for state induction). Forbidden-action tests mix static
analysis with reconciler-survival e2e.

[Group index in README.md](README.md).

---

## Observer translation (events2 → Resource.Status)

### 5.1 events2 `change resource role:Primary` → `Status.InUse=true` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #15; drbd-troubleshooting (resource-kind frames); PLAN.md observer

**Unit:** `pkg/satellite/controllers/observer_test.go` — feed `change resource ... role:Primary` → `translateEvent` emits `resourceObservation{InUse:true}` → status writeback flips Resource.Status.InUse.
**E2E:** mount /dev/drbd<N> → Pod gets Primary → `linstor r l` shows InUse.

### 5.2 events2 `change device disk:UpToDate` → `Status.Volumes[i].DiskState` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L

**Unit:** canned `change device ... disk:UpToDate` → status writes UpToDate; `disk:Inconsistent` → Inconsistent.
**E2E:** mid-sync state visible in `linstor v l`.

### 5.3 events2 `change connection` → per-peer `Status.Connections[].Message` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #3

**Unit:** feed each connection-state transition (Connected, StandAlone, Connecting, Disconnecting, BrokenPipe, NetworkFailure, Timeout, Unconnected) → observer emits matching observation; merge into `Connections[]`.
**E2E:** iptables-drop peer → linstor r l shows StandAlone(<peer>) within 30s; remove drop → Connected.

### 5.4 events2 `destroy connection` deletes peer from Connections[] — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #13; PLAN.md observer destroy-event fix this session

**Why:** Removed peer must vanish from Conns column, not linger as ghost StandAlone.

**Unit:** `pkg/satellite/controllers/observer_test.go` — feed `change connection ... action:destroy` → `Removed:true` → `mergeConnections` deletes peer.
**E2E:** `linstor r d worker-3 test` → peer's Conns column shrinks within 10s.

### 5.5 Offline satellite → Resource State `Unknown` (last-known preserved) — S, missing test

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill A1

**Why:** When satellite stream drops, observer must mark resource Unknown — not blank, not stale UpToDate. Otherwise operator can't tell if the data is fresh.

**Unit:** satellite agent shutdown → controller-side observation pipeline tags Resource.Status as Unknown after timeout.
**E2E:** `ssh worker-3 'sudo systemctl stop kubelet'` → `linstor r l -r test` shows worker-3 row State=Unknown.

### 5.6 SyncTarget progress reported but not interfered with — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill A4, A5

**Unit:** `change device ... replication:SyncTarget` → status reports SyncTarget; subsequent reconciler runs do NOT issue `drbdadm adjust`.
**E2E:** mid-sync state, attempt `rd sp <key>=<val>` → applies prop but defers `.res` re-render OR uses a render path that doesn't trigger adjust.

### 5.7 TieBreaker vs Diskless distinction — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #2

Autoplacer-stamped tiebreaker → State=`TieBreaker`. Operator-requested `linstor r c --diskless` → State=`Diskless`. Same kernel-level DRBD-9 diskless underneath; different LINSTOR-side semantics.

---

## Per-state reporting wire (regression-guard each state)

For each DRBD-9 state, induce + assert it surfaces correctly.

### 5.8 UpToDate ↔ Outdated under drain/uncordon — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #1

2-replica RD; drain worker-2 → State Outdated/Connecting; uncordon → Outdated → SyncTarget → UpToDate. State column never lands in `Unknown` (degrades gracefully).

### 5.9 Inconsistent surfaces during interrupted sync — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting #4

Write 1 GiB on Primary, kill secondary satellite mid-sync → `linstor r l` shows Inconsistent; `linstor v l` per-volume DiskState=Inconsistent.

### 5.10 StandAlone after partition surfaces in Conns — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #3

iptables-drop tcp/<port>, wait 30s, expect StandAlone or Connecting; restore, expect Ok within 30s.

### 5.11 SkipDisk auto-set on I/O errors — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"SkipDisk" (lines 4428-4460); ug9-features 3.4

DRBD reports UpToDate → Failed → Diskless on I/O error → LINSTOR auto-sets `DrbdOptions/SkipDisk=True` → satellite passes `--skip-disk` to `drbdadm adjust`. `linstor r l` shows `(R)` marker.

**Status:** Not implemented (no `SkipDisk` matches in `grep`).

**Unit (after implement):** observer detects UpToDate→Failed transition → writes prop; reconciler reads prop → passes flag.
**E2E:** inject I/O error via `dmsetup` error target → observer reports the transition → prop set → `(R)` visible → after cleanup, manual `r sp <node> <rsc> DrbdOptions/SkipDisk` (no value) clears.

**Why P1:** without SkipDisk, a single failed disk can wedge `drbdadm adjust` cluster-wide.

---

## Recovery decision tree (Cozystack SKILL Group A)

### 5.12 Branch: Unknown — verify on node — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** recovery-skill A1

Build on 5.5. Stop worker-3's kubelet → all three observability levels must agree on Unknown.

### 5.13 Branch: DELETING stuck — convert + toggle-disk path — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill A2

Satellite is gone; `r d` blocked by Unknown copy. Recipe: `rd sp ... quorum off` + `r td --diskless` + retry `r d`. Test: `r td --diskless` succeeds against blockstor; observer status updates after FlagDelete clears.

**Failure mode:** `r td --diskless` returns 501 → REST gap (verify the toggle-disk handler accepts this transition).

### 5.14 Branch: StandAlone — `connect --discard-my-data` on outdated side — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill A3; drbd-troubleshooting #10

2-replica force-split, Primary on worker-1; recipe: `drbdadm secondary --force` + `disconnect` + `connect --discard-my-data` on worker-2; `disconnect` + `connect` on worker-1. Primary never loses Primary-ship; worker-2 SyncTarget → UpToDate within 10s.

**Reconciler-survival:** blockstor's satellite must NOT fight `--discard-my-data` by re-issuing `drbdadm adjust` and reverting the side selection.

### 5.15 Branch: Inconsistent/Outdated auto-recovers — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** recovery-skill A4

Inconsistent should auto-recover once peers connect. Test verifies blockstor doesn't interfere — observer reports the progression Inconsistent → SyncTarget → UpToDate without operator intervention.

### 5.16 Branch: SyncTarget — do not interfere — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill A5

Cross-listed with 5.6. Mid-sync, reconciler must not issue `drbdadm adjust` that would abort the resync.

### 5.17 Branch: false Diskless — `r mkavail --diskful` re-registers — T

- **Priority:** P1  **Target:** e2e  **Complexity:** H (implement first)
- **Source:** recovery-skill A6; UG9 §"Toggling..." (lines 3631-3640)

After failed deletion, LINSTOR thinks Diskless but ZVOL + DRBD device alive on node. Recipe: `linstor r mkavail --diskful worker-3 fakediskless` — idempotent re-register without creating a fresh ZVOL.

**Status:** likely not implemented as a distinct endpoint. May need `pkg/rest/resource_toggle_disk.go` extension or new `r mkavail` handler.

**E2E (after implement):** induce state via force-strip (controlled experiment) → `r mkavail --diskful` re-registers without resync → observer picks up state within 10s.

**Failure modes:** creates fresh ZVOL → data loss / double-allocation.

### 5.18 Branch: Inconsistent replica blocking others — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill A7

Corrupt one replica's ZVOL while satellite down; restart; `r d <bad-node> <rd>` + `rd ap` → cluster goes 2/3 (quorate) → new replica syncs UpToDate. Primary I/O uninterrupted.

---

## Fix-recipe contracts (SKILL Group B)

### 5.19 Fix: TCP port collisions via `r deact` + `r act` — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M
- **Source:** recovery-skill B1

Two resources share a port; `linstor r deact` + `r act` reallocates via `LayerData` path. Known related upstream bug: toggle-disk doesn't preserve TCP ports (#476 fix-PR); deact+act is the WANT-new-port recipe.

**Unit:** REST handler for deact returns 200 and triggers teardown; act re-allocates.
**E2E:** induce collision → `r deact` + `r act` → `linstor rd lp` shows fresh TcpPort.

### 5.20 Fix: Suspended I/O (quorum lost) — `quorum off` + `resume-io` — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B2

3-replica, crash 2 → Primary suspends I/O; recipe `rd sp ... quorum off` (persists through satellite restart) + `drbdadm resume-io` (node-level) + restore `quorum majority` once peers return.

**Key assertion:** prop persists through satellite restart (write into `.res`, not just controller memory). `grep quorum /var/lib/linstor.d/<rd>.res` shows `off`.

### 5.21 Fix: Stuck SyncTarget — disconnect+connect to source — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** recovery-skill B3

Stall sync via iptables; recipe `drbdadm disconnect <rsc>:<source>` + `connect <rsc>:<source>` resumes. Test: blockstor reconciler stays quiet during the manual sequence.

### 5.22 Fix: Dual-Primary (Unused side demote) — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B4

Provoke dual-primary via `drbdadm primary --force` on the Unused side. Recipe: `drbdadm secondary --force` on the Unused; demotes cleanly. Both-InUse case is **operator-mediated** (documented walkthrough, not automated — needs human approval).

### 5.23 Fix: Bitmap drop / "Can not drop the bitmap" — P (xfail on 9.2.17+)

- **Priority:** P2  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B5; SKILL upstream-bug #1

DRBD 9.2.16 bitmap race during diskful→diskless toggle. Recipe: disconnect + `connect --discard-my-data`. **Skipped** (xfail) if kernel ≥ 9.2.17 (fixed upstream).

Record kernel version in test report.

### 5.24 Fix: Node-ID mismatch — recreate replica — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B6; drbd-troubleshooting #11; PLAN.md Phase 8.1 invariant

DRBD complains `Peer presented a node_id of X instead of Y`. Recipe: `linstor r d <wrong-node> <rsc> && linstor rd ap <rsc>`. **Phase 8.1 invariant:** `Status.DRBDNodeID` is stable across churn — test churns RD 5 times in a loop; dmesg clean of `node_id` errors.

### 5.25 Fix: PausedSyncS / resync-suspended:dependency — S

- **Priority:** P2  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B7

Sync paused due to graph dependency. Recipe: `drbdadm disconnect <rsc>:<primary>` + `connect <rsc>:<primary>`. Test: sync resumes within 5s.

---

## Forbidden actions / safety rails (SKILL Group C)

### 5.26 Never `drbdadm down` on Primary/InUse from reconciler — S

- **Priority:** P0  **Target:** integration  **Complexity:** M
- **Source:** recovery-skill C1

**Method:** wrap `drbdadm` with logging shim on a test satellite; run the full reconciler test suite; grep for `down` lines targeting Primary resources → zero matches.

Or use `FakeExec` in `pkg/satellite/reconciler_drbd_test.go` and assert no `down` command issued during Primary-resource operations.

### 5.27 Never strip finalizers from reconciler code — S

- **Priority:** P0  **Target:** unit (static analysis)  **Complexity:** L
- **Source:** recovery-skill C2; this session's incident

`grep -r 'finalizers.*nil\|finalizers.*\[\]' pkg/ cmd/` → only the satellite cleanup path may remove its OWN finalizer after kernel teardown.

**Why this matters:** Force-strip leaves DRBD kernel state alive holding ports → port collision on subsequent placements. This session's reproduction.

### 5.28 Never `linstor node lost` from controller retry loop — S

- **Priority:** P0  **Target:** unit (static analysis)  **Complexity:** L
- **Source:** recovery-skill C3

`grep -r 'NodeLost\|node.lost\|/v1/nodes/.*/lost' --include='*.go' cmd/controller cmd/apiserver pkg/satellite pkg/controllers` → only the REST handler in `pkg/rest/node_lifecycle.go` matches. Controllers never auto-call lost — operator-only intent.

### 5.29 Operator disconnect from satellite shell survives ≥30s — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting #5

Operator runs `drbdadm disconnect test` on a satellite container; reconciler does NOT auto-reconnect within 30s. After `connect`, state returns to Ok.

**Open design question:** does blockstor have a per-resource `Aux/operator-managed=true` prop that pauses reconciler? Test should exercise that prop if so; otherwise document the gap.

### 5.30 `drbdadm primary --force` not auto-undone — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** observability #7

Emergency: operator force-promotes when peer is unreachable. Reconciler treats Primary role as observed state, not Spec — doesn't demote. After 30s recheck, role:Primary persists.

### 5.31 `--discard-my-data` misuse doesn't amplify damage — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill C4

If operator runs `connect --discard-my-data` against the only UpToDate copy (the SKILL forbids it; operators make mistakes under pressure), blockstor must NOT auto-replicate the discard. Data loss contained to that side only.

---

## Reconciler-survival under raw drbdadm

### 5.32 `drbdadm down` reverses on next reconcile cycle — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #6; observability #6

Operator down → kernel empty briefly → reconciler re-renders `.res` + `drbdadm up` → Connected within 30s.

### 5.33 Stuck SyncTarget recovers via down+up cycle — P

- **Priority:** P2  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting #9

`drbdadm down + up` while Unused only — recipe is in SKILL Group B but blockstor's reconciler must stay quiet for the brief window. P2 because Stuck SyncTarget is rare and 5.21 (reconnect-to-source) usually fixes it first.

### 5.34 Orphaned diskless after force-strip — S (defensive sweeper open)

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting #12

After force-strip (operator-error), DRBD kernel state survives even though Resource CRD is gone. Recovery: `drbdsetup down <res>` on the satellite. **Open question:** should blockstor's satellite have a periodic sweeper that runs `drbdsetup down <res>` for kernel-resident DRBD resources with no matching Resource CRD on the local node? Would close the force-strip-aftermath loop.

---

## Mass-incident SOP (SKILL Group D)

### 5.35 Mass-incident pipeline test — S

- **Priority:** P1  **Target:** e2e  **Complexity:** H (test harness)
- **Source:** recovery-skill D1

6-replica cluster, 30 resources; induce: kill 2 workers + force-fail 5 into StandAlone + corrupt 3 ZFS volumes + apply lost-quorum taint on 2 nodes. Execute 7-step procedure (taints → DELETING → StandAlone → Connecting → Inconsistent → quorum restore → verify). All Primary-InUse workloads survive uninterrupted.

**Run cadence:** nightly on the burnin stand.

### 5.36 Resource-prioritization: zero-UpToDate first — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** recovery-skill D2

`linstor r l --faulty` ranks resources by absence of UpToDate replicas. Validates the recovery copilot's prioritization.

---

## Recovery copilot contract (SKILL Group E)

### 5.37 `r l --faulty` returns complete remediation state — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill E1

Per resource: node-name, volume index, per-volume disk-state, per-peer conn-state, DRBD `in_use` flag. Each missing field = extra REST roundtrip for the copilot.

**Unit:** REST handler returns full envelope.
**E2E:** induce faulty state via 5.20, verify `linstor r l --faulty -o json | jq` has all 5 fields.

### 5.38 `error-reports` API surfaces parseable error chains — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill E2

`GET /v1/error-reports` honours `node`, `since`, `limit` query params; returns JSON with `text` field. Used by copilot to filter "bitmap errors only" or "node X errors in last hour".

---

## Implementation-order recommendation

1. 5.1–5.4 — observer translation (existing surface, missing/partial tests)
2. 5.5, 5.7 — observer edge cases (offline node, TieBreaker vs Diskless)
3. 5.27, 5.28 — static-analysis safety rails (cheap, high-value)
4. 5.8, 5.9, 5.10 — per-state regression-guards (e2e, but simple inductions)
5. 5.26 — reconciler safety rail (logging shim) — needs harness
6. 5.12–5.16, 5.18 — decision-tree branches (mostly e2e, real DRBD inductions)
7. 5.19, 5.20, 5.22, 5.24 — fix-recipe contracts (P0/P1)
8. 5.30, 5.31, 5.32 — reconciler-survival under operator commands
9. 5.6, 5.21 — SyncTarget interference + stuck-sync (cross-listed)
10. 5.11 — SkipDisk (P1, implement first)
11. 5.17 — false-Diskless mkavail (P1, implement first)
12. 5.29, 5.34 — operator-shell scenarios + sweeper open question
13. 5.35, 5.36, 5.37, 5.38 — mass-incident + copilot contract
14. 5.23, 5.25, 5.33 — P2 edge cases

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 5 |
| P0 e2e | 12 |
| P1 unit | 2 |
| P1 e2e | 10 |
| P2 e2e | 4 |
| T (implement first) | 2 (SkipDisk, mkavail) |

**Largest group by far** — DRBD state semantics + recovery is the
domain that makes blockstor a viable LINSTOR replacement. Land 5.27,
5.28 (static analysis) on day one — zero infra cost, prevents
regressions of this session's force-strip incident.
