# Group 7 — Quorum, observability, capacity, copilot contract

The cross-cutting group. Covers:

- **Quorum policies**: `auto-quorum`, `AutoAddQuorumTiebreaker`,
  `suspend-io` vs `io-error`, `on-no-data-accessible`,
  tiebreaker reconciler.
- **Three-level cross-layer observability**: K8s ↔ LINSTOR ↔ Node
  consistency, cheat-sheet narrowing flow, error correlation.
- **Error-reports API + faulty filter + copilot data contract**:
  the REST surface the cozystack drbd-recovery copilot consumes.
- **Capacity gates**: over-subscription ratios
  (`MaxFreeCapacity-`, `MaxTotalCapacity-`, `MaxOversubscriptionRatio`).
- **QoS**: sysfs blkio throttle props (mostly P3 for cozystack).

[Group index in README.md](README.md).

---

## Quorum policies

### 7.1 Default `auto-quorum` triggers at quorum-achievable thresholds — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Auto-quorum policies" (lines 4234-4279); PLAN.md `storetest.go` pins `DrbdOptions/auto-quorum: io-error`

When ≥ 2 diskful + 1 diskless OR ≥ 3 diskful, LINSTOR auto-configures `quorum majority + on-no-quorum io-error`. Below threshold, auto-disables.

**Unit:** `rd l` after spawn → `DrbdOptions/Resource/quorum=majority`.
**E2E:** spawn 3-replica RD → `linstor rd lp <rsc> | grep quorum` shows `majority`; render the `.res` and verify `options { quorum majority; ... }` block.

### 7.2 `auto-quorum=suspend-io` for VM workloads — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Auto-quorum policies"; advanced-config #6; KB:linstor-quorum-policies-and-vm-environments

**Why:** Default `io-error` makes VMs go read-only on quorum loss. `suspend-io` blocks I/O — VM stays running, resumes on quorum return.

**Unit:** `linstor rg modify default DrbdOptions/Resource/auto-quorum suspend-io` → spawn-time `.res` includes `on-no-quorum suspend-io`.
**E2E:** mount on Primary → `dd` write loop → partition (break quorum) → `dd` hangs (process in D state, not error). Restore quorum → `dd` resumes; no filesystem corruption.

### 7.3 Manual quorum override via `auto-quorum=disabled` — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Auto-quorum policies" (lines 4248-4275)

Set `DrbdOptions/auto-quorum=disabled` → manually set `DrbdOptions/Resource/quorum off` and `DrbdOptions/Resource/on-no-quorum` (empty → deletes prop). Spawn-time `.res` reflects manual setting.

### 7.4 `on-no-data-accessible` for VM workloads — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #6

Companion to 7.2. `DrbdOptions/Resource/on-no-data-accessible=suspend-io` keeps the VM alive when all replicas become inaccessible. `.res` renderer surfaces it.

### 7.5 `Fix: Suspended I/O (quorum lost)` — quorum off persistence — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** recovery-skill B2; cross-listed with 5.20

`linstor rd sp <rsc> DrbdOptions/Resource/quorum off` MUST persist through satellite restart. Key assertion: `grep quorum /var/lib/linstor.d/<rsc>.res` shows `off` after bouncing the satellite pod.

---

## Tiebreaker reconciler

### 7.6 `ensureTiebreaker` stamps 3rd diskless replica — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #10, #11; PLAN.md `internal/controller/ensure_tiebreaker_test.go`

After 2 diskful are placed, controller stamps a 3rd diskless on the remaining worker. Reconciler picks the node with most free space (or deterministic fallback per `pick_tiebreaker_test.go`).

**Unit:** in-memory store with 2 diskful + 3 candidate nodes → reconciler picks the 3rd correctly.
**E2E:** `linstor rg spawn default test 1G` (place-count=2) → 3 rows, one State=TieBreaker.

### 7.7 `AutoAddQuorumTiebreaker=false` disables stamping — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #12; advanced-config #7

Operator wants exactly 2 replicas (e.g., 2-node DRBD pair with external quorum). Setting the prop on RG → spawn produces 2 rows only, no tiebreaker.

**Failure mode:** subsequent `linstor r d <node> test-no-tb` triggers tiebreaker auto-recreation despite prop → reconciler not consulting the override.

### 7.8 Tiebreaker reconciler doesn't fight manual `r d` — S

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** L
- **Source:** PLAN.md `remove_witnesses_test.go`

When operator deletes the tiebreaker (via `linstor r d`), reconciler waits before re-stamping — gives operator a window to set `AutoAddQuorumTiebreaker=false` if they meant to disable. Or stamps immediately if the prop allows it — pin which behaviour.

---

## Three-level cross-layer consistency

### 7.9 PVC ↔ Resource CRD ↔ DRBD device three-way match — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** observability #1

**Why:** Foundational invariant. The whole cheat-sheet narrowing depends on the three levels agreeing.

**E2E:** Bind a PVC, then assert:
- PV volumeName == linstor RD name == `.res` `resource` block
- DRBD port: `linstor r l` Port == `.res` `address` port == satellite `netstat -l` shows LISTEN
- DRBD minor: `linstor v l` MinorNr == `/dev/drbd<N>` == `.res` `volume 0 { device /dev/drbdN minor N; }`

Test in `tests/e2e/cheat-sheet-three-way.sh`.

**Failure modes:**
- PVC Bound but Resource CRD not stamped → bound-too-early bug
- `linstor r l` shows UpToDate but `.res` missing → satellite reconciler didn't render
- DRBD minor in `.res` ≠ kernel → minor allocator desync (Phase 8.1)

### 7.10 K8s-side error narrows to level 1 without descending — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** observability #2

PVC with `storagePool: nonexistent-pool` → stays Pending → `kubectl describe pvc` Events + piraeus-csi-controller logs contain the pool name + "not found" within 30s. Operator stops at level 1.

### 7.11 LINSTOR ↔ Node bridge: iptables drop confirms state at both levels — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** observability #4

Drop tcp/<drbd-port> on worker-2; `linstor r l` (level 2) shows StandAlone(worker-2); `drbdadm status` on worker-1 (level 3) shows same; `dmesg` shows TCP failure with matching timestamp. Both views must agree.

### 7.12 CSI ↔ DRBD permission-denied chain (diskless attach) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** observability #8

Pod scheduled on a no-replica node → CSI publishes via DRBD diskless → kernel rejects until diskless attach completes. Within 30s: diskless Resource CRD on worker-3, Pod transitions Running. Verify diskless row appears in `linstor r l`.

### 7.13 Operator-visible destructive ops observable everywhere — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** observability #5

`linstor r d <tb-node> <rd>` → all 3 levels reflect it: K8s PVC stays Bound + pod uninterrupted; level 2 shows tiebreaker auto-recreated on different node; level 3 on former tiebreaker shows `drbdadm status` returns "No currently configured DRBD found" + `.res` file gone.

### 7.14 `drbdadm down` from satellite shell recovered by reconciler — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** observability #6; drbd-troubleshooting #6

Operator manually downs a resource (e.g. wipe metadata); reconciler re-applies `.res` + `drbdadm up` + `adjust`, converges within 30s. Cross-listed with 5.32.

### 7.15 Pool-capacity correlation at all 3 levels — S

- **Priority:** P2  **Target:** e2e  **Complexity:** L
- **Source:** observability #9

Fill pool to near-full; PVC stuck Pending (level 1); `linstor sp list` FreeCapacity <100 MiB (level 2); `df` / `lvs` / `zfs list` on satellite (level 3). All three within 5% of each other.

---

## Error-reports API + faulty filter

### 7.16 `linstor r l --faulty` returns full remediation envelope — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill E1

Per resource: node-name, volume index, per-volume disk-state, per-peer conn-state, DRBD `in_use` flag. Each missing field = extra REST roundtrip for the recovery copilot.

**Unit:** REST handler returns full envelope shape.
**E2E:** induce faulty state via 7.5; `linstor r l --faulty -o json | jq` has all 5 fields.

### 7.17 `error-reports` API filters by node + since + limit — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** recovery-skill E2; PLAN.md `pkg/rest/error_reports.go`

**Unit:** GET with `?node=worker-1&since=2026-01-01&limit=10` → matching reports only.
**E2E:** induce errors on worker-2 (kill satellite), poll `error-reports?node=worker-2` → returns matching kernel error strings.

### 7.18 Copilot approval prompts have machine-readable metadata — P

- **Priority:** P2  **Target:** integration (cross-project with ccp)  **Complexity:** M
- **Source:** recovery-skill E3

Out of blockstor REST scope per se; in scope for the ccp drbd-recovery skill integration. Copilot's prompt must include: resource name + volumes, all replica states, exact command, reversibility classification.

Pinned here so that when the skill matures, blockstor's REST surface has the inputs ready.

---

## Capacity gates (over-subscription)

### 7.19 `MaxFreeCapacityOversubscriptionRatio` caps MaxVolumeSize — P

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M (implement first)
- **Source:** UG9 §"Configuring a maximum free capacity over provisioning ratio" (lines 3454-3499); ug9-features 4.1

**Status:** OpenAPI types include `MaxFreeCapacityOversubscriptionRatio`. Actual gating in `rg query-size-info` / autoplace — verify.

**Unit:** `pkg/rest/query_size_info_test.go` — ZFS_THIN pool 10 GiB free, ratio=2 → MaxVolumeSize=20 GiB.
**E2E:** set ratio=2, attempt `rg spawn rg too-big 25G` → rejected with "exceeds oversubscription" error.

Default value: 20.

### 7.20 `MaxTotalCapacityOversubscriptionRatio` — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (after 7.19)
- **Source:** UG9 §"Configuring a maximum total capacity over provisioning ratio" (lines 3501-3530); ug9-features 4.2

Same as 7.19 but against total pool capacity (not free). Combined: most restrictive wins.

### 7.21 `MaxOversubscriptionRatio` — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (after 7.19)
- **Source:** UG9 §"Configuring a maximum over subscription ratio..." (lines 3531-3550); ug9-features 4.3

Caps sum of provisioned sizes / usable capacity. Test: provision 100 × 1 GiB on a 50 GiB pool (oversub=2×); set ratio=1.5; 76th volume creation fails.

**Why P1 for all three:** cozystack tenants over-allocate aggressively; without gates a single tenant can OOM the pool.

---

## QoS

### 7.22 sysfs `blkio_throttle_*` properties surface to satellite — O (today)

- **Priority:** P3  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"QoS settings" (lines 4537-4609); ug9-features 10.1

Four properties (read_bps, write_bps, read_iops, write_iops) settable on volume / SP / resource / controller / node / RG / VG / RD / VD. Satellite writes them to `/sys/fs/cgroup/blkio/blkio.throttle.*_bps_device`.

**Status:** Not implemented. Cozystack uses container-level cgroup limits (kubelet enforces via Pod resources), so block-device-level QoS is **probably out of scope**.

**Caveat:** if 2.17 (`MaxThroughput` autoplacer strategy) lands, these props need to be readable to feed the score.

**Test stance:** keep the props accessible via `GET list-properties` (so they're not 404), but writes are no-op + warning. Document the design decision.

### 7.23 QoS with multi-volume RD or NVMe initiator — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"QoS settings for a LINSTOR volume having multiple DRBD devices" (4590) + §"QoS settings for NVMe" (4602)

Out of scope (depends on 7.22 + 6.11).

---

## Mass-incident SOP (recovery copilot consumer)

### 7.24 Mass-incident pipeline test — S

- **Priority:** P1  **Target:** e2e  **Complexity:** H (test harness)
- **Source:** recovery-skill D1; cross-listed with 5.35

6-replica cluster, 30 resources; induce multi-class failure; run the 7-step SOP; verify cluster recovers in <10min with Primary-InUse workloads uninterrupted throughout.

Run nightly on the burnin stand.

### 7.25 Resource-prioritization: zero-UpToDate first — S

- **Priority:** P1  **Target:** e2e + unit  **Complexity:** L
- **Source:** recovery-skill D2; cross-listed with 5.36

Verify `linstor r l --faulty` ranking matches what the recovery copilot expects (resources with zero UpToDate replicas first).

---

## Implementation-order recommendation

1. 7.1, 7.6, 7.7 — quorum + tiebreaker foundation (existing)
2. 7.16, 7.17 — recovery copilot data contract (existing surface, missing tests)
3. 7.2, 7.3, 7.4 — VM-friendly quorum modes (existing surface)
4. 7.9 — three-way consistency (foundational e2e)
5. 7.10, 7.11, 7.13, 7.14 — cheat-sheet narrowing flows
6. 7.12, 7.15 — cross-level correlation (P1/P2)
7. 7.8 — tiebreaker reconciler vs manual r d edge case
8. 7.5 — quorum-off persistence (cross-listed with 5.20)
9. 7.19 — over-subscription gate (P1, verify+implement)
10. 7.20, 7.21 — additional ratios (P1, after 7.19)
11. 7.24, 7.25 — mass-incident SOP nightly
12. 7.18, 7.22, 7.23 — P2/P3 / cross-project

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 4 |
| P0 e2e | 7 |
| P1 unit | 4 |
| P1 e2e | 5 |
| P2 (any) | 2 |
| P3 / O | 2 |
| T (implement first) | 2 |
