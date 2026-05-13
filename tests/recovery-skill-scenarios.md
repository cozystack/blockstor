# DRBD recovery skill test scenarios

Companion to:
- `tests/linstor-cli-scenarios.md` (CLI surface, happy path)
- `tests/drbd-troubleshooting-scenarios.md` (state-reporting + survival under DRBD failures)
- `tests/observability-cheat-sheet-scenarios.md` (cross-level narrowing)
- `tests/advanced-config-scenarios.md` (config-space coverage)

This file translates the **`drbd-recovery` skill**
([cozystack/ccp](https://github.com/cozystack/ccp/blob/main/skills/drbd-recovery/skills/drbd-recovery/SKILL.md))
into blockstor acceptance tests. That skill is an SOP for cluster
operators / on-call: it encodes the decision tree, fix recipes, and
forbidden actions used to recover production DRBD9+LINSTOR clusters
after incidents.

Each recipe in the SKILL is a contract the operator expects to hold —
when they run `linstor r deact <node> <rsc>` the resource must
actually deactivate, when they run `linstor r mkavail --diskful` the
LINSTOR view must re-register the existing ZFS volume. blockstor must
honour these contracts because the SKILL is what humans (and the
copilot it builds) actually execute under incident pressure.

Tests below assume:
- 3+ worker stand (`make up NAME=... && make piraeus && make
  blockstor && make pools`)
- ZFS_THIN pool (the SKILL's recipes reference `zfs list
  data/<rsc>_00000` paths)
- At least one Primary-able workload (a Pod with PVC) to exercise
  InUse states

The Cozystack skill is **defensive** — it prefers preserving data over
moving fast, and it ASKs the user before destructive operations. Our
tests therefore split into two flavours: **automated** (pin the
machinery — REST calls, status reporting, idempotency) and
**operator-mediated** (require a human to confirm the dangerous step,
documented here as a scripted walkthrough).

---

## Group A — Decision-tree branch coverage

The skill's Recovery Decision Tree has 7 branches. Each test induces
the state, then asserts that the SKILL's recommended remedy actually
works against blockstor.

### A1. Branch: Unknown — verify on the node

**Why:** SKILL §"Recovery Decision Tree" first branch: "If state ==
Unknown — verify on the node itself, don't trust LINSTOR's view." If
the node is dead, the remedy is `linstor node lost` (but the skill
forbids this without operator approval).

**Setup:** 3-replica RD on workers 1/2/3, Primary on worker-1.

**Steps:**
```bash
# Take worker-3 offline forcefully (matches "dead node"):
ssh worker-3 'sudo systemctl stop kubelet'
# Wait until satellite is Offline:
linstor n l | grep worker-3   # Online → Offline
linstor r l -r test
```

**Expected:**
- worker-3 row for `test` shows `State = Unknown` (not blank, not
  UpToDate). Blockstor's observer must translate "Offline satellite"
  into Unknown on the resource view.
- `drbdadm status test` on worker-1 / worker-2 shows `:Connecting` or
  `peer-disk:Unknown` for worker-3.
- `linstor n l --faulty` lists worker-3.

**Failure modes to guard against:**
- worker-3 row missing entirely — observer gave up on Offline
  satellites instead of preserving last-known state
- worker-3 row showing stale `UpToDate` — observer didn't tag it
  unknown when satellite stream dropped

### A2. Branch: DELETING — convert + toggle-disk path

**Why:** SKILL §"Fix: Stuck DELETING (Method 2)". When a satellite is
gone and `r d` is blocked by its Unknown copy, the skill converts the
DELETING resource into a Tiebreaker-style stub and toggles diskless on
a different node.

**Setup:** 3-replica RD `stuck` on workers 1/2/3. Stop worker-3
forcefully so its copy is Unknown.

**Steps:**
```bash
linstor r d worker-3 stuck   # marks worker-3 copy DELETING
linstor r l -r stuck         # worker-3 stays DELETING (no satellite to ack)

# SKILL method 2:
linstor rd sp stuck DrbdOptions/Resource/quorum off
linstor r td worker-3 stuck --diskless   # convert DELETING→Diskless
# wait for state to settle:
linstor r l -r stuck
linstor r d worker-3 stuck
linstor rd sp stuck DrbdOptions/Resource/quorum majority
```

**Expected:**
- `r td --diskless` must succeed against blockstor (toggle-disk path
  is supported by satellite reconciler)
- Resource leaves DELETING; second `r d` removes the Diskless stub
  cleanly
- Other replicas (workers 1/2) stay UpToDate throughout — the
  conversion does not touch them

**Failure modes:**
- `r td --diskless` returns 501 / NotImplemented — REST surface gap
- toggle succeeds but observer still reports DELETING — status didn't
  update after FlagDelete cleared

### A3. Branch: StandAlone — `connect --discard-my-data` on the right side

**Why:** SKILL §"Fix: StandAlone Connections". Always discard on the
side with the older / fewer changes; the Primary keeps its data.

**Setup:** 2-replica RD on workers 1/2, Primary on worker-1, write
128 MiB. Force worker-2 into StandAlone via:
```bash
ssh worker-2 drbdadm disconnect splitbrain
ssh worker-2 drbdadm down splitbrain && ssh worker-2 drbdadm up splitbrain
```

**Steps:**
```bash
# Verify worker-1 has data, worker-2 is the outdated side:
ssh worker-2 'drbdadm secondary --force splitbrain'
ssh worker-2 'drbdadm disconnect splitbrain'
ssh worker-2 'drbdadm connect --discard-my-data splitbrain'
ssh worker-1 'drbdadm disconnect splitbrain'
ssh worker-1 'drbdadm connect splitbrain'
linstor r l -r splitbrain
```

**Expected:**
- After ~10s: worker-2 row goes `SyncTarget` → `UpToDate`
- Conns column on worker-1 row = `Connected`
- The Primary on worker-1 never lost Primary-ship (no I/O
  interruption observable from the consuming Pod)

**Failure modes:**
- blockstor's satellite reconciler fights `--discard-my-data` by
  re-issuing `drbdadm adjust` and reverting the side selection
- Observer keeps reporting StandAlone after kernel went UpToDate
  (events2 connection-state-change not parsed)

### A4. Branch: Inconsistent/Outdated — auto-sync once peers connect

**Why:** SKILL §"Recovery Decision Tree" branch 5: Inconsistent should
auto-recover once Connected. The test verifies blockstor doesn't
interfere.

**Setup:** 3-replica RD, then:
```bash
ssh worker-3 'drbdadm disconnect inconsistent'
# Write a bunch from worker-1 (Primary) — worker-3 falls behind
dd if=/dev/urandom of=/var/lib/kubelet/pods/.../volume bs=1M count=512
ssh worker-3 'drbdadm connect inconsistent'
```

**Expected:**
- worker-3 transitions Inconsistent → SyncTarget → UpToDate without
  any operator intervention
- `linstor r l -r inconsistent` reflects the progression
- During SyncTarget, blockstor must NOT issue `drbdadm adjust` (would
  interrupt sync)

**Failure modes:**
- Observer reports `UpToDate` while DRBD is still SyncTarget (false
  positive — consumer might think it's safe to remove the source
  replica)

### A5. Branch: SyncTarget — do not interfere

**Why:** SKILL §"Core Principles" #6 — "Don't interfere with
SyncTarget." Test asserts blockstor's reconciler doesn't issue any
drbdadm commands that would abort an in-flight resync.

**Setup:** Trigger a multi-GB resync (4 GiB write, then disconnect +
reconnect a peer). Catch the resource at ~30% sync.

**Steps:**
```bash
# At 30% sync, deliberately try to disturb:
linstor r sp synctest DrbdOptions/Resource/some-knob value
# Check observer state vs drbdadm status:
ssh <syncing-node> drbdadm status synctest
linstor r l -r synctest
```

**Expected:**
- `linstor r sp` returns success but defers config-rewrite until sync
  completes (or applies in a way that doesn't trigger `drbdadm
  adjust`)
- Resync progresses linearly; no SyncTarget→Inconsistent regressions
- After sync done: applied prop visible in `linstor rd lp synctest`

**Failure modes:**
- Reconciler issues `drbdadm adjust synctest` mid-sync → resync
  restarts from 0% / drops to Inconsistent

### A6. Branch: Diskless (false) — `r mkavail --diskful` re-registers

**Why:** SKILL §"Fix: False Diskless". After a failed deletion,
LINSTOR thinks the replica is Diskless but the ZFS volume + DRBD
device are still alive on the node. The remedy is `r mkavail
--diskful`.

**Setup:** Induce the state by:
1. Create 3-replica RD `fakediskless`
2. `kubectl patch -n cozy-blockstor resource/<crd> -p
   '{"metadata":{"finalizers":[]}}' --type=merge` to strip the
   satellite finalizer (DON'T do this in prod — we're forcing the
   bug)
3. `kubectl delete resource ...`
4. ZFS volume + DRBD device remain on worker-3 because satellite
   cleanup didn't run

**Steps:**
```bash
ssh worker-3 'zfs list data/fakediskless_00000'   # confirm ZVOL exists
ssh worker-3 'drbdadm status fakediskless'        # confirm device exists
linstor r l -r fakediskless                       # worker-3 missing or Diskless
linstor r mkavail --diskful worker-3 fakediskless
linstor r l -r fakediskless                       # worker-3 row UpToDate
```

**Expected:**
- `r mkavail --diskful` re-registers the existing ZVOL without
  creating a new one (idempotent)
- No resync happens (data was already there) — or a minimal bitmap
  sync at most
- Observer picks up the re-registered state within 10s

**Failure modes:**
- `r mkavail --diskful` not implemented in blockstor REST → 501
- Implementation creates a fresh ZVOL, ignoring the existing one
  (data loss / double-allocation)
- After mkavail, observer still reports Diskless

### A7. Branch: Inconsistent replica blocking others

**Why:** SKILL §"Fix: Inconsistent Replica Blocking Others". When one
replica is permanently Inconsistent and prevents the others from
serving I/O.

**Setup:** 3-replica RD `blocker`, Primary on worker-1. Corrupt
worker-3's backing ZVOL while satellite is down:
```bash
ssh worker-3 'systemctl stop linstor-satellite'
ssh worker-3 'drbdadm down blocker'
ssh worker-3 'dd if=/dev/urandom of=/dev/zvol/data/blocker_00000 bs=1M count=10 seek=0'
ssh worker-3 'systemctl start linstor-satellite'
```

**Steps (SKILL remedy):**
```bash
linstor r d worker-3 blocker          # remove the bad replica
linstor rd ap blocker                 # autoplace a replacement
linstor r l -r blocker
```

**Expected:**
- worker-1 / worker-2 keep serving I/O throughout
- After `r d worker-3` the cluster goes 2/3 (still quorate)
- `rd ap` places a new replica on a 4th node (or back on worker-3
  with a fresh ZVOL)
- New replica syncs SyncTarget → UpToDate

**Failure modes:**
- `r d` on Inconsistent peer hangs (waiting for state ack)
- `rd ap` doesn't honour place-count after partial loss

---

## Group B — Fix-recipe contracts

The SKILL has 9 named "Fix:" sections. Tests in this group pin that
each named recipe works end-to-end against blockstor's REST surface.

### B1. `Fix: TCP Port Collisions` — deact + act cycle reallocates ports

**Why:** SKILL §"Fix: TCP Port Collisions". Two resources sharing a
port on the same node are recovered by `linstor r deact` + `r act`,
which is supposed to reallocate ports via the LayerData path.

**Setup:** Reproduce a port collision artificially:
```bash
# Find two resources on the same node:
linstor r l -n worker-1 | head
# Force one to take the other's port (raw drbdsetup):
linstor rd sp rsc-a TcpPort 7100   # if rsc-b already uses 7100
ssh worker-1 drbdadm adjust all 2>&1 | grep "is also used"
```

**Steps:**
```bash
linstor r deact worker-1 rsc-a
sleep 5
linstor r act worker-1 rsc-a
linstor rd lp rsc-a | grep TcpPort   # should be a fresh port
```

**Expected:**
- `r deact` returns 200 and the satellite tears down the kernel
  device
- `r act` reallocates a port from TcpPortAutoRange that doesn't
  collide
- `drbdadm adjust all` shows no collision warnings after

**Known bug (SKILL §"Known Upstream Bugs" #3):** Toggle-disk doesn't
preserve TCP ports — `removeLayerData` frees ports,
`ensureStackDataExists` allocates different ones. This is **expected
behaviour** for the deact+act recipe (we WANT new ports). The test
should NOT regress fix-PR #476's behaviour for plain toggle-disk
calls.

### B2. `Fix: Suspended I/O (quorum lost)` — quorum off + resume-io

**Why:** SKILL §"Fix: Suspended I/O". The recipe sets `quorum off` via
LINSTOR (must persist through satellite restarts), then `drbdadm
resume-io`, then restores `quorum majority` after.

**Setup:** 3-replica RD `quorumtest`, Primary on worker-1 with active
fio write. Crash workers 2+3 simultaneously:
```bash
ssh worker-2 'sudo systemctl stop kubelet linstor-satellite'
ssh worker-3 'sudo systemctl stop kubelet linstor-satellite'
# Primary on worker-1 should suspend I/O (lost quorum):
ssh worker-1 drbdadm status quorumtest   # io-suspended:yes
```

**Steps:**
```bash
linstor rd sp quorumtest DrbdOptions/Resource/quorum off
ssh worker-1 drbdadm resume-io quorumtest
# I/O resumes; fio should make progress
# Restart workers 2/3:
ssh worker-2 'sudo systemctl start ...'
ssh worker-3 'sudo systemctl start ...'
# After Connected:
linstor rd sp quorumtest DrbdOptions/Resource/quorum majority
```

**Expected:**
- `rd sp ... quorum off` persists across satellite restart (verify by
  bouncing worker-1's satellite after setting it)
- Property propagates to .res file (`grep quorum
  /var/lib/linstor.d/quorumtest.res` shows `off`)
- After workers 2/3 come back, switching to `majority` does not
  re-suspend I/O if all 3 are connected

**Failure modes:**
- `rd sp` writes property to RD CRD but satellite never re-renders
  .res
- Switching back to `majority` triggers an unnecessary `drbdadm
  adjust` that interrupts the Primary

### B3. `Fix: Stuck SyncTarget` — disconnect+connect to source

**Why:** SKILL §"Fix: Stuck SyncTarget". When SyncTarget stalls at 0%,
the recipe is `drbdadm disconnect <rsc>:<source>` + `drbdadm
connect <rsc>:<source>`.

**Setup:** Trigger sync, then artificially stall it (block port 7000
between two nodes via iptables for 30s, then unblock — leaves
SyncTarget but stuck).

**Steps:** Apply the disconnect+connect recipe.

**Expected:** SyncTarget resumes from where it stopped (DRBD's
bitmap-based resume) within 5s of the connect call.

**Blockstor concern:** The skill notes that if reconnect doesn't help,
a full `drbdadm down ... up` is needed — but ONLY if Unused. blockstor
should not auto-down on this path; verify reconciler stays quiet
during the operator's manual disconnect/connect.

### B4. `Fix: Dual-Primary` — demote without I/O interruption (Unused case)

**Why:** SKILL §"Fix: Dual-Primary". If one Primary is Unused,
demoting it is safe. If both InUse, the SKILL says ASK.

**Setup:** Provoke dual-primary by:
1. Create 2-replica RD `dualpri`
2. Mount on worker-1 (Primary InUse)
3. ssh worker-2: `drbdadm primary --force dualpri` (Primary Unused)
4. Both rows now `Primary`

**Steps (automated path — Unused side):**
```bash
ssh worker-2 'drbdadm secondary --force dualpri'
linstor r l -r dualpri   # worker-2 row → Secondary
```

**Expected:**
- worker-1 Primary InUse continues serving I/O throughout
- worker-2 demotes cleanly
- No split-brain (both stay Connected/UpToDate)

**Expected for operator-mediated path (both InUse):**
- Test is documented but NOT automated — requires human approval
- Document the prompt the recovery copilot would show

### B5. `Fix: Can not drop the bitmap`

**Why:** SKILL §"Fix: Can not drop the bitmap". After diskful→diskless
transitions DRBD 9.2.16 leaves stale bitmap entries; the recipe is
disconnect + `connect --discard-my-data`. The SKILL flags this as
upstream bug #1 (fixed in 9.2.17).

**Setup:** Depends on the DRBD kernel version on the stand. If 9.2.17+
is deployed, this test should be **skipped** (xfail with reason
"upstream-fixed-in-9.2.17"). If 9.2.16 is in use, induce by toggling
disk diskful→diskless→diskful in a loop until bitmap error appears in
dmesg.

**Steps:** Apply the SKILL recipe; verify the resource ends up
UpToDate.

**Tracking:** Record the kernel version in the test report (the
`dmesg | grep drbd` baseline) so we know whether the fix is needed.

### B6. `Fix: Node-ID Mismatch`

**Why:** SKILL §"Fix: Node-ID Mismatch". When DRBD complains "Peer
presented a node_id of X instead of Y", the recipe is to recreate the
broken replica.

**Setup:** Hardest to induce artificially. One way: manually edit
`/var/lib/linstor.d/<rsc>.res` on worker-2 to change worker-3's
node-id, restart `linstor-satellite`. Or capture the state from a
real incident as a regression fixture.

**Steps:**
```bash
linstor r d worker-3 noidmismatch
linstor rd ap noidmismatch
```

**Expected:** New replica gets a consistent node-id via LayerData;
dmesg clean of "node_id" warnings on all peers.

**Blockstor concern:** `linstor rd ap` (autoplace via RD) must reuse
the deficit + skip already-present nodes — same machinery as `rg
spawn` autoplace but on existing RD. Verify both paths.

### B7. `Fix: PausedSyncS / resync-suspended:dependency`

**Why:** SKILL §"Fix: PausedSyncS". Sync paused due to dependency on a
different graph edge — fix by reconnecting to the Primary (source of
truth) node.

**Setup:** 3-replica RD; force one peer offline, allow Primary to
write, bring peer back; if it shows PausedSyncS:
```bash
ssh <syncing-node> drbdadm status pausedsync
# replication:PausedSyncS resync-suspended:dependency
```

**Steps:** `drbdadm disconnect pausedsync:<primary-node>` + `drbdadm
connect pausedsync:<primary-node>`.

**Expected:** Sync resumes within 5s; observer reflects the
transition.

---

## Group C — Forbidden actions and safety rails

The SKILL's §"What NOT to Do" section is a list of operator
guardrails. blockstor must not make these mistakes from its own
reconciler.

### C1. Never `drbdadm down` on Primary/InUse from reconciler

**Test:** During every reconciler operation that could conceivably
trigger an `adjust` or `down` (RD prop change, layer-data change,
satellite restart), verify via dmesg + drbdadm status that no `down`
ever runs on a Primary device.

**Method:** Wrap drbdadm with a logging shim on the test node:
```bash
mv /usr/sbin/drbdadm /usr/sbin/drbdadm.real
cat > /usr/sbin/drbdadm <<'EOF'
#!/bin/bash
echo "$(date) drbdadm $*" >> /tmp/drbdadm-trace.log
exec /usr/sbin/drbdadm.real "$@"
EOF
```
Then run the full blockstor reconciler test suite and grep for `down`
lines targeting Primary resources.

**Expected:** Zero matches. Any match is a regression.

### C2. Never delete RD with finalizers blocking, never strip finalizers

**Test:** Pin that blockstor's controller-runtime layer never
`Patch`es a CRD to clear finalizers as a shortcut. Static analysis:
```bash
grep -r 'finalizers.*nil\|finalizers.*\[\]' pkg/ cmd/
```
**Expected:** Only the satellite cleanup path may remove its own
finalizer, and only after it has confirmed kernel-side teardown.

This pins the bug discovered in the earlier session (force-strip left
DRBD kernel state alive holding ports 7000-7002).

### C3. Never `linstor node lost` from automation

**Test:** Verify blockstor REST does not expose `node lost` to the
controller's own retry loop. The only caller should be the explicit
CLI path (`linstor node lost ...`) requiring operator intent.

**Method:** grep call sites:
```bash
grep -r 'NodeLost\|node.lost\|/v1/nodes/.*/lost' --include='*.go' \
  cmd/controller cmd/apiserver pkg/satellite pkg/controllers
```
**Expected:** Only the REST handler in `pkg/rest/node_lifecycle.go`
matches; no controller calls into it directly.

### C4. Idempotency under "discard-my-data" misuse

**Test:** If the operator runs `connect --discard-my-data` against
the only UpToDate copy (the SKILL forbids this but operators make
mistakes under pressure), blockstor must not amplify the damage by
auto-replicating the discard.

**Setup:** 2-replica RD; force one side StandAlone; deliberately
discard on the UpToDate side.

**Expected:** Data loss is contained to that side only. blockstor's
reconciler doesn't issue `disconnect`/`connect --discard-my-data` on
the other peer.

---

## Group D — Mass-incident procedure

The SKILL's §"Mass Incident Recovery Procedure" has a 7-step ordering
(taints → DELETING → StandAlone → Connecting → Inconsistent → quorum
restore → verify). Each step has prerequisites; reordering breaks the
recovery.

### D1. Pipeline test: simulated mass incident

**Setup:** 6-replica cluster, 30 resources distributed. Induce:
- Kill 2 workers simultaneously (loses quorum on subset)
- Force-fail 5 resources into StandAlone
- Stop and corrupt 3 random ZFS volumes
- Apply `drbd.linbit.com/lost-quorum` taint on 2 nodes

**Steps:** Execute the 7-step procedure scripted:
```bash
./tests/recovery-skill-massincident.sh
```

The script:
1. Removes taints
2. Identifies and resolves DELETING (deact+toggle path)
3. Identifies StandAlone, applies discard recipe
4. Identifies Connecting, runs `drbdadm adjust` per node
5. Waits for Inconsistent→UpToDate
6. Restores quorum=majority on resources where we changed it
7. Asserts `linstor r l --faulty` is empty (or only SyncTarget)

**Expected:**
- Procedure completes within 10 minutes for 30-resource cluster
- Zero resources end in unrecoverable state
- All Primary-InUse workloads remained available throughout (Pods
  consuming PVCs never lost mount)

**Failure modes to watch for:**
- Step ordering matters: if we try to "fix StandAlone" before
  removing DELETING, the DELETING entries block r d / r ap operations
- Step 6 (quorum=majority restore) too early re-suspends I/O if not
  all replicas are back

### D2. Resource-prioritization: zero-UpToDate first

**Why:** SKILL §"Mass Incident": "Prioritize by presence of UpToDate
replicas: resources with zero UpToDate copies need attention first."

**Test:** Recovery copilot (the consumer of this SKILL) must rank
resources. Verify that ranking matches what `linstor r l --faulty`
output + ZFS state inspection would produce.

**Method:**
```bash
linstor r l --faulty -o json | jq '
  .[0].resources[] |
  select(.state.in_use or
         ([.volumes[].state.disk_state] | any(. == "UpToDate") | not))
'
```
This should be the "zero-UpToDate" set.

---

## Group E — Recovery copilot contract

The SKILL is also a copilot specification. blockstor's REST API
should expose the data the copilot needs in a single call rather than
forcing N+M roundtrips.

### E1. `r l --faulty` returns complete remediation state

**Test:** `linstor r l --faulty` output must include, per resource:
- node-name (for ssh targeting)
- volume index (for `r td` argument)
- disk-state per volume (for "is there UpToDate?" check)
- conn-state per peer (for "StandAlone?" check)
- DRBD `in_use` flag (so copilot knows to ASK before destructive
  ops on InUse)

If any field is missing, the copilot has to make extra REST calls,
slowing recovery and increasing chance of stale-state decisions.

**Failure modes:**
- `in_use` not surfaced in REST → copilot can't tell InUse from
  Unused → demotes wrong Primary
- per-peer conn-state collapsed to a single string → copilot can't
  tell which side is StandAlone

### E2. `error-reports` API surfaces parseable error chains

**Why:** SKILL §"Core Principles" #7 — "check error-reports for the
real cause." The copilot needs to fetch error reports by node/time
range and filter to the resource of interest.

**Test:** Verify `GET /v1/error-reports` honours `node`, `since`,
`limit` query params and returns JSON with `text` field containing
parseable DRBD kernel error strings.

**Failure modes:**
- error-reports endpoint not implemented → copilot blind to root
  causes
- text returned as opaque blob (no `kind` field) → can't filter to
  "bitmap errors only"

### E3. Operator approval prompts have machine-readable metadata

**Why:** SKILL §"Core Principles" #8 — "ASK before dangerous ops."
The copilot needs to render approval prompts with enough context that
the operator can decide in seconds.

**Test (out-of-scope for blockstor REST, in-scope for copilot
integration test):** Given a Recovery Tree state, the copilot's
prompt must include:
- Resource name + volume(s)
- All replica states (UpToDate / Inconsistent / Diskless)
- The exact LINSTOR/drbdadm command it wants to run
- The reversibility of that command (read-only / interrupts I/O /
  destroys data)

---

## Priority and gating

| Group | Priority | Gating |
|-------|----------|--------|
| A (decision-tree branches) | P0 | Must all pass before declaring v1 production-ready |
| B (fix-recipe contracts) | P0 for B1–B4, P1 for B5–B7 | B5 (bitmap) is xfail on kernel ≥ 9.2.17 |
| C (forbidden actions) | P0 | Each is a regression-guard for past incidents |
| D (mass-incident pipeline) | P1 | Run nightly on the burnin stand |
| E (copilot contract) | P1 | Integration with cozystack/ccp; co-owned with skill maintainers |

P0 = blocks release. P1 = blocks GA / customer-facing claims.

---

## Cross-doc index

| Concept | Primary doc | Cross-refs |
|---------|-------------|------------|
| Recovery decision tree | this file (Group A) | drbd-troubleshooting §Recovery |
| Fix recipes | this file (Group B) | drbd-troubleshooting (state-side) |
| Forbidden actions | this file (Group C) | linstor-cli (CLI contract) |
| Mass-incident SOP | this file (Group D) | observability-cheat-sheet (narrowing) |
| Copilot data contract | this file (Group E) | advanced-config §Quorum |
| Quorum mechanics | advanced-config §Quorum | this file B2, D1 |
| Port collisions | this file B1 | advanced-config §TCP port range |
| ZFS volume identity | this file A6 | (no other doc — unique to recovery) |

---

## Skill provenance

This file derives from
[cozystack/ccp `drbd-recovery` SKILL.md](https://github.com/cozystack/ccp/blob/main/skills/drbd-recovery/skills/drbd-recovery/SKILL.md).
When the upstream SKILL adds a new branch or recipe, mirror it here as
a new test scenario in the appropriate group. When blockstor adds a
new layer (e.g., NVMe-oF instead of DRBD), the SKILL won't apply
directly — fork the structure but keep the decision-tree shape so the
copilot can still operate.
