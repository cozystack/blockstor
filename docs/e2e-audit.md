# E2E script audit — Tier 4 cleanup (2026-05-14)

After landing the 108-test Tier 2 integration suite (`tests/integration/group_*.go`), the legacy `tests/e2e/*.sh` corpus was audited script-by-script against the rule from `docs/test-strategy.md` Tier 4 section: **a script only stays in Tier 4 if it genuinely needs a real DRBD kernel module, kubelet mount/format, or a real Kubernetes cluster behaviour that envtest cannot model**. Pure REST/CRD/reconciler flows folded into Tier 2 are removed; the audit doc records the Tier 2 test that supersedes each.

Counts (excluding `lib.sh`, which always stays):

| Disposition | Count |
|---|---|
| KEEP (Tier 4) | 53 (including `recovery-late-vd-real-drbd.sh` already landed for Bug 79) |
| DELETE (folded into Tier 2) | 13 |
| DEMOTE (stub pointer) | 0 |
| Inbound — Bug 80–83 real-DRBD regression scripts on parallel branches | 4 (do NOT collide) |
| **Total `*.sh` files audited (excludes `lib.sh`)** | 66 |

DEMOTE was not used: the shell scripts have no programmatic relationship to the Go integration tests, so a "stub pointer" file is no better than the audit table below + git history. When a deleted script's coverage needs to be re-found, this doc plus `git log -- tests/e2e/<name>.sh` is the trail.

The 5 inbound real-DRBD regression scripts for Bugs 79–83 (`recovery-late-vd-real-drbd.sh`, `recovery-auto-place-real-drbd.sh`, `recovery-setgi-per-peer.sh`, `recovery-suspended-quorum.sh`, `recovery-poolmissing-real-zfs.sh`) are NOT touched by this audit — they are Tier 4 by design and the parallel agents own them. Of these, only `recovery-late-vd-real-drbd.sh` has landed on main; the other four are still being authored on `feat/e2e-bug-{80,81,82,83}-*` branches.

---

## DELETE (13 scripts) — covered by Tier 2

Each row names the Tier 2 test(s) that exercise the same REST/CRD/reconciler path the deleted script drove via the upstream `linstor` CLI against a port-forwarded apiserver.

| Script | Why redundant | Tier 2 superseder |
|---|---|---|
| `linstor-cli.sh` | Pure CLI↔REST plumbing smoke (list views + RD/R create/delete). Script's own header: "does not run a real DRBD bring-up on the CLI side". | Group A `TestGroupANodeList*`, Group D `TestGroupD` (RD CRUD), Group F `TestGroupFRCreateExplicit`, Group J `TestGroupJ` |
| `linstor-cli-replica-move.sh` | Full replica-move via CLI; the `drbdsetup status` peeks are advisory. | Group F `TestGroupFRMigrateDisk`, Group K `TestGroupKWFNodeEvacuateReplaceRestore` |
| `cheat-sheet-cli-level2.sh` | CLI matrix from operator cheat-sheet — node/sp/rd/vd/r/c subcommands. | Group A/B/C/D/E/F/H — full CLI surface |
| `cheat-sheet-csi-level1.sh` | Level-1 CSI commands from the cheat-sheet (CreateVolume, ControllerPublish, CreateSnapshot, DeleteSnapshot). | Group J `TestGroupJ` (CSI gRPC in-process) |
| `cheat-sheet-naming-deltas.sh` | Pod / Deployment / namespace name mismatches vs upstream LINSTOR. No kernel-state, no DRBD. | Group H `TestGroupH` (controller wire shape) + harness fixtures encode the names |
| `node-restore.sh` | PUT /v1/nodes/{n}/restore clears EVICTED flag — pure CRD flag flip. | Group A `TestGroupANodeRestorePUT` |
| `node-lost.sh` | `linstor n lost` cascade — orphan resource removal. | Group A `TestGroupANodeLostCascadesOrphans`, Group K `TestGroupKWFNodeLostCascade` |
| `node-evacuate.sh` | EVICTED flag + NodeReconciler replacement spawn. | Group A `TestGroupANodeEvacuatePUT`, Group K `TestGroupKWFNodeEvacuateReplaceRestore` |
| `node-multi-evacuate.sh` | Two sequential evacuates; the variadic CLI surface is single-node anyway (test's own finding). | Same as `node-evacuate.sh` — Group A + Group K |
| `lc-rd-delete-cascade.sh` | RD delete cascade — R CRDs gone, ports freed, re-create succeeds. | Group D `TestGroupD` (RD delete cascade), Group F `TestGroupFRDeleteCascadesSnapshots` |
| `lc-connection-cleanup.sh` | Per-peer connection entry cleanup on `r d`. | Group I `TestGroupI` (`TestNodeConnectionSetProperty` + `TestResourceConnectionPathCreate`) |
| `placement-label-sync.sh` | NodeLabelSyncReconciler mirroring k8s labels → `Aux/...` props; envtest IS real k8s API. | Group A `TestGroupANodeAuxLabelSync` |
| `recovery-false-diskless.sh` | Today asserts 404 on `r mkavail` (handler not wired). Pure wire-shape probe — no kernel-state contract. | Group F wire-shape probes; will move to Tier 4 when the handler lands and the test becomes "adopt existing on-disk state" |

`lc-rd-delete-churn.sh` is **kept** despite touching the same handlers — it runs 10 iterations and watches `dmesg`/`zfs list` for orphan ZVOLs and `node_id of X instead of Y` kernel warnings (cited by `docs/known-issues.md` as the validation source for the storage-sweeper). That assertion needs a live cluster.

---

## KEEP (53 scripts) — Tier 4 only

Real DRBD kernel-state, replication state machine, mount/format, network partition, in-cluster reconciler races, or external integrations (Ganesha NFS, piraeus affinity controller).

### Kernel replication state machine (16)

`recovery-bitmap-drop.sh`, `recovery-discard-my-data.sh`, `recovery-down-reverses.sh`, `recovery-stuck-synctarget.sh`, `recovery-stuck-synctarget-down-up.sh`, `recovery-primary-force.sh`, `recovery-node-id-mismatch.sh`, `recovery-inconsistent-blocking.sh`, `recovery-port-collision.sh`, `recovery-quorum-persistence.sh` (Tier 4 in strategy doc), `split-brain-recovery.sh`, `state-standalone-partition.sh`, `state-inconsistent-mid-sync.sh`, `state-auto-resync.sh`, `network-partition.sh` (= strategy's `iptables-partition.sh`), `quorum-loss-recovery.sh`.

### Disk / metadata replacement (4)

`disk-replace-internal-metadata.sh` (= strategy's `recovery-disk-replace.sh`), `disk-replace-external-metadata.sh`, `storage-external-drbd-meta.sh`, `backing-device-fail.sh`.

### Real backing-device behaviour (1)

`storage-error-injection.sh` — `dmsetup error` target needs a real block device.

### Multi-volume / multi-primary kernel features (3)

`two-primaries-live-migration.sh`, `two-volume-rd.sh`, `toggle-disk.sh` (real sync wait).

### Sync / replica add (1)

`replica-add-no-resync.sh` — initial-sync skip is a DRBD kernel optimisation.

### Cross-node data-plane (2)

`snap-ship-cross-node.sh`, `snapshot-restore-cross-node.sh`.

### Resize + mount + checksum (5)

`resize-luks.sh`, `resize-plain.sh`, `resize-pvc.sh`, `resize-no-drbd.sh`, `clone.sh` (md5 verification across CSI clone path).

### Cryptsetup-on-real-device (3)

`drbd-luks-stack.sh`, `luks-layer.sh`, `no-drbd.sh` (STORAGE-only stack still needs a real provider device — loopback/lvm/zfs).

### Real Kubernetes-side observability (4)

`observability-three-way.sh` (PVC↔CRD↔/dev/drbdX agreement), `observability-capacity-correlation.sh`, `observability-destructive-walk.sh`, `observability-linstor-node-bridge.sh` (iptables-drop ↔ drbdadm status).

### Live-cluster reconciler races (5)

`tiebreaker.sh` (tiebreaker spawn against live DiskState), `auto-diskful.sh`, `evacuate.sh` (full migration with `wait_uptodate`), `affinity-controller.sh` (piraeus affinity contract), `state-offline-unknown.sh` (heartbeat watchdog + DaemonSet manipulation).

### Operator-recipe contracts that simulate satellite death (2)

`recovery-deleting-convert.sh` (DELETING stuck → convert+toggle-disk recipe), `lc-rd-delete-churn.sh` (10-iter churn + `dmesg` watch).

### Lifecycle on real DRBD (2)

`lifecycle-toggle-migrate.sh`, `lifecycle-toggle-retry.sh`.

### Operator-runnable utilities (2)

`satellite-utils-smoke.sh` (drbdadm/drbdsetup/cryptsetup binary surface in the satellite container), `client-compat.sh` (explicit KEEP per strategy doc — `linstor` python-client wire-shape smoke against real DRBD).

### Day-1 lifecycle on real cluster (3)

`rwx-ganesha.sh` (Ganesha NFS mount), `rolling-upgrade.sh` (preserves I/O during deploy rollout), `node-replace-hardware.sh` (permanent node failure recovery).

### Hardware-state recovery (1)

`recovery-false-diskless.sh` — see DELETE row; today's contract is a wire-shape probe, not kernel-state. Once the `r mkavail` handler lands, the upgraded test becomes Tier 4 and re-enters this list.

### Reserved for inbound Bug 79–83 work (1 in tree + 4 inbound)

`recovery-late-vd-real-drbd.sh` (landed on main via Bug 79 branch).
Inbound (do NOT collide): `recovery-auto-place-real-drbd.sh` (Bug 80), `recovery-setgi-per-peer.sh` (Bug 81), `recovery-suspended-quorum.sh` (Bug 82, = strategy's `recovery-suspended-quorum.sh`), `recovery-poolmissing-real-zfs.sh` (Bug 83).

---

## Why not DEMOTE?

The task allowed for "DEMOTE to a smoke-only marker — keep the file but reduce it to a one-line `t.Skip(...)` pointer". That phrasing fits Go test files. For shell scripts, a 1-line stub leaves a broken executable in `tests/e2e/`, and `stand/Makefile`'s `e2e: SCENARIO=<name>` target would happily run it. The cleaner contract is: DELETE redundant scripts, record the supersession in this doc + `git log`.

If a future regression makes a deleted scenario worth re-running on real DRBD, the audit row tells the operator what Tier 2 test currently covers it and what to copy back to Tier 4.
