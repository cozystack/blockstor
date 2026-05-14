# Wave 2 — Group 8 — Snapshots (Day2 ops)

Local snapshot CRUD: create / delete / restore-into-new-RD /
rollback-in-place / auto-create (`AutoSnapshot/RunEvery` +
`AutoSnapshot/Keep`).

**Out-of-scope for this group:** snapshot shipping
(`day2-snapshot-ship-self.md`) and S3/L2L backups — see
`out-of-scope.md` and wave1 4.18. Scheduled local-only snapshot
retention is in wave2-10.

[Group index in README.md](README.md).

---

### 8.W01 `snapshot create <rd> <snap>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating a snapshot" (lines 2449-2471) via tests/scenarios/day2-snapshot-create.md

Cross-listed with wave1 4.12. DRBD coordinates consistent point-in-time on every diskful replica. State `Successful` per node. Backend dispatch: ZFS_THIN → `zfs snapshot`, LVM_THIN → `lvcreate -s`, FILE_THIN → reflink (XFS/btrfs only). Thick LVM / thick ZFS / plain FILE: returns clear `backend does not support snapshots` (see wave1 6.4).

### 8.W02 `snapshot delete <rd> <snap>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Removing a snapshot" (lines 2522-2530) via tests/scenarios/day2-snapshot-delete.md

Cross-listed with wave1 1.13 (idempotent). Required before RD delete (4.W11). Does NOT affect resources that were restored from this snapshot.

### 8.W03 `snapshot resource restore` into NEW RD — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Restoring a snapshot" (lines 2473-2497) via tests/scenarios/day2-snapshot-restore.md

Cross-listed with wave1 4.14. Two-step: `snapshot volume-definition restore --from-resource <rd1> --from-snapshot <snap> --to-resource <rd2>` then `snapshot resource restore`. New RD independently usable; mods don't affect original. Safe alternative to rollback when source is in use.

### 8.W04 `snapshot rollback <rd> <snap>` in-place — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Rolling back to a snapshot" (lines 2499-2521) via tests/scenarios/day2-snapshot-rollback.md

Cross-listed with wave1 4.13. Resource must be Unused (Secondary everywhere) — refuses if any replica `InUse`. Backend: ZFS_THIN `zfs rollback`, LVM_THIN `lvconvert --merge`. DESTRUCTIVE — post-snapshot mods unrecoverable.

**Edge case (LINSTOR ≥ 1.31.2):** rollback works on nodes added AFTER snapshot — the new node just resyncs from a peer; periodic balance task may re-add replicas on snapshot-bearing nodes.

### 8.W05 `AutoSnapshot/RunEvery` + `AutoSnapshot/Keep` periodic — S

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** M
- **Source:** UG9 §"Creating a snapshot" lines 2462-2471 via tests/scenarios/day2-snapshot-auto-create.md

Per-RD props. Default `Keep=10`. Manually-created snapshots NOT counted against the keep budget. Combine with wave2-10 schedules for off-site shipping (out-of-scope here).

**Integration:** envtest with mocked clock — set `RunEvery=15`, advance time past 5 intervals → 5 snapshots; advance further → oldest auto-created gets removed.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 3 |
| P0 e2e | 4 |
| P1 e2e | 1 |
