# CLI parity known deltas (accept-list)

Generated parity reports (`tests/operator-harness/cli-parity-refresh.sh`) fail CI when they discover a non-`PARITY` row that is **not** listed in this file. Each row here represents a delta we accept as documented divergence between blockstor's REST surface and upstream LINSTOR.

When you add or change a CLI verb or wire shape:

1. Run `cli-parity-refresh.sh` against the dev stand.
2. If a new non-`PARITY` row appears that is intentional (work-in-progress feature, downstream-only enrichment, etc.), append a row to the table below with justification.
3. If the new row represents an unintended regression, fix the controller — do **not** add it to this file.
4. `accepted_until` is either an ISO date (review by then) or `permanent` (intentional divergence forever).

Row IDs match the command-catalogue indexes used by `cli-parity-refresh.sh` (see the `COMMANDS` heredoc in that script). Either match by `# id` or by command-string + tag — both forms are honoured by the whitelist grep.

## Accepted deltas

| # | command | delta_kind | accepted_until | why |
|---|---------|------------|----------------|-----|
| 03 | `sp l` | WIRE_SHAPE | 2026-09-30 | F3 fix-list item; `DfltDisklessStorPool` auto-create + `SharedName` deferred — tracked in docs/cli-parity-audit-2026-05-14.md row 3. |
| 04 | `sp l --show-props StorDriver/*` | WIRE_SHAPE | permanent | BLOCKSTOR_SUPERSET: BS surfaces extra `StorDriver/LvmVg`/`ThinPool` keys; client glob still matches → CLI render identical. Operator-visible parity OK. |
| 05 | `rg l` | WIRE_SHAPE | 2026-09-30 | Default RG name is `dfltrscgrp` (lowercase) vs upstream `DfltRscGrp`. CSI provisioner currently set to lowercase via override; migration tracked in F5. |
| 12 | `v l` | WIRE_SHAPE | 2026-09-30 | BS populates `device_path` (BLOCKSTOR_SUPERSET) but omits `state.storage_pool_name` and `minor_number`. F9 fix item. |
| 13 | `v l --resources PARITY_RD` | WIRE_SHAPE | 2026-09-30 | Same root cause as #12. |
| 18 | `controller version` | WIRE_SHAPE | permanent | Intentional version stamping: BS reports `1.33.2 git=blockstor`. Downstream tooling MUST NOT grep a hex git_hash from BS. |
| 19 | `controller list-properties` | WIRE_SHAPE | 2026-09-30 | BS does not surface every default LINSTOR property; backlog tracked outside fix-list. |
| 21 | `advise r` | MISSING_FEATURE | 2026-12-31 | Autoplace-advisor not implemented; CSI does not depend on it. Defer until advisor design firms up. |
| 22 | `advise rd` | MISSING_FEATURE | 2026-12-31 | Same as #21. |
| 52 | `exos defaultUser` | MISSING_FEATURE | permanent | EXOS layer never supported by blockstor (out of scope; we do not target Seagate Exos hardware). |
| 53 | `backup l` | MISSING_FEATURE | 2026-12-31 | Backup/restore subsystem (F20 fix item) deferred to follow-up wave. |
| 54 | `schedule l` | MISSING_FEATURE | 2026-12-31 | Schedules subsystem deferred to follow-up wave. |

## Open (block merge until addressed)

These rows are **NOT** whitelisted on purpose — they appear in the audit but block any future refresh, so an open issue stays visible:

- #07 `rd l --resource-definitions PARITY_RD` — filter ignored (BEHAVIOR_BUG, F6).
- #10 / #11 `vd l` — empty rows (WIRE_SHAPE, F8).
- #15 `controller list-properties` (when missing rows surface).
- #16 `ps l` — physical-storage list empty (MISSING_FEATURE, F11).
- #17 `err l` — error-reports empty (MISSING_FEATURE, F12).
- #32 `r c --auto-place 99` — terse error vs upstream structured envelope (ERROR_TEXT, F13).
- #33 `s d` idempotence — WARNING vs SUCCESS envelope (ERROR_TEXT, F14).
- #40 `n c` create — no UUID + warning envelope (WIRE_SHAPE, F17).
- #42 `r d` of non-existent pair — 500 vs 200 + WARNING (ERROR_TEXT, F15, CSI-blocking).

If you fix any of those, drop the corresponding row from the open list and add it to the accepted table only if some residual divergence remains.
