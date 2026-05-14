# Wave 2 — Group 10 — Scheduled snapshots (Day2 ops)

LINSTOR's scheduled-backup machinery covers two distinct use cases:
**local-only** scheduled snapshots (the periodic `s c` that
auto-prunes via retention) and **remote shipping** (S3 / LINSTOR-to-
LINSTOR).

This group covers the **local-only** subset: scheduled snapshot
create, enable/disable/delete at each scope, modify retention/cron.
The shipping side is **out-of-scope** — see `out-of-scope.md`.

Note: LINSTOR's CLI naming bundles both under `linstor schedule`
+ `linstor backup schedule <verb>`. In blockstor we expose only the
schedule object; the `backup ship` execution path returns 501.

[Group index in README.md](README.md).

---

### 10.W01 `schedule create <name> '<full-cron>' [--incremental-cron]` — P

- **Priority:** P2  **Target:** unit  **Complexity:** M
- **Source:** UG9 §"Creating a backup shipping schedule" (lines 2964-3025) via tests/scenarios/day2-schedule-create.md

Schedule object: name + full cron + optional incremental cron + retention (`--keep-local N`, `--keep-remote N`) + failure policy (`--on-failure RETRY|SKIP`, `--max-retries N|forever`). Defined but NOT yet active (must enable separately via 10.W04).

**blockstor scope:** support `--keep-local` only; `--keep-remote` is for S3 / L2L (out-of-scope). REST handler accepts `--keep-remote` and returns 501-like ApiCallRc if shipping is invoked.

**Unit:** schedule CRUD + retention math; cron parser smoke; full+incremental same-tick yields FULL (UG behaviour).

### 10.W02 `schedule delete <name>` — P

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Deleting a backup shipping schedule" (lines 3102-3112) via tests/scenarios/day2-schedule-delete.md

Removes the schedule object cluster-wide; already-shipped backups and local snapshots NOT deleted. In-progress shipment may abort mid-way (partial backup must be cleaned manually — only relevant when shipping is in scope, which it isn't for blockstor).

### 10.W03 `backup schedule delete --rd <rd> | --rg <rg> | (controller)` (scope-specific) — P

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Deleting aspects of a backup shipping schedule" (lines 3171-3227) via tests/scenarios/day2-schedule-delete-scope.md

Removes a `(schedule, remote)` pairing from a single scope without deleting the schedule itself. Lets a higher-priority-scope's setting re-apply (RD > RG > controller). Distinct from 10.W02 (which removes the schedule object entirely).

### 10.W04 `backup schedule enable / disable` — P

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Enabling scheduled backup shipping" (lines 3114-3140) + §"Disabling a backup shipping schedule" (lines 3142-3170) via tests/scenarios/day2-schedule-enable.md + tests/scenarios/day2-schedule-disable.md

Activate/suspend a `(schedule, remote)` pair at controller / RG / RD scope. Hierarchy: RD > RG > controller; higher-priority disable overrides lower-priority enable.

**blockstor stance:** for the LOCAL-snapshot variant, treat "remote" as a synthetic local marker (or accept the spec but no-op the ship); enable/disable still gates the schedule's evaluator.

### 10.W05 `schedule modify` updates cron / retention / failure policy — P

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Modifying a backup shipping schedule" (lines 3027-3036) via tests/scenarios/day2-schedule-modify.md

Only specified fields change; unspecified retain. Sentinels: `--keep-local all` / `--keep-remote all` resets retention to unlimited; `--max-retries forever` resets retry budget.

---

## Group summary

| Tag | Count |
|-----|------:|
| P2 unit | 5 |

**Note:** all scenarios are P2 — local-only scheduled snapshots are
a nice-to-have for cozystack (Velero handles application-level DR).
The full `linstor backup schedule` shipping surface is out-of-scope
per `out-of-scope.md`.
