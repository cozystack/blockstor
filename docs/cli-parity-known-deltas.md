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
| 04 | `sp l --show-props StorDriver/*` | WIRE_SHAPE | permanent | BLOCKSTOR_SUPERSET: BS surfaces extra `StorDriver/LvmVg`/`ThinPool` keys; client glob still matches → CLI render identical. Operator-visible parity OK. |
| 08 | `r l` (tiebreaker layers DRBD,STORAGE) | WIRE_SHAPE | 2026-12-31 | F10 residual: DRBD layer enrichment (`may_promote`, `promotion_score`, `node_id`, `al_*`) not yet stamped. Audit refresh 2026-05-19 reclassifies F10 as partial — `drbdtop`-style monitoring depends on it but CSI does not. |
| 18 | `controller version` | WIRE_SHAPE | permanent | Intentional version stamping: BS reports `1.33.2+ git=blockstor`. Downstream tooling MUST NOT grep a hex git_hash from BS. |
| 19 | `controller list-properties` | WIRE_SHAPE | 2026-09-30 | BS does not surface every default LINSTOR property; backlog tracked outside fix-list. |
| 21 | `advise r` | MISSING_FEATURE | 2026-12-31 | Autoplace-advisor not implemented; CSI does not depend on it. Defer until advisor design firms up. |
| 22 | `advise rd` | MISSING_FEATURE | 2026-12-31 | Same as #21. |
| 50 | `node info` | WIRE_SHAPE | 2026-09-30 | Open until a satellite NodeStatus capability snapshot is wired into the CRD; advisory-only fields (`info.os`, `info.cpus`, `info.memory`) remain blank. |
| 52 | `exos defaultUser` | MISSING_FEATURE | permanent | EXOS layer never supported by blockstor (out of scope; we do not target Seagate Exos hardware). |
| 53 | `backup l` | MISSING_FEATURE | 2026-12-31 | Backup/restore subsystem (F20 follow-up: orchestration only — DTO already lands). |
| 54 | `schedule l` | MISSING_FEATURE | 2026-12-31 | Schedules subsystem deferred to follow-up wave. |
| 55 | `key-value-store list` | WIRE_SHAPE | 2026-09-30 | BS exposes the KVS endpoints but the wire shape lacks `props.LinstorKvs/...` namespace nesting; CSI uses a flat key set so unblocked. |
| 60 | `vd s <rd> <vn> <smaller>` (shrink) | BEHAVIOR | permanent | BLOCKSTOR_STRICTER: upstream LINSTOR has permitted `vd set-size` downward since 1.8.0 for layer stacks that support it (e.g. STORAGE/ZFS-only); BS rejects ALL shrinks with `FAIL_INVLD_VLM_SIZE` (HTTP 400) unless the operator opts in with `force=true` (body field or `?force=true`). Rationale: every BS RD is DRBD-backed and DRBD cannot shrink past the on-disk metadata position without destroying it, so an unconditional reject is the safe default. The `force=true` escape hatch covers the post-`resize2fs -s` operator. Pinned by `tests/e2e/cli-matrix/vd-shrink-rejected.sh` (L6), `replay/vd-resize-full-lifecycle.yaml` (L7), and `pkg/rest/volume_definitions_test.go::TestVolumeDefinitionsUpdateShrink{Rejected,WithForceAccepted}` + `pkg/rest/bug_383_*` (L1). |
| 61 | `rd lp` default `on-no-quorum` (auto-quorum seed) | BEHAVIOR | permanent | Corner-case B6. Upstream auto-quorum default pair is `quorum majority` + `on-no-quorum io-error` (UG9 TIP ~4251-4253). BS seeds `on-no-quorum=suspend-io` instead — Bug 297 (P1 data-loss): io-error freezes the minority replica across partition heal and breaks auto-promote; suspend-io blocks I/O until quorum returns, then resumes cleanly. `quorum majority` half MATCHES upstream verbatim; only the `on-no-quorum` value diverges, by design. Pinned by `TestCornerB_DefaultQuorumPairIsDeliberateDelta`. |
| 62 | `rd sp DrbdOptions/Resource/quorum-minimum-redundancy <bad>` | BEHAVIOR | permanent | Corner-case B8. Upstream VALIDATES generic DRBD option values against the DRBD schema and rejects e.g. `quorum-minimum-redundancy bogus` with FAIL_INVLD_PROP (oracle: `value 'bogus' is not valid … has to match (1-32) or (all\|majority\|off)`, rc=10). BS's property bag is permissive — it accepts and stores any DrbdOptions value verbatim (no per-key value validator); an invalid value is rejected downstream by drbdadm at `.res` render time. Valid values pass through and propagate to the `options{}` block correctly. Pinned by `TestCornerB_QuorumMinimumRedundancyPassthrough`. |
| 63 | `r d` quorum-property auto-removal INFO + prop strip | BEHAVIOR | permanent | Corner-case B3b. Upstream LINSTOR, on an `r d` that drops a 3-node resource below quorum feasibility, REMOVES `DrbdOptions/Resource/quorum` + `on-no-quorum` from the RD and prints two INFO lines ("…was removed as there are not enough resources for quorum", UG9 ~1380-1401). BS instead STAMPS `DrbdOptions/Resource/quorum=off` (does not remove it) and prints no INFO — Bug 309: the satellite renders an explicit quorum value into the `.res`; an absent prop would fall back to a DRBD built-in that differs from the intended `off`. Resulting DRBD behaviour (`quorum off`) is equivalent; the divergence is the visible prop shape (`off` present vs absent) and the missing INFO envelope. Stamp-off pinned by `TestEnsureTiebreakerOffOnSingleReplica`. |

## Open (block merge until addressed)

These rows are **NOT** whitelisted on purpose — they appear in the audit but block any future refresh, so an open issue stays visible.

(As of refresh 2026-05-19 the F1-F20 wave from 2026-05-14 closed every Open row from the original audit. New Open rows will populate here as `cli-parity-refresh.sh` discovers them on the stand.)

## Refresh history

- 2026-05-14 — original one-shot audit `docs/cli-parity-audit-2026-05-14.md`.
- 2026-05-19 — refresh `docs/cli-parity-audit-2026-05-19-refresh.md`; F1-F20 closed (F10 partial residual remains as accepted delta #08); L7 harness `261d9e32f` lands re-runnable cli-parity-refresh.sh as the going-forward audit driver.
