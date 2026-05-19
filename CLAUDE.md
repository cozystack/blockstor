# CLAUDE.md — blockstor project rules

Project-level guidance for AI agents working on `blockstor`. Read first.

## Test tiers (see PLAN.md → Test strategy for full text)

- **L1 unit** (`go test ./...`) — every commit.
- **L2 contract / golden** — recorded golinstor responses → our server, byte-diff.
- **L3 contract / oracle** — golinstor → both LINSTOR oracle and our server, JSON diff.
- **L4 integration (DRBD)** — `make smoke` on the talos+qemu stand.
- **L5 e2e** — csi-sanity + piraeus-operator e2e on stand.
- **L6 operator-CLI e2e** — real `linstor` CLI → REST → satellite → DRBD kernel; assert Status convergence (`tests/e2e/cli-matrix/`).
- **L7 operator-replay harness** — `tests/operator-harness/replay/<workflow>.yaml` workflows + `cli-parity-refresh.sh` BS↔upstream diff.
- **L8 property-fuzz** — skeleton only, follow-up work.

## CLI-bug-fix protocol

A user-reported CLI bug is **not closed** until ALL of the following land in the same PR:

1. L1 / L2 regression test (the bog-standard mock unit / contract pin).
2. L6 cli-matrix cell under `tests/e2e/cli-matrix/`.
3. **L7 replay YAML** under `tests/operator-harness/replay/`. Codifies the exact operator sequence + convergence assertion. Without the YAML the bug counts as open.

**Before claiming a CLI bug fixed:** run `tests/operator-harness/replay-runner.sh <stand-name> <workflow.yaml>` on the live stand and verify PASS. Local unit tests are not sufficient — the bug repros were operator-CLI level, the fix MUST be validated at operator-CLI level.

- If the fix touches `r c` / `r d` / `r td` / autoplace / tiebreaker paths, `cli-matrix/r-full-lifecycle.sh` MUST be run on stand and green. The lifecycle catcher is the ground-truth gate for that surface (user has reported regressions there 5+ times despite individual bug fixes).
- If the fix touches `vd s` / volume size / DRBD resize / provider extend paths, `cli-matrix/vd-resize-full-lifecycle.sh` MUST be run on stand and green. The shrink-rejection contract `cli-matrix/vd-shrink-rejected.sh` and the L7 replay `replay/vd-resize-full-lifecycle.yaml` must also pass.

## Adding a new CLI verb or wire-shape change

1. Refresh `docs/cli-parity-known-deltas.md` with any intentional new divergence (row id, command, delta_kind, accepted_until, why).
2. Run `tests/operator-harness/cli-parity-refresh.sh` against the stand. The script exits non-zero if any new non-PARITY row is not whitelisted. Either fix the controller or add the row.
3. If the verb is novel (not a delta), add a happy-path replay YAML under `tests/operator-harness/replay/` in the same commit.
4. If the upstream audit (`docs/cli-parity-audit-<date>.md`) is older than a month, regenerate it from the script's output.

## Adding a new replay YAML

Copy the closest existing YAML and fill:

- `name` — kebab-case, matches filename.
- `description` — what scenario it captures; reference the bug number if applicable.
- `prerequisites.min_nodes` — minimum worker count needed.
- `vars` — defaults for `{{rd}}`, `{{sp}}`, etc.
- `steps[]` — list of `cmd[]`, `expect_exit`, and optional `await` assertion.
- `teardown[]` — cleanup CLI invocations.
- `invariants[]` — currently only `no_orphans` is implemented.

Available `await.kind` values: `replica_count`, `disk_state`, `all_uptodate`, `replica_diskless`, `no_tiebreaker`, `sync_clean`, `resource_absent`, `rd_absent`, `vd_size_kib`, `pvc_capacity`, `pod_md5_invariant`. See the header comment of `tests/operator-harness/replay-runner.sh` for the contract.

## Running the harness

```bash
# CLI parity refresh (nightly + per-PR for CLI-touching changes):
BS_URL=http://127.0.0.1:3370 UP_URL=http://127.0.0.1:3371 \
    tests/operator-harness/cli-parity-refresh.sh /tmp/cli-parity

# Single replay workflow:
BS_URL=http://127.0.0.1:3370 \
    tests/operator-harness/replay-runner.sh dev-kvaps \
        tests/operator-harness/replay/pvc-lifecycle.yaml

# All replay workflows (CI):
for f in tests/operator-harness/replay/*.yaml; do
    BS_URL=http://127.0.0.1:3370 \
        tests/operator-harness/replay-runner.sh dev-kvaps "$f" || exit 1
done
```

The caller is responsible for setting up the port-forward to the BS apiserver (and the upstream LINSTOR controller for parity refresh). The runner does NOT manage port-forwards.

## See also

- `PLAN.md` → Test strategy → L7 mandatory.
- `docs/cli-parity-audit-2026-05-14.md` — original one-shot audit (history).
- `docs/cli-parity-known-deltas.md` — current whitelist.
- `docs/agent-playbook.md` — sub-agent dispatch contract (worktrees, per-group scope).
