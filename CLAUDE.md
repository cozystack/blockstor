# CLAUDE.md — blockstor project rules

Project-level guidance for AI agents working on `blockstor`. Read first.

## Test tiers

- **L1 unit** (`go test ./...`) — every commit.
- **L2 contract / golden** — recorded golinstor responses → our server, byte-diff.
- **L3 contract / oracle** — golinstor → both LINSTOR oracle and our server, JSON diff.
- **L4 integration (DRBD)** — `make smoke` on the talos+qemu stand.
- **L5 e2e** — csi-sanity + piraeus-operator e2e on stand.
- **L6 operator-CLI e2e** — real `linstor` CLI → REST → satellite → DRBD kernel; assert Status convergence (`tests/e2e/cli-matrix/`).
- **L7 operator-replay harness** — `tests/operator-harness/replay/<workflow>.yaml` workflows + `cli-parity-refresh.sh` BS↔upstream diff.
- **L8 property-fuzz** — skeleton only, follow-up work.

## How a change is validated

Every PR carries the L1 unit tests for what it changes, passes `make lint-config && make lint` and `go test ./...` with envtest, and runs the integration tests of its area (`go test -tags=integration ./tests/integration/...`). A bug fix comes with a test that fails without it.

Changes that touch the satellite, DRBD, placement, deletion or the REST wire are then validated on a real cluster before they merge: open PR branches are combined into a dev pre-release (`vX.Y.Z-alpha.N`, built from main plus the PR branches on a `dev/vX.Y.Z-alpha.N` branch), rolled out node by node to a stand that runs real workloads, and checked with the scenario list of the release (volume create and delete, snapshot and restore, resize, live migration, a hard reset of one node). The release notes of the dev pre-release name the PRs it carries, and a PR body says in Testing which pre-release exercised it.

The full CI run on main after the merge is the final gate for the whole set.

## Operator-level tools

The L6 cli-matrix scripts (`tests/e2e/cli-matrix/`) and the L7 operator harness (`tests/operator-harness/`) are available for reproducing a CLI bug at operator level and for parity checks against upstream LINSTOR. Use them when a bug is only visible through the `linstor` CLI or when a change alters the REST wire shape:

- For a CLI bug, a replay YAML under `tests/operator-harness/replay/` that reproduces it is welcome in the same PR; it is not a merge requirement when the fix is covered by tests and by a dev pre-release run.
- For a wire-shape change, record intentional divergences in `docs/cli-parity-known-deltas.md` (row id, command, delta_kind, accepted_until, why). `tests/operator-harness/cli-parity-refresh.sh` against a stand helps find unlisted ones. If `docs/cli-parity-audit-<date>.md` is older than a month, regenerate it from the script's output.
- `cli-matrix/r-full-lifecycle.sh` and `cli-matrix/vd-resize-full-lifecycle.sh` are good smoke tests for `r c` / `r d` / `r td` / autoplace / tiebreaker and for resize paths; run them on a stand when such a change is not going through a dev pre-release.

## Adding a new replay YAML

Copy the closest existing YAML and fill:

- `name` — kebab-case, matches filename.
- `description` — what scenario it captures; reference the bug number if applicable.
- `prerequisites.min_nodes` — minimum worker count needed.
- `vars` — defaults for `{{rd}}`, `{{sp}}`, etc.
- `steps[]` — list of `cmd[]`, `expect_exit`, and optional `await` assertion.
- `teardown[]` — cleanup CLI invocations.
- `invariants[]` — currently only `no_orphans` is implemented.

Available `await.kind` values: `replica_count`, `disk_state`, `all_uptodate`, `replica_diskless`, `no_tiebreaker`, `sync_clean`, `resource_absent`, `rd_absent`, `vd_size_kib`, `vd_count`, `rd_layer_stack`, `rd_annotation`, `pvc_capacity`, `pod_md5_invariant`. See the header comment of `tests/operator-harness/replay-runner.sh` for the contract.

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

- `docs/cli-parity-audit-2026-05-14.md` — original one-shot audit (history).
- `docs/cli-parity-known-deltas.md` — current whitelist.
- `docs/agent-playbook.md` — sub-agent dispatch contract (worktrees, per-group scope).
