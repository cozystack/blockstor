# blockstor test scenarios — by functional group

Consolidated index for the test scenarios that pin blockstor's
behaviour against:

- the **LINSTOR User Guide 9** (linstor-administration.adoc, linstor-kubernetes.adoc)
- Andrei Kvapil's Cozystack/LINSTOR talk
- the [DRBD9 in LINSTOR troubleshooting](https://dev.to/kvaps/troubleshooting-drbd9-in-linstor-40fn) article
- LINBIT's "Troubleshooting LINSTOR in Kubernetes" cheat sheet
- 11 LINBIT KB articles (networks, placement, port range, quorum, drive replacement, perf tuning, backends, fault injection)
- the cozystack `drbd-recovery` SKILL (decision tree, fix recipes, forbidden actions)

The earlier per-source docs in `tests/*.md` are the raw inputs; this
directory **supersedes them** by reorganising the same scenarios into
7 functional groups, sorted within each group by **priority** and
**implementation complexity**, and labelled with **test target**
(unit / integration / e2e).

## The 7 groups (Wave 1)

| # | Doc                              | What it covers |
|---|----------------------------------|----------------|
| 1 | [api-contract.md](01-api-contract.md)         | CLI / REST wire shape, list endpoints, JSON envelopes, case-insensitive lookup, pagination, error envelopes, command-completeness audit |
| 2 | [placement.md](02-placement.md)               | Autoplacer, RG constraints (`replicasOnSame/Different/x-replicas`, `AutoplaceTarget`, `do-not-place-with`, `layer-list`, `providers`), label sync, `BalanceResources`, selection strategies |
| 3 | [networking.md](03-networking.md)             | `NetInterface` CRUD, `PrefNic` on node/pool, multi-path DRBD (`resource-connection path`), `StltCon`, dedicated replication network |
| 4 | [lifecycle.md](04-lifecycle.md)               | Resource/RD/RG CRUD, `toggle-disk` (`--diskless`, `--migrate-from`, retry/cancel), snapshot CRUD + clone + restore, node evacuate/restore/lost, `auto-evict`, `auto-diskful` |
| 5 | [drbd-state-recovery.md](05-drbd-state-recovery.md) | Observer translation (events2 → `Resource.Status`), state reporting (UpToDate/Outdated/Inconsistent/Diskless/TieBreaker/Connecting/StandAlone/SyncTarget), `SkipDisk`, recovery decision tree, fix recipes, mass-incident SOP, forbidden actions |
| 6 | [storage-backends.md](06-storage-backends.md) | LVM/ZFS/FILE providers, layer stack rules (DRBD/LUKS/STORAGE — CACHE/WRITECACHE/NVME explicitly NOT supported), encryption (LUKS, DRBD shared-secret, master passphrase), external DRBD metadata pool, backend capability matrix, drive replacement, fault injection |
| 7 | [quorum-observability.md](07-quorum-observability.md) | Quorum policies (`auto-quorum`, `AutoAddQuorumTiebreaker`, `suspend-io/io-error`, `on-no-data-accessible`), tiebreaker reconciler, three-level narrowing (PVC↔Resource↔DRBD), error-reports API, copilot data contract, over-subscription ratios, QoS |

## Wave 2 — Day2 operations

Wave 2 consolidates 143 per-scenario `day2-*.md` files harvested from
UG9 §"Administering LINSTOR" + linstor-kubernetes.adoc into 11 group
docs that mirror the wave1 format. Numbering uses a `W` prefix
(`4.W01`, `4.W02`, …) to avoid collision with wave1's `4.1`, `4.2`.
Scenarios are sorted into the same functional axes as wave1, plus
four new groups for surfaces wave1 didn't have a home for (snapshots,
resource-group modify cascade, schedules, K8s integration).

| #  | Doc                              | What it covers |
|----|----------------------------------|----------------|
| 1  | [wave2-01-api-contract.md](wave2-01-api-contract.md)         | `list-properties` per scope, Aux/ set/unset, `query-size-info` preview, `cluster-state` smoke |
| 2  | [wave2-02-placement.md](wave2-02-placement.md)               | `Autoplacer/Weights/*`, `BalanceResources*` disable, StorageClass locality + zone constraints |
| 3  | [wave2-03-networking.md](wave2-03-networking.md)             | NetInterface CRUD, node `PrefNic`, `resource-connection path`, `TcpPortAutoRange`, K8s host-network ↔ container-network switch |
| 4  | [wave2-04-lifecycle.md](wave2-04-lifecycle.md)               | Node CRUD + evacuate/restore/lost/info, RD CRUD + resize/reassign/default-storpool, Resource CRUD + toggle-disk variants, multi-volume RDs, controller HA / rolling upgrade |
| 5  | [wave2-05-drbd-state-recovery.md](wave2-05-drbd-state-recovery.md) | DRBD options at every scope (RD/RG/resource/node-conn/resource-conn/unset), external metadata, disk-replace internal/external, split-brain, quorum-loss, SkipDisk auto+manual, alertmanager smoke |
| 6  | [wave2-06-storage-backends.md](wave2-06-storage-backends.md) | LVM/LVM-thin/ZFS/ZFS-thin/Diskless pool CRUD, pool delete, mixing, `physical-storage list + create-device-pool`, `MaxThroughput` strategy |
| 7  | [wave2-07-quorum-observability.md](wave2-07-quorum-observability.md) | `auto-quorum=disabled` manual mode, `AutoEvict*` tuning, `auto-diskful`, error-reports + sos-report, log-level + logback, K8s Prometheus stack, three over-subscription ratios |
| 8  | [wave2-08-snapshots.md](wave2-08-snapshots.md)               | Local snapshot CRUD: create / delete / restore-into-new-RD / rollback-in-place / `AutoSnapshot/{RunEvery,Keep}` |
| 9  | [wave2-09-resource-group.md](wave2-09-resource-group.md)     | RG full surface: create / delete-with-rds / modify-place-count / spawn (happy + impossible), all placement constraint flavours, unset-placement-property, RG DRBD options, RG FS-on-spawn |
| 11 | [wave2-11-kubernetes.md](wave2-11-kubernetes.md)             | K8s-specific: HA controller, RWX PVC (NFS), Affinity Controller, K8s evacuate-via-label, set DRBD options from K8s, K8s VolumeSnapshot CRUD |

(Group 10 — scheduled snapshots — moved to `out-of-scope.md`.)

## Out-of-scope

A long-list of upstream LINSTOR features deliberately not supported
by blockstor (backup shipping, S3/L2L remotes, NVMe layers, CACHE /
WRITECACHE, LDAP auth, TLS cert mgmt, LUKS encryption orchestration,
QoS, DB migrate/backup, LINBIT Gateway, bare-metal Prometheus,
DRBD Proxy, shared LVM) is catalogued in
[out-of-scope.md](out-of-scope.md) with the rationale per category
and the source `day2-*.md` files mapped to each.

## How each scenario is labelled

Every test inside a group carries four tags:

### Priority

- **P0** — blocks v1 release. Test gap = production risk.
- **P1** — blocks GA / customer-facing claims. Cozystack production-grade.
- **P2** — stretch goal. Useful but workarounds exist.
- **P3** — out of scope unless a customer asks.

### Target

- **unit** — pure Go test in `pkg/.../<x>_test.go`. Mocks: `FakeExec`, `Server.handleX` against in-memory `Store`, canned `events2` frames fed to `Observer.Translate`. Fast (< 1 s), runs in CI without infrastructure.
- **integration** — real component but isolated. Examples: `pkg/storage/zfs` against a loop-backed `zpool` on the CI host, single-satellite `Reconciler` against a real `drbdadm` on a loop device, REST handler against a real `controller-runtime` envtest. Needs `BLOCKSTOR_ZFS_POOL` / envtest assets. Seconds to a minute.
- **e2e** — full Talos+QEMU stand (`make up NAME=…`), Kubernetes, multiple satellites, real DRBD replication across nodes. Minutes per test. Lives in `tests/e2e/`. Uses helpers from `tests/e2e/lib.sh`.

A few tests need **hybrid** coverage — same contract tested at multiple levels (e.g., observer translation = unit on canned frames + e2e on real DRBD events). Those are tagged `unit + e2e`.

### Complexity

- **L (low)** — existing code surface; the test is the only new thing. Estimate: < 1 day to land.
- **M (medium)** — needs a new fixture / harness piece (multi-NIC stand setup, fault injector, observability shim). 1–3 days.
- **H (high)** — requires new blockstor implementation work before the test can exist. The scenario doubles as a feature spec. ≥ 1 week.

### Source

Cross-reference back to the raw input doc and section so the
reviewer can read the original context. Format:
`linstor-cli #5`, `recovery-skill A3`, `UG9 §3.4`, `cheat-sheet
level-2`, `KB:configure-separate-networks`.

## Sort order within a group

`P0` first (highest priority), then within same priority
`L → M → H` (cheapest implementation first). Tests with the same
priority + complexity stay in source order.

This sort gets the **cheapest-most-valuable** tests first: an
engineer working top-down lands the highest-impact, lowest-friction
coverage first.

## Status legend

Each test also carries one of:

- **S** (Supported) — feature exists in blockstor; test pins it
- **P** (Partial) — feature exists with caveats; test pins the supported subset
- **T** (To implement) — feature must land before the test can pass; the scenario doubles as the spec
- **O** (Out of scope) — explicitly not implementing (covered for documentation completeness, the test typically asserts the 501/empty-stub behaviour)

## Test harness reference

```bash
# Unit tests:
go test ./pkg/... ./internal/...

# Integration tests (real ZFS pool, envtest, etc.):
BLOCKSTOR_ZFS_POOL=blockstor-test go test ./pkg/storage/zfs -tags=integration
make setup-envtest && go test ./internal/controller/...

# E2E (full stand):
make up NAME=e2e7 && make piraeus NAME=e2e7 && make blockstor NAME=e2e7 && make pools NAME=e2e7
make e2e NAME=e2e7 SCENARIO=<scenario-script-name>

# Burnin / soak:
make burnin-blockstor NAME=e2e7 DURATION=86400
STORPOOL=zfs-thin make burnin-blockstor NAME=e2e7 DURATION=86400
```

All e2e scripts use `tests/e2e/lib.sh` helpers
(`satellite_pod_on`, `satellite_exec`, `wait_drbd_state`).

## Aggregate counts across all 7 groups

| Group | P0 unit | P0 e2e | P1 unit | P1 e2e | P2/P3 | T (impl) | O |
|-------|--------:|-------:|--------:|-------:|------:|---------:|--:|
| 1 — API contract            | 16 | 10 | 5 | 4  | 1  | 0 | 0 |
| 2 — Placement               | 5  | 4  | 4 | 4  | 2  | 5 | 0 |
| 3 — Networking              | 3  | 2  | 2 | 3  | 1  | 3 | 0 |
| 4 — Lifecycle               | 7  | 7  | 3 | 4  | 2  | 3 | 1 |
| 5 — DRBD state & recovery   | 5  | 12 | 2 | 10 | 4  | 2 | 0 |
| 6 — Storage backends        | 4  | 8  | 1 | 7  | 4  | 2 | 1 |
| 7 — Quorum & observability  | 4  | 7  | 4 | 5  | 2  | 2 | 2 |
| **Total**                   | **44** | **50** | **21** | **37** | **16** | **17** | **4** |

Read this as a **work-budget map**:

- **44 P0 unit tests** (existing surface, mocked deps) — fastest to land, biggest ROI. Land these in week 1.
- **50 P0 e2e tests** — need the stand but exercise existing code. Land week 2-3, parallel to e2e harness improvements.
- **17 "T" entries** require **implementation work before the test exists**. They double as feature specs. Estimate: 1-3 weeks each depending on complexity tag (L/M/H). These define the P1 backlog.
- **4 "O"** entries are out-of-scope pinning: (a) S3 snapshot ship + scheduled backup ship (k8s-side tooling owns this), (b) CACHE / WRITECACHE / NVMe-oF / NVMe-TCP layers (cozystack uses homogeneous pools + DRBD over flat L2), (c) sysfs blkio QoS (kubelet/cgroup-level limits cover this), (d) auto-passphrase orchestration (piraeus does it via the standard `encryption create-passphrase` REST endpoints). The test is the stub returning 501 / no-op + clear rejection error.

## Reading order if you're new to the codebase

1. **README.md** (this file) — taxonomy
2. **01-api-contract.md** — start here; cheapest tests, pins wire shapes
3. **05-drbd-state-recovery.md** — domain meat; the reason blockstor exists
4. **02-placement.md** — autoplacer; the brain
5. **04-lifecycle.md** — CRUD churn surface
6. **06-storage-backends.md** — providers + encryption + layers
7. **07-quorum-observability.md** — cross-cutting; depends on all of the above
8. **03-networking.md** — read last; depends on multi-NIC stand harness

## Raw input docs

Superseded by this directory but kept in `tests/` for historical
reference:

- `tests/linstor-cli-scenarios.md` — CLI happy paths (presentation transcript)
- `tests/drbd-troubleshooting-scenarios.md` — DRBD9 troubleshooting article
- `tests/observability-cheat-sheet-scenarios.md` — LINBIT cheat sheet
- `tests/advanced-config-scenarios.md` — 11 LINBIT KB articles
- `tests/recovery-skill-scenarios.md` — cozystack drbd-recovery SKILL
- `tests/linstor-ug9-feature-scenarios.md` — UG9 feature parity

Every scenario in those 6 docs maps to one (or more — cross-listed)
scenario in this directory via the **Source:** tag.
