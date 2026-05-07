# Blockstor — implementation plan

A Go reimplementation of LINSTOR that keeps the existing k8s ecosystem
(linstor-csi, piraeus-operator, ha-controller, affinity-controller,
scheduler-extender, gateway) working unchanged, by reproducing LINSTOR's
public REST API on top of CRD-backed reconcile loops.

This document is the source of truth for what is being built, in what order,
and under what rules. It is intended to allow autonomous work between
high-bandwidth check-ins with the user.

> Update the **Current status** section after every meaningful step.

---

## Current status

- **Phase**: 2 well underway. CRD-backed store live, four resource families wired end-to-end.
- **Implemented (REST + Store + tests, all both InMemory and CRD/envtest backends, golinstor compatibility verified)**:
  - `/v1/controller/version`, `/v1/healthz`
  - `/v1/nodes` — full CRUD
  - `/v1/view/storage-pools`, `/v1/nodes/{node}/storage-pools[/{pool}]` — read
  - `/v1/resource-groups` — full CRUD
  - `/v1/resource-definitions` — full CRUD (POST unwraps the upstream `{resource_definition: {...}}` envelope)
  - `/v1/view/resources` — read
- **CRDs (kubebuilder-scaffolded, NodeSpec/StatuS filled with LINSTOR-shaped fields)**:
  - `Node`, `StoragePool`, `ResourceGroup`, `ResourceDefinition`, `Resource` (composite-keyed via `<a>.<b>` metadata.name + label index)
- **Stores**: `pkg/store` (InMemory) and `pkg/store/k8s` (controller-runtime client), both behind the same `store.Store` interface and exercised by the same `pkg/store/storetest` shared suite — behavioural drift fails the same subtest immediately.
- **Tests**: 150+ unit + contract; controller-runtime envtest harness skips cleanly when `KUBEBUILDER_ASSETS` is missing, so plain `go test ./...` is green without `make setup-envtest`.
- **Lint**: `golangci-lint run ./...` zero issues. Auto-lint hook (`golangci-lint@claude-code-companions`) catches every Go-file edit.
- **Blocker**: none.
- **Next concrete steps**:
  1. `/v1/resource-definitions/{rd}/volume-definitions` (CRUD) — needed before linstor-csi can size a PVC.
  2. `/v1/resource-groups/{rg}/spawn` — the CSI provisioner calls this on every `CreateVolume`. Needs an `Autoplacer` stub: read RG select-filter, pick `place_count` storage pools, create matching `Resource`s.
  3. `/v1/key-value-store` — linstor-csi keeps its own per-volume bookkeeping here.
  4. `/v1/resource-definitions/{rd}/snapshots` + `/v1/view/snapshots` — Snapshot CRD; mostly mirrors Resource shape.
  5. Reconcilers (no-op for now → satellite-driven in Phase 3) for `Node`, `StoragePool`, `Resource`. Wire `mgr.Add` for them in `cmd/main.go`.

---

## Goals

1. **Replace all Java code in the LINSTOR stack** — controller, satellite,
   any Java tooling — with Go. The end state has zero JVM in the data path.
2. Existing LINSTOR k8s clients (linstor-csi, piraeus-operator, ha-controller,
   affinity-controller, scheduler-extender, gateway) work against the new
   server **without modification**, via 1:1 REST API compatibility.
3. State of truth lives in Kubernetes CRDs; logic is reconcile-driven.
4. Codebase smaller and easier to maintain than upstream Java.
5. Cozystack can switch to this implementation when it is ready.

## In scope (full project)

- `linstor-controller` (Java) → `blockstor-controller` (Go)
- `linstor-satellite` (Java) → `blockstor-satellite` (Go)
- Storage providers: **LVM**, **LVM-thin**, **ZFS**, **ZFS-thin**, **file**
- Replication layer: **DRBD** — and the ability to run **without DRBD**, as
  pure local storage (single-replica diskful or diskless)
- Encryption layer: **LUKS** (volume-level) and DRBD encryption passphrases
- DRBD options (full set from `drbdoptions.json`)
- DRBD proxy
- **Intra-cluster snapshot shipping** — required for clone/expand-replica
  flows. Implemented via `zfs send`/`zfs recv` for ZFS pools and
  [`thin-send-recv`](https://github.com/LINBIT/thin-send-recv) for LVM-thin.
- API surface used by all upstream LINSTOR clients:
  - `linstor-csi`, `piraeus-operator`, `piraeus-ha-controller`,
    `linstor-affinity-controller`, `linstor-scheduler-extender`,
    `linstor-gateway`, `kubectl-linstor`
  - The official **`linstor` CLI** (Python, [linstor-client](https://github.com/LINBIT/linstor-client))
  - Any other consumer of `golinstor` or the REST API
- Autoplacer with constraints (zones, node properties, replicas-on-different)
- In-cluster snapshots (LVM/ZFS), snapshot-restore as new ResourceDefinition
- Cluster bootstrap (passphrase, satellite registration, eviction/restoration)
- `linstor-common` artifacts (`properties.json`, `consts.json`,
  `drbdoptions.json`) consumed without the Java codegen step
- Stats endpoints, error reports, SOS-report, all `/v1/view/*` aggregates

## Out of scope (will not be built)

- Snapshot shipping **between clusters** (cross-cluster DR)
- Backup create/restore/ship/abort, backup queue
- Schedules (cron-driven backups)
- Remote backends: S3, EBS, Linstor remotes
- Storage providers: SPDK, NVMe-oF target/initiator, OpenFlex, Exos
- The cross-cluster halves of golinstor's `BackupProvider`, `RemoteProvider`

These endpoints will return `501 Not Implemented` with a clear message that
blockstor does not implement them.

## MVP slice (Phase 1–5 below)

The project is too big to do at once, so MVP fixes a narrow slice that lets
linstor-csi and piraeus-operator drive cozystack-style workloads:

- LVM and ZFS providers (thick + thin)
- The ~50 REST methods linstor-csi actually calls
- Replication via DRBD; also single-replica local mode without DRBD
- In-cluster snapshots and snapshot-restore
- Autoplacer with replica count + storage pool filter

Everything else from "In scope" lands in Phases 6+.

## Architecture

```
                 ┌───────────────────────────────────┐
                 │ linstor-csi, piraeus-operator,    │
                 │ ha-controller, affinity-controller│
                 │ (existing, unchanged)             │
                 └────────────────┬──────────────────┘
                                  │ REST /v1   (golinstor client)
                 ┌────────────────▼──────────────────┐
                 │  blockstor-controller (Go)        │
                 │  - REST compatibility layer       │
                 │  - CRD as source of truth         │
                 │  - Reconcile loops                │
                 │  - Autoplacer                     │
                 └────────────────┬──────────────────┘
                                  │ gRPC
                 ┌────────────────▼──────────────────┐
                 │  blockstor-satellite (Go)         │
                 │  - DRBD lifecycle                 │
                 │  - ConfFileBuilder                │
                 │  - Storage providers (LVM/ZFS)    │
                 │  - drbdsetup events2 watcher      │
                 └────────────────┬──────────────────┘
                                  │
            host: drbd9 + drbd-utils + lvm2 + zfsutils
```

## CSI MVP scope

The minimum API surface is what `linstor-csi` actually calls. From the survey:
~50 methods across `Resources`, `ResourceDefinitions`, `ResourceGroups`,
`Nodes`, `Backup` (subset), `Remote` (subset), `KeyValueStore`. Anything else
returns `501 Not Implemented` for now.

Full scope list lives in `docs/csi-api-surface.md` (to be created in Phase 1).

---

## Phases and exit criteria

### Phase 0 — Dev stand (done)

- [x] BM.HPC2.36 provisioned on OCI (terraform applied)
- [x] Host packages installed via `stand/setup-host.sh` (qemu-kvm, libvirt, ovmf, drbd-utils, zfsutils-linux, talosctl, kubectl, helm)
- [x] NVMe (6.4TB) formatted as xfs, mounted at `/var/lib/blockstor`, `.work` symlinked
- [x] iptables fixed (Ubuntu OCI image ships catch-all REJECT in INPUT and FORWARD; allow `talos+`/`virbr+` bridges)
- [x] `make up NAME=test` brings up a 3-node Talos+QEMU cluster with the `siderolabs/drbd` extension; `drbd 9.2.14` module loads on workers; install image is also factory-built so the on-disk Talos has the extension
- [x] `make piraeus` installs piraeus-operator + LinstorCluster + Talos-specific override; satellites Connected/Configured
- [x] Storage pool wired via `LinstorSatelliteConfiguration` (file-thin, ~16 GiB free per worker)
- [x] `make smoke` green: PVC create → pod mount → write → read
- [x] `make up NAME=alice` ran in parallel to `NAME=test` without bridge / IP collision
- [ ] `make oracle` deferred — piraeus-installed linstor-controller already serves as Java oracle on the in-cluster `linstor-controller.piraeus-datastore:3370`

**Exit met**: full happy-path PVC test passes against upstream Java stack, on parallelizable stand.

### Phase 1 — Skeleton + contracts

- [x] New module via `kubebuilder init`; controller-runtime manager + `pkg/rest` Runnable
- [x] `/v1/controller/version` returns a credible response (`pkg/version` constants pin LINSTOR contract version)
- [x] `golinstor.Client.Controller.GetVersion()` against our server returns no error (`pkg/rest/server_test.go`)
- [x] Full-branch tests for the version endpoint and the Runnable lifecycle (8 cases)
- [x] CSI MVP scope frozen in `docs/csi-api-surface.md`
- [x] golangci-lint v2 config + auto-lint hook (`golangci-lint@claude-code-companions`) wired
- [ ] OpenAPI types generated from `linstor-server/docs/rest_v1_openapi.yaml` (oapi-codegen)
- [ ] `apiconsts` ported from golinstor (ApiCallRc codes for error responses)
- [ ] `linstor-common` submodule (properties.json, consts.json, drbdoptions.json)

**Exit**: golinstor can talk to us for `GetVersion`; tests in CI green. **Met.**
Remaining items above land at the start of Phase 2 (codegen wiring is most
useful once we have many types to generate, which Phase 2 introduces).

### Phase 2 — CRDs + reconcile

- [ ] CRDs: `Node`, `StoragePool`, `ResourceGroup`, `ResourceDefinition`, `Resource`, `Volume`, `Snapshot`
- [ ] controller-runtime manager, reconcilers stubbed
- [ ] `Nodes.{GetAll,Get,Create,Modify,Delete}` work end-to-end against CRDs
- [ ] linstor-csi container can register a node against our server

**Exit**: piraeus-operator can create `LinstorSatellite`s and they appear as nodes in our API.

### Phase 3 — Satellite + DRBD lifecycle

- [ ] gRPC controller↔satellite proto (own, minimal)
- [ ] `cmd/satellite/`
- [ ] ConfFileBuilder in Go (.res file generation, ported from upstream Java tests as spec)
- [ ] `drbdadm up/down/adjust` driven from satellite
- [ ] `drbdsetup events2` listener (state machine)
- [ ] StoragePool: LVM and ZFS providers (create/delete/snapshot)
- [ ] Resource on 2 nodes replicates and goes UpToDate

**Exit**: smoke test with two replicas, real DRBD, PVC mounted on node A then on node B (failover).

### Phase 4 — Autoplacer + snapshots + intra-cluster shipping

- [ ] Autoplacer: storage-pool-aware replica placement
- [ ] Snapshot CRD + reconcile (LVM/ZFS snapshot wrappers)
- [ ] Snapshot restore creates a new ResourceDefinition
- [ ] Intra-cluster snapshot shipping for clone/replica-expansion:
      - ZFS pools: `zfs send | ssh | zfs recv` over satellite-to-satellite
      - LVM-thin: `thin-send-recv` (LINBIT)
- [ ] csi-sanity passes against our server

**Exit**: csi-sanity green; piraeus-operator e2e green for what they cover; PVC clone across nodes works.

### Phase 5 — Compatibility burn-in

- [ ] Stand running for 24h continuous PVC churn (create/expand/snapshot/restore/delete)
- [ ] Contract-diff suite: replay 100+ recorded golinstor traces against our server and Java oracle, JSON diff zero
- [ ] On hidora-hikube cozystack cluster: parallel install in a separate namespace, side-by-side smoke

**Exit**: 24h+ stable; contract diffs zero on MVP scope.

### Phase 6 — Encryption + DRBD options + file provider

- [ ] LUKS encryption layer (volume-level)
- [ ] DRBD encryption passphrase
- [ ] DRBD proxy enable/disable/configure
- [ ] DRBD options: full set from `drbdoptions.json`
- [ ] file storage provider (loop file / sparse file backed)
- [ ] External-file management

### Phase 7 — Cluster operations + admin

- [ ] Cluster passphrase management
- [ ] Satellite eviction / restoration / lost-and-recover
- [ ] Stats endpoints (`/v1/stats/*`)
- [ ] Error reports, SOS-report
- [ ] All `/v1/view/*` aggregates
- [ ] Property-info endpoints (`*/properties/info`)
- [ ] Resource adjust / adjust-all

### Phase 8 — Java decommission

- [ ] Cozystack staging cluster runs only blockstor for >1 week
- [ ] Cozystack production cluster fully migrated, Java pods removed
- [ ] `grep -r piraeus-server cozystack | grep image:` returns nothing

**Exit**: zero JVM in the cozystack data-path.

---

## Workflow

### Daily loop

1. Read **Current status** at the top of this file.
2. Read TODO comments in files touched last session.
3. Work the next concrete step.
4. After each logical unit: `go test ./...`, `golangci-lint run`, `make smoke` if relevant, `git commit --signoff -m "type(scope): ..."`.
5. Update **Current status** at end of unit.
6. Once a day: short progress note to the user.

### Commit rules

- One commit = one logical unit; never mix.
- Always `--signoff`.
- Never on red tests.
- Never to `main` directly — work on feature branches.
- Conventional Commits prefix (`feat`, `fix`, `chore`, `refactor`, `docs`, `test`, `ci`, `perf`, `build`).

### Push rules

- During the implementation phases, **push directly to `main`** without asking.
  The user has explicitly granted this for blockstor; the small-team velocity
  argument outweighs the formality of a feature-branch+PR loop here.
- Still rebase, never force-push to `main`.
- Reverts are fine — push a revert commit, do not rewrite history.

### When to ask the user

- Any architectural decision with non-trivial trade-offs (e.g. own proto vs reusing LINBIT proto).
- Any public action (push, PR, issue/PR comment, release, Slack/Telegram post).
- Any destructive action on the host or stand beyond `make down`/`make reset`.
- Any blocker > 30 min where I can't make progress.
- Before any spend (extra VM, paid service).
- Before secrets handling.

### When NOT to ask

- Code changes per the plan.
- Local tests.
- Bringing the stand up/down for testing.
- Installing packages on the dev host (`apt`/`dnf`).
- Creating/removing files in `blockstor` (this repo).

---

## Test strategy

**TDD is mandatory.** Tests come first, code second. A test is not a coverage
metric — it is the executable spec of the contract a unit promises to honour.
Every reasonable branch (success, error, edge case, cancellation, partial
state) must have a test. Coverage that grows without spec branches growing is
not coverage worth keeping.

For each new function/endpoint:

1. Write the test for the happy path before the implementation.
2. Write tests for every distinct error / edge case the function can produce.
3. Implement until each test goes green; do not generalise beyond what tests
   demand.
4. When fixing a bug, the regression test lands in the same commit, and it
   must fail without the fix.

| Layer | Where | Speed | Required for |
|-------|-------|-------|--------------|
| L1 unit | `go test ./...` | seconds | every commit |
| L2 contract (golden) | recorded golinstor responses → our server, byte-diff | seconds | API-changing commits |
| L3 contract (oracle) | golinstor → both Java oracle and our server, JSON diff | minutes | per PR |
| L4 integration (DRBD) | `make smoke` on talos+qemu stand | ~3 min | per PR |
| L5 e2e | csi-sanity + piraeus-operator e2e on stand | ~30 min | nightly + pre-merge |

Contract recordings live under `test/golden/`. Captured once from a real Java
controller, replayed forever in CI.

## Cost control

- BM.HPC2.36 ≈ $2.71/h × 36 OCPU. Order of $2000/mo if 24/7.
- **Auto-stop policy** to add to Makefile: `make stop-vm` / `make start-vm`
  via `oci compute instance action --action SOFTSTOP / START`.
- Stop nightly + weekends → ~$700/mo target.
- Re-evaluate at end of Phase 2; if rewrite is going to take > 3 months,
  consider downgrading to `VM.Standard.E5.Flex` with nested virt.

## Known risks

| Risk | Likelihood | Mitigation |
|------|------------|-----------|
| Java LINSTOR API has undocumented behaviour | high | contract diff against real Java oracle every PR |
| DRBD edge cases (recovery, bitmap, quorum) | very high | port ConfFileBuilder behaviour 1:1, real-DRBD tests, sos-report on failure |
| linstor-csi expects sync API; we are async | medium | block REST handler on watch with timeout; fall through to 408 |
| Schema drift between Java versions | low | pin oracle to `piraeus-server:v1.33.2` |
| Credentials leaking into logs | low | redaction for `DrbdOptions/Crypto*`, `passphrase`, AWS keys |
| Costs run away | medium | auto-stop schedule, monthly review |
| Dev host fails or is wiped | low | terraform recreates; stand state is ephemeral by design |

## Open questions for the user

1. ~~Github repo~~ — `cozystack/blockstor` public, **resolved**.
2. Pin Java oracle to `piraeus-server:v1.33.2` (current cozystack version) — OK?
3. Auto-stop schedule for the dev host — nights and weekends UTC, or your timezone? Or no auto-stop?
4. Where should I post short daily progress — this chat, a Telegram channel, a Slack channel?
5. When we get to Phase 5, may I run a parallel install on `hidora-hikube` in its own namespace, or strictly a separate test cluster?

---

## Layout (target)

```
blockstor/
├── cmd/
│   ├── controller/          # REST + reconcile manager
│   └── satellite/           # gRPC client, DRBD lifecycle
├── pkg/
│   ├── api/                 # generated OpenAPI types and handlers
│   ├── apiconsts/           # ported from golinstor
│   ├── crd/                 # CRD types + DeepCopy
│   ├── reconcile/           # per-CRD reconcilers
│   ├── autoplacer/
│   ├── drbd/                # ConfFileBuilder, drbdadm wrappers, events2 parser
│   ├── storage/
│   │   ├── lvm/
│   │   └── zfs/
│   └── compat/              # shape adapters between CRD and REST types
├── proto/                   # controller↔satellite gRPC
├── stand/                   # talos+qemu dev stand (current)
├── tests/
│   ├── smoke/               # current
│   ├── contract/            # golden + oracle diff
│   └── e2e/                 # csi-sanity etc.
├── docs/
│   ├── plan.md → PLAN.md
│   └── csi-api-surface.md
└── PLAN.md                  # this file
```
