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

- **Phase**: 3 — satellite + DRBD lifecycle in progress.
- **CRDs (7, kubebuilder-scaffolded, LINSTOR-shaped fields)**:
  `Node`, `StoragePool`, `ResourceGroup`, `ResourceDefinition`, `Resource`,
  `Snapshot`, `KVEntry`. VolumeDefinitions inline on `ResourceDefinition.Spec`.
- **Stores**: `pkg/store` (InMemory) and `pkg/store/k8s` (controller-runtime
  client), both behind the same `store.Store` interface and exercised by the
  same `pkg/store/storetest` shared suite. KeyValueStore is now CRD-backed
  (`KVEntry` per `(instance, key)`) — no ConfigMap 1 MiB limit.
- **Endpoints live (CSI MVP slice complete)**: `/v1/controller/version`,
  `/v1/healthz`, `/v1/nodes` CRUD, `/v1/view/storage-pools`,
  `/v1/nodes/{n}/storage-pools[/{p}]`, `/v1/resource-groups` CRUD +
  `/spawn`, `/v1/resource-definitions` CRUD + `/autoplace` + `/resources`
  POST/DELETE, `/v1/resource-definitions/{rd}/volume-definitions` CRUD,
  `/v1/resource-definitions/{rd}/snapshots` CRUD, `/v1/view/resources`,
  `/v1/view/snapshots`, `/v1/key-value-store` CRUD.
- **Phase 3 stubs**: `proto/satellite/v1alpha1/satellite.proto` (8 RPCs),
  `cmd/satellite/main.go`, `pkg/satellite/agent.go` (hello stub).
- **Tests**: 250+ unit + contract; envtest harness skips cleanly without
  `KUBEBUILDER_ASSETS`.
- **Lint**: `golangci-lint run ./...` zero issues. Auto-lint hook on every
  Go-file edit.
- **Blocker**: none.
- **Next concrete steps** (Phase 3 implementation):
  1. Generate Go bindings from `proto/satellite/v1alpha1/satellite.proto`
     (protoc + protoc-gen-go + protoc-gen-go-grpc). Wire generated package
     into `pkg/satellite`.
  2. Implement controller-side gRPC server (in `pkg/rest/grpc.go` or new
     `pkg/satellitecontroller`) that the satellite dials. Hello round-trips.
  3. Storage providers: `pkg/storage/{lvm,zfs}` interfaces + fake-exec
     implementations + unit tests. No real DRBD/LVM yet.
  4. ConfFileBuilder in `pkg/drbd` — port `.res` template from upstream
     Java tests (input → expected output golden tests).
  5. `drbdadm`/`drbdsetup` exec wrappers behind interfaces (testable with
     fake exec).
  6. Reconcilers actually fill Status: `Resource` reconciler invokes
     storage provider + DRBD wrapper through satellite gRPC.
  7. Phase 3 exit smoke: 2-replica DRBD on the talos stand, PVC mount on
     node A → fail node A → PVC mounts on node B.

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
- [x] `make oracle` — uses piraeus-installed `linstor-controller.piraeus-datastore:3370` as the Java oracle; no separate deploy needed.

**Exit met**: full happy-path PVC test passes against upstream Java stack, on parallelizable stand.

### Phase 1 — Skeleton + contracts (done)

- [x] New module via `kubebuilder init`; controller-runtime manager + `pkg/rest` Runnable
- [x] `/v1/controller/version` returns a credible response (`pkg/version` constants pin LINSTOR contract version)
- [x] `golinstor.Client.Controller.GetVersion()` against our server returns no error (`pkg/rest/server_test.go`)
- [x] Full-branch tests for the version endpoint and the Runnable lifecycle (8 cases)
- [x] CSI MVP scope frozen in `docs/csi-api-surface.md`
- [x] golangci-lint v2 config + auto-lint hook (`golangci-lint@claude-code-companions`) wired
- [x] `linstor-common` submodule (properties.json, consts.json, drbdoptions.json)
- [x] `apiconsts` reused from `github.com/LINBIT/golinstor` — no fork needed
- [ ] OpenAPI types generated from `rest_v1_openapi.yaml` (oapi-codegen) — deferred; types are
      hand-written for now, codegen lands when we cover stats/error-reports endpoints

**Exit met.**

### Phase 2 — CRDs + reconcile (definition side done)

- [x] CRDs: `Node`, `StoragePool`, `ResourceGroup`, `ResourceDefinition`, `Resource`, `Snapshot`, `KVEntry` (VolumeDefinition is inline in RD spec)
- [x] controller-runtime manager wired in `cmd/main.go`; reconciler stubs scaffolded by kubebuilder
- [x] `Nodes.{GetAll,Get,Create,Modify,Delete}` work end-to-end against CRDs
- [x] `Store` interface with `InMemory` and `k8s` (CRD-backed) implementations, both exercised by the same `pkg/store/storetest` shared suite
- [x] `cmd/main.go --store={k8s|memory}` flag (default `k8s`)
- [x] envtest harness (`make setup-envtest`); `go test ./...` skips cleanly without assets
- [x] All write/read paths the CSI MVP needs land via `golinstor`:
      - `/v1/nodes` (CRUD)
      - `/v1/view/storage-pools`, `/v1/nodes/{n}/storage-pools[/{p}]`
      - `/v1/resource-groups` (CRUD), `/v1/resource-groups/{rg}/spawn`
      - `/v1/resource-definitions` (CRUD)
      - `/v1/resource-definitions/{rd}/volume-definitions` (CRUD)
      - `/v1/resource-definitions/{rd}/snapshots` (CRUD), `/v1/view/snapshots`
      - `/v1/view/resources`
      - `/v1/key-value-store` (CRUD; KVEntry CRD-backed, no ConfigMap limit)
- [x] **Autoplacer** — `POST /v1/resource-definitions/{rd}/autoplace`
      computes placement and creates `Resource` objects.
- [x] **Resource POST/DELETE** — `/v1/resource-definitions/{rd}/resources[/{node}]`.
- [x] No-op reconciler stubs for all CRDs wired in `cmd/main.go` (real
      reconciliation lands in Phase 3).
- [ ] piraeus-operator can create `LinstorSatellite`s and they appear in
      our API. — Validation deferred to Phase 5 burn-in.

**Exit met (definition side).** Real reconciliation work now lives in Phase 3.

### Phase 3 — Satellite + DRBD lifecycle

- [x] gRPC controller↔satellite proto definition (`proto/satellite/v1alpha1/satellite.proto`, 8 RPCs)
- [x] `cmd/satellite/main.go` skeleton + `pkg/satellite.Agent` runtime stub
- [x] Generated Go bindings (`make proto` → `pkg/satellite/proto/*.pb.go`)
- [x] Controller-side gRPC server (`pkg/satellitecontroller`) that satellites dial; Hello registers/idempotently-updates the Node CRD and returns ClusterID. 3 contract tests green.
- [x] `pkg/satellite.Agent` actually dials the controller and round-trips Hello (2 end-to-end tests). Wired into `cmd/main.go` via `--satellite-grpc-bind-address` (default `:7000`) and `--cluster-id`.
- [x] StoragePool: LVM-thin (`pkg/storage/lvm`) and ZFS / ZFS_THIN (`pkg/storage/zfs`) providers behind `pkg/storage.Provider` interface; FakeExec drives them in unit tests, RealExec wraps os/exec in production
- [x] ConfFileBuilder in Go (`pkg/drbd/conffile.go`) — port from upstream Java; deterministic output, 7 contract tests green
- [x] `drbdadm up/down/adjust/create-md/primary/secondary` exec wrappers behind interface (`pkg/drbd/drbdadm.go`); 7 contract tests via FakeExec
- [x] `drbdsetup events2` listener (`pkg/drbd/events2.go`): line parser + Watcher streaming `Event{Action,Kind,Fields}` to a channel; 7 contract tests
- [x] Resource reconciler (`pkg/satellite.Reconciler`) routes DesiredResource batches: storage provider CreateVolume per volume, ConfFileBuilder writes /etc/drbd.d/<name>.res, drbdadm create-md (first activation, non-DISKLESS) + adjust. Status writeback from events2 stream is the next slice.
- [x] Status writeback first half: `pkg/satellite.Observer` translates parsed drbd.Event values into ResourceObservedEvent (4 contract tests). Wire-up to gRPC stream pending the controller-side handler.
- [ ] Resource on 2 nodes replicates and goes UpToDate (real DRBD smoke)

**Exit**: smoke test with two replicas, real DRBD, PVC mounted on node A then on node B (failover).

### Phase 4 — Autoplacer + snapshots + intra-cluster shipping

- [x] Autoplacer: storage-pool-aware replica placement; weighted by FreeCapacity (greatest-free-first, deterministic ties on NodeName)
- [x] Snapshot satellite-side reconcile: `Reconciler.CreateSnapshot/DeleteSnapshot` route via in-memory resource→pool map populated by Apply (3 contract tests). Snapshot CRD reconciler controller-side TBD.
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
- [x] file storage provider (`pkg/storage/file`): FILE / FILE_THIN behind same Provider seam — fallocate (thick) / truncate (thin) for create, statfs(2) for pool capacity, snapshots intentionally unsupported (caller routes to LVM/ZFS instead). 9 contract tests.
- [ ] External-file management

### Phase 7 — Cluster operations + admin

- [ ] Cluster passphrase management
- [ ] Satellite eviction / restoration / lost-and-recover
- [x] Stats endpoint (`GET /v1/stats`): cluster-wide counters (nodes, RDs, resources, storage pools, snapshots). 2 contract tests.
- [x] Error reports stub (`/v1/error-reports` LIST returns []; GET /{id} → 404). Empty-but-present so `linstor error-reports list` doesn't choke. Real persistence lands when the controller starts buffering reports.
- [ ] All `/v1/view/*` aggregates
- [x] Controller properties endpoints (`/v1/controller/properties` GET/POST) — backed by KV-store instance "ControllerProps". Covers `linstor controller list-properties` / `set-property`. 3 contract tests.
- [ ] Property-info endpoints (`*/properties/info`)
- [x] Resource adjust / adjust-all (`POST /v1/resource-definitions/{rd}/adjust` and `.../resources/{node}/adjust`): existence check + 200; per-replica `drbdadm adjust` runs out-of-band via the satellite reconciler. 4 contract tests.

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
