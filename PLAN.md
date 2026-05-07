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

- **Phase**: 0 (dev stand)
- **Last action**: BM.HPC2.36 `linstor-dev-1` provisioned on OCI (IP `129.213.29.101`); blockstor repo skeleton committed locally; SSH timing out, awaiting boot completion.
- **Blocker**: none
- **Next concrete step**: SSH into the host, identify OS, install qemu-kvm + talosctl + kubectl + helm + drbd-utils + drbd kmod, run `make up NAME=test`.

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

## In scope (full project, not just MVP)

- `linstor-controller` (Java) → `blockstor-controller` (Go)
- `linstor-satellite` (Java) → `blockstor-satellite` (Go)
- All storage providers used by upstream: LVM, LVM-thin, ZFS, ZFS-thin,
  SPDK, NVMe-oF (target/initiator), EBS, OpenFlex, file, exos, remote,
  diskless
- All API surface used by any current LINBIT/Piraeus client
- Backup shipping (S3, L2L), snapshot shipping, schedules
- Encryption-at-rest (LUKS), DRBD encryption passphrases
- DRBD proxy, DRBD options
- Autoplacer with constraints (zones, node properties, replicas-on-different)
- Cluster bootstrap (passphrase, DB-backup/restore, satellite registration)
- The `linstor-common` artifacts (`properties.json`, `consts.json`,
  `drbdoptions.json`, generated code) consumed without the Java codegen step

## MVP slice (Phase 1–5 below)

The project is too big to do at once, so MVP fixes a narrow slice that lets
linstor-csi and piraeus-operator drive cozystack-style workloads:

- LVM and ZFS providers (thick + thin)
- The ~50 REST methods linstor-csi actually calls
- Replication, snapshots, snapshot-restore (in-cluster only — no shipping)
- Autoplacer with replica count + storage pool filter

Everything else is **scheduled for Phase 6+, not dropped**.

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

### Phase 0 — Dev stand (in progress)

- [x] BM.HPC2.36 provisioned on OCI (terraform applied)
- [ ] Host packages: qemu-kvm, libvirt, virt-install, talosctl, kubectl, helm, drbd-utils, drbd kmod loaded
- [ ] `make up NAME=test` brings up a 3-node Talos+QEMU cluster with DRBD extension
- [ ] `make piraeus` installs piraeus-operator and a `LinstorCluster`
- [ ] `make smoke` provisions a PVC, mounts it, writes data — green against upstream piraeus
- [ ] `make oracle` deploys Java LINSTOR controller (`piraeus-server:v1.33.2`) for contract-diff use
- [ ] `make up NAME=alice` and `make up NAME=bob` run in parallel without collision

**Exit**: full happy-path PVC test passes against upstream Java stack, on parallelizable stand.

### Phase 1 — Skeleton + contracts

- [ ] New module `cmd/controller/`, basic HTTP server
- [ ] OpenAPI types generated from `linstor-server/docs/rest_v1_openapi.yaml` (oapi-codegen)
- [ ] `apiconsts` ported from golinstor
- [ ] `/v1/controller/version` returns a credible response
- [ ] `golinstor.Client.Controller.GetVersion()` against our server returns no error
- [ ] CSI MVP scope frozen in `docs/csi-api-surface.md`

**Exit**: golinstor can talk to us for `GetVersion`; tests in CI green.

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

### Phase 4 — Autoplacer + snapshots

- [ ] Autoplacer: storage-pool-aware replica placement
- [ ] Snapshot CRD + reconcile (LVM/ZFS snapshot wrappers)
- [ ] Snapshot restore creates a new ResourceDefinition
- [ ] csi-sanity passes against our server

**Exit**: csi-sanity green; piraeus-operator e2e green for what they cover.

### Phase 5 — Compatibility burn-in

- [ ] Stand running for 24h continuous PVC churn (create/expand/snapshot/restore/delete)
- [ ] Contract-diff suite: replay 100+ recorded golinstor traces against our server and Java oracle, JSON diff zero
- [ ] On hidora-hikube cozystack cluster: parallel install in a separate namespace, side-by-side smoke

**Exit**: 24h+ stable; contract diffs zero on MVP scope.

### Phase 6 — Snapshot/backup shipping

- [ ] Snapshot shipping between nodes/clusters
- [ ] S3 remote, EBS remote, Linstor remote
- [ ] Backup create/restore/info/abort/ship
- [ ] Schedules (cron-driven backups)

**Exit**: feature parity with `BackupProvider` and `RemoteProvider` in golinstor.

### Phase 7 — Encryption + DRBD proxy + extras

- [ ] LUKS encryption layer
- [ ] DRBD encryption passphrase
- [ ] DRBD proxy enable/disable/configure
- [ ] DRBD options: full set from `drbdoptions.json`
- [ ] External files

### Phase 8 — Additional storage providers

- [ ] SPDK
- [ ] NVMe-oF target / initiator
- [ ] OpenFlex
- [ ] File / loop
- [ ] Exos
- [ ] Diskless / remote layer combinations

### Phase 9 — Cluster operations + admin

- [ ] DB backup/restore (CRDs are the DB; export/import)
- [ ] Cluster passphrase management
- [ ] Satellite eviction/restoration
- [ ] Stats endpoints, error reports, SOS-report
- [ ] External-file management
- [ ] All views (`/v1/view/*`)

### Phase 10 — Java decommission

- [ ] All clients (golinstor, linstor-csi, piraeus-operator) use only blockstor on a real cluster
- [ ] Cozystack production cluster fully migrated, Java pods removed
- [ ] Upstream `linstor-server` Java repo no longer referenced anywhere in cozystack

**Exit**: `grep -r piraeus-server cozystack | grep image:` returns nothing.

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

- **No push without explicit user approval.**
- First push: ask user to create the github repo (or grant me permission).

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

1. Github repo: do I create `aenix-io/blockstor` (or which org/name) when we're ready to push, or do you create it and grant me access?
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
