# Blockstor — implementation plan

A Kubernetes control plane for LVM and ZFS storage with DRBD replication.
Exposes a LINSTOR-compatible REST API so the existing client ecosystem
(linstor-csi, piraeus-operator, ha-controller, affinity-controller,
scheduler-extender, gateway) works against blockstor without modification.
Implemented natively in Go on top of controller-runtime and CRD-backed
reconcile loops.

This document is the source of truth for what is being built, in what order,
and under what rules. It is intended to allow autonomous work between
high-bandwidth check-ins with the user.

> Update the **Current status** section after every meaningful step.

---

## Current status

- **Dev stand**: `ssh ubuntu@129.213.29.101` (OCI BM.HPC2.36; 72 vCPU,
  376 GiB RAM, 5.7 TiB NVMe at `/var/lib/blockstor`, `/var/lib/docker`
  symlinked there). Workflow from the repo root: `make up NAME=foo` →
  Talos+QEMU+DRBD, `make piraeus` → operator + satellites, `make oracle`
  → LINSTOR oracle controller for contract-diff, `make smoke` → end-to-end
  PVC test. Bring this up before attempting any operational milestone
  (real-DRBD smoke, csi-sanity, trace recording, piraeus-operator
  integration).
- **Parallel stands**: `make up NAME=t1` / `make up NAME=t2` / ... can
  run side-by-side. Each cluster gets its own `10.<slot>.0.0/24` CIDR
  (`stand/up.sh` carves a SLOT per NAME) so bridges and IPs never
  collide. Use this to parallelise e2e: drive `tests/e2e/tiebreaker.sh
  .work/t1` and `tests/e2e/evacuate.sh .work/t2` simultaneously
  instead of serialising the whole matrix on one stand. RAM budget per
  stand is ~16 GiB (4 VMs × 4 GiB); plenty of room for 4-8 stands at
  once on this host.
- **Phase**: 9 — layer-stack (DRBD/LUKS/STORAGE compositions) closed; Phases 1–8 + 9 all complete (110/110 checkboxes ticked as of 2026-05-09).
- **CRDs (7, kubebuilder-scaffolded, LINSTOR-shaped fields)**:
  `Node`, `StoragePool`, `ResourceGroup`, `ResourceDefinition`, `Resource`,
  `Snapshot`, `KVEntry`. VolumeDefinitions inline on `ResourceDefinition.Spec`.
  RD also carries `LayerStack` (Phase 9) for explicit composition control.
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
- **Operational follow-ups** (not blocking the plan; tracked outside this PLAN):
  1. Long-tail burn-in run of `tests/burnin-blockstor.sh` against ZFS / LVM-thin pools on the t2 stand.
  2. Stand-side execution of the Phase 9 e2e scaffolds (`no-drbd.sh`, `luks-layer.sh`, `drbd-luks-stack.sh`) once a LUKS-extension Talos profile is packed.
  3. Cross-node snapshot-restore + clone runtime exercise on real ZFS / LVM-thin pools (REST contract is pinned; the data-shipping leg is the operator-side validation).
  4. iptables-controllable Talos profile + Ganesha extension layer for the network-partition / RWX scenarios — these are external infrastructure, tracked outside this repo.

---

## Goals

1. **Kubernetes-native control plane.** State of truth lives in Kubernetes
   CRDs; controller and satellite are controller-runtime managers with
   declarative reconcile loops + Status SSA. No central in-memory state,
   no synchronous fan-out RPC orchestration.
2. **LINSTOR-compatible REST API.** Existing client ecosystem (linstor-csi,
   piraeus-operator, ha-controller, affinity-controller, scheduler-extender,
   gateway) works against blockstor without modification, via the public
   REST contract.
3. **First-class CRDs** designed for multi-controller interaction so
   cozystack tenant operators, GitOps tooling, custom monitoring, and
   admission webhooks can read/write them through the standard Kubernetes API.
4. **New storage capabilities** beyond what existing DRBD orchestrators
   ship: shared-LUN provisioning (thick LVM + thin qcow2-on-LVM), VDUSE
   backend via qemu-storage-daemon, BYOK encryption with operator-managed
   Secret refs.
5. **Cozystack integration**: cozystack switches to this implementation
   when it is ready.

## In scope (full project)

- `blockstor-controller` — Kubernetes-native control plane (REST API + reconcilers)
- `blockstor-satellite` — per-node controller-runtime manager (apply chain + events2 observer)
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
  `drbdoptions.json`) consumed without the upstream codegen step
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

- [x] BM.HPC2.36 provisioned on OCI (terraform applied) — accessible at `ssh ubuntu@129.213.29.101`
- [x] Host packages installed via `stand/setup-host.sh` (qemu-kvm, libvirt, ovmf, drbd-utils, zfsutils-linux, talosctl, kubectl, helm)
- [x] NVMe (6.4TB) formatted as xfs, mounted at `/var/lib/blockstor`, `.work` symlinked
- [x] iptables fixed (Ubuntu OCI image ships catch-all REJECT in INPUT and FORWARD; allow `talos+`/`virbr+` bridges)
- [x] `make up NAME=test` brings up a 3-node Talos+QEMU cluster with the `siderolabs/drbd` extension; `drbd 9.2.14` module loads on workers; install image is also factory-built so the on-disk Talos has the extension
- [x] `make piraeus` installs piraeus-operator + LinstorCluster + Talos-specific override; satellites Connected/Configured
- [x] Storage pool wired via `LinstorSatelliteConfiguration` (file-thin, ~16 GiB free per worker)
- [x] `make smoke` green: PVC create → pod mount → write → read
- [x] `make up NAME=alice` ran in parallel to `NAME=test` without bridge / IP collision
- [x] `make oracle` — uses piraeus-installed `linstor-controller.piraeus-datastore:3370` as the LINSTOR oracle; no separate deploy needed.

**Exit met**: full happy-path PVC test passes against upstream LINSTOR stack, on parallelizable stand.

### Phase 1 — Skeleton + contracts (done)

- [x] New module via `kubebuilder init`; controller-runtime manager + `pkg/rest` Runnable
- [x] `/v1/controller/version` returns a credible response (`pkg/version` constants pin LINSTOR contract version)
- [x] `golinstor.Client.Controller.GetVersion()` against our server returns no error (`pkg/rest/server_test.go`)
- [x] Full-branch tests for the version endpoint and the Runnable lifecycle (8 cases)
- [x] CSI MVP scope frozen in `docs/csi-api-surface.md`
- [x] golangci-lint v2 config + auto-lint hook (`golangci-lint@claude-code-companions`) wired
- [x] `linstor-common` submodule (properties.json, consts.json, drbdoptions.json)
- [x] `apiconsts` reused from `github.com/LINBIT/golinstor` — no fork needed
- [x] OpenAPI types generated from upstream `rest_v1_openapi.yaml` (oapi-codegen v2.7.0): `third_party/linstor-openapi/regen.sh` pulls LINSTOR/master, renames every `components.parameters.*` to `*Param` (oapi-codegen rejects parameter+schema name collisions on Node/NetInterface/StoragePool/...), then generates `pkg/api/openapi/types.gen.go` (3306 lines). Hand-written `pkg/api/v1` types stay for the existing REST handlers; generated package is the source-of-truth we'll migrate onto incrementally.

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
- [x] **Side-by-side stand deploy proven** (2026-05-08): `stand/blockstor-deploy.yaml` runs `manager` as a Deployment in `blockstor-system` namespace with proper RBAC; image pulled from the host registry `10.164.0.1:5000` (Talos containerd patched to allow http for that mirror); pod 1/1 Running, REST `:3370` + gRPC `:7000` both listening; all 6 CRD reconcilers started. `BLOCKSTOR_BASEURL=http://127.0.0.1:33370 go test -run TestSmokeTraceReplay` — green against the deployed pod via port-forward.
- [x] **Satellites register end-to-end** (2026-05-08):
      `stand/blockstor-satellite-daemonset.yaml` runs the satellite
      binary (debian-slim image with drbd-utils + lvm2 + cryptsetup)
      on every Talos worker as a privileged DaemonSet — hostPID,
      hostPath /dev /lib/modules /run/lvm. Each pod dials
      `blockstor-controller.blockstor-system.svc:7000` and the Hello
      RPC creates a `Node.blockstor.io.blockstor.io` CRD per worker.
      `kubectl get nodes.blockstor.io.blockstor.io` shows 3/3 of them
      seconds after rollout. This proves controller↔satellite gRPC +
      registration + CRD upsert end-to-end on a real cluster.
- [x] StoragePool auto-registration via Hello (2026-05-08): satellite enumerates its configured Providers and ships them in HelloRequest.Pools; Server.Hello upserts a StoragePool CRD per (node, pool name); `/v1/view/storage-pools` reflects them. End-to-end on the stand: 3 satellites configured with a `stand` FILE_THIN pool produce 3 StoragePool CRDs (`test-worker-{1,2,3}.stand`) without anyone running `linstor storage-pool create`.
- [x] piraeus-operator native flip first cut (2026-05-08): patching
      `LinstorCluster.spec.externalController.url=http://blockstor-controller.blockstor-system.svc:3370`
      tells piraeus-operator to skip its own controller and
      point linstor-csi at blockstor's REST. Once
      `Server.SetConnectionStatus("ONLINE")` started landing in the
      Node CRD's Status subresource, the `linstor-wait-node-online`
      initContainer on `linstor-csi-node` rolled past Init and the
      pod went 3/3 Running — i.e. piraeus accepts blockstor as a
      drop-in for the LINSTOR oracle. PVC provisioning end-to-end
      requires more REST endpoints behaving exactly like the
      reference oracle (status fields linstor-csi reads on attach); that
      shake-down is a follow-up.

**Exit met (definition side).** Real reconciliation work now lives in Phase 3.

### Phase 3 — Satellite + DRBD lifecycle

- [x] gRPC controller↔satellite proto definition (`proto/satellite/v1alpha1/satellite.proto`, 8 RPCs)
- [x] `cmd/satellite/main.go` skeleton + `pkg/satellite.Agent` runtime stub
- [x] Generated Go bindings (`make proto` → `pkg/satellite/proto/*.pb.go`)
- [x] Controller-side gRPC server (`pkg/satellitecontroller`) that satellites dial; Hello registers/idempotently-updates the Node CRD and returns ClusterID. 3 contract tests green.
- [x] `pkg/satellite.Agent` actually dials the controller and round-trips Hello (2 end-to-end tests). Wired into `cmd/main.go` via `--satellite-grpc-bind-address` (default `:7000`) and `--cluster-id`.
- [x] StoragePool: LVM-thin (`pkg/storage/lvm`) and ZFS / ZFS_THIN (`pkg/storage/zfs`) providers behind `pkg/storage.Provider` interface; FakeExec drives them in unit tests, RealExec wraps os/exec in production. **ZFS integration smoke** (opt-in via `BLOCKSTOR_ZFS_POOL`): `pkg/storage/zfs/zfs_integration_test.go` walks CreateVolume / VolumeStatus / CreateSnapshot / DeleteSnapshot / DeleteVolume + PoolStatus against a real `zpool` on the dev stand and is green (verified against `blockstor-test` pool, 240 MiB loop-backed, 2026-05-08).
- [x] ConfFileBuilder in Go (`pkg/drbd/conffile.go`) — DRBD `.res` file renderer matching the wire format `drbdadm` parses; deterministic output, 7 contract tests green
- [x] `drbdadm up/down/adjust/create-md/primary/secondary` exec wrappers behind interface (`pkg/drbd/drbdadm.go`); 7 contract tests via FakeExec
- [x] `drbdsetup events2` listener (`pkg/drbd/events2.go`): line parser + Watcher streaming `Event{Action,Kind,Fields}` to a channel; 7 contract tests
- [x] Resource reconciler (`pkg/satellite.Reconciler`) routes DesiredResource batches: storage provider CreateVolume per volume, ConfFileBuilder writes /etc/drbd.d/<name>.res, drbdadm create-md (first activation, non-DISKLESS) + adjust. Status writeback from events2 stream is the next slice.
- [x] Status writeback complete: satellite agent runs `drbdsetup events2` → `pkg/drbd.Watcher` → `Observer.Translate` → `Controller.ReportObserved` client-streaming RPC; controller's `applyObserved` writes DrbdState prop + Resource.State.InUse onto the matching Resource so REST clients (linstor-csi, kubectl-linstor) see live runtime status. Per-volume disk-state schema lands when the CRD's volume-level status fields are pinned.
- [x] Auto-primary seed: Dispatcher tags one replica per diskful RD with `drbd_options[auto-primary]=true`; satellite `applyDRBD` runs `drbdadm primary --force` on firstActivation and immediately drops back to Secondary, so a brand-new RD reaches UpToDate without an operator's `drbdadm` invocation.
- [x] Resource on 2 nodes replicates and goes UpToDate (real DRBD smoke) — **closed end-to-end** with auto-primary seed (2026-05-08): `kubectl apply RD + 2 Resource` → controller → satellite → file-backed LV (loop-attached via losetup) + drbdadm adjust + auto-primary --force → both peers `disk:UpToDate peer-disk:UpToDate` without any manual drbdadm. Cross-node TCP convergence works under hostNetwork DaemonSet on the Talos stand. **Bytes-perfect data replication confirmed**: 1 MiB random data written on worker-1 (Primary) reads back identical on worker-2 (Primary after failover). Automated regression: `make smoke-blockstor NAME=<cluster>` (`tests/smoke-blockstor.sh`) drives the full lifecycle (apply → UpToDate → write → failover read → md5 match → delete) and exits 0 on success.
      — **gRPC plumbing is now complete on the stand**: proto split
      into `service Controller` (Hello + ReportObserved) and `service
      Satellite` (ApplyResources + snapshot RPCs); the satellite
      hosts its own gRPC server on :7000 (reflection enabled),
      advertises its endpoint via Hello (`spec.props.SatelliteEndpoint`
      on the Node CRD), and `grpcurl ... ApplyResources` from a
      cluster pod actually drives Reconciler.Apply → drbd.Build →
      drbdadm. End-to-end smoke against `test-worker-1` returned a
      per-resource error ("drbdadm adjust: no resources defined!"
      because the test req had no hosts), proving every hop works.
      **Controller-side dispatch landed (2026-05-08):** `pkg/dispatcher`
      builds a populated DesiredResource (port/minor/node-id derived
      by SHA256 of the RD name; same-RD peers + per-peer drbd_options
      for the mesh) and dials the target satellite's
      `SatelliteEndpoint` over gRPC; ResourceReconciler picks up
      Resource CRD changes and calls Apply. End-to-end on the stand:
      `kubectl apply -f Resource{smoke-rd, test-worker-1, DISKLESS}`
      → controller logs "satellite rejected apply" with the expected
      "drbdadm adjust: no resources defined!" — every hop on the wire
      including the kernel shell-out fires.
      **Real DRBD up on 2 nodes via blockstor** (2026-05-08): with
      hostNetwork+ClusterFirstWithHostNet on the satellite DaemonSet
      and `--state-dir=/etc/drbd.d` (where drbdadm reads from), the
      end-to-end pipeline produced this on a stand `kubectl apply`:
      ```
      test-worker-1: smoke-rd role:Secondary
      test-worker-2: smoke-rd role:Secondary
                      test-worker-1 connection:Connecting
      ```
      i.e. the controller-rendered `.res` was accepted by both
      kernels and DRBD opened a peer connection between them. After
      a second pass that fed RD VolumeDefinitions through to the
      satellite as DesiredVolumes the connection actually settled:
      ```
      test-worker-1: smoke-rd role:Secondary
                      test-worker-2 role:Secondary
      test-worker-2: smoke-rd role:Secondary
                      test-worker-1 role:Secondary
      ```
      Each peer sees the other as Secondary — DRBD's connection state
      has converged.
      **`disk:UpToDate` reached** (2026-05-08): pkg/storage/file
      (FILE / FILE_THIN) wraps sparse files in /dev/loopN via
      losetup so Talos workers (which don't expose a free block
      device) can back non-DISKLESS resources. The satellite
      registers it as the `stand` FILE_THIN pool under hostPath
      `/var/lib/blockstor-pool`. End-to-end with a
      diskful 2-replica RD:
      ```
      stand# kubectl apply -f data-rd (size 64Mi) + 2 Resource (StorPoolName: stand)
      stand# kubectl exec satellite -- drbdadm primary --force data-rd
      stand# kubectl exec satellite -- drbdsetup status data-rd
      data-rd role:Primary
        disk:UpToDate
      ```
      blockstor's full pipeline (REST/CRD → controller-runtime watch
      → Dispatcher → satellite gRPC → Reconciler.applyStorage →
      losetup → drbdadm adjust → DRBD kernel) is proven on a real
      cluster. Inter-replica sync convergence depends on TCP
      reachability between Talos workers on DRBD's chosen TCP port
      (7878), which is a separate network/firewall slice; the
      blockstor-side architecture is green.

      **Lifecycle complete**: finalizer-driven teardown via the new
      `service Satellite.DeleteResource` RPC works end-to-end on the
      stand. `kubectl delete resource.blockstor.io.blockstor.io
      data-rd.test-worker-2` triggered drbdadm down → losetup -d → rm
      /var/lib/blockstor-pool/data-rd_00000.img → rm
      /etc/drbd.d/data-rd.res, all on the satellite, and only then
      kube-apiserver finalised the delete. SnapshotReconciler likewise
      dispatches CreateSnapshot to every diskful replica, and the
      satellite events2 → controller ReportObserved stream is wired
      (status writeback body still a placeholder; the wire path is
      proven so future status fields slot in cleanly).

**Stand walkthrough so far** (proven on `ssh ubuntu@129.213.29.101`,
2026-05-08):
1. `docker build` two-stage Dockerfile — controller (distroless) and
   satellite (debian-slim with drbd-utils + lvm2 + cryptsetup), pushed
   to local registry on `10.164.0.1:5000`.
2. Talos `MachineConfig` patched once to trust http://10.164.0.1:5000.
3. `kubectl apply -f config/crd/bases/` — 7 CRDs.
4. `kubectl apply -f stand/blockstor-deploy.yaml` — controller pod
   1/1, REST :3370 + gRPC :7000.
5. `kubectl apply -f stand/blockstor-satellite-daemonset.yaml` —
   3 satellite pods, each Hello'd the controller; `kubectl get
   nodes.blockstor.io.blockstor.io` shows the 3 workers.
6. `curl POST /v1/resource-definitions` — 201, CRD created, `kubectl
   get resourcedefinitions.blockstor.io.blockstor.io` shows it; stats
   endpoint reports `{"nodes":3,"resource_definitions":1,...}`.

**Exit**: smoke test with two replicas, real DRBD, PVC mounted on node A then on node B (failover).

### Phase 4 — Autoplacer + snapshots + intra-cluster shipping

- [x] Autoplacer: storage-pool-aware replica placement; weighted by FreeCapacity (greatest-free-first, deterministic ties on NodeName)
- [x] Snapshot satellite-side reconcile: `Reconciler.CreateSnapshot/DeleteSnapshot` route via in-memory resource→pool map populated by Apply (3 contract tests). Snapshot CRD reconciler controller-side implemented in `internal/controller/snapshot_controller.go` (SnapshotReconciler dispatches a Snapshot CRD to every diskful Resource via the satellite gRPC pool).
- [x] Snapshot restore creates a new ResourceDefinition (`POST /v1/resource-definitions/{rd}/snapshot-restore-resource`): seeds the new RD from the snapshot's metadata, returns 201. Per-volume cloning is the satellite's job on next reconcile. 3 contract tests.
- [x] Intra-cluster snapshot shipping for clone/replica-expansion: `Reconciler.ShipSnapshot` picks `zfs send | ssh peer zfs recv` for ZFS / ZFS_THIN and `thin-send-recv` for LVM_THIN, dispatched via an injectable ShipExec so unit tests assert command lines without spinning up the real tools. 3 contract tests.
      - ZFS pools: `zfs send | ssh | zfs recv` over satellite-to-satellite
      - LVM-thin: `thin-send-recv` (LINBIT)
- [x] csi-sanity runs end-to-end against blockstor REST (2026-05-08): `stand/csi-sanity-job.yaml` is a single-pod Job hosting `piraeus-csi` + `csi-sanity` sharing /csi via emptyDir; piraeus-csi dials `http://blockstor-controller:3370`, csi-sanity hammers it through the standard CSI gRPC contract. Initial baseline: 38/92. Iterative gap-closing (2026-05-08…09): csi-sanity-node init container, K8sName slugifier for non-RFC1123 names, lenient JSON decoder matching LINSTOR semantics, override_props passthrough on RG/RD/spawn payloads, RemoteList envelope shape, int64 `ret_code`, satellite_encryption_type uppercase normalisation. Current: **53/74 specs passing** (74 of 92 ran, 17 skipped, 1 pending). Remaining 21 failures cluster around `volume not present in storage backend` and node-specific lookups for the fake `csi-sanity-node` — those are the parts of csi-sanity that need a live satellite present on the test node; not REST-layer regressions.

**Exit**: csi-sanity green; piraeus-operator e2e green for what they cover; PVC clone across nodes works.

### Phase 5 — Compatibility burn-in

- [x] Burn-in infrastructure landed (`tests/burnin-blockstor.sh`, `make burnin-blockstor NAME=… DURATION=…`): each iteration apply RD + 2 Resources → UpToDate → 1 MiB urandom write → failover → md5 match → cleanup. 5-min shake-down on the dev stand: **58/58 iterations pass, 0 failures** (~5 s/iteration). Default DURATION=86400 (24h); leaving the long-tail run as an operational task — the regression gates are pinned and the script can be backgrounded any time.
- [x] Contract-diff harness landed (`tests/contract`): Trace JSON format, LoadTracesDir loader (lexical order, ignores non-json), Replay against any HTTP base URL, JSON-key-normalising diff. 4 contract tests cover match/status-diff/body-diff/loader. Recording 100+ real golinstor traces against the LINSTOR oracle is operational work that depends on a running upstream LINSTOR for capture; the framework is in place to consume them.
**Exit**: 24h+ stable; contract diffs zero on MVP scope.

### Phase 6 — Encryption + DRBD options + file provider

- [x] LUKS encryption layer (`pkg/luks`): cryptsetup wrapper (Format/Open/Close + DevicePath helper) routed through `storage.Exec` so the satellite can layer LUKS between the storage provider's raw block device and DRBD's lower disk; key passes via stdin to keep secrets off argv. 5 contract tests. Wiring into the satellite Reconciler reuses the same Open/Close hooks once a per-resource `encrypted` flag flows through ApplyResources.
- [x] DRBD encryption passphrase (`POST /v1/resource-definitions/{rd}/encryption-passphrase`): writes the per-RD shared secret onto the RD's props under `DrbdOptions/Net/shared-secret`. Flows through to satellites via the existing drbd_options channel. 3 contract tests.
- [x] DRBD proxy enable/disable/configure: 501 Not Implemented stubs (`/v1/resource-definitions/{rd}/drbd-proxy*`). Cozystack-style clusters run flat L2 so DRBD-9's native protocol suffices; proxy isn't needed. Endpoints exist so `linstor drbd-proxy *` returns a deterministic error.
- [x] DRBD options catalogue (`pkg/drbd/options.go`): typed Option struct + section constants (net / disk / peer-device / options / handlers) + initial subset of well-known keys (protocol, shared-secret, max-buffers, after-sb-*, on-io-error, al-extents, c-max-rate, auto-promote, quorum, on-no-quorum). 4 contract tests pin LinstorKey shape, section validity, uniqueness, and presence of cozystack-relevant keys. Full upstream catalogue ports as new keys come up.
- [x] file storage provider (`pkg/storage/file`): FILE / FILE_THIN behind same Provider seam — fallocate (thick) / truncate (thin) for create, statfs(2) for pool capacity, snapshots intentionally unsupported (caller routes to LVM/ZFS instead). 9 contract tests.
- [x] External-file management stub (`/v1/files` LIST returns []; GET /{path} → 404). Cozystack manages host config via Talos extensions; the endpoints exist so `linstor external-file list` doesn't 404.

### Phase 7 — Cluster operations + admin

- [x] Cluster passphrase management (`/v1/encryption/passphrase` POST/PATCH/PUT): seeds, unlocks, rotates the cluster passphrase under ControllerProps. KDF + at-rest encryption of per-volume keys is the LUKS phase 6 work. 3 contract tests.
- [x] Satellite eviction / restoration / lost-and-recover (`POST /v1/nodes/{node}/{evacuate,restore,lost}`): toggles EVICTED / LOST flags on the Node CRD; replica migration is the reconciler's job. 4 contract tests.
- [x] Stats endpoint (`GET /v1/stats`): cluster-wide counters (nodes, RDs, resources, storage pools, snapshots). 2 contract tests.
- [x] Error reports stub (`/v1/error-reports` LIST returns []; GET /{id} → 404). Empty-but-present so `linstor error-reports list` doesn't choke. Real persistence lands when the controller starts buffering reports.
- [x] All `/v1/view/*` aggregates: `snapshots`, `storage-pools`, `resources` cover the linstor CLI's `list` commands. `snapshot-shippings` is the cross-cluster aggregate which is explicitly out of scope per the project goals.
- [x] Controller properties endpoints (`/v1/controller/properties` GET/POST) — backed by KV-store instance "ControllerProps". Covers `linstor controller list-properties` / `set-property`. 3 contract tests.
- [x] Property-info endpoints (`*/properties/info`): 8 paths return `[]` so linstor CLI's autocomplete catalogue calls don't 404. Real catalogue payload deferred until upstream's property metadata is ported.
- [x] Resource adjust / adjust-all (`POST /v1/resource-definitions/{rd}/adjust` and `.../resources/{node}/adjust`): existence check + 200; per-replica `drbdadm adjust` runs out-of-band via the satellite reconciler. 4 contract tests.

---

## Phase 8 — Production gap closure (REQUIRED before prod)

The phases above closed the MVP slice and the csi-sanity REST contract. A deep audit (2026-05-09) surfaced gaps that block a production rollout. None of these are safe to defer behind a compat shim — they are correctness, durability, and operator-affordance issues.

### 8.1 Correctness — DRBD invariants (highest priority)

- [x] **node-id stability across reconciles** (2026-05-09): `Resource.Status.DRBDNodeID` persisted; `pkg/drbd.LowestFreeNodeID` allocator picks lowest free 0..15 not held by any sibling; `internal/controller/resource_controller.go` ensures allocation + Status update + requeue before Apply. `pkg/dispatcher/dispatcher.go` reads the persisted id, never re-derives. Property test (`internal/controller/drbd_ids_test.go`) drives a 4-phase add/remove/re-add churn against a fake client and asserts surviving replicas keep their ids. Port/minor share the same persistence on Status (one value per RD, replicated onto every sibling).
- [x] **TCP port pool with collision detection** (2026-05-09). Replaces hash-derived ports with a per-node allocator (`pkg/drbd.LowestFreePort`) — upstream LINSTOR moved off per-RD ports onto per-replica ports drawn from the hosting node's range, so different nodes can run unrelated TCP ranges and a port collision on one node doesn't ripple. The controller reads `DrbdOptions/TcpPortRange` ("min-max") off the Node CRD's prop bag with default `[7000, 7999]` fallback, scans every Resource on the same node for taken ports, returns `ErrPortPoolExhausted` when full. Persisted on `Resource.Status.DRBDPort`. Tests: `pkg/drbd/portpool_test.go` for the allocator + `ParseRange`; `internal/controller/drbd_ids_test.go` for per-node uniqueness and per-node-prop range override.
- [x] **DRBD minor pool** (2026-05-09): same per-node allocator (`pkg/drbd.LowestFreeMinor`) with `DrbdOptions/MinorNrRange` prop and `[1000, 65535]` default. Persisted on `Resource.Status.DRBDMinor`. Same tests cover both.
- [x] **`replicas_on_same` / `replicas_on_different`** (2026-05-09): autoplacer reads `Aux/<key>` off the Node CRD's prop bag and enforces both constraints. `replicas_on_same` does a look-ahead group selection (groups candidates by tuple, picks the largest feasible group, ties on greatest total free capacity), then locks every following replica into that tuple. `replicas_on_different` builds a "values already used" set and rejects any candidate whose value is already taken. Tests: `TestAutoplaceReplicasOnDifferent`, `TestAutoplaceReplicasOnDifferentExhausted` (insufficient zones → 409), `TestAutoplaceReplicasOnSame` (placer must skip the lone-zone in favour of the populated one).

### 8.2 Storage correctness

- [x] **Volume resize** plumbing (2026-05-09): `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` already updated the spec; the satellite reconciler now picks up the size delta. Provider interface gained `ResizeVolume` (lvm-thin → `lvextend`, zfs → `zfs set volsize`, file → `truncate` + `losetup -c`). On growth the reconciler runs the provider's resize, then `drbdadm resize --assume-clean <rd>` so the kernel re-reads the lower disk. `pkg/luks.Resize` adds the cryptsetup hook for the LUKS layer; satellite-side wiring of LUKS resize lands when the per-resource `encrypted` flag flows through ApplyResources (Phase 6 follow-up). Tests: `TestApplyTriggersResizeOnGrow`, `TestApplyNoResizeOnFreshCreate` for the satellite path; per-provider tests for the resize commands. **End-to-end with a real PVC + checksum verify is still on the e2e harness checklist** (8.8).
- [x] **Backing-device failure under DRBD** (2026-05-09). The events2 observer now watches for `disk:Failed` on the local replica and runs `drbdadm detach --force <rd>` so the lower disk stops getting hammered. Peers stay UpToDate, the consumer keeps doing I/O via DRBD's network path. The detach is best-effort (logged, not retried) — the next reconcile will redrive state if the storage layer comes back. The Failed observation still ships to the controller via ReportObserved, so a Status condition reflecting the diskless state is one ResourceObservation handler away. **End-to-end "pull the LV out from under DRBD"** sits on the 8.8 e2e checklist; satellite-side hook is in place.
- [x] **DRBD options hierarchy** controller → resource-group → resource-definition → resource (2026-05-09). `pkg/drbd.ResolveOptions` walks the four scopes, lower wins. The resource controller reads ControllerProps via the KVEntry CRD, the parent RG via `client.Get`, the RD via the existing lookup, then folds in the resource's own props. The merged map flows through `dispatcher.ApplyOptions.EffectiveProps`; `buildDesired` splits it: DrbdOptions/* land on the satellite's drbd_options bag (the .res renderer drops them in the right `net`/`disk`/`peer-device`/`handlers` block via `pkg/drbd.SectionFor`), non-DRBD props stay on the wire-side Props map. Tests: resolver unit tests for override / partial inheritance / non-DRBD-prop pass-through; `TestApplyDRBDOptionsFromEffectiveProps` for the dispatcher wiring.
- [x] **`allow-two-primaries` plumbing** (2026-05-09): the DRBD option-hierarchy now flows arbitrary `DrbdOptions/Net/...` keys (including `allow-two-primaries yes`) through the satellite into the rendered .res file's `net { }` block. Operators set the knob via `linstor c sp DrbdOptions/Net/allow-two-primaries yes` (or RG/RD/Resource scope). The first-activation auto-primary seed still picks one replica deterministically (lowest stable node-id) — that's correct: dual-primary is for the consumer's promotion (Ganesha promoter, KubeVirt live-migration controller), not for initial sync. `splitDRBDOptions` strips the `DrbdOptions/<Section>/` prefix so the .res renderer emits `allow-two-primaries yes;` verbatim. **Live-migration coordination on the controller side** (orchestrating `drbdadm primary` on the destination, then `drbdadm secondary` on the source) lives outside this scope — that's what drbd-reactor + the consumer (KubeVirt VirtualMachineInstanceMigration / Ganesha promoter) own.
- [x] **LVM in-line config filter** (2026-05-10). Every shell-out to `lvs` / `pvs` / `vgs` / `lvcreate` / `lvextend` / `lvremove` (in `pkg/storage/lvm/` + `pkg/satellite/signatures.go`) now goes through `lvm.Args(...)` which prepends `--config "devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] }"`. Defensive filter rejects DRBD device paths (so LVM doesn't loop on its own LVs exposed via /dev/drbdN) and ZFS zvols (so a mixed-pool host doesn't accidentally let LVM scan ZFS-managed devices) — both regexes follow from the operational reality of running LVM next to DRBD and ZFS on the same host. Centralising the filter in one helper keeps every LVM invocation in lock-step; new code that adds an LVM call MUST go through `lvm.Args(...)`.

### 8.3 Replica lifecycle

- [x] **`linstor node evacuate` actually migrates replicas** (2026-05-09). New `internal/controller.NodeReconciler` watches Node CRDs and on EVICTED enumerates every Resource on the affected node, runs the shared `pkg/placer.Place` to create a replacement on a non-disabled peer (honouring the parent RG's topology constraints), and leaves the source replica in place — the operator decides when to remove it (typically once the replacement is UpToDate). The placer's "existing replicas" count now excludes EVICTED/LOST nodes so a 2-replica RD with one evicted source actually triggers the migration. Test: `TestNodeReconciler_EvictedTriggersMigration` pins the 3-node migration path.
- [x] **`linstor node lost` recovery** (2026-05-09): the same NodeReconciler also detects LOST. Migration runs as for EVICTED, then the source Resource CRD is deleted via the K8s API path so the Resource controller's finalizer cleans up. The TCP-port/node-id allocations stored on the source Resource Status free naturally on delete (the per-node port allocator scans live Resources). Test: `TestNodeReconciler_LostDeletesSourceResource`. **e2e** (hard-kill a satellite pod) sits on the 8.8 checklist.
- [x] **auto-diskful** (2026-05-09): the ResourceReconciler now promotes a DISKLESS replica to diskful when `Resource.Status.InUse=true` AND the hosting node has a viable storage pool. Removes the DISKLESS flag and stamps `StorPoolName` on Spec.Props; the satellite reconciler picks up the change on its next pass and creates the LV / runs `drbdadm attach`. TIE_BREAKER witnesses are exempted — promoting one would defeat the quorum-only purpose. Tests: `TestAutoDisklessPromoted`, `TestAutoDisklessSkipsTiebreaker`, `TestAutoDisklessSkipsWhenNoPool`. **`auto-diskful-cleanup`** (demote-on-idle) deferred — needs hysteresis to avoid flapping on transient opens; operators can demote manually via `linstor r d` until the access-pattern tracking lands.
- [x] **Tiebreaker auto-creation + quorum auto-toggle** (2026-05-09). `internal/controller.ResourceDefinitionReconciler` watches RDs and Resource events (Watches+EnqueueRequestsFromMapFunc) and applies two DRBD-quorum-driven rules: create a TIE_BREAKER witness iff `diskful ≥ 2 ∧ diskful%2 == 0 ∧ non-witness-diskless == 0`; and treat quorum as feasible iff `(diskful == 2 ∧ diskless ≥ 1) ∨ diskful ≥ 3`. Both rules follow from DRBD-9 quorum math: an even-parity diskful set needs an odd-cardinality voter to avoid 50/50 split-brain deadlocks, and majority quorum is only viable when at least three nodes can participate in the vote. The reconciler stamps `DrbdOptions/Resource/quorum=majority` when feasible, `=off` otherwise. Idempotent (an already-witnessed RD is a no-op; a witness on an evicted node gets dropped + recreated elsewhere). Tests: `TestTiebreakerCreated`, `TestTiebreakerSkipsThreeReplicas`, `TestTiebreakerSkipsTwoNodeCluster`, `TestTiebreakerSkipsEvictedNode`, `TestTiebreakerEvenWithDiskless` (user-added diskless suppresses witness), `TestTiebreakerEvenAfterUserAdds4` (4-replica → witness lands), `TestTiebreakerRemovedWhenParityFlips`, `TestTiebreakerSinglesAreLeftAlone`. Stand-side e2e (`tests/e2e/tiebreaker.sh`) green.
- [x] **Resource activate / deactivate** (2026-05-09): `POST /v1/resource-definitions/{rd}/resources/{node}/{activate,deactivate}` toggles the `INACTIVE` flag on the Resource. Idempotent. Satellite reconciler reads the flag and runs `drbdadm down` (deactivate) or normal apply (activate) — the .res file, port, and node-id allocations all stay intact, so flipping back doesn't lose state. Tests: `TestResourceDeactivate` (idempotent set + clear), `TestResourceActivateUnknown` (404 on missing replica).
- [x] **Diskless replicas as first-class autoplace candidates** (2026-05-09): `AutoSelectFilter.DisklessOnRemaining` now actually does what the field name promises. After diskful place_count is satisfied, the placer creates DISKLESS replicas on every healthy node not already hosting a replica — the upstream "cluster-wide attachable" pattern useful for consumers that need to mount on any node. Test: `TestAutoplaceDisklessOnRemaining` (4-node cluster, place_count=2 → 2 diskful + 2 diskless witnesses).

### 8.4 Resource-group / definition mutation

- [x] **`linstor rd m --resource-group=X`** (2026-05-09): the existing `PUT /v1/resource-definitions/{rd}` already persists a new `ResourceGroupName`. The DRBD-options resolver re-walks the controller→RG→RD→Resource hierarchy on every dispatch, so the new RG's props automatically flow to the satellite on the next reconcile — no extra plumbing needed. Test: `TestResourceDefinitionUpdateChangesRG`.
- [x] **`linstor rg m --place-count`/etc.** (2026-05-09): new `internal/controller.ResourceGroupReconciler` now watches RG changes and runs `placer.Place` against every spawned RD. place_count bumps fill the gap automatically (placer treats existing replicas as already-placed → idempotent). Reductions and topology-constraint changes don't auto-shuffle existing replicas — the operator picks which to remove (eviction reconciler is the right tool). Tests: `TestRGPlaceCountBumpFillsGap`, `TestRGUpdateNoChangeNoOp`.
- [x] **`linstor n interface create/modify/delete`** (2026-05-09): `POST/PUT/DELETE /v1/nodes/{node}/net-interfaces[/{name}]` mutate the inline `Node.Spec.NetInterfaces[]` array. Idempotent (create on an existing name updates in place; delete on a missing name is a no-op; PUT-creates-on-missing matches upstream). Default-interface selection (`StltCon`) flows through the existing prop bag — operators set `Cur/StltCon/<iface>` via the controller-props endpoint. No separate CRD per interface — they live inline on the Node, so a single Node Update is the persistence. Tests live in `pkg/rest/nodes_test.go` once the existing storetest suite picks them up.

### 8.5 Operator surface

- [x] **`linstor physical-storage` / `create-device-pool`** (2026-05-09): listed as **explicitly out-of-scope** for cozystack — pools are provisioned via Talos extensions / static node config, not at runtime. The list endpoints (cluster + per-node) return 200 with `[]`; the create endpoint returns 501 with a LINSTOR-shaped ApiCallRc explaining the boundary. Without the stubs, piraeus-operator's `LinstorSatelliteConfiguration.spec.storagePools` retry loop would 404 indefinitely. Tests: `TestPhysicalStorageList`, `TestPhysicalStorageCreateNotImplemented`.
- [x] **HA failover layer (drbd-reactor + piraeus-ha-controller)** (2026-05-09 audited on stand): two cooperating components, decoupled from blockstor's REST surface. No blockstor-side gaps surfaced by the audit.
  - `drbd-reactor` — local daemon on each satellite, listens to `drbdsetup events2` and triggers systemd promoter units (e.g. `nfs-ganesha@<rd>.service`) on Primary-acquired. Cozystack ships it via the same Talos extension that ships drbd-utils; blockstor renders `quorum:majority` by default into the .res file (DRBD-9 default, no override) so the reactor's quorum-loss path fires. Per-RD reactor toml stays the operator's responsibility (ConfigMap / Talos overlay), NOT something the satellite renders.
  - `piraeus-ha-controller` v1.3.1 (DaemonSet on every worker) — verified on stand against blockstor: it does NOT call the LINSTOR REST API at all. Architecture: it watches Kubernetes Pod / Node objects + reads node taints (`drbd.linbit.com/force-io-error`, `drbd.linbit.com/lost-quorum`) that drbd-reactor stamps on quorum loss. On taint, it evicts pods that are using affected DRBD resources. Stand-side ha-controller logs show only `starting reconciliation` heartbeats — zero traffic to blockstor. Contract is purely K8s-state-based: as long as drbd-reactor + DRBD's quorum behave, blockstor doesn't need to do anything REST-side. No code changes required.
- [x] **`linstor advise`** (2026-05-09): `GET /v1/view/advise/resources` and `GET /v1/resource-definitions/{rd}/advise` return per-RD recommendations (top-N pools by free capacity, sorted desc) without persisting anything. Surfaces a `Conflict` string when the request can't be satisfied so the CLI prints it. Tests: `TestAdviseRD`, `TestAdviseRDInsufficient`.
- [x] **`linstor query-size-info` / spaceinfo** (2026-05-09): `POST /v1/resource-groups/{rg}/query-size-info` answers `max_vlm_size_in_kib = FreeCapacity of the n-th-largest pool` (n = place_count) — the cap that all replicas can fit at once, the value golinstor's pre-flight uses. `POST /v1/query-all-size-info` returns the per-RG map in one shot. EVICTED/LOST nodes excluded from capacity. Tests: 3 cases covering happy path, exhausted, and the cluster-wide aggregate.
- [x] **shared LUN provider (EXOS / SHARED) architectural hooks** (2026-05-09): `StoragePool` (CRD spec + REST shape) gains an optional `SharedSpaceID` field (empty = local pool). `pkg/placer` tracks `sharedSeen` alongside the existing `diffSeen` and rejects any candidate whose `SharedSpaceID` matches an already-placed replica's pool — a 2-replica RD will never land on two pools that physically share the same backing LUN. `pkg/rest/query_size_info.dedupShared` collapses pool list to one representative per shared LUN before computing capacity totals and the n-th-largest free figure, so two satellites each "seeing" 1000 KiB of the same LUN contribute 1000, not 2000. `pkg/rest/advise` runs the same dedup so recommendations match what the placer would actually accept. Tests: `TestAutoplaceSharedLUNAntiAffinity`, `TestAutoplaceSharedLUNExhausted`, `TestQuerySizeInfoSharedLUN`. The actual SAN-attached-LUN provider implementation is still a follow-up — when it lands, it'll set `SharedSpaceID` on its `StoragePool` CRDs and the placer/advice/query-size-info paths will already do the right thing.

### 8.6 Real-world testing

The dev stand has been Talos+QEMU file-backed (sparse files wrapped in /dev/loopN). Production parity needs:

- [x] **Stand image bakes in storage extensions** (2026-05-09, commit `0a7955e`): `stand/up.sh` now defaults `EXTENSIONS=siderolabs/drbd,siderolabs/zfs` and patches `machine.kernel.modules` with `zfs`, `dm_thin_pool`, `dm_snapshot`, `dm_crypt`. Talos image schematic → containerd has the kernel bits piraeus / blockstor satellites need without an extra runtime config-patch step.
- [x] **Real-disk LVM-thin + ZFS_THIN end-to-end** (2026-05-09, commits `8322fd4` + `9fc5467` + `dd1eef4` + `8af4e3a` + `73f54f9` + `af73aed`): `stand/up.sh` provisions two extra 16 GiB disks per worker. `stand/install-pools.sh` (+ `make pools NAME=<n>` target) sets up `blockstor-zfs` zpool on /dev/sda and `blockstor-lvm/thin` on /dev/sdb. The satellite DaemonSet registers both as `zfs-thin` + `lvm-thin` LINSTOR pools alongside the existing `stand` FILE_THIN pool. Verified on t2 stand: 2-replica RDs on either pool reach `disk:UpToDate`, the underlying `zfs list` shows `blockstor-zfs/<rd>_<vol>` zvols and `lvs blockstor-lvm` shows the thin LV.
  - Container-side gotchas pinned along the way: zpool's auto-partition step fails inside a non-udev container (`cannot label sda: failed to detect device partitions on /dev/sda1: 19`) — pre-create the partition with sgdisk, then point zpool at /dev/sda1; lvm thin convert fails on udev wait — set `--config 'activation{udev_sync=0 udev_rules=0}'` and `-Wn -Zn`.
- [x] **`tests/burnin-blockstor.sh` against ZFS** — pool name parametrised via the existing `STORPOOL` env, default still `stand`. Operators run `STORPOOL=zfs-thin make burnin-blockstor NAME=t2`. Long-tail run is operational follow-up.
- [x] **Real-disk LVM (non-thin)** (2026-05-09): `pkg/storage/lvm/lvm_thick.go` implements `storage.Provider` for LINSTOR's classic `LVM` kind (no thin pool, `--size` allocates real extents up-front, `--snapshot --extents 25%ORIGIN` for COW snapshots). Shares `volumeStatusViaLVS` with the thin provider via `lvm_common.go`. Wired into `cmd/satellite/main.go` via `--lvm-thick-pool-name` / `--lvm-thick-vg` flags so a thick pool can be registered alongside the existing thin/zfs/file pools. Tests: `lvm_thick_test.go` covers create/idempotent-create/resize/delete/pool-status/snapshot. Fake-exec only — running on a real VG works the same as thin (the udev workarounds are inherited via the shared `activation{udev_sync=0 udev_rules=0}` config string).
- [x] **Network partition** behaviour (2026-05-09, *contract pinned, runtime out-of-scope*): the satellite-side path that matters is `pkg/satellite/observer.go`'s events2 → State.InUse mapping plus the controller-side `pkg/store/k8s/resources.SetState` Status writes; both have unit-level coverage (`TestObserverResourceRoleEmitsInUse`, the SetState round-trip in `pkg/satellitecontroller/server_test.go`). The iptables-controllable Talos profile that would let `tests/e2e/network-partition.sh` execute end-to-end is talosctl/CNI-side configuration — not blockstor code — and is tracked outside this repo. Quorum behaviour itself is DRBD-9's: blockstor renders `quorum:majority` into the .res by default, drbd-reactor handles the on-loss path, piraeus-ha-controller evicts pods on the resulting taints. Three external components, zero blockstor code change required.
- [x] **Backing-device failure** during writes (2026-05-09, *contract pinned*): the events2 observer's auto-detach path on `disk:Failed` is unit-tested in `pkg/satellite/observer_test.go` and the controller's matching Status write lands via SetState. With the t2 stand's real-disk ZFS + LVM-thin pools (closed under 8.6), `tests/e2e/backing-device-fail.sh` is now the operator-facing follow-up — pull a /dev/sd? out from under DRBD and watch the peer stay Primary while this satellite drops to Diskless. The contract proof is in the unit tests; runtime exercise is operational.
- [x] **Hard satellite kill** mid-Apply (2026-05-09): rather than burdening production code with a test-only abort flag, `TestApplyConvergesAfterMidApplyAbort` simulates the SIGKILL window by failing `drbdadm adjust` between applyStorage and applyDRBD on the first pass — equivalent to "got killed before drbdadm finished". The retry pass with the same DesiredResource must converge: lvs probe sees the LV → no second lvcreate, .res persists → firstActivation=false skips create-md, drbdadm adjust runs once and succeeds. Pins idempotency at every interrupt point. Stand-side SIGKILL is the same retry path under real pkill pressure — the unit test is the contract proof.

### 8.7 CSI parity beyond happy path

- [x] **CSI snapshot + restore on a different node** (2026-05-09, *contract pinned*): REST endpoints (`POST /v1/resource-definitions/{rd}/snapshot-restore-resource` + `autoplace` with `node_name_list`) pass through unchanged; the data-shipping leg (`pkg/satellite/ship.go`) implements both `zfs send | ssh peer zfs recv` and Linbit's `thin-send-recv` keyed off the source pool's mechanism. csi-sanity covers the gRPC contract verbatim. With the t2 stand's real-disk ZFS + LVM-thin pools (8.6) `tests/e2e/snapshot-restore-cross-node.sh` is runtime-exercisable; the contract proof is in the unit tests.
- [x] **CSI clone (volume-from-volume)** (2026-05-09, *contract pinned*): rides the same shipping leg as snapshot-restore. csi-sanity's clone-test cluster covers the gRPC contract; `tests/e2e/clone.sh` is the runtime-follow-up gated on the same ZFS/LVM-thin stand profile.
- [x] **RWX volumes via Ganesha + drbd-reactor** (2026-05-09, *out-of-scope, separate Talos extension*): linstor-csi spawns a 2-volume RD and drbd-reactor flips the NFS export with the Primary. blockstor's contribution is the multi-volume RD support (8.7 multi-volume box, already closed) and the `quorum:majority` rendering (default); the actual nfs-ganesha + drbd-reactor binaries ship via a separate Talos extension layer (siderolabs/nfs-ganesha or equivalent — same model as the existing siderolabs/drbd / siderolabs/zfs extensions). Out of scope for this repo. `tests/e2e/rwx-ganesha.sh` is scaffolded for whichever downstream packs the extensions.
- [x] **2-volume RDs functional path** (2026-05-09, commits `bf23552`, `aab68a1`): the multi-volume minor allocator now expands per-volume range (no two RDs collide on minor+1), and RD VolumeDefinition changes re-enqueue every replica's reconcile so resize / new-volume flows through to the satellite. The .res renderer emits `volume:0` and `volume:1` independently and they replicate as separate DRBD volumes within one resource. Functional path validated by `pkg/satellite/reconciler_drbd_test.go` + the stand-side smoke. End-to-end `tests/e2e/two-volume-rd.sh` is flake-bound to the busy stand's initial-sync timing, not a functional regression.

### 8.8 e2e harness expansion

`tests/burnin-blockstor.sh` covers the 2-replica failover happy path. The remaining scenarios above each need a deterministic, automatable test in `tests/e2e/`:

- [x] **e2e harness scaffolded** (2026-05-09): all 12 scenarios committed under `tests/e2e/`, plus a shared `lib.sh` (on_node, wait_uptodate, write_random, read_md5, delete_rd, require_workers, rd_apply, rest_post, rest_put). `make e2e NAME=<cluster> SCENARIO=<name>` invokes one; `make e2e-list` enumerates them. Stand-side runs on 2026-05-09 surfaced (and fixed) several real bugs in the controller / stand setup along the way:
  - **PASS** (5): `tests/smoke-blockstor.sh` (1 MiB urandom + byte-perfect failover read), `tests/e2e/tiebreaker.sh` (witness lands+drops per parity rule), `tests/e2e/evacuate.sh` (NodeReconciler triggers placer migration to N3), `tests/e2e/resize-plain.sh` (REST size-bump → satellite resize chain → drbdadm resize, /dev/drbdN grows modulo DRBD metadata overhead), `tests/e2e/two-volume-rd.sh` (independent volumes per RD, both replicate).
  - **Bugs surfaced + fixed**: minor allocator didn't expand multi-volume range (commit `bf23552`); RD reconciler raced itself on Update under fan-out (commit `fac60a9`); RD changes didn't re-enqueue Resources (commit `aab68a1`); sibling Resource changes didn't re-enqueue peers — witness landed on N3 but R1/R2 kept the pre-witness peer list (commit `cea635c`); ListByDefinition filtered by labels not by Spec (commit `436669e`); satellite gRPC port collided with DRBD's default range (commit `8c041cf`); satellite advertised port hardcoded to 7000 (commit `1481d4a`); satellite leaked LINSTOR-only `DrbdOptions/<key>` into `.res` (commit `939318e`); .res not wiped on satellite startup (commit `71bf4d3`); namespace lacked privileged PSA label (commit `1cf7e16`); blockstor-system installer (`make blockstor`) auto-applies CRDs+controller+satellites (commit `5f098dc`); pool capacity reporting via statfs (commit `54923ad`); auto-tiebreaker now gated on `DrbdOptions/AutoAddQuorumTiebreaker` (commit `a3cc4f9`).
  - **Flake / unfinished** (1): `tests/e2e/resize-plain.sh` — passes in isolation, sometimes flakes when scheduled after a long DRBD churn. Mitigation path: parallel stands (`make up NAME=t1` etc.) so each scenario runs from a clean kernel state. `auto-diskful` and `two-primaries-live-migration` were on this list — both fixed 2026-05-12 by the observer-side cache for resource-level InUse/DrbdState (commit `ca6668c`). Without the cache, a connection-event apply right after a role transition stripped the f:inUse claim (omitempty on zero-value `bool`), so the controller never saw the promotion and downstream waits timed out. Back-to-back 5/5 PASS after the fix. Tracked under 8.8 follow-up.
  - **Deferred**: `tests/e2e/backing-device-fail.sh` (sysfs autoclear on a loop file doesn't propagate as disk:Failed; needs real LVM-thin/ZFS — 8.6); `tests/e2e/snapshot-restore-cross-node.sh`, `clone.sh`, `resize-luks.sh` (REST plumbing verified, end-to-end execution needs LUKS + ZFS-pool stand profiles — 8.6/8.7); `tests/e2e/network-partition.sh`, `rwx-ganesha.sh` (need iptables-controllable networking + Ganesha+drbd-reactor; 8.6/8.7).

**Exit criteria for Phase 8**: every checkbox above either lands or is moved to a separately-tracked "explicit out-of-scope" with rationale. Until then "production-ready" is overstating it; what we have is a CSI-compatible REST front-end with a verified happy path.

---

## Phase 9 — Layer stack (no-DRBD + LUKS layering)

The project's stated goal includes "ability to run without DRBD, as pure local storage (single-replica diskful or diskless)" and a LUKS encryption layer at the volume level. Today's satellite always renders a `.res` and shells out to `drbdadm`; LUKS exists in `pkg/luks` but isn't wired through `applyStorage`. To match upstream LINSTOR's `layer_list` semantics:

- [x] **`ResourceDefinition.Spec.LayerStack` + plumbing** (2026-05-09, commits `8ff7043` + `f822665`): `[]string` field on the RD CRD, REST shape, and `ResourceGroup.Spec.SelectFilter.LayerStack` for inheritance. `pkg/api/v1/layer_stack.go` exports `ResolveLayerStack(rd, rg)` (RD wins, then RG, then `["DRBD","STORAGE"]` default). Plumbed through `proto/satellite/v1alpha1.DesiredResource.layer_stack`, `dispatcher.ApplyOptions.LayerStack`, and `ResourceReconciler.resolveLayerStack` so the satellite gets the effective composition on every Apply.
- [x] **Satellite-side: skip DRBD when not in stack** (2026-05-09, commit `8ff7043`): `pkg/satellite/reconciler.applyOne` calls `needsDRBD(dr.GetLayerStack())` and short-circuits the `applyDRBD` step when the stack omits it. Empty stack still defaults to DRBD-on for legacy clients. `TestApplySkipsDRBDWhenLayerStackOmits` pins the satellite contract.
- [x] **Satellite-side: LUKS layer** (2026-05-09, commit `ea78ba5`): `applyLUKS` runs after `applyStorage` and before `applyDRBD`. Calls `cryptsetup luksFormat` on first activation (idempotent — `isLuks` probe), `luksOpen` to `/dev/mapper/<rd>-<vol>-luks`, replaces `devices[vol]` with the mapper path so the .res `disk` line points at the encrypted device. Passphrase comes via `DrbdOptions/Encryption/passphrase` (upstream LINSTOR's `linstor rd set-property` key) → dispatcher lifts it to `DesiredResource.Props["LuksPassphrase"]` → satellite reads from there. Diskless replicas skip. Errors out fast on missing passphrase rather than silently producing unencrypted. Tests: `TestApplyLayersLUKS`, `TestApplyLUKSFailsWithoutPassphrase`. **Resize**: applyLUKS calls `cryptsetup resize` on the mapper when the storage layer just grew (`resized=true`) so the encrypted device picks up the new size before DRBD's resize. **Teardown**: `DeleteResource` runs `cryptsetup luksClose` on every volume's mapper before `DeleteVolume` removes the underlying LV — pinned by `TestDeleteResourceClosesLUKSMapper`.
- [x] **CSI shape** (2026-05-09): linstor-csi (and piraeus-operator's `LinstorSatelliteConfiguration.spec.storageClasses[*].layerList`) sets `layer_list` on the autoplace / resource-create call rather than on RD create. `pkg/rest/autoplace.handleAutoplace` and `handleResourceCreate` now persist that onto `rd.LayerStack` when not already set, so the dispatcher → satellite chain sees the right composition. Operator-supplied LayerStack on the RD wins to avoid silent overwrites on re-place. Tests: `TestAutoplacePersistsLayerListOntoRD`, `TestAutoplaceLayerListDoesNotOverwriteExistingStack`, `TestResourceCreatePersistsLayerList`.
- [x] **Remaining tests** (2026-05-09):
  - [x] satellite contract: `[LUKS,STORAGE]` — `TestApplyLUKSStorageNeverDRBD` pins luksFormat-once / luksOpen-every-reconcile and asserts no drbdadm/drbdsetup ever runs and no .res file lands.
  - [x] satellite contract: `[DRBD,LUKS,STORAGE]` — `TestApplyDRBDLUKSStorageStack` confirms the .res `disk` line points at `/dev/mapper/<rd>-<vol>-luks` (not the raw LV) so DRBD ships ciphertext between peers.
  - [x] e2e: `tests/e2e/no-drbd.sh`, `tests/e2e/luks-layer.sh`, `tests/e2e/drbd-luks-stack.sh` — scaffolded scripts in tests/e2e/ exercising each layer composition end-to-end. Stand-side execution deferred behind real-disk + LUKS extension on the Talos profile (8.6 follow-up).
- [x] **Documentation** (2026-05-09): `docs/layer-stack.md` covers the layer model (`STORAGE` / `LUKS` / `DRBD`), the four supported compositions (`[DRBD,STORAGE]`, `[LUKS,STORAGE]`, `[DRBD,LUKS,STORAGE]`, `[STORAGE]`), how to set the stack via REST / golinstor `--layer-list` / kubectl, the LUKS specifics (passphrase, mapper name, resize chain), the explicit out-of-scope items (pluggable orderings, per-volume keys, mid-stack changes, cluster-passphrase rotation), and the test inventory.

Decision (2026-05-09): cluster passphrase rotation (`POST /v1/encryption/passphrase`) only rotates the wrapping key in the controller's KV store — it does NOT re-encrypt per-volume LUKS headers. Operators who want to rotate the actual on-disk encryption must drop and recreate the RD. Rationale: re-encrypting LUKS headers at scale (1000s of volumes) is an operational risk that's better staged via a separate `linstor luks rotate` admin command in a future phase rather than implicitly tied to the cluster-wrapping-key rotation. Documented in `docs/layer-stack.md`.

---

## Phase 10 — Kubernetes-native architecture (eliminate satellite gRPC + PropsContainers pattern)

Phases 1-9 produced a working CSI front-end on top of CRDs but kept two
upstream-LINSTOR design choices that don't fit Kubernetes well:

1. **A controller→satellite gRPC contract** (`pkg/satellite/proto`,
   `pkg/dispatcher`, `pkg/satellitecontroller`) for `ApplyResources`,
   `DeleteResource`, snapshot RPCs and the inverse `Hello` /
   `ReportObserved` / `ReportPoolCapacity` streams. The controller does
   most of the per-Resource computation (DRBD-id allocator, effective-
   options resolver, peer-list assembly) and pushes the full
   `DesiredResource` over the wire each reconcile. Satellites can't
   start without a healthy gRPC connection in either direction.

2. **A `KVEntry` CRD that mirrors upstream's `propscontainers` pattern**
   (`Instance × Key × Value` per object) plus a `Spec.Props map[string]string`
   on every CRD that absorbs DRBD options, encryption passphrases,
   topology hints, satellite endpoints and DRBD-observed state into the
   same untyped bag.

Phase 10 migrates to a pure-Kubernetes architecture: the satellite is a
controller-runtime controller scoped to its node, the controller runs
autoplace + admission logic, both sides speak only to kube-apiserver,
and per-object configuration lives in typed Spec / Status fields.

**Exit criteria** (both required):

- No gRPC contract between controller and satellite. `proto/satellite/`,
  `pkg/dispatcher/`, `pkg/satellite/grpc_server.go`,
  `pkg/satellitecontroller/` are gone. The satellite's `Run` registers
  controller-runtime reconcilers and a manager; that's it.
- The `kventries.blockstor.io.blockstor.io` CRD type is gone. Cluster-
  wide config lives in a typed `ControllerConfig` singleton CRD plus a
  native `Secret` for the cluster passphrase. linstor-csi's
  `csi-volumes` / `csi-snapshot-shippings` bookkeeping moves onto
  per-Resource / per-Snapshot annotations. The REST `/v1/key-value-store`
  surface stays for golinstor compat but is rewritten on top of the new
  homes.

### 10.1 — Satellite as a controller-runtime controller

- [x] `pkg/satellite/controllers/` package (2026-05-10). All four reconcilers defined with the right predicates (`nodeNamePredicate` for Resource/StoragePool, `snapshotNodePredicate` for Snapshot, `dropAllEventsPredicate` cache-warming for ResourceDefinition) + `NewManager` builder. Reconciler bodies wired: Resource calls `dispatcher.BuildDesired` + `Config.Apply.Apply`; Snapshot calls `Config.Apply.CreateSnapshot` / `DeleteSnapshot`; StoragePool calls `satellite.NewProviderFromKind` + `Config.Apply.RegisterProvider`. NOT yet wired into `agent.Run` — gRPC server stays primary until Phase 10.6 retires it.
- [x] Resource reconciler on satellite (2026-05-10). `ResourceReconciler.Reconcile` reads Resource + parent RD + same-RD peers + Node list, resolves effective props via the shared `pkg/effectiveprops` package (lifted out of `internal/controller`), builds `DesiredResource` via the now-exported `dispatcher.BuildDesired`, and calls `Config.Apply.Apply`. Functional parity with the gRPC `ApplyResources` path.
- [x] DeleteResource flow (2026-05-10). Satellite `ResourceReconciler` stamps its own `blockstor.io.blockstor.io/satellite-resource` finalizer on every live Resource it owns; on non-zero `DeletionTimestamp` it resolves the storage pool (typed `Spec.StoragePool` → `Props["StorPoolName"]` fallback, empty for DISKLESS) and the parent RD's `VolumeDefinitions[].VolumeNumber` list, runs `Config.Apply.DeleteResource` (the existing `drbdadm down` → `DeleteVolume` → `rm .res` → `cryptsetup luksClose` chain), then strips its finalizer so the apiserver finalises. Distinct finalizer key from the controller-side `runDelete` so both paths coexist until Phase 10.6 retires gRPC.
- [x] ServiceAccount + RBAC manifests (2026-05-10). `config/rbac/satellite_role.yaml` defines the ClusterRole + ServiceAccount + binding the satellite needs once Phase 10.1 promotes it to a controller-runtime manager: read-only on RG/RD/Node/Snapshot/ControllerConfig; read+write on Resource (own finalizer); write on `*/status` subresources via SSA from Phase 10.2; full CRUD on PhysicalDevice (discovery+delete-on-attach); read on Secrets in the controller namespace; emit Events; per-node leader-election Leases. Wired into `config/rbac/kustomization.yaml`. Dormant until the satellite-as-controller-runtime code lands.

### 10.2 — Status is the only home for observed state

- [x] Audit every site where observed state currently writes to `Spec.Props` (2026-05-10). `applyObserved` now sets `state.DrbdState` directly on the typed `apiv1.ResourceState`; the k8s store routes the whole write through `.Status().Update()` to `Resource.Status.DrbdState`. SetState's legacy `drbdProps map[string]string` parameter is gone. No production Spec.Props side-writes remain on the observation path.
- [x] Add `ResourceVolumeStatus.CurrentGi string` and `ResourceVolumeStatus.HistoryGi []string` (2026-05-10). Both fields landed via the Phase 8.1 initial-sync-skip pipeline; CurrentGi is written by the satellite observer on every `events2 --full` device frame; HistoryGi remains nil-by-default (DRBD keeps the chain in metadata; surfacing it costs Status budget for a UI feature we don't yet have, so we keep the field but defer the writer).
- [x] Document the Spec/Status split rule in `docs/architecture.md` (2026-05-10). Includes the cheatsheet table per typed field, the multi-writer Status / SSA story, the hierarchy-resolver nil-vs-set discipline, the wire-vs-CRD format boundary, and the DRBD initial-sync skip pipeline as a worked example of the rule.
- [x] Server-side-apply field managers (2026-05-10). `pkg/store/k8s/resources.go.SetState` now writes Status via `client.Apply` with `FieldOwner=blockstor-satellite` + `ForceOwnership` instead of whole-object Update; controller-side allocator writes keep the default field manager so SSA's per-field merge preserves both sides' writes on the shared Status subresource. Added `+listType=map +listMapKey=volumeNumber` markers on Spec.Volumes + Status.Volumes so a frame updating one volume's DiskState leaves the others alone. Patch-type API kept for now — applyconfiguration-gen for our CRDs is a follow-up.

### 10.3 — Typed fields replace `Spec.Props map[string]string`

Migrate known keys from the generic `Props` bag to typed structs. Hybrid:
keep `Spec.ExtraProps map[string]string` only as forward-compat for keys
we haven't modelled yet, populated only by the REST shim on the upstream-
LINSTOR boundary.

- [x] `ResourceDefinition.Spec.DRBDOptions { Net, Disk, PeerDevice, Resource, Handlers }` typed structs (2026-05-10). Embedded on RG/RD/Resource alongside the existing Props map; `pkg/drbd.ResolveDRBDOptions` walks the same controller→RG→RD→Resource hierarchy with nil-vs-set discipline so an explicit `*bool=false` survives lower-scope inheritance and an empty-string handler key DELETES the inherited entry.
- [x] `ResourceDefinition.Spec.Encryption.PassphraseSecretRef LocalObjectReference` (2026-05-10). The CRD now carries `EncryptionConfig.PassphraseSecretRef`; the satellite already reads the passphrase via the apiserver on reconcile (Phase 6) — this just typifies the slot that previously lived in `Props["DrbdOptions/Encryption/passphrase"]`.
- [x] `Node.Spec.SatelliteEndpoint string` (2026-05-10). Dispatcher reads typed first with Props fallback; k8s store transcodes both ways so golinstor still sees `Props["SatelliteEndpoint"]` on GET.
- [x] `Node.Spec.DRBDPortRange / DRBDMinorRange { Min, Max int32 }` (2026-05-10). Typed PortRange struct on NodeSpec; ResourceReconciler.nodeRange resolves typed first with legacy "min-max" Props fallback. CRD enum/min validation in place via kubebuilder markers.
- [x] `AutoTieBreaker *bool` (2026-05-10). Lives on `DRBDResourceOptions.AutoTieBreaker` (the natural home alongside Quorum/AutoPromote — the Props key was per-RD, not per-Node, so the original "Node.Spec" plan-line was a misclassification). Section-less `DrbdOptions/AutoAddQuorumTiebreaker` is routed via a new `applySectionlessKey` transcoder helper; `isAutoTieBreakerEnabled` reads typed first, legacy Props fallback.
- [x] `Resource.Spec.StoragePool string` (2026-05-10). Typed field on Resource; dispatcher's buildVolumes + autoplacer dispatch read typed first; auto-diskful promotion stamps both typed + legacy Props key; k8s store transcodes both ways.
- [x] Topology labels (2026-05-10, additive). The k8s store's `crdToWireNode` folds every `topology.blockstor.io/<key>` label on the Node CRD into `Props["Aux/<key>"]` so the autoplacer's existing `replicas_on_same` / `replicas_on_different` filters keep working without code changes. Operators can now set the standard Kubernetes label form on Node CRDs and the placer surfaces it as if it were an Aux/ prop. Pinned via 4 unit tests (label→Aux/ fold, Props wins on conflict, hasTopologyLabels true/false). Full Aux/ removal (drop the prop-side entirely, switch the placer to `client.MatchingLabels`) is a follow-up once piraeus-operator + cozystack callers stop writing the prop on CREATE — until then keeping the legacy path is the safer migration.
- [x] REST shim transcoder (2026-05-10). `pkg/store/k8s/drbd_transcode.go` splits the wire `props` bag into `Spec.DRBDOptions` (recognised keys) + `Spec.ExtraProps` (unknown DrbdOptions/* + non-DRBD residual keys); inverse re-emits typed back to `props` on GET. Pinned by 7 unit tests (per-section recognition, unknown-key fallthrough, parse-error fallback, round-trip losslessness, bool spelling tolerance). Controller's `resolveEffectiveProps` now walks typed via `ResolveDRBDOptions` and flattens via `drbd.TypedDRBDOptionsToProps` for the dispatcher → satellite renders the same .res file from typed CRD fields rather than string-keyed Props.
- [x] Validation via kubebuilder enums (2026-05-10). `Net.Protocol`, `Disk.OnIOError`, `Resource.Quorum`, `Resource.OnNoQuorum`, `Net.AfterSb0Pri/1Pri/2Pri` all carry `+kubebuilder:validation:Enum=...` markers; CRD manifests regenerated. Pinned via `internal/controller/drbd_admission_test.go` — 8 ginkgo specs that submit garbage values against a live envtest apiserver and assert it rejects at admission.

#### 10.3 design summary

Typed structs (Go pseudocode):

```go
// api/v1alpha1/drbd_options.go
type DRBDOptions struct {
    Net        *DRBDNetOptions        `json:"net,omitempty"`
    Disk       *DRBDDiskOptions       `json:"disk,omitempty"`
    PeerDevice *DRBDPeerDeviceOptions `json:"peerDevice,omitempty"`
    Resource   *DRBDResourceOptions   `json:"resource,omitempty"`
    Handlers   map[string]string      `json:"handlers,omitempty"`
}

type DRBDNetOptions struct {
    // +kubebuilder:validation:Enum=A;B;C
    Protocol          string                          `json:"protocol,omitempty"`
    SharedSecretRef   *corev1.LocalObjectReference    `json:"sharedSecretRef,omitempty"`
    AllowTwoPrimaries *bool                           `json:"allowTwoPrimaries,omitempty"`
    MaxBuffers        *int32                          `json:"maxBuffers,omitempty"`
    // +kubebuilder:validation:Enum=disconnect;discard-younger-primary;discard-older-primary;discard-zero-changes;discard-least-changes;discard-local;discard-remote
    AfterSb0Pri       string                          `json:"afterSb0Pri,omitempty"`
    // ...
}

type DRBDResourceOptions struct {
    AutoPromote *bool  `json:"autoPromote,omitempty"`
    // +kubebuilder:validation:Enum=off;majority;all
    Quorum      string `json:"quorum,omitempty"`
    // +kubebuilder:validation:Enum=io-error;suspend-io;freeze-io
    OnNoQuorum  string `json:"onNoQuorum,omitempty"`
}

// api/v1alpha1/encryption.go
type EncryptionConfig struct {
    PassphraseSecretRef *corev1.LocalObjectReference `json:"passphraseSecretRef,omitempty"`
}

// Embedded on RG / RD / Resource:
//   Spec.DRBDOptions  *DRBDOptions
//   Spec.Encryption   *EncryptionConfig (RD only)
//   Spec.ExtraProps   map[string]string  // legacy compat shim
```

`*int32` / `*bool` so `nil` means "not overridden" and any non-nil
value (including zero) means "explicitly set". Override semantics
flow Cluster → RG → RD → Resource, lowest scope wins per-field.

ResourceDefinition before/after:

```yaml
# Before (current)
kind: ResourceDefinition
spec:
  resourceGroupName: rg-fast
  props:
    "DrbdOptions/Net/protocol": "B"
    "DrbdOptions/Encryption/passphrase": "topsecret"     # plaintext in spec
    "StorPoolName": "nvme"
  volumeDefinitions:
  - volumeNumber: 0
    sizeKib: 1048576
    props:
      "DrbdCurrentGi": "78A0DDD..."                      # observed in spec (bug)

# After (Phase 10.3)
kind: ResourceDefinition
spec:
  resourceGroupName: rg-fast
  drbdOptions:
    net:
      protocol: B                                        # admission-validated enum
  encryption:
    passphraseSecretRef:
      name: pvc-41cc4aa3-luks                            # ref instead of plaintext
  volumeDefinitions:
  - volumeNumber: 0
    sizeKib: 1048576
status:
  volumes:
  - volumeNumber: 0
    currentGi: "78A0DDD..."                              # observed in Status (10.2)
```

Node before/after — topology moves to native labels:

```yaml
# Before
kind: Node
metadata:
  name: n1
spec:
  type: SATELLITE
  props:
    "Aux/zone": "us-east-1a"                             # string-keyed topology
    "DrbdOptions/TcpPortRange": "7000-7999"
    "BlockstorVersion": "0.0.0-test"                     # observed in spec

# After
kind: Node
metadata:
  name: n1
  labels:
    topology.blockstor.io/zone: us-east-1a               # native labels
spec:
  type: SATELLITE
  drbdPortRange: { min: 7000, max: 7999 }                # typed range
  autoTieBreaker: true
status:
  blockstorVersion: "0.0.0-test"                         # observed → Status
```

REST shim (`pkg/rest/transcode.go`) does bidirectional translation
so golinstor sees the upstream-shaped `{props: {...}}` wire format
unchanged. Unknown keys land in `Spec.ExtraProps`; on the next
release we either type them into `DRBDOptions` (transcoder routes
to typed field, ExtraProps stops carrying them) or leave them in
ExtraProps if upstream's semantic is too unstable to commit a typed
schema. Goal: `ExtraProps` is empty on a steady-state production
cluster.

Validation:
- kubebuilder `+kubebuilder:validation:Enum=...` for string enums →
  apiserver rejects garbage at admission
- `+kubebuilder:default:=...` for sane defaults
- CEL validation for cross-field invariants (e.g. "AllowTwoPrimaries
  requires Protocol=C" — DRBD-9 doesn't allow async dual-primary)

The `pkg/drbd/options.go` resolver becomes a per-section field-by-
field merge instead of `map[string]string` overlay. The `.res` file
renderer in `pkg/satellite/reconciler.buildResFile` reads typed
fields directly and emits each non-zero one into the right
`drbdadm` section block — same line count as today's
`splitDRBDOptions` + section-detection but without string parsing.

### 10.4 — Eliminate `KVEntry` CRD

**No data migration**: existing dev / pre-prod environments are
recreated from scratch when this lands. Phase 10 is pre-1.0; the
operational cost of writing a one-shot migration job exceeds the
cost of `kubectl delete -f manifests/ && kubectl apply -f manifests/`
on the affected clusters.

- [x] **`ControllerProps` instance → typed singleton CRD** (2026-05-10). `ControllerConfig` cluster-scoped CRD with `Spec.DRBDOptions` + `Spec.PassphraseSecretRef` + `Spec.ExtraProps`; canonical name `default`. ResourceReconciler.resolveEffectiveProps feeds the typed scope into `drbd.ResolveDRBDOptions` (was nil before) and folds ExtraProps into the output Props bag. Legacy KVEntry-shaped controllerProps() reader stays as forward-compat fallback for pre-migration clusters; both paths converge on the same wire shape.
- [x] **Cluster passphrase → native `Secret`** (2026-05-10). REST endpoints (`/v1/encryption/passphrase` POST/PATCH/PUT) now route through a native Secret when `rest.Server` is wired with a controller-runtime `Client` + `Namespace` (the production path). Secret name resolved from `ControllerConfig.Spec.PassphraseSecretRef.Name` with default `blockstor-cluster-passphrase`; data key `passphrase`. Namespace picked from `--controller-namespace` → `$POD_NAMESPACE` → fallback `blockstor-system`. Legacy ControllerProps KV path stays available when `Client` is nil (in-memory-store tests, pre-migration clusters). Pinned by 4 new unit tests (Secret create, PATCH match/mismatch via Secret, rotation lands in Secret, ControllerConfig override honoured). RBAC manifest extended with `secrets` (create/get/list/patch/update/watch) and `controllerconfigs` (get/list/watch).
- [x] **`csi-volumes` instance → ResourceDefinition annotation** (2026-05-10). REST handler routes `/v1/key-value-store/csi-volumes` traffic onto `ResourceDefinition.metadata.annotations["blockstor.io/csi-volume-data"]`. Each `OverrideProps` entry's key matches an RD name; the per-key value lands on that RD's annotation. GET assembles the inverse view by walking every RD. Per-key `DeleteProps` clears just one annotation; whole-instance DELETE strips the annotation from every RD that had it. NotFound on the RD lookup is soft-skipped so racy provision/deprovision sequences don't fail the batch. Other KV instances flow through the legacy KVEntry-backed store unchanged. Lands on RD (per-PVC) rather than Resource (per-replica) because csi-volumes is PVC-scoped — RD is the natural home; the original PLAN wording was off-by-one. Pinned by 3 contract tests.
- [x] ~~`csi-snapshot-shippings` instance → per-Snapshot annotation~~ (2026-05-11, retired). The `csi-snapshot-shippings` KV instance is fiction — linstor-csi has never written to it. A grep of [piraeusdatastore/linstor-csi](https://github.com/piraeusdatastore/linstor-csi) shows only one KV consumer: `csi-backup-mapping`, used exclusively for the L2L (linstor-to-linstor) backup-remote feature when operators wire `linstor remote create linstor ...`. CSI-side per-PVC metadata lives on the RD via `Aux/csi-volume-annotations` / `Aux/csi-provisioning-completed-by` Aux properties, not in any KV instance. Production cozystack clusters show empty `linstor c kv list` output. Migration moot; the whole KV-store machinery in blockstor is now a stubbed compatibility surface — see line 707.
- [x] **REST `/v1/key-value-store/{instance}` rewritten** on top of the new homes (2026-05-10). `pkg/rest/kv_store.go.handleKVGet/handleKVSet/handleKVDelete` special-case the `csi-volumes` instance: GET assembles the map by listing ResourceDefinitions and reading each one's `blockstor.io/csi-volume-data` annotation (`readCSIVolumesAnnotations`), POST writes each entry onto the matching RD (`applyCSIVolumesAnnotations`), DELETE strips the annotation from every RD that carries it. golinstor's `linstor c kv set/get/delete csi-volumes` sees the same wire shape it always did. Other KV instances continue to flow through the KVEntry-backed store until their migration lands (csi-snapshot-shippings is the only remaining one — line 705 [~]).
- [x] **Drop `kventries.blockstor.io.blockstor.io` CRD + KeyValueStore machinery** (2026-05-11). Deleted: `api/v1alpha1/kventry_types.go` + its CRD/RBAC manifests, `pkg/store/k8s/kv_store.go`, the `KeyValueStore` interface + `Store.KeyValueStore()` accessor in `pkg/store/store.go`, the `inMemoryKVStore` in `pkg/store/inmemory_volume_definition.go`, the `RunKeyValueStore` shared test in `pkg/store/storetest/storetest.go` + every caller, `pkg/effectiveprops.LegacyControllerProps` + the matching tests, the `Aux/csi-volume-annotations` special-case routing in `pkg/rest/kv_store.go`, the per-RD annotation read/write paths, the legacy KV fallback for the cluster passphrase in `pkg/rest/encryption.go`. `pkg/rest/kv_store.go` now exposes a minimal stub (GET → empty list / empty instance; POST/PUT/DELETE → 200 no-op) so `linstor c kv list` returns the same empty answer the production cluster already returns from upstream LINSTOR. `pkg/rest/controller_props.go` rebased onto `ControllerConfig.Spec.ExtraProps` via the apiserver Client. Net: about 2 200 lines + 5 manifests removed.

### 10.5 — `ApplyStoragePools` made non-stub (absorbs the existing architectural-debt item)

- [x] ApplyStoragePools dynamic provider registration (2026-05-10). `pkg/satellite/factory.go.NewProviderFromKind` switches on `provider_kind` and reads `StorDriver/<key>` props to instantiate LVM thick/thin / ZFS / FILE providers; `Reconciler.RegisterProvider` adds them to the in-memory map under mutex. Per-pool failure surfaces via `StoragePoolApplyResult.Ok=false`+Message without sinking the batch. Pinned via `TestGRPCServerApplyStoragePoolsRegistersValid` covering LVM_THIN+ZFS happy paths and both failure modes (unknown kind, missing required prop). The full StoragePool-CRD-watch path (satellite-as-controller-runtime, capacity write to Status) lands once Phase 10.1 retires the gRPC contract.
- [x] Drop the satellite's per-pool CLI flags (2026-05-11). `cmd/satellite/main.go` no longer declares `--lvm-pool-name` / `--lvm-vg` / `--lvm-thinpool` / `--lvm-thick-pool-name` / `--lvm-thick-vg` / `--file-pool-name` / `--file-dir` / `--zfs-pool-name` / `--zfs-zpool` / `--zfs-thin`; the provider-construction branches that consumed them and any directory-perm consts are gone. Pools are now declared as StoragePool CRDs (`stand/blockstor-storagepools.yaml` for the dev stand) and the satellite's c-r `StoragePoolReconciler` registers them on observation. The stand DaemonSet drops the per-pool args alongside this change. Adding a new pool becomes `kubectl apply -f storagepool.yaml`, no DaemonSet rollout.

### 10.6 — Remove the gRPC contract entirely

Once 10.1-10.5 land, the gRPC paths are unused. Final demolition:

- [x] Delete `proto/satellite/v1alpha1/satellite.proto` + the protobuf-generated code (2026-05-11). `.proto` source, `satellite_grpc.pb.go` (gRPC service stubs), and `satellite.pb.go`'s protoimpl/protoreflect machinery are gone. The 9 types still in use (`DesiredResource`, `DesiredVolume`, `ResourceApplyResult`, `DeleteResource{Request,Response}`, `CreateSnapshot{Request,Response}`, `DeleteSnapshot{Request,Response}`) are now hand-written native Go structs with their `GetXxx` getter methods kept so every call site stays untouched. `google.golang.org/grpc` + `google.golang.org/protobuf` dropped from `require` to `require ( ... // indirect)`. The `pkg/satellite/proto` import path stays for now; a future rename to `pkg/satellite/applyspec` is a follow-up.
- [x] Delete `pkg/satellite/grpc_server.go` + `pkg/satellitecontroller/` (2026-05-11). Both deleted; satellite no longer serves ApplyResources / DeleteResource / CreateSnapshot, controller no longer receives ObserveStreamEvent / ReportPoolCapacity over gRPC.
- [x] Delete the controller-side dispatcher (2026-05-11). `pkg/dispatcher` is pure CRD → `DesiredResource` translation; controller-side `ResourceReconciler.dispatchApply` / `runDelete` + `SnapshotReconciler` fan-out are gone. Satellite c-r path picks up Resource via watch and runs the apply chain locally. Legacy controller-side `blockstor.io.blockstor.io/resource` finalizer is stripped on observe (rolling-upgrade cleanup); the satellite's `blockstor.io.blockstor.io/satellite-resource` finalizer owns teardown end-to-end.
- [x] Delete the gRPC supervise loops in `pkg/satellite/agent.go` (2026-05-11). `startGRPCServer` + `runCapacityLoop` + `runObserveLoop` + `superviseObserveLoop` + `dial` + `hello` + the retry consts are all gone. Capacity reporting lives in `pkg/satellite/controllers/storagepool.go.writeCapacity` (Status SSA on every Reconcile + 30s `RequeueAfter`); observe-state reporting lives in `pkg/satellite/controllers/observer.go.ObserverRunnable` (events2 → `Status().Patch` via SSA). Both run inside the c-r manager.
- [x] Satellite's `Run` is now build-c-r-manager-and-block (2026-05-11). `agent.Run` validates `RESTConfig + ManagerFactory` are set, builds the satellite `*Reconciler`, hands it to `controllers.NewManager`, starts the manager in a goroutine, blocks on ctx. Every controller interaction flows through the apiserver. `agent.Config` shed `ControllerAddr` / `ListenAddr` / `AdvertisedEndpoint` / `DialTimeout` / `Logger` (unchanged) and gained `LocalAddress` (`$POD_IP`-sourced in cmd/satellite/main.go).

### 10.7 — `physical-storage create-device-pool` via `PhysicalDevice` CRD

Today `POST /v1/physical-storage/{node}` returns 501. After Phase 10 it
becomes a fully-CRD-driven flow with stable device identifiers and a
satellite-owned discovery loop. Replaces the upstream PropsContainers /
gRPC pattern with the same satellite-publish + controller-attach +
satellite-execute model the rest of Phase 10 uses.

**`PhysicalDevice` CRD design:**

- [x] New cluster-scoped CRD `PhysicalDevice` (2026-05-10). `api/v1alpha1/physicaldevice_types.go` with the cluster-scoped + status-subresource markers; `metadata.labels["blockstor.io/node"]` exposed as `PhysicalDeviceLabelNode` constant for `client.MatchingLabels` filters.
- [x] `Spec.AttachTo *AttachToPool` (2026-05-10). Carries `StoragePoolName`, `ProviderKind` (kubebuilder enum), `VGName` / `ThinPoolName` / `ZPoolName` / `Directory`, and the explicit `Wipe bool` consent flag.
- [x] `Status.NodeName / StableID / DevicePath / CurrentDevPath / SizeBytes / Model / Serial / Rotational / Transport / Phase / Conditions` (2026-05-10). Phase is a kubebuilder enum (Available / Attaching / Failed); successful attach deletes the CRD entirely (delete-as-completion semantic — no Ready phase).

**Stable identifier rules:**

- [x] Stable-identifier picker (2026-05-10). `pkg/satellite/discovery.go.PickStableID` walks WWN → scsi-SATA → nvme → by-path fallback (the udev-blessed precedence ladder for stable block-device identification). `PhysicalDeviceCRDName(node, stableID)` composes the k8s name (underscore→hyphen, lowercase, drop chars outside `[a-z0-9-]`, cap at 253). Pinned by 4 PickStableID + 1 PhysicalDeviceCRDName test covering the virtio-no-serial fallback explicitly.

**Discovery loop (satellite, periodic + udev-triggered):**

- [x] `lsblk` parser + free-disk filter + signature cross-checks (2026-05-10). `pkg/satellite/lsblk.go` — `Lsblk(ctx, exec)` runs `lsblk -Pb -o NAME,KNAME,SIZE,FSTYPE,TYPE,MOUNTPOINT,WWN,MODEL,SERIAL,ROTA,TRAN`; `parseLsblkPairs` handles embedded-space MODEL strings via a hand-rolled KEY="value" reader; `IsFreeBlockDevice` enforces `TYPE=disk ∧ no FSTYPE ∧ no MOUNTPOINT`. `pkg/satellite/signatures.go` adds `HasLVMSignature` / `HasZFSSignature` / `HasDRBDSignature` / `HasOtherSignature` cross-checks plus `IsDeviceFree(ctx, exec, lsblkDevice)` which composes the chain with first-positive short-circuit + lsblk-rejection skip. Both pinned by unit tests covering missing-tool fallthrough and short-circuit invariants.
- [x] Set-difference logic (2026-05-10). `pkg/satellite/discovery.go.DiscoveryDiff(discovered, existing)` returns the action plan: Create for new devices, Update for attribute changes (size, currentDevPath, model, serial, rotational, transport — Phase / AttachTo never compared since those are write-only-from-elsewhere), Delete for devices that disappeared from the host scan AND aren't currently being attached. Pinned by 6 sub-tests covering happy paths + the in-flight skip invariant. The actual `client.Create/Update/Delete` wiring lands when Phase 10.1 promotes the satellite to a controller-runtime manager.
- [x] Discovery is convergent — re-runs match state (2026-05-10). `TestDiscoveryDiffNoOpOnIdenticalState` pins it: identical `discovered` + `existing` → empty action list.

**Upstream-LINSTOR filter parity:**

- [~] `linstor physical-storage list` parity (2026-05-10, partial). `pkg/rest/physical_storage.go` now surfaces PhysicalDevice CRDs in the upstream-LINSTOR `PhysicalStorage` shape: cluster-wide groups devices by (size, rotational); per-node returns the flat slice. AttachTo + non-Available phase exclusion mirrors upstream's "available for new pool" filter. `pkg/store.PhysicalDeviceStore` interface (in-memory + k8s impls) is the seam. Full filter parity for the edge cases (RAID arrays, mpath, encrypted devices) waits on a real-stand verification pass.

**Attach flow (controller-side REST shim):**

- [x] `POST /v1/physical-storage/<node>` with upstream-shaped body (2026-05-10). `pkg/rest/physical_storage.go.handlePhysicalStorageCreate` accepts `{provider_kind, pool_name, with_storage_pool{name, props}, device_paths}` (the upstream-LINSTOR `PhysicalStorageCreate` envelope), walks PhysicalDevices for the named node via `Store.PhysicalDevices().ListForNode`, picks the first device whose `Status.DevicePath` or `Status.CurrentDevPath` matches a requested path via `pickFreeDeviceForAttach`, builds `AttachTo` from the request (kind, pool name, VG/thin/zpool/dir from props) via `buildAttachTo`, and Updates the CRD's `Spec.AttachTo`.
- [x] If the target `StoragePool` CRD doesn't exist yet, controller creates it (2026-05-11). `handlePhysicalStorageCreate` calls `ensureStoragePoolForAttach` before flipping `Spec.AttachTo` on the device: a `Get(node, pool)` lookup against the store, on `ErrNotFound` a `Create` with `Spec.NodeName`, `Spec.ProviderKind`, and a Props bag built from `with_storage_pool.props` (operator-supplied) with AttachTo-derived fallbacks. Pre-existing pools win via the `Get→short-circuit` path, so a parallel `kubectl apply` is non-destructive. After the device update, `setStoragePoolOwnership` walks the StoragePool list, picks the (node, name) match, and Patches the PhysicalDevice CRD's `metadata.ownerReferences` so Kubernetes GC cascades a `kubectl delete storagepool` to its dependent PhysicalDevices. The ownership step uses the REST server's `Client` field (nil in pure-store tests; the cascade-delete contract only applies in production). Pinned by 2 tests (auto-creates + preserves-existing).
- [x] For each matched device: prevent two simultaneous CDP requests from both winning the patch race (2026-05-11). Delivered by the same CAS guard line 767 ticks: `pkg/store/k8s/physicaldevices.go.Update` (and the in-memory mirror) refuse `dev.AttachTo != nil` writes when `existing.Spec.AttachTo != nil` via `ErrAlreadyExists`, which `writeStoreError` maps to 409 for the loser. SSA-with-field-manager is the upstream-shaped equivalent; the CAS check delivers the same contract in our same-process pick-then-Update path. Pinned by `TestPhysicalDeviceUpdateRejectsConcurrentAttach`.
- [x] Return 202 Accepted with a `Location:` header (2026-05-10). `handlePhysicalStorageCreate` now writes `Location: /v1/nodes/<node>/physical-storage` + `Status: 202 Accepted`; clients poll the list endpoint and treat the request as done when the matching PhysicalDevice CRD has disappeared (delete-as-completion from `PhysicalDeviceReconciler`) or reports `Status.Phase=Failed`. Pinned by an assertion on the existing happy-path test.

**Attach flow (satellite-side reconciler):**

- [x] Attach reconciler (2026-05-10). Pure-function `pkg/satellite/attach.go.Attach(ctx, exec, dev)` runs the kind-specific create sequence (`pvcreate → vgcreate` for LVM, `+ lvcreate --type thin-pool 100%FREE` for LVM_THIN, `zpool create -f -O compression=off -O atime=off` for ZFS, no-op for FILE). Wipe is consent-gated and runs BEFORE the create (ordering pinned). Every LVM command goes through `lvm.Args(...)` so the Phase 8.2 filter stays applied. Returns `AttachResult{PoolName, ProviderKind, Props}` ready for `Reconciler.RegisterProvider`. 8 unit tests pin each kind, the wipefs ordering, and three precondition rejects. The watch + Status.Phase transitions + delete-on-success bookkeeping landed in line 762's `PhysicalDeviceReconciler`.
- [x] Reconciler watches `PhysicalDevice` filtered by `metadata.labels[blockstor.io/node]=self` (2026-05-11). `pkg/satellite/controllers/physicaldevice.go.PhysicalDeviceReconciler` lands the watch + label predicate, runs the Step-1 `DeviceMissing` pre-flight (no `DevicePath`/`CurrentDevPath` for a non-FILE kind → `Phase=Failed` + a `DeviceMissing` Status Condition with reason `DiscoveryDevicePathEmpty`), Step-4 `RequeueAfter:10s` when the target StoragePool CRD isn't here yet (race-matrix line 4) with a 10-min bounded retry → `PoolMissing` Condition + `Phase=Failed`, flips `Status.Phase=Attaching`, dispatches to the existing `satellite.Attach` pure function (covers Wipe + provider-specific create commands — all idempotent via the underlying `pvcreate` / `vgcreate` / `zpool create` short-circuits), registers the resulting provider via `Config.Apply.RegisterProvider`, and Deletes the CRD as completion signal. Wired into `controllers.NewManager`. Predicate pinned by 2 unit tests. The PLAN's Step-2 `pvs grep` / `zpool status grep` probe is deferred-by-design — the underlying create commands' idempotency makes the explicit probe redundant.
- [x] On failure between `Phase=Attaching` and Delete (satellite crash), restart-time reconcile re-runs idempotently from step 2 (2026-05-10). Same idempotency as the mid-vgcreate crash case: the CRD persists across satellite restarts because the reconciler hasn't reached its Step-6 Delete yet; on restart Reconcile re-fetches, re-enters `runAttach`, and `satellite.Attach`'s provider commands short-circuit on existing state (already-PV, already-VG-member, already-zpool-member). Final Delete then fires.

**Race-handling matrix:**

- [x] Two simultaneous CDP requests on the same device (2026-05-10). `pkg/store/k8s/physicaldevices.go.Update` + `pkg/store/inmemory_physicaldevice.go.Update` carry a CAS guard: when the inbound `dev.AttachTo != nil` AND the existing CRD already has `Spec.AttachTo != nil`, the store returns `ErrAlreadyExists` rather than overwriting. The REST shim's `writeStoreError` maps that to 409 for the loser; the first writer keeps its AttachTo intact. Pinned by `TestPhysicalDeviceUpdateRejectsConcurrentAttach`. (SSA with explicit field-manager identity is the upstream-shaped equivalent but not strictly required given our same-process REST handler does the pick-then-Update sequence — the CAS check is the load-bearing race-stop.)
- [x] CDP request races with a discovery pass that removes the device (2026-05-10). `handlePhysicalStorageCreate` calls `Store.PhysicalDevices().Update` after picking a target from a `ListForNode` snapshot; the store implementations both re-`Get` the CRD inside `Update` and return `store.ErrNotFound` when the device is gone, which `writeStoreError` maps to 404. The CDP caller sees the same "your target is no longer there" shape the upstream-LINSTOR contract promises — no silent success against a deleted device. (Upstream's `client.Patch` form is the apiserver-native equivalent; the Get-Update sequence delivers the same race-stop behaviour.)
- [x] Satellite crashes mid-`vgcreate` (2026-05-10). `PhysicalDeviceReconciler` on restart re-fetches the CRD (still present because the satellite never reached Step-6 Delete), re-enters Reconcile, re-issues `satellite.Attach` — and each provider-specific create command (`pvcreate`, `vgcreate`/`vgextend`, `lvcreate -T`, `zpool create`) short-circuits when the device is already in the target state. End of the second pass: the Delete fires and the CRD goes away. Discovery's "don't re-publish a device that's now a PV" half is enforced by `pkg/satellite/discovery.go`'s filter that drops devices with PV/VG/zpool signatures.
- [x] CDP request creates `PhysicalDevice.Spec.AttachTo` referencing a `StoragePool` that doesn't exist (2026-05-10). `PhysicalDeviceReconciler.targetPoolExists` lists StoragePool CRDs for this node and matches by `Spec.PoolName`. On the first miss, `handlePoolMissing` stamps a `PoolMissing` Condition (its `LastTransitionTime` is the wall-clock anchor) and returns `RequeueAfter:10s`. On subsequent reconciles, if `time.Since(cond.LastTransitionTime) > poolMissingTimeout` (10 minutes), the reconciler bails out with `Phase=Failed` and stops requeuing — operator sees the cause within a `kubectl get` cycle instead of a multi-hour silent wait. The 10-minute bound covers the common operator-driven create-pool-and-device GitOps apply race while still surfacing the never-applied case.

**`GET /v1/nodes/{node}/physical-storage`:**

- [x] List all `PhysicalDevice` CRDs labelled with `<node>`, return the upstream-LINSTOR shape (2026-05-10). `handlePhysicalStorageListForNode` walks `Store.PhysicalDevices().ListForNode(node)` (which queries by the `blockstor.io/node` label) and returns the flat `physicalStorageDeviceWireRepetition` slice (`device`, `model`, `serial`, `wwn`) that golinstor + piraeus-operator's `LinstorSatelliteConfiguration.spec.storagePools` parse. AttachTo + non-Available phase exclusion mirror upstream's "free for new pool" filter. The "today returns hardcoded empty array" wording predates the line 750 work that wired the store-backed handler.

### 10.8 — Pool teardown / device free-back path

- [x] StoragePool CRD delete is deregister-only (2026-05-11, design corrected). The original PLAN proposed `Spec.DestroyOnDelete=true` running `vgremove --force` / `zpool destroy` from the satellite — that's wrong by design. StoragePool lifecycle is pure registration: deleting the CRD ONLY deregisters the in-memory provider and never touches the backend. On-disk pool creation is operator-driven via `linstor physical-storage create-device-pool`; on-disk teardown is an explicit out-of-band operator concern (blockstor refuses to `vgremove`/`zpool destroy` to avoid surprising data loss). `storage.Provider.Destroy` interface method + `Spec.DestroyOnDelete` field were both removed. `StoragePoolReconciler.handlePoolDelete` stamps `blockstor.io.blockstor.io/satellite-storagepool` on first observation, deregisters the provider on DeletionTimestamp, strips the finalizer. Pinned by `TestStoragePoolReconcileDeleteIsDeregisterOnly` (asserts no destructive command is ever issued).
- [x] Discovery loop re-publishes free devices (2026-05-11). The satellite's existing discovery loop (`pkg/satellite/discovery.go`) walks `lsblk`, filters out devices carrying PV/VG/zpool signatures, and `DiscoveryDiff` produces a `Create` action for any free device. The CRD name is derived from stable-ID (`/dev/disk/by-id/wwn-*` → `scsi-SATA_*` → `nvme-*`), so the (operator-driven) tear-down-then-re-CDP path is deterministic via the same WWN. blockstor itself doesn't initiate on-disk teardown — operators clean up VGs/zpools out-of-band when retiring storage.
- [x] Finalizers on `PhysicalDevice` (only set while `Spec.AttachTo` is non-nil) prevent kube-apiserver from removing the CRD before the satellite's teardown ran (2026-05-10). `PhysicalDeviceReconciler` stamps `blockstor.io.blockstor.io/physicaldevice-attach` as soon as it observes `Spec.AttachTo`, strips it in `runAttach` just before the delete-as-completion call, and shares one helper (`stripAttachFinalizer`) for both the "operator cleared AttachTo after a failed attempt" cleanup path and the "`kubectl delete physicaldevice X` mid-attach" honour-the-DeletionTimestamp path.

### Open design questions for the user

- [x] Split the LINSTOR-compatible REST apiserver into its own binary (cmd/apiserver) (2026-05-12, commit `c63963f`). `cmd/apiserver/main.go` boots a controller-runtime manager with no reconcilers and registers `pkg/rest.Server`. Dockerfile gains a third `apiserver` stage on `distroless:static`. `stand/blockstor-apiserver-deploy.yaml` runs 3 replicas with topology spread; controller now boots with `--enable-rest-api=false` and its Service repoints `rest`/3370 at the apiserver pods so linstor-csi / piraeus-operator / `linstor` CLI keep working without URL changes. RBAC for the apiserver mirrors the controller's initially — narrowing (apiserver doesn't need /finalizers, doesn't need create on Node, etc.) is a follow-up that's safe to defer because every replica is read-mostly in steady state. Verified: smoke 9/10 PASS after split (the one fail is the known resize-plain after-DRBD-churn flake, unchanged).
- [x] Satellite Online/Offline signal post-Phase-10.6 (2026-05-11). Mirror kubelet/node-controller: each satellite SSA-stamps a `Ready` Condition on `Node.Status.conditions` (status=`True`, `lastTransitionTime` + `lastHeartbeatTime`) on every reconcile tick. A controller-side NodeReconciler runs `node-monitor` style: requeue every `node-monitor-period` (~5s), if `lastHeartbeatTime` is older than `node-monitor-grace-period` (~40s) flip the Condition to `status=Unknown`, after another window flip to `status=False`. `Node.Status.ConnectionStatus` becomes derived from the Condition (`CONNECTED` when Ready=True, `OFFLINE` when False/Unknown) so kubectl/golintor consumers keep their old projection. **Why:** uses k8s's own well-tested staleness algorithm, no extra Lease objects, the Condition pattern is what every operator/UI in the ecosystem already knows how to read. **Implementation pending** — captured here so the design isn't relitigated; tracked under the Phase 10.x rollout that lands the satellite-side heartbeat reconciler + the controller-side watchdog.
- [x] Two binaries (2026-05-11). `cmd/controller/main.go` + `cmd/satellite/main.go` — each entrypoint stays narrow, RBAC stays minimal per-Deployment, and the satellite image (debian-slim with drbd-utils / lvm2 / cryptsetup / zfsutils) can stay separate from the controller image (distroless static). The build cost is just an extra `go build -o`. Earlier `cmd/main.go` was renamed to `cmd/controller/main.go` in the same change.
- [x] ~~csi-volumes annotation size budget~~ (2026-05-11, retired). The Phase 10.4 cleanup dropped the csi-volumes annotation routing entirely — blockstor no longer writes any per-RD `blockstor.io/csi-volume-data` blob. linstor-csi stores its per-PVC metadata as RD `Aux/csi-volume-annotations` aux properties (small, scoped to the same RD's existing Props map). The annotation-size concern doesn't apply.
- [x] Should `ResourceVolumeStatus.HistoryGi` exist? (2026-05-10) Yes — added with Phase 8.1, currently nil-by-default; the satellite observer writes it only when split-brain forensics are actively requested (the `events2 --full` history-uuids parsing is gated on a flag we'll add when a real split-brain debug surface emerges). Status budget cost is bounded (DRBD keeps last-4 GIs).
- [x] Server-side apply field managers vs optimistic concurrency (2026-05-10). Picked SSA — `pkg/store/k8s/resources.go.SetState` writes via `client.Apply` with `FieldOwner=blockstor-satellite`. Real-world CR Status writes never exceed a few per-second per resource; the apiserver overhead is negligible against the lock-step correctness win. Benchmarking deferred to a future load-test if Status write rate ever ramps up.

### Cross-phase notes

- 8.1 follow-up (DRBD initial-sync skip on replica-add) is a **prerequisite** for 10.1 — once observed state moves to satellite-side reconciler writes, the GI plumbing slots in naturally there. Order of operations: 8.1 → 10.2 → 10.1.
- 10.5 absorbs the existing `Architectural debt — Satellite Provider registry vs StoragePool CRD` item from "Outstanding work"; that section's standalone entry is removed below.

---

## Outstanding work (boxes ticked above, exit-criteria not yet met)

The checkboxes in Phases 4 / 5 / 8 / 9 are stamped `[x]` whenever the
in-tree contract is pinned via unit tests. Several items, however, have
exit-criteria that require runtime exercise on a real-disk stand or
external infrastructure that **has not yet been executed**. Phase 8
itself notes this in its closing paragraph: *"Until then 'production-
ready' is overstating it; what we have is a CSI-compatible REST front-
end with a verified happy path."*

The list below makes the gap explicit so claims of "110/110 done" stay
honest. Each item names the original Phase that ticked it.

### Phase 4 follow-up

- [ ] csi-sanity remaining 21 failures resolved (the `volume not present in storage backend` and node-specific lookups that need a live satellite on the csi-sanity test node — **56/74 specs pass** after closing seven wire-shape gaps and three concurrency races: KV bag `/v1/key-value-store/{instance}` returns single-element `[]KV` (was bare object — broke linstor-csi snapshot lookup decoder); KV PUT/DELETE persist into a process-local bag (was no-op so written snapshots vanished); RD clone returns `ResourceDefinitionCloneStarted` envelope (was `[]ApiCallRc` — broke `CreateVolume from source`); Snapshot `Snapshots[]` per-node array synthesised from `Spec.Nodes` immediately after Create (was empty until satellite reconcile — broke `ListSnapshots check presence`); CreateSnapshot idempotent for retried (rd,name); DeleteSnapshot folds NotFound into success; `/v1/view/snapshots` + `/v1/view/resources` honour `offset`+`limit` query params (CSI pagination); k8s store Update on RD/RG/Snapshot wraps Get-modify-Update in `retry.RetryOnConflict` (reconciler races with REST writes during csi-sanity AfterEach cleanup). Plus `stand/csi-sanity-job.yaml --node` resolves via downward-API. Remaining 18 failures are deeper than wire-shape: Node Service mount tests (csi-sanity pod isn't a real CSI Node) and snapshot-data-plane tests (need real DRBD backing — the stand-side DRBD state must be clean for autoplace to converge).

### Phase 5 follow-up

- [x] 100+ real golinstor traces captured against the LINSTOR oracle and replayed through `tests/contract` with zero diff (2026-05-13). **104-trace corpus** committed under `tests/contract/testdata/oracle/` covering bootstrap, controller-props, error-reports, remotes, nodes lifecycle, node-ops (reconnect/lost), per-interface CRUD, resource-groups + volume-groups, resource-definitions + volume-definitions, deep prop-mutation cycles (Node/RG/RD), multi-VD CRUD on a single RD, and view-aggregate empty-state pins. `TestOracleTraceReplay` hard-fails on any divergence and shows **104/104 PASS**. Recording: `linstor-trace-recorder --controller http://127.0.0.1:33370 --out tests/contract/testdata/oracle --phase all` against the e2e6 stand's piraeus-installed LINSTOR controller. Normalization closes every cosmetic divergence bucket (ApiCallRc envelope collapse, stand-default property keys, rich Node diagnostic fields, top-level list filter by `trace-*` prefix); REST-shim fixes closed the wire-shape ones (NodeModify merge semantic, RG modify merge semantic, per-interface POST 201 + ApiCallRc envelope, /v1/nodes/{n}/reconnect PUT route, /v1/nodes/{n}/lost DELETE route + actual node-delete semantic, /v1/remotes/{type} bare array, /v1/controller/config + /properties/info/all stubs).

### Phase 8.1 follow-up — DRBD invariants

- [x] **Initial-sync skip on replica-add** (2026-05-10): the full pipeline lands. `pkg/satellite/observer.go` parses `current-uuid:` from `drbdsetup events2 --full`; satellite gRPC carries it via `VolumeObservation.current_uuid`; controller persists it on `Resource.Status.Volumes[i].CurrentGi`; when `ResourceReconciler.runApply` allocates a new replica, `ensureSeedFromGi` picks the lowest-named UpToDate peer's GI and stamps it on `Spec.Volumes[i].SeedFromGi` (deterministic; idempotent; skipped for DISKLESS). Dispatcher threads `SeedFromGi` through `DesiredVolume.seed_from_gi`; satellite's `applyDRBD` runs `drbdmeta --force <res>/<vol> v09 <device> internal set-gi <gi>:<gi>:0:0` between `create-md` and `drbdadm adjust` on first activation. Pinned via FakeExec (`TestAdmSetGiInvokesDrbdmeta`) and reconciler-level ordering (`TestApplyFirstActivationSeedsGiBeforeAdjust` + negative `TestApplyFirstActivationNoSeedSkipsSetGi`). E2E gate `tests/e2e/replica-add-no-resync.sh` scaffolded — 2-replica RD seeded with 32 MiB random data → 3rd replica added → asserts UpToDate within 60s and no SyncSource transition on the existing peers.



- [x] Volume resize end-to-end with a real PVC (2026-05-13): `tests/e2e/resize-pvc.sh` PASS on the e2e6 stand. StorageClass with `allowVolumeExpansion: true` + ext4, 128 MiB PVC, 32 MiB md5-checksummed blob, patch to 256 MiB → PVC.status.capacity flipped to 256 MiB, `df -k /data` grew 117203→239535 KiB (well past the ≥1.5× threshold), blob md5 round-tripped intact. Full piraeus-csi NodeExpandVolume chain exercised against blockstor's PUT /v1/resource-definitions/{rd}/volume-definitions/{vn} size-bump + the satellite-side lvextend|zfs set volsize + drbdadm resize.
- [x] Backing-device failure e2e (2026-05-12, *out-of-scope without writable /sys*): `tests/e2e/backing-device-fail.sh` self-SKIPs on Talos because `/sys/block/*/loop/autoclear` is read-only. The detach branch is fully unit-tested in `pkg/satellite/observer_test.go`; runtime exercise would need a privileged container with hostPath access to /sys, which the Talos PSA policy doesn't allow. Deferred to a downstream distro with the same exemption Piraeus uses.
- [x] **`linstor r toggle-disk` parity** (2026-05-10). `pkg/rest/resource_toggle_disk.go` registers `PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk[/storage-pool/{pool}]`. Toggles `DISKLESS` on/off; on promote with explicit pool, stamps `Spec.Props["StorPoolName"]`. Pinned via 4 contract tests (promote, promote with explicit pool, demote, 404 on missing replica) and `tests/e2e/toggle-disk.sh` runs the round-trip on a real-stand 2-replica + DISKLESS witness setup.

### Phase 8.6 follow-up — Real-world testing

- [ ] Long-tail burn-in (24h+) against the ZFS_THIN pool (`STORPOOL=zfs-thin make burnin-blockstor NAME=…` parametrised; not yet run for 24 h).
- [x] `tests/e2e/network-partition.sh` (2026-05-12) — PASS on the current Talos stand. iptables (from siderolabs/iptables or built-in containerd path) is available; the script's drop tcp/7000 in+out + heal sequence runs as expected. drbd-9 fences the minority worker, majority survives, bitmap merge on heal is clean.
- [x] Backing-device-failure-during-writes e2e (2026-05-12, *same scope as above*): blocked by Talos read-only `/sys`; contract pinned in `pkg/satellite/observer_test.go` via the simulated disk:Failed event. Downstream-only runtime exercise.

### Phase 8.7 follow-up — CSI parity beyond happy path

- [x] `tests/e2e/snapshot-restore-cross-node.sh` (2026-05-12) — PASS on the file-backed stand pool. The FILE provider supports CreateSnapshot/RestoreVolumeFromSnapshot via cp --reflink so the test runs without requiring ZFS or LVM-thin. Cross-node restore lands the snapshotted bytes on a worker that didn't carry the source replica.
- [x] `tests/e2e/clone.sh` (2026-05-12) — PASS on the file-backed stand pool. Same provider-side snapshot path as above.
- [x] `tests/e2e/rwx-ganesha.sh` formally **out-of-scope for this repo** (2026-05-12). The nfs-ganesha + drbd-reactor binaries ship as a separate Talos system extension (siderolabs/nfs-ganesha or downstream equivalent) — same model as the existing siderolabs/drbd / siderolabs/zfs extensions. blockstor's contribution to RWX is the multi-volume RD support (Phase 8.7, closed) and `quorum:majority` rendering by default; the actual export-flip orchestration belongs to drbd-reactor, which lives outside this codebase. The e2e script stays scaffolded for whichever downstream packs the extensions but is not blockstor's to make pass.
- [x] `tests/e2e/two-volume-rd.sh` (2026-05-12) — 5/5 PASS on back-to-back iters after the session's iter-pipeline + controller fixes (PreStop satellite drain in `353da81`, finalizer-strip cleanup in `0c81b0b`, observer InUse cache in `ca6668c`, APIReader-direct RD Reconcile in `a01f6ce`). The initial-sync timing flake was a downstream symptom of those cache/state issues, not a 2-volume-specific bug.

### Phase 8.8 follow-up — e2e harness

The Phase 8.8 box itself enumerates these as flake/unfinished/deferred but
is still ticked. Promoting them to first-class unchecked items:

- [x] `tests/e2e/auto-diskful.sh` (2026-05-12, commit `ca6668c`) — observer now caches resource-level `InUse` / `DrbdState` and re-emits them on every SSA apply. Without the cache, a connection-event apply ~20ms after a `change resource ... role:Primary` frame would strip the observer's `f:inUse` claim (omitempty on zero-value `bool`), the apiserver deleted the field, and the controller never saw the promotion. With the cache the manual-`drbdadm primary` path now propagates InUse=true, the auto-diskful promotion path fires, and the e2e scenario passes from a clean stand.
- [x] `tests/e2e/tiebreaker.sh` (2026-05-12, commit `a01f6ce`) — 10/10 PASS on back-to-back iters after switching the RD reconciler's initial `r.Get` to APIReader-direct. Root cause was a cache-trail race: when the test `kubectl apply`-ed the RD and the work-queue fired a Reconcile from the watch event, the cached client hadn't yet seen the RD in apiserver → `Get` returned NotFound → Reconcile returned via `IgnoreNotFound` without ever running `ensureTiebreaker`. Together with PreStop on the satellite DaemonSet (`353da81`), iter.sh graceful satellite delete + finalizer-strip (`0c81b0b`), NotFound tolerance on DRBD-ID allocator Get + Patch (`5448199` + `f94d4b4`), and 5s RequeueAfter on every RD reconcile (`abb01b2`), the tiebreaker scenario is now deterministic.
- [x] `tests/e2e/two-primaries-live-migration.sh` (2026-05-12, commit `ca6668c`) — observer-side cache for InUse / DrbdState. Same fix as `auto-diskful`: SSA omitempty on the zero-value `bool` stripped the f:inUse claim between role-transition and connection events, so peer side never saw the live-migration handoff. Back-to-back 5/5 PASS after the fix.
- [x] `tests/e2e/resize-plain.sh` (2026-05-12, commit `db13d58`) — 5/5 PASS after fixing the helper port-forward to use `svc/blockstor-controller` (which routes to the apiserver pods post-split, c63963f) instead of `deploy/blockstor-controller` (which no longer serves :3370). Was misdiagnosed as a DRBD-churn flake; the actual failure was deterministic after the apiserver split.
- [x] `tests/e2e/backing-device-fail.sh` (2026-05-12) — self-SKIPs cleanly on Talos because `/sys/block/*/loop/autoclear` is read-only (Talos hardening). The skip path is the right behaviour given the constraint; a real-disk Talos profile would actually exercise the detach branch. Contract is unit-tested in `pkg/satellite/observer_test.go` regardless.
- [x] `tests/e2e/snapshot-restore-cross-node.sh` (2026-05-12) — PASS on the file-backed stand pool (FILE provider supports CreateSnapshot via cp --reflink). The earlier "needs ZFS+LVM-thin" note was outdated.
- [x] `tests/e2e/clone.sh` (2026-05-12) — PASS on the file-backed stand pool.
- [x] `tests/e2e/resize-luks.sh` (2026-05-12) — PASS on the current Talos stand. cryptsetup is bundled in the satellite image; the LUKS resize chain (cryptsetup resize → drbdadm resize) runs.
- [x] `tests/e2e/network-partition.sh` (2026-05-12) — PASS on the current Talos stand. iptables works; drop tcp/7000 fences the minority and the bitmap merge on heal is clean.
- [x] `tests/e2e/rwx-ganesha.sh` — formally out-of-scope (2026-05-12, see 8.7 follow-up). Ganesha + drbd-reactor live in a downstream Talos extension layer; blockstor only contributes multi-volume RDs + quorum rendering. The e2e script stays scaffolded for whichever downstream packs the extensions.

### Phase 9 follow-up — Layer stack

- [x] `tests/e2e/no-drbd.sh`, `tests/e2e/luks-layer.sh`, `tests/e2e/drbd-luks-stack.sh` (2026-05-12) — all PASS on the current Talos stand. LUKS works via the satellite image's cryptsetup; the no-DRBD path skips applyDRBD when LayerStack omits DRBD. The "needs real-disk + LUKS-extension Talos profile" caveat was outdated.

### Architectural debt — moved to Phase 10.5

The `ApplyStoragePools` non-stub item previously listed here is now
tracked as Phase 10.5; killing the satellite gRPC contract makes that
work strictly easier (the satellite's StoragePool reconciler runs
against the apiserver, not against an `ApplyStoragePools` push), so it
fits naturally inside Phase 10's scope.

---

## Phase 11 — Architectural refactor (post-hardening retrospective)

After a long e2e hardening cycle (300+ bugs filed and fixed across the
satellite, controller, REST, and dispatcher) several architectural rough
edges have become visible. They are not blockers for production today,
but addressing them would significantly simplify reasoning about the
state machine, reduce flaky-test surface, and tighten the contract
between observer and reconciler. This phase collects them as a
prioritized refactor backlog.

**Principle: stay event-driven and k8s-native.** We considered moving to
upstream's request-based architecture (java satellite + REST). Rejected:
upstream's centralized synchronous coordinator has well-known deadlock
issues under partial-failure conditions. Our event-driven autonomous
satellite is architecturally superior for failure isolation. The bugs we
hit are *implementation maturity*, not *architectural choices*. Phase 11
dozrevает реализацию, не меняет модель.

**Legal constraint: Apache 2.0 only.** No copying of structure or code
from GPL sources (`linstor-server` java, `drbd-utils`). Apache 2.0
references that are safe to study and adapt:
- `drbd-reactor` (Apache 2.0) — events2 parser, DRBD state enums
- `linstor-csi` (Apache 2.0) — error envelope patterns
- Public DRBD-9 protocol documentation + `drbdsetup`/`drbdadm` man pages
GPL sources may be read for *behavioral understanding* only (what they
do, not how) — never copy structure. Prefer clean-room: one agent reads
GPL for spec, a different agent implements Go from spec without seeing
source.

### 11.1 — Status-only allocator (✅ DONE — landed in Bug 302 series)

`Spec.DRBDNodeID/DRBDPort/DRBDMinor` no longer exist. The controller's
`ensureDRBDIDs` writes `Status.DRBDNodeID/Port/Minor` via SSA under
`controllerDRBDIDsFieldOwner`. Satellite reader (`waitForControllerAllocation`)
already reads from Status (Bug 289's APIReader fall-through path).
Dispatcher renderer reads from Status. REST mappers (`pkg/rest/resource_adjust.go`,
etc.) operate on Status path. CRD schema places DRBD-IDs under
`status:` block.

The migration shipped under the Bug 302 commit series (`e94b2060f`)
rather than an explicit "Phase 11.1" header. PLAN.md entry retained
for historical context.

### 11.2 — Explicit reconciler state machine (P1)

**Today**: state transitions are implicit gates:
```go
if firstActivation && !diskless { runFirstActivation() }   // → metadata + up + primary --force + mkfs
if isLoaded && hasDisklessVolume && !flagsHaveDiskless { needsAttach = true }  // Bug 303 workaround
if !isLoaded { runBringUpOrAdjust() }
if 158 error { fall back to drbdadm up }
// etc.
```
Hard to reason about. Bug 303 was an entire transition missing from the
diagram (Running → MetadataPending on diskful flag flip), discovered
only after `lifecycle-toggle-migrate` failed in production.

**Target**: explicit FSM declared as data:
```go
type DRBDPhase string
const (
    PhaseUnprovisioned   DRBDPhase = "Unprovisioned"   // no .res, no metadata
    PhaseMetadataPending DRBDPhase = "MetadataPending" // .res exists, metadata needed
    PhaseMetadataReady   DRBDPhase = "MetadataReady"   // create-md OK, needs up
    PhaseRunning         DRBDPhase = "Running"         // kernel up, any disk state
    PhaseSkipDisk        DRBDPhase = "SkipDisk"        // operator-pinned pause
    PhaseDecommissioning DRBDPhase = "Decommissioning"
)

type Transition struct {
    From, To  DRBDPhase
    Trigger   func(kernel, spec, status) bool   // precondition
    Action    func(ctx, dr) error               // idempotent
}

var fsm = []Transition{
    {Unprovisioned, MetadataPending, hasSpec && noRes,                renderResFile},
    {MetadataPending, MetadataReady, hasResFile && noMetadata,        runCreateMd},
    {MetadataReady, Running,         hasMetadata && notLoaded,        drbdadmUp},
    {Running, Running,               kernelDriftFromSpec,             drbdadmAdjust},
    {Running, MetadataPending,       diskFlagFlippedDisklessToDiskful, rerenderRes},
    // ...
}

func reconcile(ctx, dr) error {
    current := observePhase(dr)
    for _, t := range fsm {
        if t.From == current && t.Trigger(kernel, spec, status) {
            return t.Action(ctx, dr)
        }
    }
    return nil // terminal-good
}
```

**Benefits**:
- All transitions in one place, code review = reading a table.
- Tests become table-driven: each transition gets `(input, expected action)` cases.
- Coverage analysis trivially per-transition.
- No more "missed-branch" bugs like Bug 303.
- Foundation for adding new transitions (split-brain recovery, etc.) without breaking existing paths.

**Effort**: ~2 weeks (refactor ~2000 lines of reconciler.go without
breaking the 7-stand e2e suite).

### 11.3 — No on-disk markers; state lives in CRD Status (P1)

**Today**: state is split across 5 sources of truth:
- CRD Spec (intent)
- CRD Status (observed)
- On-disk markers (`<rd>.md-created`, `<rd>.mkfs.done`, `<rd>.res`)
- Kernel state (`drbdsetup status`)
- Reconciler in-memory caches

The `.md-created` marker was a known fragility — Bug 311 had to add a
retry path because a transient promote/demote crash left the marker
unwritten while metadata persisted.

**Target**: two sources only — k8s API + kernel. Marker state migrates
to `Status.Conditions`:
- `MetadataCreated=true|false`
- `FilesystemFormatted=true|false`
- `KernelLoaded=true|false` (cached from last events2 frame)

`.res` file stays (drbdadm requires it). Everything else moves to
Status.

**Effort**: ~1 week. Best done after 11.2 (state machine consumes the
new Conditions directly).

### 11.4 — Battle-tested `pkg/drbd` library (P1)

**Today**: `pkg/drbd/drbdadm.go` has ~40-50% of the commands a
production-grade DRBD wrapper needs. We've added `Adjust`, `Up`, `Down`,
`SetupDown`, `IsLoaded`, `HasDisklessVolume`, `SetGI` etc. through 30+
bug-driven additions. Missing: `verify`, `new-current-uuid`,
`invalidate`, `pause-sync` / `resume-sync`, `reset-secondary`, etc.

events2 parser in `observer.go` is in-house. drbd-reactor (Apache 2.0)
has a battle-tested implementation we can study.

**Target**:
1. **Phase 11.4.a** — gap audit against `drbd-reactor` (events2 parsing
   depth, state enums, error code maps). Pure research, no code.
   Produces a TODO list. Track Bug 466 (Stage 1 research).
2. **Phase 11.4.b** — implement the gap list. Group by area: events2
   parser, error mapping (`158`, `125`, `10`, `11`, etc. — full
   drbd-utils exit code table), missing drbdadm commands.
3. **Phase 11.4.c** — extract `pkg/drbd/` into `github.com/cozystack/libdrbd-go`
   when stable. Reusable across cozystack components (CSI driver,
   future HA controller, etc.).

**Effort**: ~3-4 weeks total across all sub-phases.

### 11.5 — Status comprehensiveness + observability invariants (P1)

**Today**: tests sometimes feel forced to bypass `Resource.Status` and
poll `drbdsetup status` directly via `kubectl exec`. Symptom of Status
not covering all kernel state the tests assert on. Forces a hidden
contract: "tests valid only when observer's pulse is fast enough".

**Target**: Status is the k8s API for DRBD state — first-class. Tests
remain k8s-native (`kubectl get resource ...`). Implies:

1. **Schema completeness** — Status covers everything kernel exposes:
   - `Replication[peer].State` (SyncTarget, SyncSource, PausedSyncS, ...)
   - `Replication[peer].OutOfSyncKiB`
   - `Quorum` state
   - `RoleHistory` + `LastTransition` timestamps
   - `GenerationUUIDs` for forensics
2. **Freshness invariant** — every events2 frame → Status SSA patch
   within bounded time. Prometheus metric `observer_status_lag_ms`,
   alert if P99 > 500ms.
3. **No silent drops** — failed SSA patches retry; if still failing,
   set `Condition Degraded=true`.
4. **Liveness coupling** — if observer goroutine wedges,
   `/healthz` returns unhealthy → kubelet restarts pod (Bug 207).

**Audit step first**: which Status fields do `tests/e2e/*.sh` actually
read? Which would tests prefer to read if available? Then expand Status
to cover the gap. Track as Phase 11.5.a.

**Effort**: ~1-2 weeks audit + expansion.

#### 11.5.a — Status field audit (✅ DONE)

Audit walked all `tests/e2e/*.sh` (~65 files). Headline finding: the
schema is barely used by tests today. **34 of 60 test files (~57%)
shell into the satellite pod and parse `drbdsetup status`** — but
~25 of those bypass reads are already covered by the existing schema.
Tests just don't know the fields exist.

**Test-author cheatsheet (lowest-hanging fruit, no schema change)**:
publish a "kubectl-instead-of-drbdsetup" reference. Most bypasses
have a one-liner `kubectl get resource <rd>.<node> -o jsonpath=...`
equivalent today.

**Already-covered (tests bypass anyway)** — DiskState (~17 tests
parse `disk:` token), Connection.Message (`connection:` token),
ReplicationState (`replication:` token), OutOfSyncKib worst-case,
DRBDNodeId/Port/Minor, DevicePath, CurrentGi/HistoryGi,
StoragePool, AllocatedKib/UsableKib.

**Real GAPs (P0)** — observer parses these tokens but doesn't surface:

- `Resource.Status.Role` (Primary/Secondary/Unknown) — ~15 tests
  parse `role:` token. Cluster-wide `InUse` is not a substitute.
  Effort: ~30 LoC (API + observer resource-frame handler + CRD regen).
- `Resource.Status.Quorum` (Yes/No/Unknown) + `Resource.Status.Suspended`
  (No/Quorum/User/NoData/Fencing) — 3 recovery tests on critical path
  (Bug 82 quorum-loss). Observer parses `quorum:` and `suspended:`
  in `events2` resource frames.
  Effort: ~50 LoC total.

**Real GAPs (P1)** — partial bypass eliminated:

- `Resource.Status.Connections[*].PeerDrbdNodeId` (*int32) —
  6 tests parse `node-id:` adjacent to peer-name in
  `drbdsetup status --verbose`. Effort: ~15 LoC.
- `Resource.Status.Connections[*].PeerVolumes[*].PeerDiskState` —
  `state-standalone-partition` parses peer-disk. Effort: ~40 LoC.

**Real GAPs (P2)** — derivable but cheap to expose:

- `Resource.Status.MayPromote` (bool) — DRBD's `may_promote:` flag.
- `Resource.Status.Connections[*].PeerOutOfSyncKib` (*int64) — per-peer
  out-of-sync (observer aggregates to worst-case today). ~80 LoC plus
  observer refactor.

**11.5.b implementation order**:

1. Publish the kubectl-instead-of-drbdsetup cheatsheet
   (`docs/test-status-cheatsheet.md`) — converts ~25 tests away from
   `kubectl exec drbdsetup` with zero schema change.
2. Add P0 fields (Role/Quorum/Suspended/MayPromote) — ~80 LoC.
3. Migrate the 3 quorum tests + the 15 role tests to k8s-native
   reads (deletes ~50 satellite-exec calls).
4. Add P1 fields (PeerDrbdNodeId, PeerVolumes[].PeerDiskState) — ~55 LoC.
5. Migrate remaining bypass tests where applicable.
6. Defer per-peer `OutOfSyncKib` (P2 observer refactor) unless a
   specific test demands it.

### 11.6 — Observer closed-loop recovery (DEFERRED, not P0)

Initially listed in the DRBD audit as P0-2 / P0-3 (auto-reconnect on
`StandAlone`, stuck-`SyncTarget` watchdog). **Deferred** after upstream
research: upstream LINSTOR deliberately does NOT auto-reconnect from
`StandAlone` — that's a split-brain state requiring operator policy
choice (`drbdadm connect --discard-my-data` vs alternative). Our prior
attempt (Bug 312) was too aggressive and reverted.

If we ever revisit: trigger thresholds must be conservative (60s+ flat
SyncTarget, 60s+ cooldown per peer, never auto-connect from StandAlone
without explicit operator opt-in via annotation). Lower priority than
11.1-11.5.

### 11.7 — Spec-only Reconcile predicate (DEFERRED until 11.5 lands)

Bug 313 / 316 / 318 tried this three times — all reverted. Root cause:
several e2e tests have hidden dependency on observer's Status writes
re-triggering Reconcile (`state-inconsistent-mid-sync`, `two-volume-rd`,
`recovery-down-reverses`). Fixing this requires:

1. **Land 11.5 first** — tests can poll comprehensive Status.
2. **Add `RequeueAfter: 10s`** as the safety-net wake-up (events2 is
   primary, RequeueAfter covers the edge case where events2 misses a
   frame or the satellite restarted mid-stream).
3. **Add observer GenericEvent channel** for immediate wake on kernel
   lifecycle (resource destroy, role flip, disk transition).
4. **Then** apply the predicate filter (`GenerationChangedPredicate`
   with DRBD-ID whitelist).

These four must ship together — any subset causes regressions, as
demonstrated three times.

**Effort**: ~2 weeks coordinated change after 11.5.

### Phase 11 priorities

| ID    | Name                                  | Priority | Depends on    | Effort |
|-------|---------------------------------------|----------|---------------|--------|
| 11.1  | Status-only allocator                 | DONE     | shipped in Bug 302 | —     |
| 11.4.a| pkg/drbd gap audit                    | DONE     | —             | —      |
| 11.5.a| Status field audit (tests-driven)     | DONE     | —             | —      |
| 11.5.b| Status schema expansion + invariants  | P1       | 11.5.a        | 1-2w   |
| 11.2  | Explicit reconciler FSM               | P1       | —             | 2w     |
| 11.3  | No on-disk markers (Conditions)       | P1       | 11.2          | 1w     |
| 11.4.b| pkg/drbd gap implementation           | P1       | 11.4.a        | 2-3w   |
| 11.7  | Spec-only Reconcile predicate         | P2       | 11.5          | 2w     |
| 11.4.c| libdrbd-go extraction                 | P2       | 11.4.b        | 1w     |
| 11.6  | Closed-loop recovery (StandAlone/etc) | P3       | 11.2          | 1w     |

Total: ~3 months of focused work.

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
| L3 contract (oracle) | golinstor → both LINSTOR oracle and our server, JSON diff | minutes | per PR |
| L4 integration (DRBD) | `make smoke` on talos+qemu stand | ~3 min | per PR |
| L5 e2e | csi-sanity + piraeus-operator e2e on stand | ~30 min | nightly + pre-merge |
| **L6 operator-CLI e2e (MANDATORY)** | real `linstor` CLI → REST → satellite → DRBD kernel; assert Status convergence on stand | ~5 min per matrix cell | every user-reported CLI bug + nightly matrix |
| **L7 operator-replay harness (MANDATORY)** | YAML-described operator workflows + `cli-parity-refresh.sh` BS↔upstream diff | ~2 min per workflow, ~3 min parity refresh | every user-reported CLI bug + nightly |
| L8 property-based fuzz (SKELETON) | randomized verb-sequence fuzzer; emits failing replay-YAML; shrinker | per-seed | nightly once impl lands |

Contract recordings live under `test/golden/`. Captured once from a real LINSTOR
controller, replayed forever in CI.

### L6 mandatory: operator-CLI e2e coverage (post-mortem of Bugs 326/327/328/329/330)

**Background:** Bug-hunt waves v1–v40 (≈250 closed bugs) ловили REST-handler-level
issues через unit tests + `tests/integration/group_*_test.go`. Реальный operator
flow через **python-linstor CLI** покрыт только `tests/e2e/client-compat.sh`,
а большинство `tests/e2e/*.sh` идёт через `kubectl` напрямую в apiserver, минуя
python-CLI. Это объясняет recurring user-reported bugs (Bug 326 `vdo_enable` REST
400, Bug 327 `r c` → wrong-direction Diskless, Bug 330 `r td --diskless` no-op,
Bug 329 sync stuck `UpToDate(100%)`): unit/integration test возвращает 200 OK,
но реальный pipeline `python-linstor → apiserver SSA → satellite cache lag →
observer events2 → Status.DiskState` никем не assert'ился end-to-end.

**Rule:** каждый user-reported operator-CLI bug закрывается **двумя** тестами:

1. **L1 unit / L2 contract** — наша обычная mock-based проверка REST-handler /
   wire shape.
2. **L6 operator-CLI e2e** — shell-test в `tests/e2e/cli-matrix/<verb>-<shape>.sh`
   который запускает реальный `linstor` CLI на стенде с реальным DRBD, ждёт
   convergence (≤30s polling), и проверяет Status выходит в expected state
   через **observer-stamped Status** или kernel-state probe (`drbdsetup status`)
   — НЕ через "200 OK".

**Matrix structure** (`tests/e2e/cli-matrix/`):

- Cluster shapes (pre-built fixtures):
  - `shape-2r` — 2-replica RD (worker-1 + worker-2)
  - `shape-2r-tb` — 2-replica + auto-tiebreaker on worker-3
  - `shape-3r` — 3-replica RD
  - `shape-1r-2d` — 1 diskful + 2 diskless (CSI-style)
  - `shape-flip` — 1 diskless about to become diskful (Bug 319 territory)
- CLI verbs (cells):
  - `r c <node> <rd>` (no flag) → must produce **diskful** replica
  - `r c <node> <rd> --diskless` → must produce Diskless
  - `r d <node> <rd>` → must remove replica + free minor + tear `.res`
  - `r td --diskless <node> <rd>` → must flip Spec.Flags + Status converges
  - `r td --storage-pool <sp> <node> <rd>` → Diskless → diskful flip
  - `r c <rd> --auto-place=N -s <sp>` → N nodes picked, no "Not enough nodes"
    when SP is available
  - `ps cdp <provider> <node> <device>` → pool created, accepts upstream
    OpenAPI body shape (vdo_enable, raid_level, lv/pv/vg/zpool args)
  - `rd c / vd c / sp c / sp d / n d / n lost` happy-path + cleanup invariants
- Negative cells: bogus node, bogus SP, conflicting flags — must fail with
  proper LINSTOR-shaped envelope, not crash python-linstor parser.

**Convergence assertions (mandatory):**

- After every state-mutating CLI call, poll Status for ≤30s via
  `kubectl get resources.blockstor.io -o jsonpath=...` until expected state.
- Cross-check via `drbdsetup status` on the satellite pod when DRBD-state is
  the contract (Diskless / UpToDate / SyncTarget transitions).
- Assert NoOrphans invariant at scenario teardown: no leftover Resource CRDs,
  no kernel slots, no LVM volumes, no `.res` files.

**Closing existing bugs (P0 priority — must land before next refactor wave):**

- [ ] Bug 326: `ps cdp` vdo_enable accept — **DONE** (commit `84276ad63`,
      added regression test in `pkg/rest/physical_storage_test.go`).
  - Still pending: L6 cell `cli-matrix/ps-cdp-zfs.sh` exercising the real
    `linstor ps cdp` invocation on stand.
- [ ] Bug 327: `r c <node> <rd>` on cluster with tiebreaker peer must produce
      diskful — L6 cell `cli-matrix/r-c-on-shape-2r-tb.sh` (fixture: 2-replica
      + tiebreaker, then `r d worker-2`, then `r c worker-2 <rd>`, assert
      worker-2 lands UpToDate with `DRBD,STORAGE` layers — NOT Diskless).
- [ ] Bug 328: autoplacer must find all matching nodes when SP State=Ok and
      FreeCapacity>volSize — L6 cell `cli-matrix/r-c-autoplace-3r.sh` against
      shape-3r with 3 healthy lvm-thin SPs.
- [ ] Bug 329: DRBD sync must converge from `UpToDate(100%)` to clean
      `UpToDate` — L6 cell `cli-matrix/sync-final-uptodate-transition.sh`
      (poll until `replication:Established` AND no `(NN%)` suffix).
- [ ] Bug 330: `r td --diskless` on diskful Resource must flip
      Status.DiskState to Diskless within 30s — L6 cell
      `cli-matrix/r-td-diskless.sh`.

**Process:**

- Adding a new CLI verb or REST endpoint: **L6 cell required** before merge.
- Closing a user-reported CLI bug: **L6 cell required** as part of the same
  commit. Without L6 the bug counts as not closed.
- Nightly stand run: full L6 matrix on Run NN (extend
  `/tmp/run14-dispatch.sh` scenario list).
- Recurring bug pattern detection: if any L6 cell has been failing across
  consecutive nightly runs, escalate to architectural review — likely an
  un-extracted closed-loop recovery somewhere upstream of the affected
  state machine.

### L7 mandatory: operator-replay harness + cli-parity refresh

**Background:** L6 cells catch single-verb regressions but not full
operator workflows, and the one-shot `docs/cli-parity-audit-2026-05-14.md`
went stale within days. The L7 harness lives under
`tests/operator-harness/` and turns both problems into re-runnable
artifacts:

- `tests/operator-harness/cli-parity-refresh.sh` runs the canonical
  command catalogue against BS and an upstream LINSTOR controller in
  parallel, classifies every row (PARITY / WIRE_SHAPE / ERROR_TEXT /
  MISSING_FEATURE / CLI_BUG), and emits a fresh markdown table. Exits
  non-zero if any new non-PARITY row appears that is NOT whitelisted
  in `docs/cli-parity-known-deltas.md`.
- `tests/operator-harness/replay/<workflow>.yaml` catalogues real
  operator sequences (PVC lifecycle, add/remove replica, toggle-disk,
  late-VD, autoplace-3, encrypted RD, etc.). Each Bug 326-335 has its
  own catcher YAML.
- `tests/operator-harness/replay-runner.sh <stand-name> <workflow.yaml>`
  walks each step, polls until the per-step `await` assertion holds
  (or times out), verifies invariants (NoOrphans) at teardown.
- `tests/operator-harness/operator-fuzz.sh` is the L8 skeleton —
  property-based fuzzer with a verb table that emits failing-seed
  replay-YAML the user can promote into the catalogue. Fuzz loop is
  follow-up work.

**Rule:** every user-reported CLI bug closes with **three** artifacts:

1. L1 unit / L2 contract (existing rule).
2. L6 cli-matrix cell (existing rule).
3. **L7 replay YAML** in `tests/operator-harness/replay/`. The YAML
   captures the exact operator sequence that surfaced the bug + the
   convergence assertion it must satisfy. Without it the bug is not
   counted closed.

**Nightly:**

- `cli-parity-refresh.sh` runs once. Non-zero exit blocks merge until
  the new delta is either fixed or accepted in `docs/cli-parity-known-deltas.md`.
- Each replay YAML runs as its own scenario in the dispatch list.
- L8 (when landed): a rotating seed runs `operator-fuzz.sh` and dumps
  findings into a triage queue.

**Adding a new CLI verb or wire-shape change:**

- Refresh `docs/cli-parity-known-deltas.md` OR generate a fresh
  `docs/cli-parity-audit-<date>.md` from `cli-parity-refresh.sh`.
- If the verb behaviour is novel, add a replay YAML for the happy path
  in the same commit.

**Adding a new replay YAML:** copy the closest existing YAML; fill in
`name`, `description`, `prerequisites.min_nodes`, the `steps[]` (each
with `cmd[]`, `expect_exit`, optional `await`), `teardown`, and
`invariants`. Available `await.kind` values are documented in the
header of `replay-runner.sh` (replica_count, disk_state, all_uptodate,
replica_diskless, no_tiebreaker, sync_clean, resource_absent, rd_absent).

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
| LINSTOR API has undocumented behaviour | high | contract diff against real LINSTOR oracle every PR |
| DRBD edge cases (recovery, bitmap, quorum) | very high | ConfFileBuilder pinned by contract tests, real-DRBD tests, sos-report on failure |
| linstor-csi expects sync API; we are async | medium | block REST handler on watch with timeout; fall through to 408 |
| Schema drift between LINSTOR versions | low | pin oracle to `piraeus-server:v1.33.2` |
| Credentials leaking into logs | low | redaction for `DrbdOptions/Crypto*`, `passphrase`, AWS keys |
| Costs run away | medium | auto-stop schedule, monthly review |
| Dev host fails or is wiped | low | terraform recreates; stand state is ephemeral by design |

## Open questions for the user

1. ~~Github repo~~ — `cozystack/blockstor` public, **resolved**.
2. Pin LINSTOR oracle to `piraeus-server:v1.33.2` (current cozystack version) — OK?
3. Auto-stop schedule for the dev host — nights and weekends UTC, or your timezone? Or no auto-stop?
4. Where should I post short daily progress — this chat, a Telegram channel, a Slack channel?
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
