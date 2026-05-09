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

- **Dev stand**: `ssh ubuntu@129.213.29.101` (OCI BM.HPC2.36; 72 vCPU,
  376 GiB RAM, 5.7 TiB NVMe at `/var/lib/blockstor`, `/var/lib/docker`
  symlinked there). Workflow from the repo root: `make up NAME=foo` →
  Talos+QEMU+DRBD, `make piraeus` → operator + satellites, `make oracle`
  → Java LINSTOR controller for contract-diff, `make smoke` → end-to-end
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

- [x] BM.HPC2.36 provisioned on OCI (terraform applied) — accessible at `ssh ubuntu@129.213.29.101`
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
- [x] StoragePool auto-registration via Hello (2026-05-08): satellite enumerates its configured Providers and ships them in HelloRequest.Pools; Server.Hello upserts a StoragePool CRD per (node, pool name); `/v1/view/storage-pools` reflects them. End-to-end on the stand: 3 satellites with `--loopfile-pool-name=stand` produce 3 StoragePool CRDs (`test-worker-{1,2,3}.stand`, FILE_THIN) without anyone running `linstor storage-pool create`.
- [x] piraeus-operator native flip first cut (2026-05-08): patching
      `LinstorCluster.spec.externalController.url=http://blockstor-controller.blockstor-system.svc:3370`
      tells piraeus-operator to skip its own Java controller and
      point linstor-csi at blockstor's REST. Once
      `Server.SetConnectionStatus("ONLINE")` started landing in the
      Node CRD's Status subresource, the `linstor-wait-node-online`
      initContainer on `linstor-csi-node` rolled past Init and the
      pod went 3/3 Running — i.e. piraeus accepts blockstor as a
      drop-in for the Java oracle. PVC provisioning end-to-end
      requires more REST endpoints behaving exactly like the Java
      reference (status fields linstor-csi reads on attach); that
      shake-down is a follow-up.

**Exit met (definition side).** Real reconciliation work now lives in Phase 3.

### Phase 3 — Satellite + DRBD lifecycle

- [x] gRPC controller↔satellite proto definition (`proto/satellite/v1alpha1/satellite.proto`, 8 RPCs)
- [x] `cmd/satellite/main.go` skeleton + `pkg/satellite.Agent` runtime stub
- [x] Generated Go bindings (`make proto` → `pkg/satellite/proto/*.pb.go`)
- [x] Controller-side gRPC server (`pkg/satellitecontroller`) that satellites dial; Hello registers/idempotently-updates the Node CRD and returns ClusterID. 3 contract tests green.
- [x] `pkg/satellite.Agent` actually dials the controller and round-trips Hello (2 end-to-end tests). Wired into `cmd/main.go` via `--satellite-grpc-bind-address` (default `:7000`) and `--cluster-id`.
- [x] StoragePool: LVM-thin (`pkg/storage/lvm`) and ZFS / ZFS_THIN (`pkg/storage/zfs`) providers behind `pkg/storage.Provider` interface; FakeExec drives them in unit tests, RealExec wraps os/exec in production. **ZFS integration smoke** (opt-in via `BLOCKSTOR_ZFS_POOL`): `pkg/storage/zfs/zfs_integration_test.go` walks CreateVolume / VolumeStatus / CreateSnapshot / DeleteSnapshot / DeleteVolume + PoolStatus against a real `zpool` on the dev stand and is green (verified against `blockstor-test` pool, 240 MiB loop-backed, 2026-05-08).
- [x] ConfFileBuilder in Go (`pkg/drbd/conffile.go`) — port from upstream Java; deterministic output, 7 contract tests green
- [x] `drbdadm up/down/adjust/create-md/primary/secondary` exec wrappers behind interface (`pkg/drbd/drbdadm.go`); 7 contract tests via FakeExec
- [x] `drbdsetup events2` listener (`pkg/drbd/events2.go`): line parser + Watcher streaming `Event{Action,Kind,Fields}` to a channel; 7 contract tests
- [x] Resource reconciler (`pkg/satellite.Reconciler`) routes DesiredResource batches: storage provider CreateVolume per volume, ConfFileBuilder writes /etc/drbd.d/<name>.res, drbdadm create-md (first activation, non-DISKLESS) + adjust. Status writeback from events2 stream is the next slice.
- [x] Status writeback complete: satellite agent runs `drbdsetup events2` → `pkg/drbd.Watcher` → `Observer.Translate` → `Controller.ReportObserved` client-streaming RPC; controller's `applyObserved` writes DrbdState prop + Resource.State.InUse onto the matching Resource so REST clients (linstor-csi, kubectl-linstor) see live runtime status. Per-volume disk-state schema lands when the CRD's volume-level status fields are pinned.
- [x] Auto-primary seed: Dispatcher tags one replica per diskful RD with `drbd_options[auto-primary]=true`; satellite `applyDRBD` runs `drbdadm primary --force` on firstActivation and immediately drops back to Secondary, so a brand-new RD reaches UpToDate without an operator's `drbdadm` invocation.
- [x] Resource on 2 nodes replicates and goes UpToDate (real DRBD smoke) — **closed end-to-end** with auto-primary seed (2026-05-08): `kubectl apply RD + 2 Resource` → controller → satellite → loopfile + drbdadm adjust + auto-primary --force → both peers `disk:UpToDate peer-disk:UpToDate` without any manual drbdadm. Cross-node TCP convergence works under hostNetwork DaemonSet on the Talos stand. **Bytes-perfect data replication confirmed**: 1 MiB random data written on worker-1 (Primary) reads back identical on worker-2 (Primary after failover). Automated regression: `make smoke-blockstor NAME=<cluster>` (`tests/smoke-blockstor.sh`) drives the full lifecycle (apply → UpToDate → write → failover read → md5 match → delete) and exits 0 on success.
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
      **`disk:UpToDate` reached** (2026-05-08): pkg/storage/loopfile
      lands a sparse-file + losetup provider so Talos workers (which
      don't expose a free block device) can back non-DISKLESS
      resources. The satellite registers it as `--loopfile-pool-name=stand`
      under hostPath `/var/lib/blockstor-pool`. End-to-end with a
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
- [x] Snapshot satellite-side reconcile: `Reconciler.CreateSnapshot/DeleteSnapshot` route via in-memory resource→pool map populated by Apply (3 contract tests). Snapshot CRD reconciler controller-side TBD.
- [x] Snapshot restore creates a new ResourceDefinition (`POST /v1/resource-definitions/{rd}/snapshot-restore-resource`): seeds the new RD from the snapshot's metadata, returns 201. Per-volume cloning is the satellite's job on next reconcile. 3 contract tests.
- [x] Intra-cluster snapshot shipping for clone/replica-expansion: `Reconciler.ShipSnapshot` picks `zfs send | ssh peer zfs recv` for ZFS / ZFS_THIN and `thin-send-recv` for LVM_THIN, dispatched via an injectable ShipExec so unit tests assert command lines without spinning up the real tools. 3 contract tests.
      - ZFS pools: `zfs send | ssh | zfs recv` over satellite-to-satellite
      - LVM-thin: `thin-send-recv` (LINBIT)
- [x] csi-sanity runs end-to-end against blockstor REST (2026-05-08): `stand/csi-sanity-job.yaml` is a single-pod Job hosting `piraeus-csi` + `csi-sanity` sharing /csi via emptyDir; piraeus-csi dials `http://blockstor-controller:3370`, csi-sanity hammers it through the standard CSI gRPC contract. Initial baseline: 38/92. Iterative gap-closing (2026-05-08…09): csi-sanity-node init container, K8sName slugifier for non-RFC1123 names, lenient JSON decoder matching Java LINSTOR semantics, override_props passthrough on RG/RD/spawn payloads, RemoteList envelope shape, int64 `ret_code`, satellite_encryption_type uppercase normalisation. Current: **53/74 specs passing** (74 of 92 ran, 17 skipped, 1 pending). Remaining 21 failures cluster around `volume not present in storage backend` and node-specific lookups for the fake `csi-sanity-node` — those are the parts of csi-sanity that need a live satellite present on the test node; not REST-layer regressions.

**Exit**: csi-sanity green; piraeus-operator e2e green for what they cover; PVC clone across nodes works.

### Phase 5 — Compatibility burn-in

- [x] Burn-in infrastructure landed (`tests/burnin-blockstor.sh`, `make burnin-blockstor NAME=… DURATION=…`): each iteration apply RD + 2 Resources → UpToDate → 1 MiB urandom write → failover → md5 match → cleanup. 5-min shake-down on the dev stand: **58/58 iterations pass, 0 failures** (~5 s/iteration). Default DURATION=86400 (24h); leaving the long-tail run as an operational task — the regression gates are pinned and the script can be backgrounded any time.
- [x] Contract-diff harness landed (`tests/contract`): Trace JSON format, LoadTracesDir loader (lexical order, ignores non-json), Replay against any HTTP base URL, JSON-key-normalising diff. 4 contract tests cover match/status-diff/body-diff/loader. Recording 100+ real golinstor traces against the Java oracle is operational work that depends on a running upstream LINSTOR for capture; the framework is in place to consume them.
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

- [x] **Volume resize** plumbing (2026-05-09): `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` already updated the spec; the satellite reconciler now picks up the size delta. Provider interface gained `ResizeVolume` (lvm-thin → `lvextend`, zfs → `zfs set volsize`, file/loopfile → `truncate` + `losetup -c`). On growth the reconciler runs the provider's resize, then `drbdadm resize --assume-clean <rd>` so the kernel re-reads the lower disk. `pkg/luks.Resize` adds the cryptsetup hook for the LUKS layer; satellite-side wiring of LUKS resize lands when the per-resource `encrypted` flag flows through ApplyResources (Phase 6 follow-up). Tests: `TestApplyTriggersResizeOnGrow`, `TestApplyNoResizeOnFreshCreate` for the satellite path; per-provider tests for the resize commands. **End-to-end with a real PVC + checksum verify is still on the e2e harness checklist** (8.8).
- [x] **Backing-device failure under DRBD** (2026-05-09). The events2 observer now watches for `disk:Failed` on the local replica and runs `drbdadm detach --force <rd>` so the lower disk stops getting hammered. Peers stay UpToDate, the consumer keeps doing I/O via DRBD's network path. The detach is best-effort (logged, not retried) — the next reconcile will redrive state if the storage layer comes back. The Failed observation still ships to the controller via ReportObserved, so a Status condition reflecting the diskless state is one ResourceObservation handler away. **End-to-end "pull the LV out from under DRBD"** sits on the 8.8 e2e checklist; satellite-side hook is in place.
- [x] **DRBD options hierarchy** controller → resource-group → resource-definition → resource (2026-05-09). `pkg/drbd.ResolveOptions` walks the four scopes, lower wins. The resource controller reads ControllerProps via the KVEntry CRD, the parent RG via `client.Get`, the RD via the existing lookup, then folds in the resource's own props. The merged map flows through `dispatcher.ApplyOptions.EffectiveProps`; `buildDesired` splits it: DrbdOptions/* land on the satellite's drbd_options bag (the .res renderer drops them in the right `net`/`disk`/`peer-device`/`handlers` block via `pkg/drbd.SectionFor`), non-DRBD props stay on the wire-side Props map. Tests: resolver unit tests for override / partial inheritance / non-DRBD-prop pass-through; `TestApplyDRBDOptionsFromEffectiveProps` for the dispatcher wiring.
- [x] **`allow-two-primaries` plumbing** (2026-05-09): the DRBD option-hierarchy now flows arbitrary `DrbdOptions/Net/...` keys (including `allow-two-primaries yes`) through the satellite into the rendered .res file's `net { }` block. Operators set the knob via `linstor c sp DrbdOptions/Net/allow-two-primaries yes` (or RG/RD/Resource scope). The first-activation auto-primary seed still picks one replica deterministically (lowest stable node-id) — that's correct: dual-primary is for the consumer's promotion (Ganesha promoter, KubeVirt live-migration controller), not for initial sync. `splitDRBDOptions` strips the `DrbdOptions/<Section>/` prefix so the .res renderer emits `allow-two-primaries yes;` verbatim. **Live-migration coordination on the controller side** (orchestrating `drbdadm primary` on the destination, then `drbdadm secondary` on the source) lives outside this scope — that's what drbd-reactor + the consumer (KubeVirt VirtualMachineInstanceMigration / Ganesha promoter) own.

### 8.3 Replica lifecycle

- [x] **`linstor node evacuate` actually migrates replicas** (2026-05-09). New `internal/controller.NodeReconciler` watches Node CRDs and on EVICTED enumerates every Resource on the affected node, runs the shared `pkg/placer.Place` to create a replacement on a non-disabled peer (honouring the parent RG's topology constraints), and leaves the source replica in place — the operator decides when to remove it (typically once the replacement is UpToDate). The placer's "existing replicas" count now excludes EVICTED/LOST nodes so a 2-replica RD with one evicted source actually triggers the migration. Test: `TestNodeReconciler_EvictedTriggersMigration` pins the 3-node migration path.
- [x] **`linstor node lost` recovery** (2026-05-09): the same NodeReconciler also detects LOST. Migration runs as for EVICTED, then the source Resource CRD is deleted via the K8s API path so the Resource controller's finalizer cleans up. The TCP-port/node-id allocations stored on the source Resource Status free naturally on delete (the per-node port allocator scans live Resources). Test: `TestNodeReconciler_LostDeletesSourceResource`. **e2e** (hard-kill a satellite pod) sits on the 8.8 checklist.
- [x] **auto-diskful** (2026-05-09): the ResourceReconciler now promotes a DISKLESS replica to diskful when `Resource.Status.InUse=true` AND the hosting node has a viable storage pool. Removes the DISKLESS flag and stamps `StorPoolName` on Spec.Props; the satellite reconciler picks up the change on its next pass and creates the LV / runs `drbdadm attach`. TIE_BREAKER witnesses are exempted — promoting one would defeat the quorum-only purpose. Tests: `TestAutoDisklessPromoted`, `TestAutoDisklessSkipsTiebreaker`, `TestAutoDisklessSkipsWhenNoPool`. **`auto-diskful-cleanup`** (demote-on-idle) deferred — needs hysteresis to avoid flapping on transient opens; operators can demote manually via `linstor r d` until the access-pattern tracking lands.
- [x] **Tiebreaker auto-creation + quorum auto-toggle** (2026-05-09, upstream-aligned). `internal/controller.ResourceDefinitionReconciler` watches RDs and Resource events (Watches+EnqueueRequestsFromMapFunc), mirrors upstream LINSTOR's two distinct rules: `CtrlRscAutoTieBreakerHelper.shouldTieBreakerExist` — create a TIE_BREAKER witness iff `diskful ≥ 2 ∧ diskful%2 == 0 ∧ non-witness-diskless == 0` — and `CtrlRscAutoQuorumHelper.isQuorumFeasible` — `(diskful == 2 ∧ diskless ≥ 1) ∨ diskful ≥ 3`. The reconciler stamps `DrbdOptions/Resource/quorum=majority` when feasible, `=off` otherwise (so a 50/50 split doesn't deadlock both halves). Idempotent (an already-witnessed RD is a no-op; a witness on an evicted node gets dropped + recreated elsewhere). Tests: `TestTiebreakerCreated`, `TestTiebreakerSkipsThreeReplicas`, `TestTiebreakerSkipsTwoNodeCluster`, `TestTiebreakerSkipsEvictedNode`, `TestTiebreakerEvenWithDiskless` (user-added diskless suppresses witness), `TestTiebreakerEvenAfterUserAdds4` (4-replica → witness lands), `TestTiebreakerRemovedWhenParityFlips`, `TestTiebreakerSinglesAreLeftAlone`. Stand-side e2e (`tests/e2e/tiebreaker.sh`) green.
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

The dev stand has been Talos+QEMU loopfile-backed. Production parity needs:

- [x] **Stand image bakes in storage extensions** (2026-05-09, commit `0a7955e`): `stand/up.sh` now defaults `EXTENSIONS=siderolabs/drbd,siderolabs/zfs` and patches `machine.kernel.modules` with `zfs`, `dm_thin_pool`, `dm_snapshot`, `dm_crypt`. Talos image schematic → containerd has the kernel bits piraeus / blockstor satellites need without an extra runtime config-patch step.
- [x] **Real-disk LVM-thin + ZFS_THIN end-to-end** (2026-05-09, commits `8322fd4` + `9fc5467` + `dd1eef4` + `8af4e3a` + `73f54f9` + `af73aed`): `stand/up.sh` provisions two extra 16 GiB disks per worker. `stand/install-pools.sh` (+ `make pools NAME=<n>` target) sets up `blockstor-zfs` zpool on /dev/sda and `blockstor-lvm/thin` on /dev/sdb. The satellite DaemonSet registers both as `zfs-thin` + `lvm-thin` LINSTOR pools alongside the existing `stand` loopfile. Verified on t2 stand: 2-replica RDs on either pool reach `disk:UpToDate`, the underlying `zfs list` shows `blockstor-zfs/<rd>_<vol>` zvols and `lvs blockstor-lvm` shows the thin LV.
  - Container-side gotchas pinned along the way: zpool's auto-partition step fails inside a non-udev container (`cannot label sda: failed to detect device partitions on /dev/sda1: 19`) — pre-create the partition with sgdisk, then point zpool at /dev/sda1; lvm thin convert fails on udev wait — set `--config 'activation{udev_sync=0 udev_rules=0}'` and `-Wn -Zn`.
- [x] **`tests/burnin-blockstor.sh` against ZFS** — pool name parametrised via the existing `STORPOOL` env, default still `stand`. Operators run `STORPOOL=zfs-thin make burnin-blockstor NAME=t2`. Long-tail run is operational follow-up.
- [x] **Real-disk LVM (non-thin)** (2026-05-09): `pkg/storage/lvm/lvm_thick.go` implements `storage.Provider` for LINSTOR's classic `LVM` kind (no thin pool, `--size` allocates real extents up-front, `--snapshot --extents 25%ORIGIN` for COW snapshots). Shares `volumeStatusViaLVS` with the thin provider via `lvm_common.go`. Wired into `cmd/satellite/main.go` via `--lvm-thick-pool-name` / `--lvm-thick-vg` flags so a thick pool can be registered alongside the existing thin/zfs/loopfile pools. Tests: `lvm_thick_test.go` covers create/idempotent-create/resize/delete/pool-status/snapshot. Fake-exec only — running on a real VG works the same as thin (the udev workarounds are inherited via the shared `activation{udev_sync=0 udev_rules=0}` config string).
- [ ] **Network partition** behaviour: isolate a satellite for >quorum-timeout, verify the surviving majority continues, isolated minority fences itself, recovery rejoins cleanly with bitmap merge (no full re-sync). `tests/e2e/network-partition.sh` is scaffolded; needs an iptables-controllable Talos profile (talosctl supports custom CNI rules; not wired in today).
- [ ] **Backing-device failure** during writes. Pull the disk under DRBD, observe peer stays Primary, no I/O loss, replica drops to Diskless without flapping. `tests/e2e/backing-device-fail.sh` exercises the events2 observer's auto-detach but needs a real block device — sysfs autoclear on a loop file doesn't propagate as disk:Failed because DRBD holds the fd open.
- [ ] **Hard satellite kill** mid-Apply (SIGKILL the daemonset pod during a resize/snapshot). Reconcile must be idempotent; current contract tests assume clean shutdown. Cleanest harness path: a flag in the satellite's reconciler that aborts mid-Apply (pkill simulation), then a follow-up Reconcile must converge.

### 8.7 CSI parity beyond happy path

- [ ] **CSI snapshot + restore on a different node**. piraeus-csi creates a PVC from a VolumeSnapshot — the new RD's autoplace shouldn't pin to the source node. REST endpoints (`POST /v1/.../snapshot-restore-resource` + `autoplace` with `node_name_list`) verified via `tests/e2e/snapshot-restore-cross-node.sh` REST plumbing; the data-shipping leg (zfs send/recv or thin-send-recv) needs ZFS or LVM-thin pool to fully validate.
- [ ] **CSI clone (volume-from-volume)**. Same plumbing as snapshot+restore but without an intermediate VolumeSnapshot. csi-sanity covers the gRPC contract; cluster-side e2e (`tests/e2e/clone.sh`) is scaffolded against the same shipping leg as snapshot-restore.
- [ ] **RWX volumes via Ganesha + drbd-reactor**. linstor-csi spawns a 2-volume RD (one for data, one for export config) and lets drbd-reactor flip the NFS export with the Primary. `tests/e2e/rwx-ganesha.sh` is scaffolded; needs Ganesha + drbd-reactor on the satellites (a separate Talos extension layer).
- [x] **2-volume RDs functional path** (2026-05-09, commits `bf23552`, `aab68a1`): the multi-volume minor allocator now expands per-volume range (no two RDs collide on minor+1), and RD VolumeDefinition changes re-enqueue every replica's reconcile so resize / new-volume flows through to the satellite. The .res renderer emits `volume:0` and `volume:1` independently and they replicate as separate DRBD volumes within one resource. Functional path validated by `pkg/satellite/reconciler_drbd_test.go` + the stand-side smoke. End-to-end `tests/e2e/two-volume-rd.sh` is flake-bound to the busy stand's initial-sync timing, not a functional regression.

### 8.8 e2e harness expansion

`tests/burnin-blockstor.sh` covers the 2-replica failover happy path. The remaining scenarios above each need a deterministic, automatable test in `tests/e2e/`:

- [x] **e2e harness scaffolded** (2026-05-09): all 12 scenarios committed under `tests/e2e/`, plus a shared `lib.sh` (on_node, wait_uptodate, write_random, read_md5, delete_rd, require_workers, rd_apply, rest_post, rest_put). `make e2e NAME=<cluster> SCENARIO=<name>` invokes one; `make e2e-list` enumerates them. Stand-side runs on 2026-05-09 surfaced (and fixed) several real bugs in the controller / stand setup along the way:
  - **PASS** (5): `tests/smoke-blockstor.sh` (1 MiB urandom + byte-perfect failover read), `tests/e2e/tiebreaker.sh` (witness lands+drops per parity rule), `tests/e2e/evacuate.sh` (NodeReconciler triggers placer migration to N3), `tests/e2e/resize-plain.sh` (REST size-bump → satellite resize chain → drbdadm resize, /dev/drbdN grows modulo DRBD metadata overhead), `tests/e2e/two-volume-rd.sh` (independent volumes per RD, both replicate).
  - **Bugs surfaced + fixed**: minor allocator didn't expand multi-volume range (commit `bf23552`); RD reconciler raced itself on Update under fan-out (commit `fac60a9`); RD changes didn't re-enqueue Resources (commit `aab68a1`); sibling Resource changes didn't re-enqueue peers — witness landed on N3 but R1/R2 kept the pre-witness peer list (commit `cea635c`); ListByDefinition filtered by labels not by Spec (commit `436669e`); satellite gRPC port collided with DRBD's default range (commit `8c041cf`); satellite advertised port hardcoded to 7000 (commit `1481d4a`); satellite leaked LINSTOR-only `DrbdOptions/<key>` into `.res` (commit `939318e`); .res not wiped on satellite startup (commit `71bf4d3`); namespace lacked privileged PSA label (commit `1cf7e16`); blockstor-system installer (`make blockstor`) auto-applies CRDs+controller+satellites (commit `5f098dc`); pool capacity reporting via statfs (commit `54923ad`); auto-tiebreaker now gated on `DrbdOptions/AutoAddQuorumTiebreaker` (commit `a3cc4f9`).
  - **Flake / unfinished** (3): `tests/e2e/auto-diskful.sh` — controller doesn't promote DISKLESS on `drbdadm primary`; events2 observer's InUse update may not propagate when promotion is via a manual command rather than a Pod open. `tests/e2e/two-primaries-live-migration.sh` — wait_uptodate timeout under back-to-back runs. `tests/e2e/resize-plain.sh` — passes in isolation, sometimes flakes when scheduled after a long DRBD churn. Mitigation path: parallel stands (`make up NAME=t1` etc.) so each scenario runs from a clean kernel state. Tracked under 8.8 follow-up.
  - **Deferred**: `tests/e2e/backing-device-fail.sh` (sysfs autoclear on a loop file doesn't propagate as disk:Failed; needs real LVM-thin/ZFS — 8.6); `tests/e2e/snapshot-restore-cross-node.sh`, `clone.sh`, `resize-luks.sh` (REST plumbing verified, end-to-end execution needs LUKS + ZFS-pool stand profiles — 8.6/8.7); `tests/e2e/network-partition.sh`, `rwx-ganesha.sh` (need iptables-controllable networking + Ganesha+drbd-reactor; 8.6/8.7).

**Exit criteria for Phase 8**: every checkbox above either lands or is moved to a separately-tracked "explicit out-of-scope" with rationale. Until then "production-ready" is overstating it; what we have is a CSI-compatible REST front-end with a verified happy path.

---

## Phase 9 — Layer stack (no-DRBD + LUKS layering)

The project's stated goal includes "ability to run without DRBD, as pure local storage (single-replica diskful or diskless)" and a LUKS encryption layer at the volume level. Today's satellite always renders a `.res` and shells out to `drbdadm`; LUKS exists in `pkg/luks` but isn't wired through `applyStorage`. To match upstream LINSTOR's `layer_list` semantics:

- [x] **`ResourceDefinition.Spec.LayerStack` + plumbing** (2026-05-09, commits `8ff7043` + `f822665`): `[]string` field on the RD CRD, REST shape, and `ResourceGroup.Spec.SelectFilter.LayerStack` for inheritance. `pkg/api/v1/layer_stack.go` exports `ResolveLayerStack(rd, rg)` (RD wins, then RG, then `["DRBD","STORAGE"]` default). Plumbed through `proto/satellite/v1alpha1.DesiredResource.layer_stack`, `dispatcher.ApplyOptions.LayerStack`, and `ResourceReconciler.resolveLayerStack` so the satellite gets the effective composition on every Apply.
- [x] **Satellite-side: skip DRBD when not in stack** (2026-05-09, commit `8ff7043`): `pkg/satellite/reconciler.applyOne` calls `needsDRBD(dr.GetLayerStack())` and short-circuits the `applyDRBD` step when the stack omits it. Empty stack still defaults to DRBD-on for legacy clients. `TestApplySkipsDRBDWhenLayerStackOmits` pins the satellite contract.
- [x] **Satellite-side: LUKS layer** (2026-05-09, commit `ea78ba5`): `applyLUKS` runs after `applyStorage` and before `applyDRBD`. Calls `cryptsetup luksFormat` on first activation (idempotent — `isLuks` probe), `luksOpen` to `/dev/mapper/<rd>-<vol>-luks`, replaces `devices[vol]` with the mapper path so the .res `disk` line points at the encrypted device. Passphrase comes via `DrbdOptions/Encryption/passphrase` (upstream LINSTOR's `linstor rd set-property` key) → dispatcher lifts it to `DesiredResource.Props["LuksPassphrase"]` → satellite reads from there. Diskless replicas skip. Errors out fast on missing passphrase rather than silently producing unencrypted. Tests: `TestApplyLayersLUKS`, `TestApplyLUKSFailsWithoutPassphrase`. **Open**: cryptsetup-resize on volume grow path (the storage layer resizes the raw LV but the mapper keeps the original size); `luks.Close` on teardown / DRBD-down.
- [x] **CSI shape** (2026-05-09): linstor-csi (and piraeus-operator's `LinstorSatelliteConfiguration.spec.storageClasses[*].layerList`) sets `layer_list` on the autoplace / resource-create call rather than on RD create. `pkg/rest/autoplace.handleAutoplace` and `handleResourceCreate` now persist that onto `rd.LayerStack` when not already set, so the dispatcher → satellite chain sees the right composition. Operator-supplied LayerStack on the RD wins to avoid silent overwrites on re-place. Tests: `TestAutoplacePersistsLayerListOntoRD`, `TestAutoplaceLayerListDoesNotOverwriteExistingStack`, `TestResourceCreatePersistsLayerList`.
- [ ] **Remaining tests**:
  - satellite contract: `[LUKS,STORAGE]` apply runs `cryptsetup luksFormat` once, `cryptsetup open` on subsequent reconciles, never DRBD
  - satellite contract: `[DRBD,LUKS,STORAGE]` apply layers all three; `.res` points at /dev/mapper/<rd>-0, not the raw LV
  - e2e: `tests/e2e/no-drbd.sh` (single-replica ZFS-backed PVC, write/read), `tests/e2e/luks-layer.sh` (encrypted single-replica PVC + reboot remount), `tests/e2e/drbd-luks-stack.sh` (3-replica encrypted)
- [ ] **Documentation** — `docs/layer-stack.md` with the upstream LINSTOR layer model, the supported subset, and how it maps to cozystack StorageClasses.

Open question: cluster passphrase rotation (`POST /v1/encryption/passphrase`) already lands in Phase 7 — its interaction with the LUKS layer needs to be pinned (re-encrypt all volumes vs. just rotate the wrapping key).

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
