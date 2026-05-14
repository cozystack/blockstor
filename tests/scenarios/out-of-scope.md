# Out-of-scope scenarios

These upstream LINSTOR features are deliberately not supported by
blockstor or are deferred to a later phase. Each category links back
to the per-scenario `day2-*.md` files harvested from UG9 so the
upstream wording remains greppable.

This file is the source of truth for "we will not implement X" —
when a customer asks for one of these, the conversation is "here's
why, here's the workaround," not "we'll add it in a sprint."

---

## Backup (S3 + LINSTOR-to-LINSTOR shipping)

- **What:** `linstor backup create|delete|restore|ship` against S3 / WAN remote LINSTOR clusters. Includes the wide `linstor backup *` CLI surface and the `backup ship` execution path.
- **Why deferred:** Cozystack handles cross-cluster DR at the platform level (Velero / application-layer tooling). Embedding a second DR path inside blockstor duplicates the responsibility and inherits the LINSTOR S3 / passphrase orchestration complexity for no operator benefit.
- **Sources:** day2-backup-create-s3.md, day2-backup-delete.md, day2-backup-restore-s3.md, day2-backup-restore-s3-kubernetes.md, day2-backup-ship-linstor-to-linstor.md, day2-backup-ship-linstor-to-linstor-wan.md

## Snapshot shipping

- **What:** Pushing a local snapshot to another node/cluster as a separate replication primitive (`snapshot ship` / `backup ship --node ...`).
- **Why deferred:** Same rationale as backup. Local snapshot CRUD (create/delete/rollback/restore) IS in scope — see wave2-08. Only the *shipping* / *remote target* half is excluded.
- **Sources:** day2-snapshot-ship-self.md

## Remotes (S3 + LINSTOR-to-LINSTOR)

- **What:** The `linstor remote` object — S3 buckets and peer LINSTOR clusters used as ship targets.
- **Why deferred:** Without backup / snapshot shipping in scope, remotes have no consumer. `pkg/rest/remotes.go` stubs return deterministic 501-style responses so operator gets a clear error rather than silent success.
- **Sources:** day2-remote-create-linstor.md, day2-remote-create-s3.md, day2-remote-delete.md, day2-remote-modify.md

## NVMe-oF / NVMe-TCP layer

- **What:** `--layer-list nvme,storage` for diskless-NVMe-initiator + diskful-NVMe-target replacing DRBD's diskless attach.
- **Why deferred:** Cozystack runs HCI on flat L2 networks where DRBD-9's native protocol covers the use case. NVMe-oF adds a target/initiator split that duplicates DRBD's diskless attach without the replication. See wave1 6.11 for the rejection-pin contract.
- **Sources:** day2-nvme-target-and-initiator.md

## CACHE / WRITECACHE layers

- **What:** dm-cache (separate data + cache + metadata) and dm-writecache (cache + data) layers between DRBD and STORAGE for tiered storage.
- **Why deferred:** Cozystack uses homogeneous pools (one tier per cluster, NVMe in current generations); the caching layer adds complexity without a use case. Operators needing tiering can layer it below LINSTOR (e.g., bcache under the LVM PV). See wave1 6.11 for the rejection-pin contract.
- **Sources:** day2-cache-layer.md, day2-writecache-layer.md

## LDAP / external authentication

- **What:** `[ldap]` block in `linstor.toml` restricting controller access by LDAP-authenticated user / group membership.
- **Why deferred:** Cozystack handles authentication at the platform layer (OIDC via the K8s API server). blockstor's REST surface is in-cluster only; auth is K8s RBAC on the apiserver Service.
- **Sources:** day2-ldap-authentication.md

## TLS certificate management (mTLS satellite-controller, HTTPS REST API)

- **What:** Java keystore + truststore management for controller-satellite mTLS on port 3367 and HTTPS REST API on port 3371.
- **Why deferred:** Cozystack handles cluster TLS at platform level (cert-manager / K8s Service mesh). blockstor's apiserver doesn't manage certs; satellites talk to the kube-apiserver via `ctrl.GetConfig()` per Phase 10.6 (see wave1 3.10).
- **Sources:** day2-tls-controller-satellite.md, day2-tls-rest-api.md

## QoS (sysfs blkio throttle)

- **What:** Per-volume `sys/fs/blkio_throttle_{read,write}_{bps,iops}` props that satellite writes to cgroup v1 sysfs.
- **Why deferred:** kernel-level cgroup-v1 sysfs is unsupported under cgroup v2 (most current cozystack stands). Cozystack uses container-level limits via kubelet Pod resources; block-device-level throttle is a separate concern. See wave1 7.22 for the keep-keys-accessible-but-no-op stance.
- **Sources:** day2-qos-set-throttle.md

## DB backup / migration (`linstor-database` tool)

- **What:** Export/import LINSTOR's controller DB via the bundled `linstor-database` Java tool; migrate between H2/PostgreSQL/MariaDB/etcd backends.
- **Why deferred:** blockstor uses K8s CRDs (etcd via kube-apiserver) as the controller DB. There is no H2 / JDBC backend to back up; etcd backup is handled at the K8s cluster level (etcd-backup-restore / Velero).
- **Sources:** day2-controller-backup-db.md, day2-controller-migrate-db.md

## DRBD Proxy (long-distance replication buffer)

- **What:** `linstor drbd-proxy enable | options | compression` for the separately-licensed LINBIT proxy that buffers DRBD replication over WAN links, plus `DrbdProxy/AutoEnable` cross-site automation.
- **Why deferred:** DRBD Proxy is a paid LINBIT product and a WAN-replication primitive. Cozystack's DR story rides on Velero / application-layer tools (see Backup section above). No code in blockstor today (`grep DrbdProxy` returns zero hits).
- **Sources:** day2-drbd-proxy-enable.md, day2-drbd-proxy-auto-enable.md, day2-drbd-proxy-modify-compression.md

## Bare-metal Prometheus exporter

- **What:** Scraping LINSTOR's controller `/metrics` directly from a bare-metal Prometheus host plus optional `drbd-reactor` sidecar on each satellite.
- **Why deferred:** blockstor is K8s-only — `day2-monitoring-prometheus-kubernetes.md` (in scope, wave2-07 7.W08) covers the K8s monitoring path with ServiceMonitor + PodMonitor + PrometheusRule. The bare-metal path is documented in UG9 but doesn't apply.
- **Sources:** day2-monitoring-prometheus.md

## External controller (Operator points at bare-metal LINSTOR)

- **What:** `LinstorCluster.spec.externalController.url` pointing the piraeus Operator at a LINSTOR controller running outside the K8s cluster.
- **Why deferred:** Cozystack runs blockstor inside the cluster — replacing the bare-metal controller is the reason blockstor exists. Pin the operator wiring (CR accepts the field) but blockstor's apiserver is independent of this knob.
- **Sources:** day2-k8s-external-controller.md

## Shared LVM2 storage pools (SAN-style multi-attach)

- **What:** `linstor sp create lvm --shared-space <uuid> --external-locking` for shared VGs across multiple nodes via sanlock / lvmlockd.
- **Why deferred (today):** Cozystack runs HCI with node-local storage; shared LVM defeats the DRBD-replication premise. See wave2-06 6.W10 — REST handler currently returns 501 `unsupported in blockstor` text.
- **API extensibility requirement:** `pkg/api/v1/storage_pool.go` and the StoragePool CRD MUST keep the wire shape extensible so the feature can land later without breaking changes. Specifically:
  - Preserve `Props["StorDriver/SharedSpaceID"]` and `Props["StorDriver/ExternalLocking"]` keys round-trip through REST + store (no allow-list filtering that silently drops unknown StorDriver props).
  - Reserve `SharedSpaceID` field on `apiv1.StoragePool` (already present per Bug 59) — do NOT drop it; it survives an unsupported-flag rejection so a future enable path doesn't need a schema migration.
  - REST 501 path MUST preserve and echo the operator's submitted props in the error envelope so a future-enable wizard can resume from the same body.
  - When shared-LVM lands, the wire shape should change in zero places — only the handler's "unsupported" check goes away and the satellite provider gets the sanlock plumbing.
- **Sources:** day2-storage-pool-shared.md
