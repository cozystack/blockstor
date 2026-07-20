# Migrating a LINSTOR cluster to blockstor

`linstor-migrate` converts a LINSTOR k8s-backend database dump into blockstor CRDs. blockstor then **adopts** the existing on-disk data (zvols/LVs + DRBD metadata) in place — no data is copied or resynced. This runbook is the required procedure; the converter is only one step of it.

> **Scope.** This adopts a LINSTOR deployment whose satellites already run on the nodes blockstor will manage, keeping the same storage pools, DRBD minors, node-ids and (with the port capture below) TCP ports. It does not move data between nodes and does not change replica placement.

## What the converter does and does not migrate

Migrated: nodes, storage pools, resource groups, resource definitions (+ volume definitions), resources (replicas), and SUCCESSFUL snapshots. Controller-wide `DrbdOptions/*` become a `ControllerConfig`. DRBD minors, per-replica node-ids, the shared-secret and (when captured) TCP ports are preserved verbatim. Every RD is stamped `Initialized=true` and every replica `skipInitialSync=false`, so any replica added **after** migration performs a real sync instead of falsely coming up UpToDate. Snapshots are stamped `blockstor.io/adopted` so the controller records them as complete without re-taking them.

**NOT migrated — read the report on stderr for the exact list per cluster:**

- **LUKS passphrases.** LUKS-encrypted volumes convert with their layer stack intact, but the per-volume passphrase (encrypted with the LINSTOR master key) is not decrypted. You must provision `spec.encryption` for those RDs by hand before the encrypted volumes can be opened. Each affected RD is reported.
- **Backup-shipping remotes**, unknown/runtime flag bits, and DELETE'd / FAILED_DEPLOYMENT rows are skipped and reported. None affect live data availability.
- **DRBD TCP ports** are absent from most LINSTOR 1.33 dumps — see "Capture live DRBD ports" below.

## Pre-flight (do all of these before touching anything)

1. **Cluster is healthy.** `linstor r l --faulty` is empty; every resource is `UpToDate` on its diskful replicas; no node is `OFFLINE`. Do not migrate a degraded cluster — an Inconsistent/SyncTarget replica adopted as-is stays that way.

2. **Take the database dump** (LINSTOR's k8s-backend CRDs):

   ```bash
   mkdir dump && cd dump
   kubectl get crds | grep -o ".*.internal.linstor.linbit.com" \
     | xargs -I{} sh -c 'kubectl get {} -ojson > {}.json'
   cd ..
   ```

3. **Capture the live DRBD ports.** LINSTOR 1.33 does not persist the port in its database, so the dump alone cannot preserve it. If blockstor adopts with a different port, `drbdadm adjust` moves the connection endpoint under live I/O (a reconnect blip, and on a marginal quorum a transient quorum loss). Capture the running ports from every satellite and union them:

   ```bash
   for pod in $(kubectl -n cozy-linstor get pods -l app.kubernetes.io/component=linstor-satellite \
                  -o jsonpath='{.items[*].metadata.name}'); do
     kubectl -n cozy-linstor exec "$pod" -- sh -c '
       for f in /var/lib/linstor.d/*.res; do
         rd=$(basename "$f" .res)
         port=$(grep -oE "address[^;]*:[0-9]+" "$f" | grep -oE "[0-9]+$" | head -1)
         [ -n "$port" ] && echo "$rd $port"
       done'
   done | sort -u > drbd-ports.txt
   ```

   Every replica of an RD shares one port, so duplicates across nodes are expected; `sort -u` collapses them. If an RD appears with two different ports, stop and investigate before proceeding.

4. **Back up the ZFS/LVM pools' snapshots** (optional but recommended): a `zfs snapshot -r` of each pool gives an instant rollback point that is independent of both control planes.

5. **Verify the zvol names blockstor will adopt match what is on disk.** Adoption is name-based: blockstor addresses each volume as `<zpool>/<rd-name-lowercased>_<volume-number-%05d>` (e.g. `data/pvc-abc…_00000`), and `CreateVolume` is idempotent only when that dataset already exists. A byte mismatch (ZFS names are case-sensitive) makes blockstor create a fresh EMPTY zvol next to the real one. Multi-replica DRBD would self-heal by SyncTarget, but **single-replica `["STORAGE"]` volumes have no resync fallback** — a mismatch there silently presents an empty disk. Cross-check before cutover, read-only, on each satellite:

   ```bash
   # what is on disk:
   zfs list -H -o name -t volume | sort > /tmp/ondisk-zvols.txt
   # what blockstor will look for (from the converted manifests):
   #   <zpool>/<metadata.name of each Resource>_<vol %05d>
   # compare the two sets; every single-replica STORAGE-only volume MUST
   # already exist on disk under the blockstor name.
   ```

   If any single-replica volume's computed name is absent from `ondisk-zvols.txt`, STOP — do not cut over until the naming is reconciled (`zfs rename`, or fix the converter).

   **Adopted snapshots are addressed the same way** — `<zpool>/<rd-name-lowercased>_00000@<snapshot-name>` — so run the same cross-check for them, otherwise a name miss only surfaces when someone tries to restore, long after cutover:

   ```bash
   zfs list -H -o name -t snapshot | sort > /tmp/ondisk-snapshots.txt
   # every migrated Snapshot's <zpool>/<rd>_00000@<snap> must appear here
   ```

6. **Rehearse on staging first.** Do a full dry-run adoption of a COPY of the pools (or a representative subset) on a non-production cluster: apply the manifests, confirm every StoragePool registers a provider (no `provider requires … in props` errors in the satellite log), every Resource reaches `UpToDate` with `out-of-sync=0` (adopted, not resynced), and no new empty zvols appear. Only after a clean staging rehearsal proceed to production.

## Convert

```bash
linstor-migrate -in ./dump -drbd-ports ./drbd-ports.txt -out ./blockstor-manifests.yaml
```

Read the report on stderr end to end. Every `warning:` line is a thing the converter refused to guess — decide per line whether it matters for your data before continuing. In particular resolve every `LUKS passphrase NOT migrated` and every `no DRBD port` line (the latter should be empty if the port capture was complete).

## Cutover

The manifests do not migrate a cluster by themselves — the control plane must be swapped. Perform this in a maintenance window.

1. **Freeze provisioning.** Scale the CSI provisioner/attacher to zero (or cordon new PVC creation) so no new volumes are created against the old controller mid-cutover.

2. **Stop the LINSTOR controller.** Scale `deploy/linstor-controller` to 0. The satellites and the running DRBD devices keep serving I/O — only the control plane goes away. Do NOT stop the satellites or unload DRBD.

3. **Deploy blockstor** (apiserver + controller + satellites) pointed at the same nodes and pools. The satellites must land on the same nodes with the same pool backing devices. Do not let blockstor create-md or mkfs — the adoption guards (existing DRBD metadata is not re-created, a populated filesystem is not re-formatted) protect the data, but verify the satellite image is the intended version first.

4. **Apply the manifests in order.** `WriteManifests` already emits them in dependency order, so a single apply works:

   ```bash
   kubectl apply -f ./blockstor-manifests.yaml
   ```

   If you prefer staged application, the order is: `ControllerConfig` → `Node` → `StoragePool` → `ResourceGroup` → `ResourceDefinition` → `Resource` → `Snapshot`.

5. **Wait for adoption to converge.** Every `Resource` should report its DRBD device `UpToDate` (adopted, not resynced — watch that `out-of-sync` stays 0, i.e. no unexpected full sync started). `linstor r l --faulty` against the blockstor apiserver is empty. Storage pools report their real free capacity.

6. **Repoint and unfreeze CSI.** Point linstor-csi at the blockstor apiserver, then scale the provisioner/attacher back up. Provision one test PVC and confirm it binds and that an existing PVC still mounts read-write.

## Verify (before declaring done)

- Object counts match the source: `linstor n l`, `sp l`, `rg l`, `rd l`, `r l` against blockstor equal the pre-cutover counts (the converter's report prints them).
- Spot-check ≥5 RDs: size, per-volume `/dev/drbd<minor>`, per-replica node-id and port match the pre-migration values. A changed minor or node-id is a stop-the-line defect.
- Every diskful replica is `UpToDate`; no replica is unexpectedly `SyncTarget`.
- Adopted snapshots list as present without any suspend-io having fired on the parent RDs (check the satellites did not log `suspend-io`).
- For LUKS RDs: their volumes stay closed until you provision `spec.encryption`; confirm consumers of those volumes are quiesced until then.

## Rollback

Until CSI is repointed and confirmed, rollback is: scale blockstor to 0, scale `deploy/linstor-controller` back to 1, repoint CSI at LINSTOR. The on-disk data and running DRBD devices were never touched, so this is a control-plane-only revert. If a pool-level `zfs snapshot` was taken in pre-flight, it is the last-resort data rollback point.

## Known limitations (accept or mitigate explicitly)

| Item | Impact | Mitigation |
| --- | --- | --- |
| LUKS passphrases not migrated | Encrypted volumes cannot be opened until `spec.encryption` is provisioned | Provision by hand; keep those consumers quiesced during migration |
| DRBD port not in dump (LINSTOR 1.33) | Adoption reconnects the mesh on a new port if not captured | Capture with the pre-flight command and pass `-drbd-ports` |
| Unknown flag bits dropped | Cosmetic runtime bookkeeping only | Reported; verified not to carry availability semantics |
| Placement/data unchanged | The converter never moves data | Rebalance with blockstor after migration if desired |
| Single-replica zvol name must byte-match on disk | A mismatch presents an empty disk (no DRBD resync fallback) | Pre-flight step 5 cross-check + staging rehearsal (step 6) |
| Snapshots cover volume 0 only | blockstor addresses a snapshot as `<zpool>/<rd>_00000@<snap>`; a snapshot that captured a multi-volume resource adopts with only its first volume restorable | Reported per snapshot by the converter; re-take those snapshots after migration if the other volumes matter |
| Undecoded flag bits are reported, not interpreted | A `non-zero vlm_flags` / `node_flags` / `unhandled flags bits` line means the source cluster had a marker this tool refuses to guess at (e.g. a size-semantics or eviction bit) | Look the object up in the source cluster before cutover (`linstor v l` / `n l`) and confirm it should be adopted as-is; the volume's `SizeKib` is carried verbatim either way |
| Node type mapping is empirically calibrated | Only `node_type=2` (SATELLITE) was confirmed against a live cluster; a CONTROLLER/AUXILIARY ordering error would adopt a controller node as AUXILIARY instead of skipping it | Controller nodes carry no pools or replicas, so impact is bounded — but check the report for unexpected node kinds |
