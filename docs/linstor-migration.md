# Migrating a LINSTOR cluster to blockstor

`linstor-migrate` converts a LINSTOR k8s-backend database dump into blockstor CRDs. blockstor then **adopts** the existing on-disk data (zvols/LVs + DRBD metadata) in place — no data is copied or resynced. This runbook is the required procedure; the converter is only one step of it.

> **Scope.** This adopts a LINSTOR deployment whose satellites already run on the nodes blockstor will manage, keeping the same storage pools, DRBD minors, node-ids and (with the port capture below) TCP ports. It does not move data between nodes and does not change replica placement.

## What the converter does and does not migrate

Migrated: nodes, storage pools, resource groups, resource definitions (+ volume definitions), resources (replicas), and SUCCESSFUL snapshots. Cluster-wide `DrbdOptions/*` become a `ControllerConfig`. LINSTOR keeps them in two bags and both are read: `/CTRL` holds the knobs its controller acts on (the auto tiebreaker, the verify-alg allow-list), and `/STLT` holds everything else `linstor controller set-property` is given, which is where the `.res` options such as `DrbdOptions/Net/ping-int`, `timeout` or `verify-alg` live, including the ones piraeus-operator writes from `LinstorCluster.spec.properties`. LINSTOR's satellites render `/STLT` below resource, definition, group and node, which is the rung `ControllerConfig` holds in blockstor, so the values keep their precedence. Where the two bags disagree on a key, the converter keeps the value LINSTOR acted on (`/STLT` for a key with a DRBD section, `/CTRL` for a section-less controller key) and reports the other. DRBD minors, per-replica node-ids and (when captured) TCP ports are preserved verbatim.

Peer authentication is carried as LINSTOR ran it. LINSTOR 1.33 writes `cram-hmac-alg sha1;` and the definition's `shared-secret` into a resource's net section only when `DrbdOptions/auto-verify-alg` resolves for it (through the replica, definition, group, node and `/STLT`) to something other than `windrbd`; otherwise its kernels run without authentication. blockstor writes `cram-hmac-alg` whenever it writes a secret (`sha1` unless `DrbdOptions/Net/cram-hmac-alg` names another digest), so the converter carries the secret exactly where LINSTOR rendered it. A definition LINSTOR did not authenticate arrives without its secret and is reported, so adoption leaves its connections as they are; set `DrbdOptions/Net/shared-secret` on it after cutover to turn authentication on. blockstor also renders `DrbdOptions/auto-verify-alg` as `verify-alg` when no explicit `DrbdOptions/Net/verify-alg` is set and `DrbdOptions/auto-verify-algo-disable` is unset or `false`, as LINSTOR does. An RD is stamped `Initialized=true` whenever its data still exists somewhere, and every replica gets `skipInitialSync=false`, so any replica added **after** migration performs a real sync instead of falsely coming up UpToDate. The latch is conditional, and it errs towards staying on: it is dropped only where no data-bearing replica is being adopted. That covers two shapes — every replica already DELETE-flagged, so nothing is migrating at all, and every replica diskless, so replicas are migrating but a client or quorum witness carries no copy to latch on. A definition whose replicas were held back because their nodes did not migrate keeps the latch, because that data is still on those nodes; so does one whose replicas were skipped for pool divergence. Unlatching in either of those would let the controller seed an empty first sync over data that exists. The report on stderr names every definition in these states, and names each reason that applies rather than only the first, so do not clear a latch by hand on the strength of a definition arriving without replicas. Snapshots are stamped `blockstor.org/adopted` so the controller records them as complete without re-taking them.

**NOT migrated — read the report on stderr for the exact list per cluster:**

- **LUKS volume passwords** are carried over when the converter is given the LINSTOR master passphrase (`-linstor-passphrase-file`): it decrypts each replica's and each snapshot's password the way LINSTOR does and re-wraps it under a blockstor master key, so the volumes open with the headers they have. LUKS-encrypted volumes convert with their layer stack intact (an RD whose `layer_stack` column is empty takes the stack its replicas were built with, read from `LAYER_RESOURCE_IDS`). Without the passphrase, a dump with LUKS volumes is refused. Pre-flight step 7 says how.
- **Definitions with external DRBD metadata** are skipped with their replicas and reported: a replica with a `.meta` layer child or a meta pool on its DRBD volumes, or a replica the dump has no layer chain for while `StorPoolNameDrbdMeta` names a pool in any scope LINSTOR reads it from (volume definition, volume group, replica, definition, resource group, node). blockstor keeps DRBD metadata on the data device and does not adopt a separate metadata volume, so such a replica would be brought up with internal metadata written over the end of its data. Changing the property does not move existing metadata, so copy the data to a volume with internal metadata in LINSTOR and convert that one. For a replica without a chain, check in LINSTOR where its metadata is first: if it is on the data device, remove the property and convert again.
- **`StorPoolNameDrbdMeta` over internal metadata** does not stop the definition: when every replica's chain has no `.meta` child, the metadata is on the data device (LINSTOR applies the property only to replicas created after it was set, and `.internal` asks for internal metadata). The definition converts, and the property is not carried over to the definition or its replicas, the two places blockstor reads it from, so the satellite does not look for a separate metadata volume. The report names the scope the property was set on.
- **Definitions whose replicas disagree on the layer stack** are skipped with their replicas and reported: replicas built two ways, or with their layers in another order, under a definition with no stack of its own; a diskful replica the dump shows no layer chain for, under such a definition; or a replica built otherwise than the stack the definition names. A `layer_stack` column the converter cannot read counts as no stack, and the warning names its value. Adopted as they are, such a replica would be brought up under a stack it was not built with, DRBD metadata over a LUKS volume's ciphertext for one. Settle the stack in LINSTOR and convert again.
- **Properties with no blockstor equivalent** are listed, keys only, one line per LINSTOR props instance (`LINSTOR props /CTRL: not migrated, ...`): controller runtime state left in `/CTRL` and `/STLT` (connector ports, cluster id, master-key material), and every instance blockstor has no object for, such as volume and volume-group props, node and resource connections, and the props LINSTOR freezes into a snapshot. Props of an object that was skipped are not listed again: the object's own line covers them. A property that would carry a passphrase in plaintext is named the same way and never written out.
- **DRBD options set on a node, a storage pool or a volume definition** are carried onto that object but never rendered: blockstor resolves DRBD options from the controller, resource group, definition and replica only, while LINSTOR also reads the node between group and `/STLT`. Each one is reported. Move it to one of the levels blockstor resolves before cutover, or `drbdadm adjust` takes it off the adopted resources.
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

3. **Capture the live DRBD ports.** LINSTOR 1.33 does not persist the port in its database, so the dump alone cannot preserve it, and it can give each node its own port for one definition. If blockstor adopts a replica with a port other than the one its node listens on, `drbdadm adjust` moves the connection endpoint under live I/O (a reconnect blip, and on a marginal quorum a transient quorum loss). Capture the port every replica listens on, from the kernel on every satellite:

   ```bash
   for pod in $(kubectl -n cozy-linstor get pods -l app.kubernetes.io/component=linstor-satellite \
                  -o jsonpath='{.items[*].metadata.name}'); do
     node=$(kubectl -n cozy-linstor get pod "$pod" -o jsonpath='{.spec.nodeName}')
     kubectl -n cozy-linstor exec "$pod" -- drbdsetup show | awk -v node="$node" '
       /^resource / { rd = $2; gsub(/"/, "", rd) }
       /_this_host ipv[46]/ && rd != "" { n = split($3, a, ":"); port = a[n]; sub(/;$/, "", port); print rd, node, port; rd = "" }'
   done | sort -u > drbd-ports.txt
   ```

   Each line is `<rd> <node> <port>`, so replicas of one definition on different ports keep their own. The node is the Kubernetes node the satellite runs on; if your LINSTOR node names differ from the Kubernetes ones, put the LINSTOR name there. The converter refuses a map that gives one replica two ports, and refuses manifests that would put two replicas on one port on the same node. It reports a replica it has no port for while it has its peers', and map entries for nodes that host no replica of the definition. The older `<rd> <port>` form still works for a cluster where every replica of a definition shares one port, and is refused when one definition shows two ports.

4. **Back up the ZFS/LVM pools' snapshots** (optional but recommended): a `zfs snapshot -r` of each pool gives an instant rollback point that is independent of both control planes.

5. **Verify the zvol names blockstor will adopt match what is on disk.** Adoption is name-based: blockstor addresses each volume as `<zpool>/<rd-cr-name>_<volume-number-%05d>` (e.g. `data/pvc-abc…_00000`), where `<rd-cr-name>` is the RD's `metadata.name` in the converted manifests: the LINSTOR name lowercased, or, for a name that is not a valid RFC 1123 label once lowercased, a hashed slug such as `35d21682-my-res` for `my_res`. Such a definition never matches the LINSTOR volume on disk, so rename it or the volume before cutover. `CreateVolume` is idempotent only when that dataset already exists. A byte mismatch (ZFS names are case-sensitive) makes blockstor create a fresh EMPTY zvol next to the real one. Multi-replica DRBD would self-heal by SyncTarget, but **single-replica `["STORAGE"]` volumes have no resync fallback** — a mismatch there silently presents an empty disk. Cross-check before cutover, read-only, on each satellite:

   ```bash
   # what is on disk:
   zfs list -H -o name -t volume | sort > /tmp/ondisk-zvols.txt
   # what blockstor will look for (from the converted manifests):
   #   <zpool>/<metadata.name of each Resource>_<vol %05d>
   # compare the two sets; every single-replica STORAGE-only volume MUST
   # already exist on disk under the blockstor name.
   ```

   If any single-replica volume's computed name is absent from `ondisk-zvols.txt`, STOP — do not cut over until the naming is reconciled (`zfs rename`, or fix the converter).

   **Adopted snapshots are addressed the same way** — `<zpool>/<rd-cr-name>_00000@<snapshot-name>` — so run the same cross-check for them, otherwise a name miss only surfaces when someone tries to restore, long after cutover:

   ```bash
   zfs list -H -o name -t snapshot | sort > /tmp/ondisk-snapshots.txt
   # every migrated Snapshot's <zpool>/<rd>_00000@<snap> must appear here
   ```

6. **Rehearse on staging first.** Do a full dry-run adoption of a COPY of the pools (or a representative subset) on a non-production cluster: apply the manifests, confirm every StoragePool registers a provider (no `provider requires … in props` errors in the satellite log), every Resource reaches `UpToDate` with `out-of-sync=0` (adopted, not resynced), and no new empty zvols appear. Only after a clean staging rehearsal proceed to production.

7. **Have the LINSTOR master passphrase at hand, and keep the passphrase Secret.** LINSTOR gives each LUKS replica a password of its own and keeps it in its database, encrypted under its master key; the master passphrase, the value of `MASTER_PASSPHRASE` in the Secret piraeus reads (`linstorPassphraseSecret`), opens that key. The converter needs the passphrase in a file, byte for byte, with no newline the Secret does not have:

   ```bash
   kubectl -n <piraeus namespace> get secret <passphrase secret> -o jsonpath='{.data.MASTER_PASSPHRASE}' | base64 -d > /run/linstor-pass
   ```

   Keep `/run/linstor-pass` on tmpfs and remove it after the conversion (`shred -u /run/linstor-pass`). Keep the Secret itself: with `-keyring-out` the new keyring's one slot is the LINSTOR passphrase, so the same Secret unlocks blockstor. It must be in blockstor's namespace (under Cozystack both backends share `cozy-linstor`; elsewhere copy it there), and `ControllerConfig.spec.passphraseSecretRef` must name it. Nothing to do on the nodes: no `luksAddKey`, no key read from `dmsetup`, whether or not the volumes were opened through the kernel keyring.

   Only a cluster whose master passphrase is lost cannot be converted this way. Its LUKS volumes have to be copied to new volumes created under blockstor, or stay on LINSTOR.

8. **Capture the live DRBD configuration** on every satellite, to compare against after adoption. blockstor applies the `.res` it renders with `drbdadm adjust` as soon as a resource is adopted, and does not compare it with the running kernel first, so this is the record that shows whether adoption changed anything:

   ```bash
   for pod in $(kubectl -n cozy-linstor get pods -l app.kubernetes.io/component=linstor-satellite                   -o jsonpath='{.items[*].metadata.name}'); do
     node=$(kubectl -n cozy-linstor get pod "$pod" -o jsonpath='{.spec.nodeName}')
     kubectl -n cozy-linstor exec "$pod" -- drbdsetup show > "drbd-before-$node.txt"
   done
   ```

   `drbdsetup show` prints what the kernel holds, in the formatting of the drbd-utils it comes from. If the blockstor satellite image ships another drbd-utils version, take both captures with the same binary, for example from a debug pod on the node, or the diff shows formatting rather than configuration.

## Convert

```bash
linstor-migrate -in ./dump -drbd-ports ./drbd-ports.txt -out ./blockstor-manifests.yaml \
  -linstor-passphrase-file /run/linstor-pass -keyring-out ./blockstor-keyring.yaml -keyring-namespace <blockstor namespace>
```

`-linstor-passphrase-file` and `-keyring-out` are needed only when the dump has LUKS volumes; a wrong passphrase stops the run before anything is written. A passphrase that is given is checked against LINSTOR's master key even when no volume is encrypted, since the keyring made from it is what blockstor will unlock with. If LINSTOR never had a master passphrase (no `encrypted/masterkey` in the dump), there is nothing to check it against, and the run is refused: drop the LUKS flags. `./blockstor-keyring.yaml` is the keyring Secret: it holds the master key wrapped under the LINSTOR passphrase and nothing in the clear. It is applied before the blockstor controller starts, not with the manifests: see cutover step 3. To convert into a blockstor cluster that already has a keyring, pass `-keyring-in <keyring.json> -blockstor-passphrase-file <file>` instead of `-keyring-out`, where `keyring.json` is the `keyring.json` key of its `blockstor-master-key` Secret.

Read the report on stderr end to end. Every `warning:` line is a thing the converter refused to guess — decide per line whether it matters for your data before continuing. In particular check that every LUKS definition reports its passwords migrated, and resolve every `no DRBD port` line, whether it names a definition, one replica, or a map whose node names match no replica. The latter should be empty if the port capture was complete: each replica it names gets a fresh port and reconnects on adoption, and `spec.drbdPort` cannot be changed after apply.

## Cutover

The manifests do not migrate a cluster by themselves — the control plane must be swapped. Perform this in a maintenance window.

1. **Freeze provisioning.** Scale the CSI provisioner/attacher to zero (or cordon new PVC creation) so no new volumes are created against the old controller mid-cutover.

2. **Stop the LINSTOR controller.** Scale `deploy/linstor-controller` to 0. The satellites and the running DRBD devices keep serving I/O — only the control plane goes away. Do NOT stop the satellites or unload DRBD.

3. **Apply the keyring Secret before the blockstor controller starts** (only with `-keyring-out`). A blockstor controller that finds a passphrase Secret and no keyring creates a keyring of its own from that Secret, with a new master key, on its first pass and then every 30 seconds. Under Cozystack the Secret `linstor-passphrase` already exists and `passphraseSecretRef` names it, so this happens as soon as the controller is up. The converted Resources carry passwords wrapped under the master key in `./blockstor-keyring.yaml`, not under that new one, and would never open. Apply the converted keyring first, into blockstor's namespace (it must exist; under Cozystack it is `cozy-linstor`):

   ```bash
   kubectl apply -f ./blockstor-keyring.yaml
   ```

   If the controller has already started and created its own keyring, replace that keyring with the converted one. This is safe only while no LUKS volume has been formatted under it, that is, while no Resource and no Snapshot carries a LUKS password yet; with provisioning frozen and the manifests not applied, that holds. Check, then replace:

   ```bash
   kubectl get resources.blockstor.org,snapshots.blockstor.org -o json \
     | jq '[.items[] | select(([.spec.volumes[]?.luksKey] | any) or ((.spec.luksKeys // []) | length > 0))] | length'
   # must print 0; otherwise stop and investigate before replacing anything
   kubectl replace -f ./blockstor-keyring.yaml
   ```

   Either way, confirm the keyring in place is the converted one before applying the manifests. `blockstor encryption list-passphrases` prints its `key ID`, which must equal the `luksKey.keyID` the converted manifests carry:

   ```bash
   blockstor encryption list-passphrases | grep '^key ID:'
   grep -m1 'keyID:' ./blockstor-manifests.yaml
   ```

   A different key ID means the controller's own keyring is in place: replace it as above before going on.

4. **Deploy blockstor** (apiserver + controller + satellites) pointed at the same nodes and pools. The satellites must land on the same nodes with the same pool backing devices. Do not let blockstor create-md or mkfs — the adoption guards (existing DRBD metadata is not re-created, a populated filesystem is not re-formatted) protect the data, but verify the satellite image is the intended version first.

5. **Apply the manifests in order.** `WriteManifests` already emits them in dependency order, so a single apply works:

   ```bash
   kubectl apply -f ./blockstor-manifests.yaml
   ```

   If you prefer staged application, the order is: `ControllerConfig` → `Node` → `StoragePool` → `ResourceGroup` → `ResourceDefinition` → `Resource` → `Snapshot`.

6. **Wait for adoption to converge.** Every `Resource` should report its DRBD device `UpToDate` (adopted, not resynced — watch that `out-of-sync` stays 0, i.e. no unexpected full sync started). `linstor r l --faulty` against the blockstor apiserver is empty. Storage pools report their real free capacity.

7. **Repoint and unfreeze CSI.** Point linstor-csi at the blockstor apiserver, then scale the provisioner/attacher back up. Provision one test PVC and confirm it binds and that an existing PVC still mounts read-write.

## Verify (before declaring done)

- Object counts match the source: `linstor n l`, `sp l`, `rg l`, `rd l`, `r l` against blockstor equal the pre-cutover counts (the converter's report prints them).
- Spot-check ≥5 RDs: size, per-volume `/dev/drbd<minor>`, per-replica node-id and port match the pre-migration values. A changed minor or node-id is a stop-the-line defect.
- `drbdsetup show` on every node, taken the same way as in pre-flight step 8, matches `drbd-before-<node>.txt` apart from what the report said would change (a fresh port for a replica the port capture missed). A net or disk option that went missing or changed is a stop-the-line defect: compare it with the report's `not migrated` and `does not resolve DRBD options at this level` lines.
- Every diskful replica is `UpToDate`; no replica is unexpectedly `SyncTarget`.
- Adopted snapshots list as present without any suspend-io having fired on the parent RDs (check the satellites did not log `suspend-io`).
- For LUKS RDs: `linstor encryption status` against blockstor says `unlocked`, and each LUKS replica opens; a `LUKSKeyMismatch` or `MasterKeyLocked` Event on a Resource is a stop-the-line defect.

## Rollback

Until CSI is repointed and confirmed, rollback is: scale blockstor to 0, scale `deploy/linstor-controller` back to 1, repoint CSI at LINSTOR. The on-disk data and running DRBD devices were never touched, so this is a control-plane-only revert. If a pool-level `zfs snapshot` was taken in pre-flight, it is the last-resort data rollback point.

## Known limitations (accept or mitigate explicitly)

| Item | Impact | Mitigation |
| --- | --- | --- |
| LINSTOR master passphrase lost | Its LUKS volume passwords cannot be decrypted, and the dump is refused | Copy those volumes to new ones under blockstor, or keep them on LINSTOR |
| DRBD port not in dump (LINSTOR 1.33) | Adoption reconnects the mesh on a new port if not captured | Capture with the pre-flight command and pass `-drbd-ports` |
| Unknown flag bits dropped | Cosmetic runtime bookkeeping only | Reported; verified not to carry availability semantics |
| Placement/data unchanged | The converter never moves data | Rebalance with blockstor after migration if desired |
| `Initialized` is conditional | Left unlatched only where no data-bearing replica is adopted — every replica DELETE-flagged, or every replica diskless; a definition held back by an unmigrated node or by pool divergence keeps the latch and arrives short of replicas | Read the report, which names every reason that applies. Do not clear a latch by hand because a definition has no replicas — that is what re-enables the auto-primary election this latch exists to suppress |
| Single-replica zvol name must byte-match on disk | A mismatch presents an empty disk (no DRBD resync fallback) | Pre-flight step 5 cross-check + staging rehearsal (step 6) |
| Snapshots cover volume 0 only | blockstor addresses a snapshot as `<zpool>/<rd>_00000@<snap>`; a snapshot that captured a multi-volume resource adopts with only its first volume restorable | Reported per snapshot by the converter; re-take those snapshots after migration if the other volumes matter |
| Undecoded flag bits are reported, not interpreted | A `non-zero vlm_flags` / `node_flags` / `unhandled flags bits` line means the source cluster had a marker this tool refuses to guess at (e.g. a size-semantics or eviction bit) | Look the object up in the source cluster before cutover (`linstor v l` / `n l`) and confirm it should be adopted as-is; the volume's `SizeKib` is carried verbatim either way |
| No pre-adjust comparison | The satellite applies the rendered `.res` with `drbdadm adjust` on adoption without first comparing it with the kernel's configuration, so an option the converter could not carry is taken off a live connection | Read the report's `not migrated` lines before cutover, capture `drbdsetup show` in pre-flight step 8 and compare after adoption; rehearse on staging |
| Node type mapping is empirically calibrated | Only `node_type=2` (SATELLITE) was confirmed against a live cluster; a CONTROLLER/AUXILIARY ordering error would adopt a controller node as AUXILIARY instead of skipping it | Controller nodes carry no pools or replicas, so impact is bounded — but check the report for unexpected node kinds |
