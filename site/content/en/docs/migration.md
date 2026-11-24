---
title: "Migrating from LINSTOR"
linkTitle: "Migrating from LINSTOR"
weight: 70
description: "Take over an existing LINSTOR cluster in place, without copying data."
---

Blockstor can take over a LINSTOR cluster that runs on Kubernetes. The `linstor-migrate` tool converts LINSTOR's database into blockstor resources, and blockstor then adopts the volumes and DRBD metadata that are already on disk. No data is copied or resynced, and replica placement does not change.

Do this in a maintenance window, after a full rehearsal on a staging cluster.

## What moves and what does not

Converted:

- nodes, storage pools, resource groups, resource definitions with their volumes, replicas, and successful snapshots;
- cluster-wide DRBD options, both the ones LINSTOR's controller acts on and the ones it keeps for its satellites, which is where `linstor controller set-property DrbdOptions/Net/...` and piraeus-operator's `LinstorCluster.spec.properties` put them;
- DRBD minor numbers and node IDs, unchanged;
- the shared secret, wherever LINSTOR used it to authenticate peers (see below);
- DRBD ports, if you capture them from the running nodes (step 3 below).

Not converted, and listed in the tool's report:

- Backup shipping remotes, and replicas or snapshots that LINSTOR marked as deleted or failed.
- Snapshots of definitions with more than one volume are adopted with their first volume only.
- Properties blockstor has no place for, one line per LINSTOR props container, keys only: controller runtime state such as connector ports and the cluster ID, volume and volume-group props, connection props, and the props LINSTOR freezes into a snapshot.
- DRBD options set on a node, a storage pool or a volume definition. They are kept on the object, but blockstor renders DRBD options only from the controller, resource group, definition and replica, so the adopted resources lose them. Move them to one of those levels first.

LINSTOR writes `cram-hmac-alg sha1` and the shared secret into a resource only when its `DrbdOptions/auto-verify-alg` is set to something other than `windrbd`; without it, the resource runs with no peer authentication. Blockstor writes `cram-hmac-alg` only when `DrbdOptions/Net/cram-hmac-alg` is set, so the tool carries the secret and sets `DrbdOptions/Net/cram-hmac-alg` to `sha1` only where LINSTOR wrote them, and the adopted resource authenticates exactly as before. A definition without them is reported. To turn authentication on after the migration, set `DrbdOptions/Net/shared-secret` and `DrbdOptions/Net/cram-hmac-alg` on it together, at a time when no node is rebooting: each node applies them on its own, and a connection that drops between two nodes while only one of them has applied them stays down until the other one has. Blockstor also renders `DrbdOptions/auto-verify-alg` as `verify-alg` when no explicit one is set, as LINSTOR does.

## Before you start

1. **Check that the cluster is healthy.** `linstor resource list --faulty` is empty, every replica with data is `UpToDate`, and no node is offline. A degraded replica is adopted as it is and stays degraded.

2. **Dump the LINSTOR database.** LINSTOR on Kubernetes keeps it in custom resources:

   ```sh
   mkdir dump && cd dump
   kubectl get crds | grep -o ".*.internal.linstor.linbit.com" \
     | xargs -I{} sh -c 'kubectl get {} -ojson > {}.json'
   cd ..
   ```

3. **Capture the DRBD ports.** LINSTOR 1.33 does not store them in its database, and it can give each node its own port for one volume. Without them, blockstor would assign new ports, and every replica would reconnect under load. Collect the port each replica listens on, from the kernel on every satellite:

   ```sh
   for pod in $(kubectl -n cozy-linstor get pods -l app.kubernetes.io/component=linstor-satellite \
                  -o jsonpath='{.items[*].metadata.name}'); do
     node=$(kubectl -n cozy-linstor get pod "$pod" -o jsonpath='{.spec.nodeName}')
     kubectl -n cozy-linstor exec "$pod" -- drbdsetup show | awk -v node="$node" '
       /^resource / { rd = $2; gsub(/"/, "", rd) }
       /_this_host ipv[46]/ && rd != "" { n = split($3, a, ":"); port = a[n]; sub(/;$/, "", port); print rd, node, port; rd = "" }'
   done | sort -u > drbd-ports.txt
   ```

   Change the namespace and label to match your installation. Each line is `<volume> <node> <port>`, so replicas of one volume on different ports keep their own. If your LINSTOR node names differ from the Kubernetes ones, put the LINSTOR name in the second column. The converter refuses a map that gives one replica two ports, and refuses manifests that would put two replicas on one port on the same node. It reports a replica it has no port for while it has its peers', and map entries for nodes that host no replica of the definition.

4. **Take pool snapshots.** A recursive `zfs snapshot` of every pool gives you a rollback point that does not depend on either control plane. Blockstor does not track these snapshots. A volume deleted while it holds one is renamed to `_deleted_<name>_<time>` and kept, as LINSTOR does, so destroy the pool snapshots once you no longer need the rollback point and then remove those renamed volumes by hand.

5. **Check the volume names on disk.** Blockstor finds existing volumes by name: `<pool>/<definition CR name>_<volume number, five digits>`, for example `data/pvc-1234_00000`. The CR name is the definition's `metadata.name` in the converted manifests: its LINSTOR name in lowercase, or, for a name that is not a valid Kubernetes name once lowercased, a hashed slug such as `35d21682-my-res` for `my_res`. A volume of such a definition never matches its LINSTOR name on disk, so rename the definition or the volume before you switch over. If the name does not match exactly, blockstor creates a new empty volume next to the real one. Replicated volumes would heal from their peers, but a volume with a single replica and no DRBD would come up empty. On every node, compare the list of volumes with the names the converted manifests expect:

   ```sh
   zfs list -H -o name -t volume | sort
   zfs list -H -o name -t snapshot | sort
   ```

   Snapshots are found as `<pool>/<definition CR name>_00000@<snapshot>`. Do not continue while any single-replica volume is missing under its expected name.

   The same comparison the other way round lists the volumes no converted replica names, usually leftovers of earlier LINSTOR deletes. Blockstor never deletes a volume that no replica claims, so they stay until you remove them; `linstor-migrate` cannot list them, since it reads only the dump. Volumes named `_deleted_*` are ones LINSTOR renamed because they still had snapshots. Blockstor finishes them the way LINSTOR would: deleting the last of a migrated `Snapshot`'s copies on that node destroys the renamed volume with it. One whose snapshots no `Snapshot` tracks stays.

6. **Record the DRBD configuration.** Blockstor applies the configuration it renders as soon as it adopts a resource, without comparing it with what the kernel runs, so keep a copy to compare against afterwards:

   ```sh
   for pod in $(kubectl -n cozy-linstor get pods -l app.kubernetes.io/component=linstor-satellite \
                  -o jsonpath='{.items[*].metadata.name}'); do
     node=$(kubectl -n cozy-linstor get pod "$pod" -o jsonpath='{.spec.nodeName}')
     kubectl -n cozy-linstor exec "$pod" -- drbdsetup show > "drbd-before-$node.txt"
   done
   ```

   If the blockstor satellite ships a different drbd-utils version, take the copy and the later comparison with the same binary, or the difference is only formatting.

7. **Rehearse on staging.** Run the whole procedure on a copy of the pools. Check that every pool registers, every replica reaches `UpToDate` without a resync, and no new empty volumes appear.

## Convert

`cmd/linstor-migrate` is in no release up to v0.1.17. It ships in the release after that, so pick that one or a newer one for `<VERSION>`. Build the tool from the blockstor release you are installing and run it on the dump:

```sh
go install github.com/cozystack/blockstor/cmd/linstor-migrate@v<VERSION>
linstor-migrate -in ./dump -drbd-ports ./drbd-ports.txt -out ./blockstor-manifests.yaml \
  -linstor-passphrase-file /run/linstor-pass -keyring-out ./blockstor-keyring.yaml
```

The last two flags are for a cluster with LUKS volumes. `/run/linstor-pass` holds the LINSTOR master passphrase byte for byte, the `MASTER_PASSPHRASE` of the Secret piraeus reads. The tool decrypts every volume's password the way LINSTOR does and wraps it under a new blockstor master key, written to `./blockstor-keyring.yaml` with the LINSTOR passphrase as its one slot. Keep the Secret: point `ControllerConfig.spec.passphraseSecretRef` at it and blockstor unlocks from it as LINSTOR did. A wrong passphrase stops the run before anything is written, and without it a dump with LUKS volumes is refused. The passphrase is checked even when no volume is encrypted; if LINSTOR never had a master passphrase, there is nothing to check it against and the run is refused, so leave the two flags out.

Read the whole report it prints. Each `warning:` line is something the tool refused to guess. Resolve every one of these before you go on:

- `keeps its DRBD metadata on a separate volume`, or `would put its DRBD metadata on a separate volume` for a replica the dump has no layer chain for. The definition is skipped with its replicas: blockstor keeps DRBD metadata on the data device, and adopted as it is the replica would get new metadata over the end of its data. Changing the property does not move existing metadata. For a replica without a chain, check in LINSTOR where its metadata is first. If it is on the data device, remove the property and convert again. Otherwise, copy the data to a volume with internal metadata in LINSTOR and convert that one.
- `keep their DRBD metadata on the data device, though StorPoolNameDrbdMeta ... is set`. Nothing to do: the metadata is internal, because LINSTOR applies the property only to replicas created after it was set, and `.internal` asks for internal metadata. The definition converts, and the property is not carried over to the definition or its replicas, so blockstor does not look for a separate metadata volume.
- `no DRBD port`, for a definition, for one replica, or for every replica when no node in the port map hosts one. Each replica it names gets a fresh port and reconnects on adoption, and `spec.drbdPort` cannot be changed after apply. Capture the missing ports, or fix the node names in the map, and convert again.
- `skipped with its replicas`. The definition's replicas cannot be adopted under one layer stack: they were built two ways, a diskful replica has no layer chain in the dump, or a replica was built otherwise than the stack the definition names. The reason in the line says which. An unreadable `layer_stack` column counts as no stack, and the line names its value. Adopted as it is, such a replica would come up under a stack it was not built with, which for a LUKS volume writes DRBD metadata over its ciphertext. Settle the stack in LINSTOR and convert again.
- `unreadable layer_stack`. The definition's `layer_stack` column is not a list the converter can read. When its replicas agree, it takes the stack they were built with and the line names it; check that this is the stack you expect. With no diskful replica the stack is left unset and the controller pins one; see [How the stack is pinned](../layer-stack/#how-the-stack-is-pinned).

A definition with no layer stack whose replicas all agree takes their stack without a warning.

## Cut over

1. **Stop new volumes.** Scale the CSI provisioner and attacher to zero.
2. **Stop the LINSTOR controller.** Scale `deploy/linstor-controller` to zero. Leave the LINSTOR satellites and the DRBD devices running: they keep serving I/O.
3. **Apply the keyring before blockstor starts**, if you converted with `-keyring-out`. A blockstor controller that finds the passphrase Secret and no keyring makes a keyring of its own, with a new master key, within 30 seconds of starting. Under Cozystack `linstor-passphrase` already exists, so this happens at once. The converted volumes' passwords are wrapped under the master key in `./blockstor-keyring.yaml`, and would not open under that new one. Apply it into blockstor's namespace first:

   ```sh
   kubectl apply -f ./blockstor-keyring.yaml
   ```

   If blockstor has already started and made its own keyring, replace it with `kubectl replace -f ./blockstor-keyring.yaml`. That is safe only while no LUKS volume has been formatted under it: no Resource and no Snapshot carries a `luksKey` yet, which holds while provisioning is stopped and the manifests are not applied. Then check that `blockstor encryption list-passphrases` prints the same `key ID` as the `luksKey.keyID` in `./blockstor-manifests.yaml`. Do not go on while they differ.
4. **Install blockstor** as described in [Installation](../installation/), with satellites on the same nodes and pools. Check the image versions before you start them.
5. **Apply the converted manifests.** They are already in dependency order:

   ```sh
   kubectl apply -f ./blockstor-manifests.yaml
   ```

6. **Wait for adoption.** Every replica should report `UpToDate` without starting a resync, `blockstor resource list --faulty` should be empty, and every pool should report its real free space.
7. **Point CSI at blockstor**, as in [Provisioning volumes with CSI](../csi/), and scale the provisioner and attacher back up. Create a test PVC, and check that an existing PVC still mounts read-write.

## Check before you call it done

- The number of nodes, pools, groups, definitions and replicas matches the counts in the converter's report.
- For at least five volumes, the size, `/dev/drbd<minor>`, node IDs and port match the values from before the migration. A changed minor or node ID is a reason to stop and roll back.
- No replica is unexpectedly syncing.
- `drbdsetup show` on every node matches the copy from step 6 of "Before you start", apart from a port the report said would change. A DRBD option that changed or went missing is a reason to stop: look for it in the report.
- Encrypted volumes stay closed until you set up their passphrase. Keep their consumers stopped until then.

## Roll back

Until CSI has been pointed at blockstor and checked, rolling back means: scale blockstor to zero, scale `linstor-controller` back up, and point CSI at LINSTOR again. Blockstor did not touch the data on disk or the running DRBD devices, so this undoes only the control plane. The pool snapshots from step 4 are the last-resort rollback for the data itself.
