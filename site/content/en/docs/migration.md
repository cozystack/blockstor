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
- controller-wide DRBD options;
- DRBD minor numbers, node IDs and the shared secret, unchanged;
- DRBD ports, if you capture them from the running nodes (step 3 below).

Not converted, and listed in the tool's report:

- **LUKS passphrases.** Encrypted volumes convert with their layers intact, but blockstor cannot open them until you set up the passphrase by hand.
- Backup shipping remotes, and replicas or snapshots that LINSTOR marked as deleted or failed.
- Snapshots of definitions with more than one volume are adopted with their first volume only.

## Before you start

1. **Check that the cluster is healthy.** `linstor resource list --faulty` is empty, every replica with data is `UpToDate`, and no node is offline. A degraded replica is adopted as it is and stays degraded.

2. **Dump the LINSTOR database.** LINSTOR on Kubernetes keeps it in custom resources:

   ```sh
   mkdir dump && cd dump
   kubectl get crds | grep -o ".*.internal.linstor.linbit.com" \
     | xargs -I{} sh -c 'kubectl get {} -ojson > {}.json'
   cd ..
   ```

3. **Capture the DRBD ports.** LINSTOR 1.33 does not store them in its database. Without them, blockstor would assign new ports, and every replica would reconnect under load. Collect them from the satellites:

   ```sh
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

   Change the namespace and label to match your installation. Every replica of a volume shares one port. If one volume shows two different ports, stop and find out why.

4. **Take pool snapshots.** A recursive `zfs snapshot` of every pool gives you a rollback point that does not depend on either control plane.

5. **Check the volume names on disk.** Blockstor finds existing volumes by name: `<pool>/<definition name, lowercase>_<volume number, five digits>`, for example `data/pvc-1234_00000`. If the name does not match exactly, blockstor creates a new empty volume next to the real one. Replicated volumes would heal from their peers, but a volume with a single replica and no DRBD would come up empty. On every node, compare the list of volumes with the names the converted manifests expect:

   ```sh
   zfs list -H -o name -t volume | sort
   zfs list -H -o name -t snapshot | sort
   ```

   Snapshots are found as `<pool>/<definition>_00000@<snapshot>`. Do not continue while any single-replica volume is missing under its expected name.

6. **Rehearse on staging.** Run the whole procedure on a copy of the pools. Check that every pool registers, every replica reaches `UpToDate` without a resync, and no new empty volumes appear.

## Convert

`cmd/linstor-migrate` is in no release up to v0.1.17. It ships in the release after that, so pick that one or a newer one for `<VERSION>`. Build the tool from the blockstor release you are installing and run it on the dump:

```sh
go install github.com/cozystack/blockstor/cmd/linstor-migrate@v<VERSION>
linstor-migrate -in ./dump -drbd-ports ./drbd-ports.txt -out ./blockstor-manifests.yaml
```

Read the whole report it prints. Each `warning:` line is something the tool refused to guess. Resolve every one of these before you go on:

- `LUKS passphrase NOT migrated`.
- `keeps its DRBD metadata on a separate volume`, or `would put its DRBD metadata on a separate volume` for a replica the dump has no layer chain for. The definition is skipped with its replicas: blockstor keeps DRBD metadata on the data device, and adopted as it is the replica would get new metadata over the end of its data. Changing the property does not move existing metadata. For a replica without a chain, check in LINSTOR where its metadata is first. If it is on the data device, remove the property and convert again. Otherwise, copy the data to a volume with internal metadata in LINSTOR and convert that one.
- `keep their DRBD metadata on the data device, though StorPoolNameDrbdMeta ... is set`. Nothing to do: the metadata is internal, because LINSTOR applies the property only to replicas created after it was set, and `.internal` asks for internal metadata. The definition converts, and the property is not carried over to the definition or its replicas, so blockstor does not look for a separate metadata volume.
- `no DRBD port`.
- `skipped with its replicas`. The definition's replicas cannot be adopted under one layer stack: they were built two ways, a diskful replica has no layer chain in the dump, or a replica was built otherwise than the stack the definition names. The reason in the line says which. An unreadable `layer_stack` column counts as no stack, and the line names its value. Adopted as it is, such a replica would come up under a stack it was not built with, which for a LUKS volume writes DRBD metadata over its ciphertext. Settle the stack in LINSTOR and convert again.
- `unreadable layer_stack`. The definition's `layer_stack` column is not a list the converter can read. When its replicas agree, it takes the stack they were built with and the line names it; check that this is the stack you expect. With no diskful replica the stack is left unset and the controller pins one; see [How the stack is pinned](../layer-stack/#how-the-stack-is-pinned).

A definition with no layer stack whose replicas all agree takes their stack without a warning.

## Cut over

1. **Stop new volumes.** Scale the CSI provisioner and attacher to zero.
2. **Stop the LINSTOR controller.** Scale `deploy/linstor-controller` to zero. Leave the LINSTOR satellites and the DRBD devices running: they keep serving I/O.
3. **Install blockstor** as described in [Installation](../installation/), with satellites on the same nodes and pools. Check the image versions before you start them.
4. **Apply the converted manifests.** They are already in dependency order:

   ```sh
   kubectl apply -f ./blockstor-manifests.yaml
   ```

5. **Wait for adoption.** Every replica should report `UpToDate` without starting a resync, `blockstor resource list --faulty` should be empty, and every pool should report its real free space.
6. **Point CSI at blockstor**, as in [Provisioning volumes with CSI](../csi/), and scale the provisioner and attacher back up. Create a test PVC, and check that an existing PVC still mounts read-write.

## Check before you call it done

- The number of nodes, pools, groups, definitions and replicas matches the counts in the converter's report.
- For at least five volumes, the size, `/dev/drbd<minor>`, node IDs and port match the values from before the migration. A changed minor or node ID is a reason to stop and roll back.
- No replica is unexpectedly syncing.
- Encrypted volumes stay closed until you set up their passphrase. Keep their consumers stopped until then.

## Roll back

Until CSI has been pointed at blockstor and checked, rolling back means: scale blockstor to zero, scale `linstor-controller` back up, and point CSI at LINSTOR again. Blockstor did not touch the data on disk or the running DRBD devices, so this undoes only the control plane. The pool snapshots from step 4 are the last-resort rollback for the data itself.
