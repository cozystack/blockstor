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

Build the tool from the blockstor release you are installing and run it on the dump:

```sh
go install github.com/cozystack/blockstor/cmd/linstor-migrate@v<VERSION>
linstor-migrate -in ./dump -drbd-ports ./drbd-ports.txt -out ./blockstor-manifests.yaml
```

Read the whole report it prints. Each `warning:` line is something the tool refused to guess. Resolve every `LUKS passphrase NOT migrated` and every `no DRBD port` line before you go on.

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
