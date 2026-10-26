---
title: "Day-2 operations"
linkTitle: "Operations"
weight: 65
description: "Reading health, adding and removing nodes, growing and moving volumes, snapshots, backups and upgrades."
---

Everything on this page can be done with the `blockstor` CLI or by editing the custom resources with `kubectl`. The CLI commands are shown; [Resources](../resources/) has the matching YAML.

## Reading health

Start with the replicas that are not healthy:

```sh
blockstor resource list --faulty
```

A replica counts as faulty when one of its volumes has a disk state that has not settled, or when one of its DRBD connections is anything other than `Connected`. A replica the satellite has not reported on yet is not listed, so an empty result means "nothing broken that we can see", not "everything verified".

The custom resources carry the same information for `kubectl` and for monitoring:

```sh
kubectl get nodes.blockstor.cozystack.io          # Status ONLINE or OFFLINE
kubectl get storagepools                          # free and total KiB per pool
kubectl get resourcedefinitions                   # group, DRBD port, layers
kubectl get resources                             # one row per replica: node, pool, port, state, in use
```

Use the full name `nodes.blockstor.cozystack.io`, because plain `nodes` is the Kubernetes kind.

A `Node` is `ONLINE` while its satellite stamps a heartbeat every 10 seconds. When the heartbeat is more than 40 seconds old, the controller sets the `Ready` condition to `Unknown` and the status to `OFFLINE`.

The `State` column of `kubectl get resources` is the resource-level DRBD state. Per-volume disk state and per-peer connection state are in the status:

```sh
kubectl get resource <definition>.<node> -o jsonpath='{.status.volumes[*].diskState}'
kubectl get resource <definition>.<node> -o jsonpath='{range .status.connections[*]}{.peerNodeName}={.message} {end}'
```

A `Resource` also has conditions `MetadataCreated`, `FilesystemFormatted` and `KernelLoaded`, which tell you how far a new replica has got.

When you need the kernel's own view, run the DRBD tools inside the satellite pod of that node:

```sh
POD=$(kubectl -n blockstor-system get pod -l app=blockstor-satellite --field-selector spec.nodeName=<node> -o name)
kubectl -n blockstor-system exec $POD -c satellite -- drbdsetup status <definition> --verbose
```

Logs:

```sh
kubectl -n blockstor-system logs -l app=blockstor-controller
kubectl -n blockstor-system logs -l app=blockstor-apiserver
kubectl -n blockstor-system logs $POD -c satellite
```

## Adding a node

1. Make sure the DRBD module is loaded on the node, and the ZFS module if it will host ZFS pools. The satellite DaemonSet schedules on every node without the `node-role.kubernetes.io/control-plane` label, so the pod appears on its own.
2. Register the node. The satellite does not create the `Node` object for you. Without it, the satellite logs that the node is unregistered and waits:

   ```sh
   blockstor node create worker-4 10.0.0.14
   ```

3. Give it a pool, from a raw disk or from an existing volume group or ZFS pool. `create-device-pool` wipes the device it is given:

   ```sh
   blockstor physical-storage create-device-pool zfs worker-4 /dev/sdb --pool-name data
   blockstor storage-pool create lvmthin worker-4 data my_vg/my_thinpool
   ```

The new node shows `ONLINE` within a minute, and the pool reports its capacity once the satellite has probed it. The satellite refreshes pool capacity every 30 seconds.

## Adding a storage pool

A pool is one `StoragePool` per node, named `<pool>.<node>`. Use the same pool name on every node that should take replicas of the same StorageClass, because CSI selects by pool name. The providers are `lvm`, `lvmthin`, `zfs`, `zfsthin`, `file`, `filethin` and `diskless`. Snapshots need a provider with copy-on-write: ZFS, LVM thin and thin file pools support them, thick LVM does not. `blockstor storage-pool list` shows this in the `CanSnapshots` column.

## Taking a node out of service

To drain a node for maintenance:

```sh
blockstor node evacuate worker-2
```

This sets the `EVICTED` flag on the `Node`. The placer stops choosing that node, and for every replica with data on it the controller first places a replacement on another node and only deletes the source once the replacement is `UpToDate`. The command refuses while a replica on the node is in use, that is, mounted and `Primary`. Stop the consumers first, or pass `--force` if you know what you are doing.

If the node comes back before the migration finishes:

```sh
blockstor node restore worker-2
```

This only clears the flag. Replicas that have already moved are not moved back.

Blockstor also evacuates nodes on its own. A node that has been `OFFLINE` for longer than `DrbdOptions/AutoEvictAfterTime` minutes, 60 by default, gets the `EVICTED` flag automatically. Turn this off per node or for the whole cluster with `DrbdOptions/AutoEvictAllowEviction false`. As a safety against a controller-side network problem, auto-evict stops acting when more than `DrbdOptions/AutoEvictMaxDisconnectedNodes` nodes are offline at once, 2 by default:

```sh
blockstor controller set-property DrbdOptions/AutoEvictAfterTime 120
blockstor node set-property worker-2 DrbdOptions/AutoEvictAllowEviction false
```

When a node is gone for good:

```sh
blockstor node lost worker-2
```

This deletes the node's replicas and pools, then the `Node`. It is refused while the satellite still reports `ONLINE`, unless you pass `--force`. Surviving replicas are left alone, and the controller places replacements for them. A satellite pod that restarts on that host afterwards does not re-create the `Node`; register it again with `node create` if the node returns.

`blockstor node delete` removes only the `Node` object and refuses while replicas or pools still point at it. With `--force` it deletes them too, like `node lost`.

## Replacing a replica with a bad disk

If a pool's backing volume group or ZFS pool disappears, the satellite marks the `StoragePool` with `poolMissing: true`, zeroes its capacity, and `blockstor storage-pool list` shows `Faulty`. The placer skips the pool. The next successful probe clears the state, so recreating the backing pool under the same name brings it back.

A replica whose local data is damaged shows `Inconsistent` and does not finish syncing. Rebuild it instead of repairing it. Delete the replica on that node, then let the placer put a fresh one somewhere:

```sh
blockstor resource delete worker-3 my-volume
blockstor resource-definition auto-place my-volume --place-count 3
```

`resource delete` refuses to remove the last complete copy while another replica is still syncing from it, unless you pass `--force`. The new replica syncs from the remaining `UpToDate` peers while the volume stays in use.

## Growing a volume

For a PVC, change `spec.resources.requests.storage`. The StorageClass must have `allowVolumeExpansion: true`. The CSI driver grows the volume definition, blockstor grows the backing devices and DRBD online, and Kubernetes grows the filesystem.

For a volume you manage directly:

```sh
blockstor volume-definition set-size my-volume 0 20G
```

Shrinking is refused. `--force` lowers the size in the definition only: the satellite grows devices and never shrinks them, so the backing devices and DRBD keep their size.

## Moving a replica

Move a replica with data to another node without lowering redundancy:

```sh
blockstor resource toggle-disk worker-3 my-volume --migrate-from worker-1
```

The destination gets a new replica with data. When all of its volumes are `UpToDate`, the controller deletes the source. The command refuses when the source is diskless, when the source is `Primary` and in use, or when source and destination are the same node.

To change a single replica's kind in place:

```sh
blockstor resource toggle-disk worker-3 my-volume --storage-pool data   # give it data
blockstor resource toggle-disk worker-3 my-volume --diskless            # make it diskless
```

`--diskless` does not check what is left: it demotes the replica even when it is the last one with data. Before you run it, make sure another replica of the volume is `UpToDate`. A conversion that is stuck can be unwound with `--cancel`.

## Snapshots as an in-cluster backup

Snapshots live in the same pool as the volume, so they protect against mistakes, not against losing the node or the disk. Take them before upgrades and risky changes:

```sh
blockstor snapshot create my-volume before-upgrade
blockstor snapshot list
```

Restore into a new volume. The original is not touched. The new volume gets its replicas on the nodes that hold the snapshot, in the same pool, not through the autoplacer:

```sh
blockstor snapshot resource restore \
    --from-resource my-volume --from-snapshot before-upgrade --to-resource my-volume-restored
```

In-place rollback is not available on purpose. A definition cannot be deleted while it still has snapshots, so delete them first:

```sh
blockstor snapshot delete my-volume before-upgrade
```

## Backing up the control plane

Blockstor keeps all of its state in custom resources, so a backup is a dump of them. Include the `ResourceDefinition` and `Resource` objects. The controller allocates DRBD minors into the definitions, in `spec.volumeDefinitions[].drbdMinor`, and DRBD ports and node IDs into the spec of each `Resource`. A restore that invents new ones would make every replica reconnect.

```sh
for kind in controllerconfigs nodes storagepools physicaldevices \
            resourcegroups resourcedefinitions resources snapshots; do
  kubectl get "$kind.blockstor.cozystack.io" -o yaml > "blockstor-$kind.yaml"
done
kubectl -n blockstor-system get secret blockstor-cluster-passphrase -o yaml > blockstor-passphrase.yaml
```

The full names, such as `resources.blockstor.cozystack.io`, keep `kubectl` from picking another product's resource with the same plural. The last line applies only if you use encrypted volumes. The Secret holds the cluster passphrase, and the volumes cannot be opened without it. Its name can be changed through `spec.passphraseSecretRef` on the `ControllerConfig`. Store the backup like any other secret material.

The passphrase can also be set as a property, `DrbdOptions/EncryptPassphrase` on the controller or `DrbdOptions/Encryption/passphrase` on a definition, and a property set there wins over the Secret. A property is stored in plaintext on its resource, so the YAML dump above then carries the key to every encrypted volume. Keep the passphrase in the Secret only, and treat the dump as secret material if such a property was ever set.

## Upgrading

Upgrade in the same order as the installation. Update the manifests to the new tag with the same `sed` as in [Installation](../installation/#get-the-manifests), then apply:

1. The CRDs, `kubectl apply -f config/crd/bases/`. New fields arrive before the code that writes them.
2. The controller, `rendered/blockstor-deploy.yaml`.
3. The API server certificates, `rendered/blockstor-apiserver-tls.yaml`.
4. The API server, `rendered/blockstor-apiserver-deploy.yaml`. It runs three replicas, so clients keep working through the rollout.
5. The satellites, `rendered/blockstor-satellite-daemonset.yaml`.

Rolling the satellites does not interrupt I/O. The DRBD state lives in the kernel and survives the pod, the rendered DRBD configuration lives on the host under `/var/lib/blockstor-drbd.d`, and the pod is given 10 seconds to stop and does not try to disconnect DRBD on the way out. When the new satellite starts, it looks at every DRBD resource the kernel holds and tears down only the ones with no usable local disk and no live peer connection. Everything else is left for the reconciler to adjust. A replica in the middle of a sync keeps syncing.

Watch the rollout and the result:

```sh
kubectl -n blockstor-system rollout status daemonset/blockstor-satellite
blockstor resource list --faulty
```
