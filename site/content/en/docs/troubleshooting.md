---
title: "Troubleshooting"
linkTitle: "Troubleshooting"
weight: 75
description: "Symptoms, where to look, what they mean, and what to do."
---

Each section below is one symptom. [Day-2 operations](../operations/#reading-health) explains the commands used to look at state.

Where a fix needs the DRBD tools, run them inside the satellite pod of the node in question:

```sh
POD=$(kubectl -n blockstor-system get pod -l app=blockstor-satellite --field-selector spec.nodeName=<node> -o name)
kubectl -n blockstor-system exec $POD -c satellite -- drbdadm status <definition>
```

## Replicas stay in Connecting

Look at the connection state of the replica, and at the node addresses:

```sh
kubectl get resource <definition>.<node> -o jsonpath='{range .status.connections[*]}{.peerNodeName}={.message} {end}'
kubectl get nodes.blockstor.cozystack.io -o wide
```

A peer stuck in `Connecting` cannot reach the other replica's address and port. The two causes are a wrong address and a blocked port.

The `Address` column is the first entry of `spec.netInterfaces`. It must be the node's own IP, reachable from the other nodes. A pod IP there means something rewrote it, most often piraeus-operator managing the same nodes. Blockstor works around that by preferring the Kubernetes node's `InternalIP` when it renders the DRBD configuration. That address is added in memory and never shows up on the `Node` object, and it needs the satellite's ClusterRole to read `nodes`. Fix the first interface anyway:

```sh
kubectl patch nodes.blockstor.cozystack.io <node> --type merge \
  -p '{"spec":{"netInterfaces":[{"name":"default","address":"10.0.0.11"}]}}'
```

If the addresses are right, the port is blocked. The `Port` column of `kubectl get resources` is the TCP port this volume uses. It must be open between the storage nodes in both directions. See [Networking and ports](../networking/).

## StandAlone after a network split

Look at:

```sh
blockstor resource list --faulty
kubectl get resource <definition>.<node> -o jsonpath='{range .status.connections[*]}{.peerNodeName}={.message} {end}'
```

`StandAlone` on both sides of a connection after a network partition, with `Split-Brain detected` in the node's kernel log (`dmesg`), means both replicas were written while apart. DRBD refuses to pick a side. This can only happen to a volume without a working tiebreaker, so check the tiebreaker first for volumes with an even number of replicas.

Decide which replica keeps its data, the survivor, and which one discards its writes, the victim. On the victim's node:

```sh
drbdadm disconnect <definition>
drbdadm secondary <definition>
drbdadm -- --discard-my-data connect <definition>
```

On the survivor's node, if it also shows `StandAlone`:

```sh
drbdadm disconnect <definition>
drbdadm connect <definition>
```

The victim resyncs from the survivor. While the slot is `StandAlone` the satellite still adjusts it, but with `--skip-net`, so it leaves the connection to you. The victim's independent writes are lost, so if you are not sure which side is right, stop and copy the data off first.

## Inconsistent or SyncTarget that never finishes

Look at the disk state of each replica and at the replication state towards the peer that should be the source:

```sh
kubectl get resource <definition>.<node> -o jsonpath='{.status.volumes[*].diskState}'
kubectl get resource <definition>.<node> -o jsonpath='{range .status.connections[*]}{.peerNodeName}={.replicationState} {end}'
```

Two different situations look alike:

- `SyncTarget` with the done percentage not moving for minutes. The resync stream stalled on a network blip, and DRBD's own retry takes a long time. Restart the connection to the source from the target's node. The bitmap is kept, so the sync resumes where it was:

  ```sh
  drbdadm disconnect <definition>:<source-node>
  drbdadm connect <definition>:<source-node>
  ```

- `Inconsistent` with no sync in progress, often after a disk error or a crash. The local data cannot be trusted and nothing is bringing it up to date. Rebuild the replica, as in [Replacing a replica with a bad disk](../operations/#replacing-a-replica-with-a-bad-disk): delete it and let the placer create a new one, which does a full sync from the remaining `UpToDate` peers.

Never delete the last replica that is `UpToDate`. `blockstor resource delete` refuses that while a peer is still syncing from it, unless you pass `--force`.

## Metadata re-created after an unclean shutdown

The symptom is a replica that was the only copy, and after a node reboot comes back empty and starts a full sync from a peer, or has nothing to sync from. Look at the satellite log on that node:

```sh
kubectl -n blockstor-system logs $POD -c satellite | grep -i -E 'create-md|apply-al|dump-md'
```

Before it creates DRBD metadata, the satellite probes the device with `drbdmeta dump-md`. If the probe answers that metadata is present but its activity log needs applying, which is what an unclean shutdown of a writing `Primary` leaves behind, the current release treats that as missing metadata, creates it afresh and syncs the replica from its peers. That is fine when another replica holds the data. It is not fine when this replica is the only `UpToDate` copy, because recreating the metadata discards the generation identifiers and the dirty bitmap.

The probe runs when the satellite brings the resource up, which is at satellite start for a resource the kernel does not hold yet, for example after a node reboot. If the replica is the only copy you have, do not let the satellite get there. Keep the satellite pod off that node, and apply the activity log yourself from a privileged pod on the node that uses the satellite image and mounts `/dev`:

```sh
drbdadm apply-al <definition>
```

Then let the satellite start again. The probe now sees clean metadata and skips `create-md`.

## Resource definitions left behind by CSI

Look at the two lists side by side:

```sh
kubectl get pv -o custom-columns=NAME:.metadata.name,CLAIM:.spec.claimRef.name
blockstor resource-definition list
```

A PersistentVolume has the same name as its resource definition. A definition with no PersistentVolume is a volume the CSI driver started to create or delete and did not finish, for example because the API call timed out. Before you delete one, make sure nothing has it open and nothing else refers to it:

```sh
kubectl get resources | grep <definition>     # In-Use must be false on every row
blockstor snapshot list -r <definition>
```

Then:

```sh
blockstor resource-definition delete <definition>
```

The delete is refused while the definition still has snapshots.

## PVC stays Pending

Look at the PVC's events and at the API server:

```sh
kubectl describe pvc <name>
kubectl -n blockstor-system logs -l app=blockstor-apiserver --tail=200
```

With `volumeBindingMode: WaitForFirstConsumer` a PVC is `Pending` until a pod that uses it is scheduled. That is not an error.

Otherwise the events carry the `CreateVolume` error from the driver. The common ones:

- The driver cannot reach the API server. See the TLS section below.
- No pool with that name, or not enough nodes with it. Each replica needs a node that has a pool named as in the StorageClass's `storagePool` parameter, with free capacity. `blockstor storage-pool list` shows what exists. Blockstor places as many replicas as it can and reports the shortfall; it adds the rest when nodes with the pool appear.
- The nodes are `OFFLINE`. The placer does not use them. Check the satellites.

## certificate signed by unknown authority

This error from the CSI driver, the `linstor` client or any other API client means one of the two sides does not trust the other's certificate. Port 3371 requires a client certificate and verifies it against the CA from `--tls-client-ca-file`.

Check that:

- The client's certificate was issued by the `blockstor-api-ca` Issuer. The Secret `blockstor-apiserver-client-tls` in `blockstor-system` carries a working `tls.crt`, `tls.key` and `ca.crt`. A client in another namespace needs its own Certificate from the same Issuer.
- The client trusts the `ca.crt` from that Secret as its root CA. With `linstor-csi` that is `LS_ROOT_CA`.
- The client connects to a name on the serving certificate: `blockstor-apiserver` or `blockstor-controller`, with or without the `.blockstor-system.svc` suffix. An IP address or a port-forward to `localhost` on port 3371 does not match.

For a quick check without certificates, port-forward the plain port 3370 of a pod and use `http://`. See [Compatibility with LINSTOR](../compatibility/#using-the-linstor-client).

## Satellite pod not Ready or node OFFLINE

Look at:

```sh
kubectl -n blockstor-system get pod -l app=blockstor-satellite -o wide
kubectl -n blockstor-system logs $POD -c satellite --tail=100
kubectl get nodes.blockstor.cozystack.io
```

The readiness probe answers `/readyz` only after the satellite has started and synced its caches. A pod that never becomes ready, or that restarts, usually says why in the first lines of its log.

- The DRBD kernel module is not loaded on the host. `drbdadm` and `drbdsetup` fail and nothing can be set up. Load it on the host, `modprobe drbd`, or on Talos add the `siderolabs/drbd` extension. The satellite image carries the tools, not the module.
- The node is not registered. The satellite logs that the `Node` object is missing and that it will not create it. Run `blockstor node create <name> <address>`. This also happens after `node lost`.
- The pod is running but the node shows `OFFLINE`. The heartbeat is more than 40 seconds old. The satellite may be wedged; the liveness probe restarts it after three failures. If it stays `OFFLINE` for an hour the node gets evacuated automatically unless you turned that off. See [Taking a node out of service](../operations/#taking-a-node-out-of-service).

## Storage pool shows no capacity

Look at:

```sh
blockstor storage-pool list
kubectl get storagepool <pool>.<node> -o yaml
```

- `State` is `Faulty` and `poolMissing: true`. The satellite's last probe of the backing pool failed. The volume group, thin pool, ZFS pool or directory named in `spec.props` does not exist on that node, or the `zfs` or `lvm` command failed. Create or fix the backing pool; the next probe, within 30 seconds, clears the state.
- Free and total are empty and the node is `ONLINE`. The satellite has not probed yet, or the `StoragePool` name does not match `<spec.poolName>.<spec.nodeName>`. The API refuses a mismatched name, so check that the object exists at all.
- Free and total are empty and the node is `OFFLINE`. The satellite is the problem, not the pool. See the previous section.
