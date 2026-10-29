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

The symptom is a replica that was the only copy, and after a node reboot comes back without its generation identifiers and bitmap: it starts a full sync from a peer, or has nothing to sync from. The satellite log does not show the cause. The satellite does not log the `drbdadm` and `drbdmeta` commands it runs, and re-creating the metadata succeeds without a message; only a failed `dump-md` or `create-md` shows up, inside an error. What tells you whether it can have happened is whether a satellite pod started on the node after the node came back:

```sh
kubectl -n blockstor-system get $POD -o jsonpath='{.status.startTime}{"\n"}'
kubectl get node <node> -o jsonpath='{.status.conditions[?(@.type=="Ready")].lastTransitionTime}{"\n"}'
```

Before it creates DRBD metadata, the satellite probes the device with `drbdadm dump-md`. If the probe answers that metadata is present but its activity log needs applying, which is what an unclean shutdown of a writing `Primary` leaves behind, the current release treats that as missing metadata, creates it afresh and syncs the replica from its peers. That is fine when another replica holds the data. It is not fine when this replica is the only `UpToDate` copy, because recreating the metadata discards the generation identifiers and the dirty bitmap.

### What the satellite cannot do

The satellite cannot protect such a replica after an unplanned reboot. The DaemonSet starts the satellite as soon as the node is back. On start, the satellite removes the rendered `.res` file of every resource the kernel does not hold, and after a reboot the kernel holds none. The reconciler then renders the configuration again, runs the probe and creates the metadata afresh. Nothing in the current release stops that sequence. Keep a second replica with data of any volume you cannot lose.

What you can do is prepare a way to keep the satellite off a node, and use it in the time between the node going down and the node booting again.

### Keeping the satellite off a node

A taint does not work: the DaemonSet tolerates every taint (`tolerations: [{operator: Exists}]`). Its node affinity does work. Add a condition on a label of your own choosing to it, once, ahead of time. The example uses the key `example.com/blockstor-satellite-hold`:

```sh
kubectl -n blockstor-system patch daemonset blockstor-satellite --type json -p '[{"op":"add","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/-","value":{"key":"example.com/blockstor-satellite-hold","operator":"DoesNotExist"}}]'
```

This changes the pod template, so the DaemonSet rolls every satellite once, one node at a time. Re-apply the patch whenever you re-apply the satellite manifest, which overwrites it.

When a node that holds the only copy of a volume goes down unexpectedly, label it before it boots again:

```sh
kubectl label node <node> example.com/blockstor-satellite-hold=true
```

The DaemonSet controller then deletes the satellite pod of that node. While the node is down, the pod stays `Terminating`. Force-delete it, so that the kubelet finds no satellite pod for the node when it comes back:

```sh
kubectl -n blockstor-system delete $POD --force --grace-period=0
kubectl -n blockstor-system get pod -l app=blockstor-satellite --field-selector spec.nodeName=<node>
```

The second command must list no pod. Label first and delete second: deleted without the label, the pod is created again for the node at once.

If the node is already back and a satellite pod has started on it, it is too late for that replica. Do not wait for a log line to confirm it: re-creating the metadata leaves none. Assume the metadata was re-created once the satellite has run on the node after the reboot.

### Applying the activity log by hand

With the satellite kept off, the `.res` file from before the reboot is still on the host, in `/var/lib/blockstor-drbd.d/<definition>.res`, because only a starting satellite removes it. Start a privileged pod on the node with the satellite's image, the host's `/dev` and that directory:

```sh
IMAGE=$(kubectl -n blockstor-system get daemonset blockstor-satellite \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="satellite")].image}')
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: drbd-repair
  namespace: blockstor-system
spec:
  nodeName: <node>
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  containers:
    - name: repair
      image: $IMAGE
      command: [sleep, infinity]
      securityContext: {privileged: true}
      volumeMounts:
        - {name: dev, mountPath: /dev}
        - {name: drbd-cfg, mountPath: /etc/drbd.d, readOnly: true}
  volumes:
    - {name: dev, hostPath: {path: /dev, type: Directory}}
    - {name: drbd-cfg, hostPath: {path: /var/lib/blockstor-drbd.d, type: Directory}}
EOF
kubectl -n blockstor-system exec -it drbd-repair -- cat /etc/drbd.d/<definition>.res
```

In the `on <node>` block for this node, each `volume` has three lines you need: `device /dev/drbd<minor> minor <minor>;`, `disk <backing-device>;` and `meta-disk internal;`. The minor is also in `spec.volumeDefinitions[].drbdMinor` of the `ResourceDefinition`. These steps cover only `meta-disk internal`. If the `disk` device does not exist on the node after the reboot, for example because it is an encrypted volume's mapping that only the satellite opens, stop here: these steps do not cover bringing it up.

For each volume, run `drbdmeta` directly against the backing device. It needs no configuration file. Give the minor as a plain number, without `--force`, so that `drbdmeta` locks the minor and refuses to touch a device the kernel has attached:

```sh
kubectl -n blockstor-system exec -it drbd-repair -- drbdmeta <minor> v09 <backing-device> internal dump-md
kubectl -n blockstor-system exec -it drbd-repair -- drbdmeta <minor> v09 <backing-device> internal apply-al
kubectl -n blockstor-system exec -it drbd-repair -- drbdmeta <minor> v09 <backing-device> internal dump-md
```

The first `dump-md` should fail with `Found meta data is "unclean", please apply-al first`. After `apply-al`, the second one prints the metadata. Then delete the repair pod and remove the label:

```sh
kubectl -n blockstor-system delete pod drbd-repair
kubectl label node <node> example.com/blockstor-satellite-hold-
```

The satellite starts on the node again. Its probe now sees clean metadata and skips `create-md`, and the replica comes up with its generation identifiers and bitmap.

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
- The candidate nodes are excluded. The placer skips nodes that are evicted or lost, nodes with the property `AutoplaceTarget` set to `false`, and nodes without the pool. It does not skip a node only because it is `OFFLINE`: that node stays a candidate until auto-evict flags it, and a replica placed there cannot come up until its satellite runs. Check the satellites.

## certificate signed by unknown authority

This error from the CSI driver, the `linstor` client or any other API client means one of the two sides does not trust the other's certificate. Port 3371 requires a client certificate and verifies it against the CA from `--tls-client-ca-file`.

Check that:

- The client's certificate was signed by the `blockstor-api-ca` CA. The Secret `blockstor-apiserver-client-tls` in `blockstor-system` carries a working `tls.crt`, `tls.key` and `ca.crt`. `blockstor-api-ca` is a namespaced Issuer, so a Certificate in another namespace that names it is never issued: such a Certificate stays not `Ready`. Mirror the CA or copy a client Secret into that namespace, as in [Client certificates in another namespace](../csi/#client-certificates-in-another-namespace).
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
