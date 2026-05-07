# cozystack-blockstor

Development stand and integration tests for a Go reimplementation of LINSTOR.

## What's here

- `stand/` — scripts to bring up a 3-node Talos cluster on QEMU/KVM with DRBD,
  install Piraeus operator + linstor-csi, and (optionally) the Java LINSTOR
  controller for contract-diff testing.
- `tests/` — smoke tests against a running stand.
- `docs/` — design notes.

## Requirements (host)

- Linux x86_64 with KVM enabled (`/dev/kvm` accessible)
- `talosctl`, `kubectl`, `helm`, `qemu-system-x86_64`
- DRBD9 kernel module loaded on host (`modprobe drbd`)
- ~8 GB free RAM and ~20 GB disk per cluster

## Quick start

```sh
# Single cluster (default name "blockstor")
make up
make piraeus
make smoke
make down

# Multiple parallel clusters
make up      NAME=alice
make up      NAME=bob
make piraeus NAME=alice
make piraeus NAME=bob
make smoke   NAME=alice
make smoke   NAME=bob
make down    NAME=alice
make down    NAME=bob
```

Each cluster gets its own talos+kube config under `.work/<NAME>/`.

## Selecting a stand from your shell

```sh
eval "$(make use NAME=alice)"
kubectl get nodes
```

## Layout

```
stand/
  up.sh            create talos+qemu cluster with DRBD extension
  down.sh          destroy cluster, free libvirt resources
  reset.sh         down + up
  use.sh           print TALOSCONFIG/KUBECONFIG export lines
  install-piraeus.sh
  install-oracle.sh   Java LINSTOR controller pod for contract tests
tests/
  smoke.sh         PVC -> Pod -> write -> replicate, golden path
```
