# Layer stack

blockstor mirrors LINSTOR's `layer_list` model: a `ResourceDefinition`
declares an ordered chain of layers, each adding capabilities on top
of the layer below. The satellite walks the chain bottom-up when
provisioning and top-down when tearing down.

## Layers

| Layer     | What it does                                                          |
|-----------|-----------------------------------------------------------------------|
| `STORAGE` | Allocates the raw block device (LVM-thin LV, ZFS volume, loopfile, …) |
| `LUKS`    | `cryptsetup luksFormat` + `luksOpen` over the storage device          |
| `DRBD`    | Renders `.res` and runs `drbdadm` to replicate over the network       |

The first entry in `layerStack` is the topmost layer the consumer Pod
mounts. The last entry is always `STORAGE` (the storage layer is
required — every replica needs a backing device, even diskless ones
where it's the network leg).

## Common compositions

| Stack                       | Use case                                                            |
|-----------------------------|---------------------------------------------------------------------|
| `["DRBD","STORAGE"]`        | Default. Replicated PVC, the cozystack production case.             |
| `["LUKS","STORAGE"]`        | Single-replica encrypted PVC. No DRBD overhead.                     |
| `["DRBD","LUKS","STORAGE"]` | Encrypted at-rest + replicated. Per-volume cipher.                  |
| `["STORAGE"]`               | Single-replica local mode. Ephemeral cache, scratch, ZFS dataset.   |

The default is `["DRBD","STORAGE"]`, used when neither the RD nor its resource group sets a stack.

An RD's stack is fixed once and then read only from `RD.Spec.LayerStack`. The REST create and `rg spawn` paths copy the group's stack onto the RD when it is created. An RD that ends up with an empty stack in a group that sets one (created with `kubectl apply` or GitOps, or moved by `rd modify --resource-group`) is pinned by the controller, and the satellite builds nothing for it until then:

- An RD that holds no data yet gets the group's stack.
- An RD that already holds data (a replica reported DRBD metadata or a device, or `spec.initialized` is set) keeps `["DRBD","STORAGE"]`, the stack its storage was built with. When that is missing a layer the group asks for, the group's stack is recorded in the `blockstor.io/unapplied-group-layer-stack` annotation and a `LayerStackNotInherited` warning Event is emitted on the RD. Such a volume is not what its group promises (for example, a plaintext volume in a LUKS group); move its data to a new volume to get the group's stack. The controller re-checks every RD with a decided stack each time it reconciles it: the annotation names the group's current stack whenever the RD's stack lacks a layer of it, set by hand or pinned, and goes once the group no longer asks for a missing layer or is deleted. A newly flagged RD gets the same warning Event.

Clones and restores copy bytes, so the RD they create takes the stack those bytes were written under: the source's own, or the default when neither the source nor its group sets one. A source still waiting for the controller to pin its stack is answered 503 (the CLI exits with an error) until it is pinned, and a prepared restore target in the same state is refused the same way before anything is written to it. A copy whose stack is short of what its own group asks for carries the `blockstor.io/unapplied-group-layer-stack` annotation. A clone of a volume-less RD copies no bytes: it takes the source's stack, unless that is the default stack and the source carries the annotation, which is how a stack pinned over data looks. Such a clone, and one of a source with no stack, is left for the controller to pin from its own group.

Changing a group's stack later never reshapes RDs that already carry one, as in upstream LINSTOR, where the stack is decided when a resource is created.

As a last guard, the LUKS layer refuses to `luksFormat` a device that already carries a signature `blkid -p` recognises (DRBD metadata, a filesystem, a partition table). The replica's apply fails with `luks: device already holds data` and names what it found. The satellite wipes a thick LV it creates before the volume takes its name, so a fresh volume never inherits a signature from extents an earlier volume used; one created outside blockstor may, so clear it with `wipefs -a <device>` once you have checked it holds nothing you need.

Find the RDs short of a layer their group asks for:

```bash
kubectl get resourcedefinitions.blockstor.cozystack.io \
  -o custom-columns='NAME:.metadata.name,GROUP_STACK:.metadata.annotations.blockstor\.io/unapplied-group-layer-stack' \
  | awk '$2 != "<none>"'
```

## Setting the stack

### Per-RD via REST

```bash
# the plain port listens on the pod's loopback; reach it through a port-forward:
# kubectl -n blockstor-system port-forward deploy/blockstor-apiserver 3370:3370
curl -XPOST http://localhost:3370/v1/resource-definitions \
  -H 'Content-Type: application/json' \
  -d '{"resource_definition":{"name":"pvc-1","layer_stack":["LUKS","STORAGE"]}}'
```

### Per-RG via spawn template

```bash
linstor rg create encrypted-rg
linstor rg sp encrypted-rg DrbdOptions/Encryption/passphrase '<32-byte-secret>'
linstor rg c --place-count 2 encrypted-rg
linstor rg spawn encrypted-rg pvc-2 1G --layer-list LUKS DRBD
```

(blockstor accepts `linstor`'s `--layer-list` flag verbatim because
the REST shape mirrors upstream LINSTOR.)

### Per-RD via kubectl

```yaml
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: pvc-encrypted}
spec:
  layerStack: ["LUKS", "STORAGE"]
  props:
    DrbdOptions/Encryption/passphrase: "32-byte-secret"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 1048576}
```

## LUKS specifics

- Passphrase is per-RD via `DrbdOptions/Encryption/passphrase`. The
  upstream `linstor rd set-property` key.
- The dispatcher folds the resolver-resolved passphrase onto the wire
  as `DesiredResource.Props["LuksPassphrase"]`; the satellite reads it
  from there.
- Empty passphrase with `LUKS` in the stack fails the apply rather
  than silently producing an unencrypted volume.
- Mapper name: `<rd>-<vol>-luks` → `/dev/mapper/<rd>-<vol>-luks`. Stable
  across reconciles so reopen on satellite restart re-uses it.
- Volume grow: the satellite runs `cryptsetup resize` on the mapper
  after the storage layer has resized the underlying LV, before DRBD
  resizes the replicated device.

## What blockstor doesn't yet support

- **Pluggable layer ordering**: the stack must be one of the four
  rows in the table above. Arbitrary orderings (e.g. `["LUKS","DRBD","STORAGE"]`
  with LUKS-over-DRBD instead of DRBD-over-LUKS) aren't yet validated
  or rendered.
- **Per-volume LUKS keys**: every volume on a 2-volume RD currently
  uses the same RD-level passphrase. Per-volume keys (with master-key
  wrapping in the controller's KV store) is a follow-up.
- **Mid-stack changes**: editing `layerStack` after a Resource is
  active doesn't re-encrypt or unwrap existing data. Treat it as
  set-once-per-RD until migration support lands.
- **Cluster-passphrase rotation**: the cluster passphrase
  (`POST /v1/encryption/passphrase`) wraps RD passphrases in the
  controller's KV store; rotating it doesn't re-encrypt LUKS headers.
  Rotating the per-RD passphrase requires `cryptsetup luksChangeKey`
  which isn't wired yet — operators should drop the RD and recreate.

## Testing

- `pkg/api/v1/layer_stack_test.go` — RD → RG → default resolution.
- `pkg/satellite/reconciler_drbd_test.go::TestApplySkipsDRBDWhenLayerStackOmits`
  — `["STORAGE"]` produces no `.res` and no drbdadm.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLayersLUKS` — pin
  cryptsetup luksFormat + luksOpen run on first activation.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLUKSFailsWithoutPassphrase`
  — pin the explicit-error path.
