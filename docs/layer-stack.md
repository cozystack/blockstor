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
| `["DRBD","LUKS","STORAGE"]` | Encrypted at-rest + replicated. Per-replica key.                    |
| `["STORAGE"]`               | Single-replica local mode. Ephemeral cache, scratch, ZFS dataset.   |

The default is `["DRBD","STORAGE"]`, used when neither the RD nor its resource group sets a stack.

An RD's stack is fixed once and then read only from `RD.Spec.LayerStack`. The REST create and `rg spawn` paths copy the group's stack onto the RD when it is created. An RD that ends up with an empty stack in a group that sets one (created with `kubectl apply` or GitOps, or moved by `rd modify --resource-group`) is pinned by the controller, and the satellite builds nothing for it until then:

- An RD that holds no data yet gets the group's stack.
- An RD that already holds data (a replica reported DRBD metadata or a device, or `spec.initialized` is set) keeps `["DRBD","STORAGE"]`, the stack its storage was built with. When that is missing a layer the group asks for, the group's stack is recorded in the `blockstor.org/unapplied-group-layer-stack` annotation and a `LayerStackNotInherited` warning Event is emitted on the RD. Such a volume is not what its group promises (for example, a plaintext volume in a LUKS group); move its data to a new volume to get the group's stack. The controller re-checks every RD with a decided stack each time it reconciles it: the annotation names the group's current stack whenever the RD's stack lacks a layer of it, set by hand or pinned, and goes once the group no longer asks for a missing layer or is deleted. A newly flagged RD gets the same warning Event.

Clones and restores copy bytes, so the RD they create takes the stack those bytes were written under: the source's own, or the default when neither the source nor its group sets one. A source still waiting for the controller to pin its stack is answered 503 (the CLI exits with an error) until it is pinned. A prepared restore target (the definition linstor-csi creates in the target class's group before restoring into it) takes the source's stack together with the restore marker, whatever stack it was created with and whether or not its group's stack is decided yet: nothing is on it, and the copy has to be read back through the layers that wrote it. This is how LINSTOR builds the restored resource too, from the snapshot's layer data; unlike LINSTOR, blockstor records that stack on the definition (`docs/cli-parity-known-deltas.md` row 103). A copy whose stack is short of what its own group asks for carries the `blockstor.org/unapplied-group-layer-stack` annotation. A clone of a volume-less RD copies no bytes: it takes the source's stack, unless that is the default stack and the source carries the annotation, which is how a stack pinned over data looks. Such a clone, and one of a source with no stack, is left for the controller to pin from its own group.

Changing a group's stack later never reshapes RDs that already carry one, as in upstream LINSTOR, where the stack is decided when a resource is created.

As a last guard, the LUKS layer refuses to `luksFormat` a device that already carries a signature `blkid -p` recognises (DRBD metadata, a filesystem, a partition table). The replica's apply fails with `luks: device already holds data` and names what it found. The satellite wipes a thick LV it creates before the volume takes its name, so a fresh volume never inherits a signature from extents an earlier volume used; one created outside blockstor may, so clear it with `wipefs -a <device>` once you have checked it holds nothing you need.

Find the RDs short of a layer their group asks for:

```bash
kubectl get resourcedefinitions.blockstor.org \
  -o custom-columns='NAME:.metadata.name,GROUP_STACK:.metadata.annotations.blockstor\.org/unapplied-group-layer-stack' \
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
linstor encryption create-passphrase --passphrase '<32-byte-secret>'
linstor rg create encrypted-rg
linstor rg c --place-count 2 encrypted-rg
linstor rg spawn encrypted-rg pvc-2 1G --layer-list LUKS DRBD
```

(blockstor accepts `linstor`'s `--layer-list` flag verbatim because
the REST shape mirrors upstream LINSTOR.)

### Per-RD via kubectl

The cluster needs a master key first. `linstor encryption create-passphrase` creates one, or, GitOps style and the way piraeus sets LINSTOR up, apply a passphrase Secret and name it in the ControllerConfig; the controller creates the keyring from it on its next pass:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: linstor-passphrase
  namespace: blockstor-system          # blockstor's own namespace
stringData:
  MASTER_PASSPHRASE: "a long random passphrase"
---
apiVersion: blockstor.org/v1alpha1
kind: ControllerConfig
metadata: {name: default}
spec:
  passphraseSecretRef: {name: linstor-passphrase}   # key defaults to MASTER_PASSPHRASE
---
apiVersion: blockstor.org/v1alpha1
kind: ResourceDefinition
metadata: {name: pvc-encrypted}
spec:
  layerStack: ["LUKS", "STORAGE"]
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 1048576}
```

## What LUKS in this stack actually protects

**At rest, on each node. Not the replication link.**

In `["DRBD","LUKS","STORAGE"]` the satellite hands DRBD the dm-crypt mapper as its lower disk (`maybeLUKS` rewrites the device map before `applyDRBD`, `pkg/satellite/reconciler.go`; `tests/e2e/drbd-luks-stack.sh` asserts the `.res` `disk` line points at `/dev/mapper/<rd>-<vol>-luks`). The mapper is the *plaintext* side of the crypt device. So:

- the pod writes plaintext into `/dev/drbdN`;
- DRBD replicates **plaintext** to its peers;
- each node encrypts independently on the way to its own LV/zvol, and only the backing device holds ciphertext.

Anyone who needs the replication traffic protected has to secure it separately; the layer stack does not do it.

## LUKS specifics

The design is [encryption-keys.md](encryption-keys.md).

- Every LUKS replica's volume has its own random password, the only key in its LUKS header. The satellite generates it before formatting and stores it on the replica's Resource, `spec.volumes[].luksKey`, wrapped under the cluster master key; a device is formatted only after its password is stored, and only if `blkid -p` finds nothing on it. No master passphrase opens a disk.
- The master key lives in the keyring Secret `blockstor-master-key`, wrapped under one slot per master passphrase. Each component unlocks it from the passphrase Secret the ControllerConfig names, or, without one, from the Secret `blockstor-unlock` that `create-passphrase` generates; blockstor never writes the operator's passphrase Secret.
- `linstor encryption status` is `unset` with no keyring, `locked` when the passphrase Secret opens no slot, `unlocked` otherwise. Creating an RD with `LUKS` in its stack is refused while it is `unset`.
- Passphrases change without touching a volume: `modify-passphrase` replaces a slot, and `blockstor encryption add-passphrase`, `remove-passphrase` and `list-passphrases` manage the others. To change the passphrase the Secret holds: add a slot with the new one, update the Secret, remove the old slot. Every step unlocks.
- A device with a LUKS header and no stored password is neither opened nor formatted (`LUKSKeyMissing`); a stored password that does not open its header is never re-formatted (`LUKSKeyMismatch`). Both are Warning Events on the Resource. A restored or cloned volume takes the password its snapshot recorded.
- A passphrase is never read from a property. The REST API answers 400 to any write that would store `DrbdOptions/EncryptPassphrase`, `DrbdOptions/Encryption/passphrase` or `LuksPassphrase` (any case, on any object), and so does the bundled CLI's `set-property`: a property sits in plaintext on its CR. Deleting such a property is still allowed. This differs from upstream LINSTOR, where `linstor controller set-property DrbdOptions/EncryptPassphrase` is accepted.
- New headers use PBKDF2 at its minimum cost: the password is random, and Argon2id would cost seconds and up to a GiB per open after a reboot.
- Mapper name: `<rd>-<vol>-luks` → `/dev/mapper/<rd>-<vol>-luks`. Stable across reconciles so reopen on satellite restart re-uses it.
- A replica adopted from LINSTOR can still run on LINSTOR's mapper, `Linstor-Crypt-<rd>_<vol, five digits>`. The satellite keeps it, and points DRBD's disk at it, while `dmsetup table` shows one crypt target over the replica's own device; it never opens a second mapper over a device one already holds, nor closes one under DRBD. Deactivating, toggling to diskless or deleting the replica closes it after DRBD is down, and the next open is under blockstor's name.
- Volume grow: the satellite runs `cryptsetup resize` on the mapper after the storage layer has resized the underlying LV, before DRBD resizes the replicated device.

### Upgrading a cluster with LUKS volumes from the single cluster passphrase

Before this release every LUKS volume was formatted with the one cluster passphrase. Such a volume is not opened after the upgrade, and not re-formatted either: its replica carries no stored password, so the satellite reports `LUKSKeyMissing` and leaves the data on disk. Copy the data of every such volume to a volume created after the upgrade, then delete the old one. Remove any passphrase property left on the CRs (`linstor controller set-property DrbdOptions/EncryptPassphrase ""` and the like) so the plaintext value leaves the next backup.

## What blockstor doesn't yet support

- **Pluggable layer ordering**: the stack must be one of the four
  rows in the table above. Arbitrary orderings (e.g. `["LUKS","DRBD","STORAGE"]`
  with LUKS-over-DRBD instead of DRBD-over-LUKS) aren't yet validated
  or rendered.
- **Changing a volume's own password**: `linstor vd modify-passphrase` answers 400; a volume keeps the password it was formatted with. Re-keying a header (`luksChangeKey`) is a follow-up.
- **Per-RD key Secrets**: `ResourceDefinition.spec.encryption.passphraseSecretRef` exists in the CRD but is **not implemented**: nothing reads it, and the RD's replicas get their own passwords wrapped under the cluster master key like those of every other RD. A LUKS-layered RD that sets it gets an `EncryptionKeyIgnored` Condition on each of its Resources saying so:

  ```bash
  kubectl get resources.blockstor.org -o json \
    | jq -r '.items[] | select(.status.conditions[]?
        | select(.type=="EncryptionKeyIgnored" and .status=="True"))
        | .metadata.name'
  ```

  It is a Condition and not a refusal on purpose: the field has always been inert, so a live cluster may carry RDs that set it and run fine. Failing their applies would move working volumes to broken ones. Clear the field to silence the Condition.
- **Mid-stack changes**: editing `layerStack` after a Resource is
  active doesn't re-encrypt or unwrap existing data. Treat it as
  set-once-per-RD until migration support lands.

## Testing

- `pkg/api/v1/layer_stack_test.go` — RD → RG → default resolution.
- `pkg/satellite/reconciler_drbd_test.go::TestApplySkipsDRBDWhenLayerStackOmits`
  — `["STORAGE"]` produces no `.res` and no drbdadm.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLayersLUKS` — pin
  cryptsetup luksFormat + luksOpen run on first activation.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLUKSFailsWithoutPassphrase`
  — pin the explicit-error path.
