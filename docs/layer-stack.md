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
linstor encryption create-passphrase --passphrase '<32-byte-secret>'
linstor rg create encrypted-rg
linstor rg c --place-count 2 encrypted-rg
linstor rg spawn encrypted-rg pvc-2 1G --layer-list LUKS DRBD
```

(blockstor accepts `linstor`'s `--layer-list` flag verbatim because
the REST shape mirrors upstream LINSTOR.)

### Per-RD via kubectl

The cluster passphrase Secret has to exist first; `linstor encryption
create-passphrase` writes it, or apply it yourself (GitOps):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: blockstor-cluster-passphrase   # or ControllerConfig spec.passphraseSecretRef.name
  namespace: blockstor-system          # the controller's namespace
type: Opaque
stringData:
  passphrase: "32-byte-secret"
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: pvc-encrypted}
spec:
  layerStack: ["LUKS", "STORAGE"]
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 1048576}
```

## LUKS specifics

- The passphrase is the cluster master passphrase, and it lives only in
  the Secret `linstor encryption create-passphrase` writes
  (`blockstor-cluster-passphrase` in the controller's namespace, unless
  ControllerConfig `spec.passphraseSecretRef` names another). Creating
  an RD with `LUKS` in its stack is refused until that Secret exists.
- A passphrase is never read from a property. The REST API answers 400
  to any write that would store `DrbdOptions/EncryptPassphrase`,
  `DrbdOptions/Encryption/passphrase` or `LuksPassphrase` (any case, on
  any object), and so does the bundled CLI's `set-property`: a property
  sits in plaintext on its CR, so any dump of the CRs would carry the
  key to every LUKS volume. Deleting such a property is still allowed.
  This differs from upstream LINSTOR, where `linstor controller
  set-property DrbdOptions/EncryptPassphrase` is accepted.
- The satellite reads the Secret, drops any passphrase property that
  reached a definition's props (one put there with kubectl, say), and
  hands the Secret's value to the dispatcher, which puts it on the wire
  as `DesiredResource.Props["LuksPassphrase"]` for the LUKS layer.
- No passphrase with `LUKS` in the stack fails the apply rather than
  silently producing an unencrypted volume.
- Mapper name: `<rd>-<vol>-luks` → `/dev/mapper/<rd>-<vol>-luks`. Stable
  across reconciles so reopen on satellite restart re-uses it.
- Volume grow: the satellite runs `cryptsetup resize` on the mapper
  after the storage layer has resized the underlying LV, before DRBD
  resizes the replicated device.

### Upgrading a cluster that kept the passphrase in a property

Before this release a passphrase property won over the Secret: `DrbdOptions/EncryptPassphrase` on the controller, or `DrbdOptions/Encryption/passphrase` on a group or a definition. LUKS volumes created while one was set were formatted with its value. From this release on the satellite opens them with the Secret's value only, and logs `ignoring LUKS passphrase properties` with the keys it dropped. Do this before the upgrade.

1. Find the properties. Their values are redacted on the REST API, so read the CRs:

   ```sh
   kubectl get controllerconfigs.blockstor.cozystack.io -o jsonpath='{.items[*].spec.extraProps}'
   kubectl get resourcegroups.blockstor.cozystack.io,resourcedefinitions.blockstor.cozystack.io -o json \
     | jq -r '.items[] | select(.spec.props // {} | keys | any(test("passphrase|Passphrase"))) | .metadata.name'
   ```

2. Make the Secret open every LUKS volume:
   - **A controller property and no Secret**: run `linstor encryption create-passphrase` with the property's exact value.
   - **A controller property set from the start, and a Secret with another value**: no LUKS volume was formatted with the Secret's value, so overwrite the Secret with the property's value. The API refuses this (create answers 409 because a passphrase exists, modify answers 409 because LUKS volumes exist), so do it with kubectl:

     ```sh
     kubectl -n blockstor-system create secret generic blockstor-cluster-passphrase \
       --from-literal=passphrase='<the property value>' --dry-run=client -o yaml | kubectl apply -f -
     ```

   - **Per-definition or per-group values, or a Secret that some volumes were formatted with**: the volumes use different keys, so no single Secret value opens them all. Add the Secret's value to each LUKS device that was formatted with another key, on every node with a replica, with `cryptsetup luksAddKey <device>`; it asks for the key the device has now.

3. Delete the properties, so the plaintext value leaves the CRs and the next backup:

   ```sh
   linstor controller set-property DrbdOptions/EncryptPassphrase ""
   linstor resource-group set-property <group> DrbdOptions/Encryption/passphrase ""
   linstor resource-definition set-property <definition> DrbdOptions/Encryption/passphrase ""
   ```

A volume missed in step 2 fails to open on the next satellite restart or mapper close; the satellite log line names the property it was formatted with.

## What blockstor doesn't yet support

- **Pluggable layer ordering**: the stack must be one of the four
  rows in the table above. Arbitrary orderings (e.g. `["LUKS","DRBD","STORAGE"]`
  with LUKS-over-DRBD instead of DRBD-over-LUKS) aren't yet validated
  or rendered.
- **Per-volume LUKS keys**: every LUKS volume in the cluster uses the
  cluster master passphrase. `linstor vd modify-passphrase` answers 400:
  it used to store the value in the volume definition's properties,
  in plaintext, where nothing read it. Per-volume keys (with master-key
  wrapping) are a follow-up.
- **Mid-stack changes**: editing `layerStack` after a Resource is
  active doesn't re-encrypt or unwrap existing data. Treat it as
  set-once-per-RD until migration support lands.
- **Cluster-passphrase rotation**: the cluster passphrase is the LUKS
  key itself, not a wrapping key. `linstor encryption
  modify-passphrase` (`PUT /v1/encryption/passphrase`) rewrites the
  Secret, and nothing runs `cryptsetup luksChangeKey` on the existing
  headers, so volumes formatted with the old value would stop opening.
  It answers 409 while any definition has LUKS in its stack, naming
  one of them; rotate only once there are none.

## Testing

- `pkg/api/v1/layer_stack_test.go` — RD → RG → default resolution.
- `pkg/satellite/reconciler_drbd_test.go::TestApplySkipsDRBDWhenLayerStackOmits`
  — `["STORAGE"]` produces no `.res` and no drbdadm.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLayersLUKS` — pin
  cryptsetup luksFormat + luksOpen run on first activation.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLUKSFailsWithoutPassphrase`
  — pin the explicit-error path.
