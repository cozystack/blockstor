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

The default is `["DRBD","STORAGE"]` — when `RD.Spec.LayerStack` is
empty the controller inherits from the parent
`ResourceGroup.Spec.SelectFilter.LayerStack`, and when both are empty
the dispatcher falls through to the satellite's default-DRBD path.

## Setting the stack

### Per-RD via REST

```bash
curl -XPOST http://controller:3370/v1/resource-definitions \
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

## What blockstor doesn't yet support

- **Pluggable layer ordering**: the stack must be one of the four
  rows in the table above. Arbitrary orderings (e.g. `["LUKS","DRBD","STORAGE"]`
  with LUKS-over-DRBD instead of DRBD-over-LUKS) aren't yet validated
  or rendered.
- **Per-volume LUKS keys**: every LUKS volume in the cluster uses the
  cluster master passphrase. `linstor vd set-passphrase` answers 400:
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
