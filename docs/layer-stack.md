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
