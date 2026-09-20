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

## What LUKS in this stack actually protects

**At rest, on each node. Not the replication link.**

In `["DRBD","LUKS","STORAGE"]` the satellite hands DRBD the dm-crypt
mapper as its lower disk (`maybeLUKS` rewrites the device map before
`applyDRBD`, `pkg/satellite/reconciler.go`; `tests/e2e/drbd-luks-stack.sh`
asserts the `.res` `disk` line points at `/dev/mapper/<rd>-<vol>-luks`).
The mapper is the *plaintext* side of the crypt device. So:

- the pod writes plaintext into `/dev/drbdN`;
- DRBD replicates **plaintext** to its peers;
- each node encrypts independently on the way to its own LV/zvol, and
  only the backing device holds ciphertext.

Anyone who needs the replication traffic protected has to secure it
separately — the layer stack does not do it. Several comments in this
repository used to claim the opposite ("DRBD ships ciphertext between
peers"); they were wrong and are corrected.

## LUKS specifics

- The normal source is the **cluster** passphrase — the Secret
  `blockstor-cluster-passphrase` written by `linstor encryption
  create-passphrase`. One key per cluster, used verbatim as the
  `cryptsetup` passphrase for every encrypted volume.
- A per-RD override exists via `DrbdOptions/Encryption/passphrase`
  (the upstream `linstor rd set-property` key). ⚠️ It is a **plaintext
  string on the ResourceDefinition CRD** — readable by anyone with
  `get resourcedefinitions`, which is a wider audience than Secret
  RBAC. The REST read path redacts it; `kubectl get -o yaml` does not.

- **Precedence, and it is not what you would guess.** The dispatcher's
  `pickLUKSPassphrase` takes the first non-empty of:

  1. `DrbdOptions/EncryptPassphrase` — the cluster-scope key. Set
     either as a legacy controller property, or injected from the
     cluster Secret by the satellite.
  2. `DrbdOptions/Encryption/passphrase` — the per-RD key.

  So the per-RD override wins over the cluster **Secret** (the
  satellite skips the injection when either prop is already present),
  but **loses to the legacy controller property**. On a cluster where
  an operator has run `controller set-property
  DrbdOptions/EncryptPassphrase`, every per-RD key is silently
  overridden by the cluster one. Nothing warns about it.
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
  wrapping) is a follow-up — see `docs/byok-design.md`.
  `linstor vd set-passphrase` therefore returns a structured **501**
  and stores nothing. It previously answered 200 and wrote the
  operator's key in cleartext onto the RD, where no code path read it.

  ⚠️ **Keys written by those earlier releases are still in etcd.** The
  refusal stops new writes; it does not clean up old ones. Find the
  affected resource definitions with:

  ```bash
  kubectl get resourcedefinitions.blockstor.cozystack.io -o json \
    | jq -r '.items[]
        | select([.spec.volumeDefinitions[]?.props?["DrbdOptions/Encrypt/Passphrase"]] | any)
        | .metadata.name'
  ```

  Then drop the property from each listed volume definition. Treat any
  value found this way as disclosed and rotate it wherever else it is
  used — it was readable to every holder of `get resourcedefinitions`.
- **Per-RD key Secrets**: `ResourceDefinition.spec.encryption.passphraseSecretRef`
  exists in the CRD but is **not implemented** — nothing reads it, and
  the volume is encrypted with the cluster key. A LUKS-layered RD that
  sets it now gets an `EncryptionKeyIgnored` Condition on each of its
  Resources saying exactly that:

  ```bash
  kubectl get resources.blockstor.cozystack.io -o json \
    | jq -r '.items[] | select(.status.conditions[]?
        | select(.type=="EncryptionKeyIgnored" and .status=="True"))
        | .metadata.name'
  ```

  It is a Condition and not a refusal on purpose: the field has always
  been inert, so a live cluster may carry RDs that set it and run fine
  on the cluster key. Failing their applies would move working volumes
  to broken ones. Clear the field to silence the Condition.
- **Mid-stack changes**: editing `layerStack` after a Resource is
  active doesn't re-encrypt or unwrap existing data. Treat it as
  set-once-per-RD until migration support lands.
- **Cluster-passphrase rotation**: the cluster passphrase
  (`POST /v1/encryption/passphrase`) is used **verbatim** as the
  `cryptsetup` passphrase — it does not wrap a per-volume key. Rotating
  it therefore leaves every existing LUKS header locked with the old
  value, and the volumes stop opening at the next attach, node reboot
  or resize (mappers already open survive until then, so the cluster
  looks healthy in the meantime). `PUT /v1/encryption/passphrase`
  (`linstor encryption modify-passphrase`) now **refuses with 409**
  while any LUKS-layered RD exists; `force=true` (PUT body field or
  `?force=true`) overrides and strands them. Note the `linstor` CLI has
  no flag for it, so forcing needs a direct HTTP call.

  ⚠️ **That guard covers the REST verb only.** Two other paths reach
  the same stranding and are NOT guarded: repointing
  `ControllerConfig.spec.passphraseSecretRef` at a different Secret,
  and editing the Secret's contents with `kubectl` directly. Both are
  Kubernetes-door writes, and blockstor ships no admission webhook to
  intercept them. The fix that covers every door is the format-time key
  fingerprint in `docs/byok-design.md` §4 — until it exists, treat the
  cluster passphrase as write-once. Re-keying an existing
  volume needs `cryptsetup luksChangeKey`, which isn't wired —
  operators should drop the RD and recreate.
- **Adopting encrypted LINSTOR volumes**: not possible today. LINSTOR
  keys are per volume; blockstor has one cluster key. See
  `docs/byok-design.md` §5.

## Testing

- `pkg/api/v1/layer_stack_test.go` — RD → RG → default resolution.
- `pkg/satellite/reconciler_drbd_test.go::TestApplySkipsDRBDWhenLayerStackOmits`
  — `["STORAGE"]` produces no `.res` and no drbdadm.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLayersLUKS` — pin
  cryptsetup luksFormat + luksOpen run on first activation.
- `pkg/satellite/reconciler_drbd_test.go::TestApplyLUKSFailsWithoutPassphrase`
  — pin the explicit-error path.
