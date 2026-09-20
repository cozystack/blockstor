# BYOK (bring your own key) — design note

Status: **proposal, no decision taken.** Written to answer one question before any
BYOK code lands: *what does "your own key" mean on top of blockstor's current
single-cluster-master-key model?*

Audience: blockstor maintainers.

## 1. What BYOK actually asks for

The interesting deployments are not the ones without encryption — they are the
ones that already have it. An operator running storage classes that include the
LUKS layer already gets `DRBD,LUKS,STORAGE` volumes and data encrypted at rest.

What BYOK adds is the *key custody* half: the tenant, not the platform, holds the
key that opens the volume. Under upstream LINSTOR that needs either a patched
controller or a wrapper around it; blockstor is that wrapper, which is what makes
the feature tractable at all.

Two consequences follow, and they shape everything below:

1. **BYOK is a property of the key hierarchy, not of the cipher.** The volumes
   are already LUKS. Nothing about `cryptsetup` changes. What changes is who can
   derive the key that unlocks a given volume.
2. **BYOK is downstream of migration.** The deployments that want it have their
   encrypted volumes under LINSTOR today. A BYOK design that cannot adopt those
   volumes is not deliverable to them, whatever its merits. See §5.

## 2. What blockstor does today

One passphrase, cluster-wide, used **verbatim** as the `cryptsetup` passphrase
for every encrypted volume.

The chain, end to end:

| Step | Where |
|---|---|
| Operator stamps the value | `POST /v1/encryption/passphrase` → Secret `blockstor-cluster-passphrase`, key `passphrase` (`pkg/rest/encryption.go`) |
| Satellite resolves it | `injectLUKSMasterPassphrase` folds it into effective props under `DrbdOptions/EncryptPassphrase` (`pkg/satellite/controllers/luks_passphrase.go:67`) |
| Dispatcher lifts it to the wire | `wireProps["LuksPassphrase"]` (`pkg/dispatcher/dispatcher.go:255`) |
| Satellite uses it | `cryptsetup luksFormat/luksOpen --key-file -` with that exact byte string (`pkg/satellite/reconciler.go:1060`, `pkg/luks/luks.go`) |

A per-RD override exists via the `DrbdOptions/Encryption/passphrase` property,
which the dispatcher also accepts (`pickLUKSPassphrase`, first non-empty wins).
It is a plaintext string on the `ResourceDefinition` CRD.

**The master passphrase is the data-encryption key.** It is not a key-encryption
key, there is no per-volume key, and there is no wrapping step. This one fact is
the root of everything in §3.

### What the key protects — state this precisely

**At rest, on each node. Not the replication link.**

In `["DRBD","LUKS","STORAGE"]` the satellite hands DRBD the dm-crypt mapper as
its lower disk — `maybeLUKS` rewrites the volume→device map before `applyDRBD`
(`pkg/satellite/reconciler.go`), and `tests/e2e/drbd-luks-stack.sh` asserts the
rendered `.res` `disk` line points at `/dev/mapper/<rd>-<vol>-luks`. The mapper
is the *plaintext* side. So the pod writes plaintext into `/dev/drbdN`, DRBD
replicates **plaintext** to its peers, and each node encrypts on the way to its
own backing device.

Five comments across `pkg/validate`, `pkg/dispatcher`, `pkg/satellite`,
`pkg/rest` and the e2e suite claimed the opposite — that DRBD ships ciphertext.
They are corrected in this branch. It matters here because it decides what a BYOK
deployment can honestly be told: the key makes the disks unreadable if a node or
a drive is taken, and does nothing against a network attacker between nodes.
Anyone who needs the replication link protected has to secure it separately.

It also explains §5: since each node encrypts independently, nothing requires
peers to share a key — and LINSTOR duly gives each replica its own.

## 3. Three consequences of the current model, all confirmed in code

These are not BYOK feature gaps. They are defects that exist today, and any BYOK
design has to clear them first.

### 3.1 Rotating the master passphrase latently bricks every encrypted volume

`PUT /v1/encryption/passphrase` (`linstor encryption modify-passphrase`)
verifies the old value, writes the new one to the Secret, and returns
`200 Master passphrase modified` (`pkg/rest/encryption.go:521`).

Because the Secret value *is* the `cryptsetup` passphrase, every existing LUKS
header still holds the **old** key. On the next reconcile that needs to open a
mapper, `Cryptsetup.Open` fails with the new key, is correctly classified as not
`ErrAlreadyOpen`, and the apply bubbles an error
(`pkg/satellite/reconciler.go:1073`).

Precision matters here:

- **No data is destroyed.** `Cryptsetup.Format` probes `isLuks` first and no-ops
  on an already-formatted device (`pkg/luks/luks.go:47`), so there is no header
  rewrite and no key-slot wipe.
- **The damage is deferred and silent.** Mappers already open stay open — they
  are kernel dm-crypt mappings and survive a satellite pod restart. The cluster
  therefore looks healthy right up until a node reboots, a volume is attached
  elsewhere, or a resize fires (`Resize` also takes the key). Then the volume
  cannot be opened at all.
- The operator got a `200` and an encouraging success line while this was set up.

`docs/layer-stack.md` already documents rotation as unsupported ("operators
should drop the RD and recreate"). The API does not enforce what the doc says.

Under upstream LINSTOR the same command is safe, because there the master
passphrase only wraps stored per-volume keys — rotation re-wraps them and never
touches a LUKS header. blockstor inherited the verb without the model that makes
it safe.

### 3.2 `ResourceDefinition.spec.encryption.passphraseSecretRef` is declared, documented, and never read

The CRD field exists (`api/v1alpha1/drbd_options.go:180`). Its description —
shipped in `config/crd/bases/blockstor.cozystack.io_resourcedefinitions.yaml` —
promises: *"The satellite reads it via the apiserver during reconcile; the
passphrase never lands on Spec in plaintext."*

Nothing reads it. The only non-test code that touches
`Spec.Encryption` at all is `pkg/store/k8s/resource_definitions.go:149,272`,
which carefully **preserves** the field across updates (Bug 209) so it is not
wiped — a field protected from loss and never consumed.

The failure mode is the worst available one: an operator who sets a per-RD key
Secret gets an accepted object, no error on either door, and a volume encrypted
with the **cluster** key. The BYOK promise is silently false rather than loudly
unimplemented.

### 3.3 `linstor vd set-passphrase` writes the operator's key in plaintext into a CRD and drops it

`PUT /v1/resource-definitions/{rd}/volume-definitions/{n}/encryption-passphrase`
stamps the supplied key into `VolumeDefinition.Props["DrbdOptions/Encrypt/Passphrase"]`
and returns `200 VD passphrase stored; cluster-side LUKS rotation pending Phase 12`
(`pkg/rest/vd_passphrase_bug_233.go:94`).

Volume definitions are inline in `ResourceDefinition.spec.volumeDefinitions`, so
the key lands in cleartext in etcd, reachable by anyone with `get
resourcedefinitions`. That RBAC surface is materially wider than Secret RBAC,
and in blockstor the CRDs *are* the store — a GitOps controller or a `kubectl
get -o yaml` reads it directly.

Mitigating, and worth stating exactly: the REST read path **does** redact it.
`DrbdOptions/Encrypt/Passphrase` matches the `encrypt` substring in
`sensitivePropSubstrings` (`pkg/rest/security_props_redaction.go`), and VD props
are scrubbed on read (`pkg/rest/volume_definitions.go:187,263`). So this is not a
REST disclosure. It is an at-rest disclosure through the Kubernetes door.

And the prop key it writes (`DrbdOptions/Encrypt/Passphrase`) is not one of the
two keys the dispatcher lifts (`DrbdOptions/EncryptPassphrase`,
`DrbdOptions/Encryption/passphrase`). It is never consumed. The volume keeps the
cluster key.

Note also that this verb exists on the REST door only; `internal/cli` implements
`encryption create-passphrase` / `enter-passphrase` and no per-VD equivalent.
Today that asymmetry is harmless because the REST verb is a no-op. It becomes a
two-doors divergence the moment the feature is real.

## 4. The design question

Four readings of "your own key", from weakest to strongest. They are not
alternatives on a spectrum of effort so much as different promises to the
operator, and picking one is the decision this note exists to force.

**(A) Operator-supplied cluster key.** The single master passphrase is supplied
by the operator rather than generated by the platform. This is essentially what
exists today — `create-passphrase` already takes an operator value. It gives the
tenant nothing a platform-generated key does not: one key still opens every
volume in the cluster, and the platform still holds it in a Secret it can read.
Not worth calling BYOK.

**(B) Per-tenant key.** One key per Cozystack tenant, held in a Secret in the
tenant's namespace. Tenant isolation becomes real: a compromised tenant key does
not open another tenant's volumes. Requires the key hierarchy of (C) to be safe
to rotate, and requires deciding what happens to a volume when a tenant key is
withdrawn.

**(C) Per-volume data key wrapped by a key-encryption key.** The upstream LINSTOR
model, and the one our own migration model already assumes LINSTOR uses
(`pkg/linstormigrate/model.go:221`: "the per-volume LUKS passphrase encrypted
with the LINSTOR master key"). Each volume gets a random data key; the KEK wraps
it; only the wrapped form is stored. Rotation re-wraps and never touches a LUKS
header, so §3.1 stops being a data-availability cliff. BYOK then means: the
tenant supplies the KEK.

**(D) External KMS / no platform-held key.** The KEK never rests in the cluster;
unlocking calls out to an external key manager (OpenBAO, Vault, a cloud KMS).
This is the only reading under which the platform genuinely cannot decrypt tenant
data — and correspondingly the only one that puts a network dependency in the
volume-attach path, which for a storage system is a serious operational
commitment.

**Recommendation: build (C) as the mechanism, expose (B) as the product surface,
and keep (D) as a pluggable KEK provider behind the same interface.**

Reasoning: (C) is not optional. It is forced by §3.1 — without a wrapped
per-volume key, key rotation cannot be made safe, and a storage feature whose
key cannot be rotated is not a security feature. (C) is also forced by §5:
LINSTOR already keeps a distinct key per replica, and a model with one key per
cluster cannot represent them.

Worth noting that (C) was always the intent here. This repository's own scenario
spec describes the rotation verb as *"old passphrase decrypts existing wrapped
keys, new passphrase re-wraps and rewrites the cluster Secret ... atomic — if any
decrypt fails, the whole rotation rolls back"*
(`tests/scenarios/wave2-06-storage-backends.md` §6.W14). That is exactly option
(C). What shipped was the verb without the key hierarchy underneath it — which is
precisely why §3.1 exists. So this is less a new design than finishing one that
was specified and then collapsed. Once (C) exists, (B) is a policy question about
where the KEK Secret lives, and (D) is one implementation of "fetch the KEK".

The two fields that already exist —
`ResourceDefinition.spec.encryption.passphraseSecretRef` and
`ControllerConfig.spec.passphraseSecretRef` — are the right shape for (B)/(C).
They should be wired, not removed.

### The safety property that any implementation must carry

Whatever the hierarchy, **the identity of the key a volume was formatted with
must be pinned at format time, and a change must be refused loudly rather than
attempted.** This does not exist today, and it is why §3.2 cannot simply be
"wired" as a one-liner: making `passphraseSecretRef` take effect on an RD that
was already formatted under the cluster key would move the volume from a working
state to an unopenable one, which is exactly the §3.1 failure with a new trigger.

Concretely: store a non-reversible fingerprint of the resolved key (not the key)
on the `Resource` status at format time, compare on every subsequent apply, and
fail the apply with a distinct, actionable condition on mismatch. This is
cheaper than it sounds and it converts the entire class from "silent brick" to
"loud refusal".

## 5. The migration interlock — why this is blocked on a live encrypted cluster

`pkg/linstormigrate/convert.go:1190` already knows the shape of the problem. On
any RD with a LUKS layer the converter emits:

> `resource definition %s: LUKS passphrase NOT migrated (encrypted with the LINSTOR master key) — provision spec.encryption manually before adopting`

Two things about that warning.

First, it points the operator at `spec.encryption` — the field of §3.2, which
does nothing. Following the advice produces a volume encrypted with the cluster
key, not the one the operator provisioned. The warning is worse than no warning.

Second, the blocker that binds for every cluster shape, and the one the converter
itself states first: **LINSTOR's stored keys are wrapped with its master key and
this converter implements no unwrap.** Whatever granularity blockstor could
express, the migration cannot recover the plaintext key material to put there.
That alone makes adoption impossible today, and it is the piece that has to be
built.

Granularity is the second constraint, and it binds less universally than an
earlier draft of this note claimed. LINSTOR keeps a distinct stored secret **per
replica**, and blockstor has exactly one for the whole cluster — but the per-RD
property (§2) can already express one key per resource definition, which is
enough for the one-volume-per-RD shape the converter's own fixtures use. So
granularity is not what blocks a single-volume RD; the unwrap is. Granularity
becomes binding for multi-volume RDs and for any per-replica key material.

That is measured, not assumed. `LAYER_LUKS_VOLUMES` is keyed by
`(layer_resource_id, vlm_nr)`, and `layer_resource_id` resolves to a
`(node, resource)` pair — so there is one row per replica per volume number. The
migration fixture in `pkg/linstormigrate/testdata/dump` carries exactly that
shape: `PVC-VOL3` volume 0 has two LUKS rows, id 9 on `NODE-A` and id 12 on
`NODE-B`, with **different** `encrypted_password` values.

Different keys per replica is not an accident of the fixture — it follows from
§2's threat model. Because DRBD sits above LUKS and replicates plaintext, each
node encrypts its own copy independently, so nothing requires the peers to share
a key. LINSTOR gives each its own; blockstor gives them all the cluster's.

So adoption needs two things, in this order: an unwrap of LINSTOR's stored key
material, and somewhere in blockstor to put the result — which for anything
beyond one volume per RD means the per-volume keys of §4.

What remains genuinely unverified: that the stored value is wrapped with the
master key via LINSTOR's KDF at all. That claim comes from our own converter's
comment (`pkg/linstormigrate/convert.go`), not from LINSTOR source. Since it is
the load-bearing blocker, confirming it against LINSTOR's implementation is the
first task of the migration work, not a footnote.

This is the concrete, technical reason BYOK is blocked on migrating a live
encrypted LINSTOR cluster — not merely a sequencing preference. The migration and
the BYOK design are the same piece of work approached from two ends:

- adopting LINSTOR's encrypted volumes **requires** per-volume keys, i.e. (C);
- (C) is exactly the mechanism BYOK needs.

Doing (C) first and migration second means writing the unwrap path twice. Doing
the migration on a cluster with no encrypted volumes proves nothing about the
case that matters.

Three things have to be settled before this can be scoped honestly:

1. A blockstor migration of a **non-production** cluster that actually carries
   encrypted volumes.
2. How many encrypted volumes it has and on which storage classes, so the unwrap
   path is exercised on real key material rather than a synthetic fixture.
3. Whether the target is (B) per-tenant keys or (D) externally-held KEKs, since
   that decides whether the KEK provider interface needs a network client in the
   attach path.

## 6. Interim posture taken in this branch

No BYOK mechanism is implemented here — the hierarchy decision in §4 is still
open. What this branch does is stop the three surfaces above from lying, so that
whoever implements §4 starts from loud errors instead of silent fallbacks:

- rotation that would strand encrypted volumes is refused instead of returning
  `200`;
- a LUKS-layered RD naming `spec.encryption.passphraseSecretRef` gets an
  `EncryptionKeyIgnored` Condition on its Resources, and the CRD description now
  says NOT IMPLEMENTED, instead of both promising the field works and quietly
  using the cluster key;
- `vd set-passphrase` returns a structured `501` instead of persisting the
  operator's key in cleartext and dropping it.

Two things worth stating plainly about what that does and does not cover.

**The per-RD key signal is a Condition, not a refusal.** An earlier revision
failed the apply. That wedges resource definitions which
reconcile correctly today — the field has always been inert, so a live cluster
may well carry RDs that set it and run fine on the cluster key — and that the
wedge left nothing on the object explaining itself, because the error goes to
controller-runtime's backoff rather than to Status. Moving a working volume to a
broken one to punish a misleading field is the wrong trade in a storage system.
The silence was the defect; the Condition ends it without the collateral.

**The rotation guard closes the REST verb only.** Two other paths reach the same
stranding and are not guarded: repointing
`ControllerConfig.spec.passphraseSecretRef` at another Secret, and editing the
Secret's contents with `kubectl`. Both are Kubernetes-door writes, nothing in
`pkg/rest` or `internal/cli` can set them, and blockstor ships no admission
webhook — adding one would mean introducing the project's first, with the cert
plumbing that implies, to cover a path the real fix covers anyway. That real fix
is the format-time key fingerprint below, which sits in the apply path every door
must pass through.

**The fingerprint, concretely.** At first successful format, stamp a
non-reversible fingerprint of the key actually used onto the Resource status. On
every later apply, compare. On mismatch, fail with a specific, actionable
condition instead of letting `cryptsetup luksOpen` fail with a generic error
several layers down. This costs little, covers every door including the two
above, and is a prerequisite for honouring `passphraseSecretRef` at all — since
taking that field into account on an already-formatted RD would itself move a
working volume to an unopenable one.

Each of these is a one-line flip once the hierarchy lands. All three are
deliberate divergences from upstream LINSTOR and are recorded in
`docs/cli-parity-known-deltas.md`.

## See also

- `docs/layer-stack.md` — the LUKS layer as it exists today.
- `docs/linstor-migration.md` — adoption of a live LINSTOR cluster.
- `pkg/linstormigrate/convert.go` — the converter, including the LUKS warning.
