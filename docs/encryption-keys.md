# Per-volume LUKS keys under a master key with several passphrases (BYOK stage 1)

Status: stage 1 (section 14, PRs 1 to 5) is implemented; the optional volume re-key (PR 6) and stage 2 are not. Written against `cozystack/blockstor` `origin/main` at `e37f3320` (v0.2.1), PR #192 at `9ec5e81e`, `LINBIT/linstor-server` v1.33.1 and `piraeusdatastore/piraeus-operator` at `4036bcc`.

Every choice where another option was reasonable is marked **Decision** and names the option taken and why. The list is collected in section 15.

Three things are given, not open:

- the scheme is LINSTOR's: a random password per LUKS volume, stored wrapped under a master key, with several master passphrases able to unlock that key;
- **this is a breaking change with no migration for blockstor's own LUKS volumes.** A volume that blockstor formatted with the single cluster passphrase is not supported after the change: it will not be opened, and it will not be re-formatted either. The assumption is that nobody runs LUKS on blockstor today. Anyone who does copies the data to a volume created after the change. The Secret layout, the REST semantics and the CLI semantics change wherever LINSTOR compatibility allows;
- the master passphrase can be supplied the way piraeus supplies it, from a Secret with the key `MASTER_PASSPHRASE`, and blockstor unlocks from it unattended. That Secret is input only; blockstor never writes it.

## 1. What the operator gets

- Every LUKS replica has its own random password, and it is the only key in its LUKS header. No passphrase opens a disk directly.
- Changing, adding or removing a master passphrase is one Secret update. No volume is re-keyed, no disk is touched, no satellite restarts.
- Several passphrases can unlock the cluster at once: the one the cluster starts from, plus recovery passphrases kept offline. Losing one is not losing the data.
- A cluster migrated from piraeus keeps its passphrase Secret as it is. The LINSTOR master passphrase becomes one of blockstor's master passphrases, and the LUKS volumes move over without `luksAddKey`, without reading keys out of `dmsetup`, and without a data copy.
- The linstor python client keeps working: `create-passphrase`, `enter-passphrase` and `modify-passphrase` keep their paths and bodies.

What it does not give, stated up front: stage 1 does not take the key away from the platform. Whoever can read the passphrase Secret and the keyring Secret can open every volume, exactly as with piraeus. That is stage 2 (section 13).

## 2. Where we start

### 2.1 blockstor today

The cluster passphrase is used verbatim as the `cryptsetup` passphrase of every LUKS volume. All of this is replaced.

| Step | Where |
|---|---|
| Secret `blockstor-cluster-passphrase`, key `passphrase`, name overridable through `ControllerConfig.spec.passphraseSecretRef` | `pkg/passphrase/passphrase.go` (`DefaultSecretName`, `SecretKey`, `SecretName`, `Read`) |
| REST `POST/PATCH/PUT/GET /v1/encryption/passphrase` writes and compares that Secret | `pkg/rest/encryption.go` (`handlePassphraseCreate`, `handlePassphraseEnter`, `handlePassphraseModify`, `handlePassphraseStatus`) |
| `PUT` (modify) is refused with 409 while any RD has LUKS in its stack | `pkg/rest/encryption.go` `refuseRotationOverLUKSVolumes` |
| `blockstor encryption create-passphrase` / `enter-passphrase` talk to the Kubernetes API directly, not to REST | `internal/cli/encryption.go` |
| The satellite itself reads the Secret on every reconcile of a LUKS RD and folds it into the props | `pkg/satellite/controllers/luks_passphrase.go` `injectLUKSMasterPassphrase` |
| The dispatcher lifts it onto the wire prop `LuksPassphrase` | `pkg/dispatcher/dispatcher.go` `pickLUKSPassphrase` |
| The LUKS layer formats, opens and resizes with it over stdin (`--key-file -`) | `pkg/satellite/reconciler.go` `applyLUKS`, `pkg/luks/luks.go` |
| Passphrase properties on CRs are refused and dropped (since #222) | `pkg/passphrase` `IsPlaintextPropKey`, `DropPlaintextProps` |

Three facts shape the design:

1. **There is no controller-to-satellite channel.** Satellites are controller-runtime managers that read CRs and the Secret straight from the API server. "What the satellite receives" is a question of which objects it may read, not of a message.
2. **Satellite RBAC on Secrets is cluster-wide.** `config/rbac/satellite_role.yaml` is a ClusterRole bound with a ClusterRoleBinding and grants `get/list/watch` on all Secrets. The apiserver has a namespaced Role with `get/create/update/patch` (`stand/blockstor-apiserver-deploy.yaml`).
3. **Clones and restores copy bytes below LUKS.** `zfs clone`, a thin snapshot or `zfs send | recv` moves the backing device with its LUKS header (`pkg/rest/rd_clone.go` `cloneLayerStackIsHonourable`, `docs/layer-stack.md`). The copy opens with the source's password. Upstream LINSTOR clones through the opened devices with `dd` and gives the clone a new password; blockstor does not.

Backup shipping to a remote is not implemented (`POST /v1/remotes/{remote}/backups/ship` answers 501, `pkg/rest/remotes.go`).

### 2.2 How piraeus supplies the passphrase

Paths are relative to `piraeus-operator`.

| Piece | Value | Source |
|---|---|---|
| Field | `LinstorCluster.spec.linstorPassphraseSecret`, a Secret name (a string, not an object reference) | `api/v1/linstorcluster_types.go:71-78` |
| Secret | in the operator's namespace, with the entry `MASTER_PASSPHRASE` | `docs/reference/linstorcluster.md:145-152` |
| Delivery | a patch adds env `MASTER_PASSPHRASE` from `secretKeyRef {name, key: MASTER_PASSPHRASE}` to the `linstor-controller` container | `pkg/resources/cluster/patches/passphrase.yaml`, `internal/controller/linstorcluster_controller.go:398-399` |
| LINSTOR reads it | env `MASTER_PASSPHRASE`, no `LS_` prefix | `linstor-server` `controller/src/main/java/com/linbit/linstor/core/cfg/CtrlEnvParser.java:12,32` |
| On controller start | no `encrypted/*` props yet: generate a master key and store it wrapped under this passphrase. Props present: decrypt the master key with it and hand it to the satellites. | `linstor-server` `controller/src/main/java/com/linbit/linstor/systemstarter/PassphraseInitializer.java:63-98` |

So the contract is: a Secret in the storage namespace, one key `MASTER_PASSPHRASE`, the value taken byte for byte as UTF-8, read at start, never written by LINSTOR or by the operator. The first start with it creates the master key; every later start unlocks with it. Cozystack's cluster has it as `cozy-linstor/linstor-passphrase`.

One consequence the piraeus setup already has: `linstor encryption modify-passphrase` changes the database and leaves the Secret holding the old value, so the next controller start cannot unlock until someone updates the Secret. Section 7 does better than that.

### 2.3 LINSTOR's scheme, verified against source

Paths are relative to `linstor-server` at v1.33.1.

| Piece | Value | Source |
|---|---|---|
| Master key | 16 random bytes | `controller/src/main/java/com/linbit/linstor/core/apicallhandler/controller/helpers/EncryptionHelper.java:67,142-145` |
| Passphrase KDF | PBKDF2-HMAC-SHA1, 5000 iterations, 128-bit key | `jclcrypto/src/main/java/com/linbit/linstor/modularcrypto/JclSymmetricKeyCipher.java:41,44,156-160` |
| Salt | 16 random bytes | `EncryptionHelper.java:68,177` |
| Cipher, for both the master key and volume passwords | AES-128 CFB, no padding, random 16-byte IV prepended to the ciphertext, **no MAC** | `JclSymmetricKeyCipher.java:33-38,245-256,329-339` |
| Length padding before encryption | 2-byte big-endian length, data, random fill to a multiple of 32 | `jclcrypto/.../LengthPaddingImpl.java:19,48-63,73-88` |
| Stored controller props | `encrypted/masterkey` (IV + ciphertext, 48 bytes), `encrypted/passphrasesalt`, `encrypted/masterhash`, all standard base64 | `EncryptionHelper.java:63-66,177-191`, `server/src/main/java/com/linbit/utils/Base64.java:5-6` |
| Props instance | `/CTRL`; `/CTRLCFG` before the 2024-10-24 migration | `server/.../api/prop/LinStorObject.java:10`, `server/.../dbdrivers/migration/MigrationUtils_SplitSnapProps.java:18` |
| Passphrase check | SHA-512 over the decrypted, still padded 32-byte block, compared with `masterhash` | `EncryptionHelper.java:237-257` |
| Volume password | base64 text of 20 random bytes (28 ASCII characters); 24 characters for a clone; arbitrary when set by the user | `controller/.../layer/resource/RscLuksLayerHelper.java:69,302-318`, `CtrlRscDfnApiCallHandler.java:1123` |
| `ENCRYPTED_PASSWORD` | base64 of `IV(16) + AES-CFB(len + password + fill)` under the master key; one row per replica per volume | `controller/.../core/objects/LayerLuksVlmDbDriver.java:57-60,79` |
| Replicas | each replica gets its own password unless the volume definition carries a passphrase prop | `RscLuksLayerHelper.java:302-312` |
| What the satellite receives | the **plaintext master key** plus the encrypted passwords; it decrypts them itself | `server/proto/javainternal/c2s/MsgIntCryptKey.proto:8-11`, `satellite/.../apicallhandler/StltCryptApiCallHelper.java:167-170` |
| cryptsetup | password plus `\n` on stdin, no `--key-file` | `satellite/.../layer/luks/CryptSetupCommands.java:85-102,152-171` |
| `modify-passphrase` | re-encrypts **only the master key** under the new passphrase; volume passwords untouched | `controller/.../CtrlConfApiCallHandler.java:1833-1841` |
| Snapshot restore | reuses the snapshot's password | `RscLuksLayerHelper.java:426-431` |
| Backup restore | recovers the source master key from the source passphrase, re-encrypts every volume password under the local master key; the LUKS header is not changed | `controller/.../CtrlBackupRestoreApiCallHandler.java:705-727,796-813` |
| REST | `POST {"new_passphrase"}`, `PUT {"new_passphrase","old_passphrase"}`, `PATCH` with a bare JSON string, `GET` status | `controller/.../api/rest/v1/Encryption.java:49-147` |

Not verified from source and to be closed with a test vector from a real controller (section 14): the JDK's handling of non-ASCII passphrase bytes in `PBEKeySpec` (each byte is sign-extended to a `char`, `JclSymmetricKeyCipher.java:150-154`, then UTF-8 encoded by the JDK), and that `AES/CFB/NoPadding` is CFB with a 128-bit segment. Both are JDK behaviour, not repository code.

We take LINSTOR's shape (random master key, wrapped per-volume passwords, passphrase change re-wraps only the master key) and none of its primitives: PBKDF2-SHA1 at 5000 iterations and unauthenticated AES-128-CFB are not something to carry into new code. They live only in the importer.

## 3. Key hierarchy

```
passphrase 1 ──Argon2id──> KEK 1 ──AES-256-GCM──┐
passphrase 2 ──Argon2id──> KEK 2 ──AES-256-GCM──┼──> master key MK (32 random bytes)
passphrase N ──Argon2id──> KEK N ──AES-256-GCM──┘        │ HKDF-SHA256
                                                         ├──> key ID (8 bytes, public)
                                                         └──> wrap key WK (32 bytes)
                                                                  │ AES-256-GCM
                                              volume password of replica (node, RD, volume)
                                                                  │ cryptsetup
                                                          LUKS header key slot 0
```

| Element | Choice | Why |
|---|---|---|
| Master key | 32 bytes from `crypto/rand` | Never derived from a passphrase, so passphrases can change without it changing. |
| Slot KDF | Argon2id, `time=3`, `memory=64 MiB`, `parallelism=4`, 16-byte random salt, 32-byte output; parameters stored per slot | RFC 9106's second recommended parameter set. It is what LUKS2 itself uses for passphrases. 64 MiB fits a satellite with a 512 Mi limit (`stand/blockstor-satellite-daemonset.yaml`), and the derivation runs once per process, not per volume. Stored parameters let the cost be raised later slot by slot. |
| Slot AEAD | AES-256-GCM, 12-byte random nonce, AAD `blockstor.org/master-key-slot/v1` + slot ID | Standard library, FIPS-approved, and the tag doubles as the passphrase check: no separate verification hash like LINSTOR's `masterhash`. |
| Sub-keys | HKDF-SHA256 over MK with info `blockstor.org/key-id/v1` and `blockstor.org/volume-key-wrap/v1` | The key ID can be published without revealing MK. Stage 2 can add purposes without touching stored data. |
| Volume password wrap | AES-256-GCM under WK, 12-byte random nonce, AAD `blockstor.org/luks-volume-key/v1` + key ID | Random nonces are safe here: one key wraps at most millions of passwords, far below the 2^32 bound for random GCM nonces. |
| Plaintext framing | 2-byte big-endian length, password, zero fill to a multiple of 64 bytes | Hides the length of an imported, user-chosen volume password. |
| Volume password | 32 random bytes, encoded base64url without padding (43 ASCII characters) | 256 bits of entropy; ASCII so that an operator can type it into `cryptsetup` in a recovery shell. |
| Passphrase bytes | the Secret value or the request string as UTF-8, verbatim, no trimming | Matches LINSTOR (`getBytes(UTF_8)`); a trailing newline in the Secret is part of the passphrase there too. |

The AAD deliberately does not name the node or the resource definition. Clones, restores and the importer copy a wrapped password from one object to another without holding MK; binding the blob to an object name would force every such copy through an unlocked component. An attacker who can write CRs can already delete a replica, and gains nothing readable by swapping blobs.

**Decision (D7): KDF.** Argon2id (chosen) or PBKDF2-HMAC-SHA256 at 600 000 iterations for a FIPS-only build. Argon2id needs `golang.org/x/crypto`, which is not in `go.mod` today. Chosen: Argon2id as the only implemented KDF, with the `kdf.type` field in the slot so PBKDF2 can be added if a FIPS deployment asks. cryptsetup on the nodes already uses Argon2id, so a PBKDF2-only blockstor would not make the stack FIPS-clean anyway.

**Decision (D9): LUKS header KDF for new volumes.** With a 256-bit random password the header's own password KDF protects nothing, yet cryptsetup's default Argon2id costs about two seconds and up to 1 GiB per open, inside the satellite's cgroup, for every volume after a node reboot. Options: keep the defaults; cap memory as LINSTOR does (`--pbkdf-memory 262144`); or format new volumes with `--pbkdf pbkdf2 --pbkdf-force-iterations 1000`. Chosen: the last one. Imported volumes keep the header they have. This one is easy to defer.

## 4. Storage and RBAC

### 4.1 The keyring: blockstor's own slots

A Secret `blockstor-master-key` in the blockstor namespace, type `blockstor.org/master-key`, one data key `keyring.json`. blockstor owns it and is the only writer.

```json
{
  "version": 1,
  "keyID": "3f9a1c0e5b7d2a44",
  "createdAt": "2026-11-14T10:00:00Z",
  "slots": [
    {
      "id": "a41c09d2",
      "name": "secret",
      "origin": "Secret",
      "createdAt": "2026-11-14T10:00:00Z",
      "kdf": {"type": "argon2id", "salt": "<b64 16>", "time": 3, "memoryKiB": 65536, "parallelism": 4},
      "aead": "aes-256-gcm",
      "nonce": "<b64 12>",
      "wrappedKey": "<b64 48>"
    }
  ]
}
```

`origin` is `Secret` (enrolled from the passphrase Secret), `API` (given through REST or the CLI) or `Generated` (section 6.2). One object means every slot operation is one atomic update guarded by `resourceVersion`. At most 8 slots, as in LUKS1; each verification attempt costs one Argon2id run per slot.

**Decision (D6): where the keyring lives.** A dedicated Secret (chosen), or a field on `ControllerConfig`, or a new CRD. The slots contain no key, but they are what an attacker needs to guess a weak passphrase offline, so they deserve Secret RBAC and Secret encryption at rest. A CR would also put them into every `kubectl get -o yaml` dump of the blockstor objects.

### 4.2 The passphrase Secret: piraeus's shape, read-only

`ControllerConfig.spec.passphraseSecretRef` names it. The reference gains an optional `key`, default `MASTER_PASSPHRASE`:

```yaml
apiVersion: v1
kind: Secret
metadata: {name: linstor-passphrase, namespace: <blockstor namespace>}
data:
  MASTER_PASSPHRASE: <base64>
---
apiVersion: blockstor.org/v1alpha1
kind: ControllerConfig
spec:
  passphraseSecretRef: {name: linstor-passphrase}   # key defaults to MASTER_PASSPHRASE
```

blockstor reads the Secret **by reference, through the API**, in every component that needs the key. The chart does not map it into an environment variable the way piraeus does for `linstor-controller`: satellites need the passphrase as well as the controller, an env value is fixed at pod start so a changed Secret would need a DaemonSet restart, and a reference keeps one source for all components. The chart exposes one value, the Secret's name, and renders it into `ControllerConfig.spec.passphraseSecretRef`; Cozystack sets it to `linstor-passphrase`.

This is the object piraeus users already have. blockstor reads it and never creates, updates or deletes it; its RBAC on it is `get`. The Secret must be in blockstor's namespace, the same rule piraeus has. The old data key `passphrase` and the old default name `blockstor-cluster-passphrase` are gone.

### 4.3 Per-volume wrapped passwords

On the replica's own `Resource` CR, next to the existing per-volume seeding hints:

```yaml
apiVersion: blockstor.org/v1alpha1
kind: Resource
spec:
  volumes:
    - volumeNumber: 0
      luksKey:
        keyID: 3f9a1c0e5b7d2a44
        wrapped: <b64 nonce(12) + ciphertext + tag(16)>
        origin: Generated        # Generated | LinstorImport
```

Snapshots carry a copy taken when the snapshot is created, because the source replica may be deleted later:

```yaml
kind: Snapshot
spec:
  luksKeys:
    - {nodeName: node-a, volumeNumber: 0, keyID: 3f9a1c0e5b7d2a44, wrapped: <b64>}
```

Why `spec` and not `status`: losing this value loses the replica's data, and backup tools (Velero among them) do not restore `status`. Why `spec.volumes[]`: the store already carries that list across its wire-shaped rewrites of the spec (`pkg/store/k8s/resources.go`, the Bug 206 carry-across in `Update` and `PatchResourceSpec`), so the field inherits the protection instead of needing a new one. A CEL transition rule on the CRD forbids removing `luksKey` from a volume that stays in the list.

**Decision (D4): one password per replica or per volume.** LINSTOR is per replica. Chosen: per replica. The importer has to represent LINSTOR's distinct per-replica passwords anyway, so the model needs it regardless; a second, per-volume model for native volumes would be a second code path through clone and restore.

**Decision (D5): which object holds the wrapped password.** `Resource.spec.volumes[]` (chosen), a dedicated cluster-scoped CR, or one Secret per replica. The Resource keeps the key and the replica in one object: same lifecycle, same backup, same manifest from the importer. A dedicated CR gives narrower RBAC, but the blob is AEAD ciphertext under a 256-bit key and reveals nothing, and two objects can be restored out of step. Secrets per replica would need satellites to create Secrets.

### 4.4 RBAC

| Component | Keyring Secret | Passphrase Secret (operator's) | Generated unlock Secret (6.2) | `Resource.spec.volumes[].luksKey` |
|---|---|---|---|---|
| satellite | get | get | get | read, write on its own node's Resources (it already has `update/patch` on `resources`) |
| controller | get, create | get | none | read |
| apiserver (REST) | get, create, update | get | get, create, update | read, copy on clone and restore |
| `blockstor` CLI | the operator's own rights | the operator's own rights | the operator's own rights | none |

The satellite's Secret access moves from the ClusterRole to a namespaced Role in the blockstor namespace, with `get` only. All components read the Secrets past the cache, as they do today, so no Secret informer is started.

## 5. Threat model

LUKS here protects data at rest on each node and nothing else: in `DRBD,LUKS,STORAGE` the DRBD lower disk is the opened mapper, so replication carries plaintext (established in #192 and unchanged).

| The attacker has | Obtains |
|---|---|
| a disk or a whole powered-off node | nothing: a LUKS header whose only key is a 256-bit random password that is not on the node |
| read access to blockstor CRs (`Resource`, `Snapshot`), a CR backup, a GitOps repository of them | wrapped passwords: AEAD ciphertext under WK. Nothing usable without MK. |
| the keyring Secret alone | the slots. A weak passphrase can be attacked offline at Argon2id cost per guess. No key. |
| the passphrase Secret alone | a passphrase that opens nothing by itself. **Today this single Secret opens every disk.** |
| the passphrase Secret and the keyring, or etcd unencrypted, or a full namespace backup | MK, and with the CRs every volume password. Same exposure as piraeus has. Stage 2 addresses it. |
| one volume password (a node memory dump, a recovery session) | that one replica. Today the same leak is every volume in the cluster. |
| root on one node | MK from the satellite's memory or through its ServiceAccount, hence every wrapped password in the API; but it needs the other nodes' disks to use them. Same as LINSTOR, where the satellite is sent the master key. |
| write access to CRs | can destroy a replica's wrapped password (as it can delete the replica); cannot read or forge one that opens anything. |

Rotating a passphrase after it leaked does not help if the attacker also had the keyring at that time: they hold MK. The answer to that is master-key rotation (section 11), which is not in stage 1.

## 6. Unlock flow

### 6.1 From the passphrase Secret

This is the piraeus contract carried over, with one translation: in LINSTOR the controller unlocks and sends the master key to the satellites; in blockstor there is no such channel, so each component that needs the key unlocks from the same two Secrets.

**First start with a passphrase Secret and no keyring.** The controller (leader) generates MK and creates the keyring with one slot, `name: secret`, `origin: Secret`, from the Secret's passphrase. This is what LINSTOR's `PassphraseInitializer` does when its `encrypted` namespace is empty. `Create` is atomic, so two writers cannot both win. The controller never creates a keyring while any Resource carries a `luksKey`: a lost keyring must not be silently replaced by a new master key.

**Every later start, and whenever either Secret changes (new `resourceVersion`).** The component reads the passphrase Secret and the keyring, tries the slots with Argon2id, and keeps MK and WK in memory, keyed by the two `resourceVersion`s. One derivation per process, not per reconcile.

**Opening a volume.** `applyLUKS` takes the replica's `luksKey` from its own Resource, checks `keyID` against the unlocked key, unwraps and hands the password to `cryptsetup` over stdin as now. The wire prop `LuksPassphrase` and `injectLUKSMasterPassphrase` go away; the password never passes through a props map. Mappers already open survive a satellite restart, as today.

**Locked.** A keyring exists, and the passphrase Secret is missing or opens no slot. Opened mappers keep working. New opens fail with a `MasterKeyLocked` condition on the Resource, and `GET /v1/encryption/passphrase` reports `locked`. The fix is to put a passphrase that opens a slot into the Secret. LINSTOR under piraeus fails the same way, at controller start.

**A cluster migrated from piraeus.** The Secret stays as it is; only `passphraseSecretRef` has to name it. The keyring written by `linstor-migrate` (section 9) has a slot for the LINSTOR master passphrase, so the first start finds a Secret that opens a slot and is unlocked with no operator step.

### 6.2 With no passphrase Secret referenced

`linstor encryption create-passphrase <p>` must still leave a cluster that provisions LUKS volumes and survives a restart, with the Secret being read-only input that nobody supplied.

**Decision (D2): the unlock source when no Secret is referenced.**

- (a) blockstor keeps its own: `create-passphrase` generates MK, enrols the given passphrase as a slot (`origin: API`), and adds a second slot (`origin: Generated`) whose random passphrase it stores in a Secret it owns, `blockstor-unlock`, key `MASTER_PASSPHRASE`. The cluster unlocks from that. The operator's passphrase is never stored in clear. **Chosen.**
- (b) the Secret is mandatory: `create-passphrase` without a referenced Secret is refused with a message naming the Secret to create. Simplest, and it is how every piraeus cluster runs, but the upstream verb then never succeeds on blockstor.
- (c) as (a), but store the operator's own passphrase in the blockstor-owned Secret. One slot fewer; the passphrase the operator typed rests in the cluster.

With (a) the unlock code path is the same in both modes: a Secret with `MASTER_PASSPHRASE` that opens a slot. The only difference is who owns the object. When `passphraseSecretRef` is set, `blockstor-unlock` is not created and not read.

**Decision (D1): does the satellite see the master key.** Chosen for stage 1: yes, satellites unlock MK themselves. It is what LINSTOR does (`MsgIntCryptKey` carries the plaintext master key to every satellite), and it needs no new transport. The alternative, the controller unwrapping and delivering per-node passwords, cannot be enforced with RBAC while all satellites share one ServiceAccount; it needs passwords sealed to a per-node key. That belongs with stage 2, and the stored format does not change when it lands.

Entering the passphrase by hand after every restart, as bare LINSTOR does without `MASTER_PASSPHRASE`, is not offered: it needs a place that holds MK in memory and a channel to the satellites.

## 7. Passphrase operations

One package, `pkg/keyring`, implements all of them; the REST handlers and the `blockstor` CLI both call it. The CLI keeps talking to the Kubernetes API directly, as it does now.

Passphrases given through REST or the CLI live only in the keyring's slots. The operator's passphrase Secret is never written by any of these.

| Operation | LINSTOR-compatible door | blockstor door | Effect |
|---|---|---|---|
| create | `POST /v1/encryption/passphrase {"new_passphrase"}`, `linstor encryption create-passphrase` | `blockstor encryption create-passphrase` | Keyring exists: 409, as LINSTOR answers once a passphrase is set; under a referenced Secret that is always the case after first start. No keyring and no Secret referenced: section 6.2. |
| enter | `PATCH` with a bare string or the two wrapped forms already accepted | `blockstor encryption enter-passphrase` | Verifies the passphrase against all slots: 200 or 401. On a locked cluster that runs on the generated unlock Secret (6.2a), it also re-creates that Secret with a fresh generated slot. Under a referenced Secret it only verifies, and the response says which Secret to fix. |
| modify | `PUT {"old_passphrase","new_passphrase"}`, `linstor encryption modify-passphrase` | `blockstor encryption modify-passphrase` | Replaces the slot that `old` opens with one for `new`, in one keyring update. The 409 refusal over LUKS volumes is removed. If `old` is the passphrase the Secret supplies: see below. |
| add | none in LINSTOR | `blockstor encryption add-passphrase --name <label>`; `POST /v1/encryption/passphrases {"passphrase","new_passphrase","name"}` | Adds a slot. Requires a passphrase that opens an existing slot. |
| remove | none | `blockstor encryption remove-passphrase <slot>`; `DELETE /v1/encryption/passphrases/{slot}` | Removes a slot. Refused for the last slot, and for the slot the cluster currently unlocks with. |
| list | none | `blockstor encryption list-passphrases`; `GET /v1/encryption/passphrases` | Slot ID, name, origin, creation time, KDF parameters, and which slot the cluster unlocks with. No key material. |
| status | `GET /v1/encryption/passphrase` | same | `unset` (no keyring), `locked`, `unlocked`. Derived from the Secrets, not from the per-process flag the handler keeps today (`s.passphraseUnlocked`), which disagrees between apiserver replicas. |

The three upstream verbs keep their paths, bodies and status codes, so the linstor python client and golinstor work unchanged. The new verbs live under a different path (`/passphrases`) and are blockstor-only; they are listed in `docs/cli-parity-known-deltas.md`.

### How the Secret's passphrase and the other slots relate

The Secret's passphrase is one slot among equals, and the one the cluster starts from. Every other slot is a recovery or administrative passphrase: it can verify, authorise `add`/`modify`, and take over as the unlock passphrase by being put into the Secret.

Changing the passphrase in the Secret is the operator's action, since blockstor does not write it:

1. `blockstor encryption add-passphrase` with the new passphrase (authorised by the current one);
2. update the Secret, by hand or through GitOps; blockstor notices the new `resourceVersion` and unlocks through the new slot;
3. `blockstor encryption remove-passphrase` for the old slot.

Every intermediate state unlocks. Editing the Secret without step 1 gives `locked`, which is visible and undone by restoring the value or doing step 1 from another slot.

**Decision (D3): `modify-passphrase` when `old` is the passphrase the Secret supplies.** blockstor cannot complete it, because the Secret is not its to write.

- (a) Do step 1 and stop: add the slot for `new`, keep the slot for `old` marked `superseded`, answer 200 with a warning that names the Secret to update. Once the Secret opens another slot, the controller removes the superseded one. **Chosen**: the upstream verb succeeds, nothing can lock, and the cleanup needs no further command.
- (b) Refuse with 409 and the three steps above in the message. Simpler, but the python client's `modify-passphrase` then never works on a piraeus-style cluster.
- (c) Replace the slot as usual. This is what LINSTOR under piraeus does, and it leaves a cluster that cannot unlock at the next restart. Rejected.

When the cluster runs on the generated unlock Secret (6.2a), `modify` simply replaces the operator's slot; the generated slot is untouched.

## 8. New volumes; existing blockstor volumes are not carried over

Before the LUKS layer touches a volume it looks at two things: whether the Resource carries a `luksKey` for it, and whether the device carries a LUKS header.

| `luksKey` | LUKS header | Action |
|---|---|---|
| no | no | New volume, but only if the device is empty: the existing `blkid -p` probe in `Cryptsetup.Format` must find no signature at all, and it fails closed (`ErrDeviceHoldsData`, or the probe's own error). Then generate a password, wrap it, write `luksKey` to the Resource with an optimistic lock, read it back, and `luksFormat` with it. |
| yes | no | Interrupted first format, or a device re-created after toggle-disk. `luksFormat` with the stored password, after the existing empty-device probe. |
| yes | yes | Open with the stored password. A failure sets `LUKSKeyMismatch`; the device is never re-formatted. |
| no | yes | Not supported. Condition `LUKSKeyMissing`; the device is neither opened nor formatted. |

A replica without a `luksKey` is therefore never formatted over existing data: with a LUKS header it stops in the last row, with any other signature (a filesystem, DRBD metadata, a partition table) it stops in the first row's probe, and only a device seen to be empty is formatted. The same probe guards the second row.

The password is persisted before the format, so a crash can leave a stored key without a header (harmless, second row) but never a header without a stored key. Each Resource has one writer for this field, the satellite of its node.

**The last row is where every LUKS volume formatted by today's blockstor lands.** There is no legacy mode and no adoption: the cluster passphrase that formatted it is no longer read from anywhere, and the old Secret key `passphrase` is ignored. The release note says so, and says what to do: before upgrading, copy the data to a non-LUKS volume or keep the old release; after upgrading, create the LUKS volume again. The row fails loudly and never formats, so an overlooked volume keeps its data and can still be opened by hand with the old passphrase.

**Decision (D8): who generates the password.** The satellite at format time (chosen) or the controller when the replica is created (LINSTOR's way). The satellite is the only component that sees the device, which is what tells the rows of the table apart; it holds MK in stage 1 anyway; and provisioning does not gain a dependency on the controller being up. If stage 2 moves unwrapping to the controller, generation moves with it.

`vd set-passphrase` (a user-supplied password for one volume) stays refused in stage 1. It needs `cryptsetup luksChangeKey` with a two-phase record (write the new wrapped password as pending, change the key, promote), which is LINSTOR's `modify_password` protocol (`LuksLayer.java:220-240`).

## 9. LINSTOR import in `linstor-migrate`

`linstor-migrate` is an offline converter: it reads table dumps and writes manifests (`cmd/linstor-migrate/main.go`). It already loads `propscontainers` and `layerluksvolumes` (`pkg/linstormigrate/dump.go:102,109`). The import adds:

**Inputs**

- `-linstor-passphrase-file <path>` (`-` for stdin): the LINSTOR master passphrase, the value of `MASTER_PASSPHRASE` in the piraeus Secret. Never a flag value.
- one of:
  - `-keyring-out <path>`: create a new blockstor keyring whose first slot is the LINSTOR master passphrase itself. This is the default path, and it is what lets the existing piraeus Secret unlock blockstor unchanged;
  - `-keyring-in <path>` with `-blockstor-passphrase-file`: wrap under the keyring of a blockstor cluster that already exists.

**Steps**

1. Read `encrypted/masterkey`, `encrypted/passphrasesalt`, `encrypted/masterhash` from the props rows of instance `/CTRL` or `/CTRLCFG`.
2. Derive the KEK (PBKDF2-HMAC-SHA1, 5000, 16 bytes), decrypt `masterkey` (AES-128-CFB, IV = first 16 bytes), compare SHA-512 of the padded block with `masterhash`. A mismatch is a wrong passphrase: stop before writing anything.
3. For every `LayerLuksVolumeRow`, replicas and snapshots alike: base64-decode `encrypted_password`, decrypt under the LINSTOR master key, strip the length padding, and check the result is plausible (length within the block). CFB has no MAC, so this is a sanity check, not proof; proof is the satellite opening the volume.
4. Wrap each password under the blockstor WK.

**Outputs**

- every converted `Resource` carries `spec.volumes[].luksKey` with `origin: LinstorImport`; every converted `Snapshot` carries `spec.luksKeys`;
- with `-keyring-out`, a manifest of the keyring Secret with one slot (`name: secret`, `origin: Secret`) for the LINSTOR passphrase. It contains wrapped material only;
- no passphrase Secret is emitted: the operator keeps the one piraeus used, moves or copies it into blockstor's namespace if that differs, and points `ControllerConfig.spec.passphraseSecretRef` at it;
- the report line `LUKS key NOT migrated` (`pkg/linstormigrate/convert.go:1747`) becomes a per-RD count of migrated passwords; without `-linstor-passphrase-file` the converter refuses a dump that has LUKS volumes, where today it warns;
- no plaintext password is written anywhere, and none is logged.

The password bytes carry over exactly: LINSTOR writes the password and a newline to cryptsetup's stdin, which strips the newline; blockstor passes the same bytes with `--key-file -`, which takes them verbatim. A test pins this (section 14).

Edge cases: a volume whose `set-passphrase` was accepted but not yet applied has a newer value in the volume-definition prop than in `ENCRYPTED_PASSWORD`; the header matches `ENCRYPTED_PASSWORD` (`CtrlRscLayerDataMerger.java:210-211`), so that is what is imported, with a warning. Non-ASCII passphrases follow the JDK quirk noted in section 2.3.

Considered and rejected: teaching blockstor to read LINSTOR's own format in place (a `linstor` slot type and CFB blobs on Resources). It would keep unauthenticated AES-128-CFB and PBKDF2-SHA1 in the running system for good. Re-wrapping at import confines them to the converter.

### Issue #257

Superseded. It proposed adding the cluster passphrase to every volume with `luksAddKey`; with per-volume passwords there is no cluster passphrase to add. Proposed replacement, to be applied by the maintainer:

> **Title:** linstor-migrate: decrypt LINSTOR's LUKS volume passwords and re-wrap them under the blockstor master key
>
> blockstor now stores a wrapped password per LUKS replica, as LINSTOR does. The converter already loads every replica's LINSTOR password from the dump (`LayerLuksVolumeRow.EncryptedPassword`). Given the LINSTOR master passphrase from a file, it decrypts them the way LINSTOR does and writes them, wrapped under the blockstor master key, into the converted Resources and Snapshots. It also writes the keyring, with the LINSTOR master passphrase as its first slot, so the passphrase Secret piraeus used keeps unlocking the cluster. No `luksAddKey`, no key read from `dmsetup`, no data copy, and it works whether or not the volume was opened through the kernel keyring.
>
> Step 7 of `docs/linstor-migration.md` is replaced by "pass `-linstor-passphrase-file` and keep the passphrase Secret", and the copy path stays only for a cluster whose master passphrase is lost.

## 10. Clone, snapshot, restore, resize

The rule is one sentence: **the wrapped password follows the bytes.**

| Operation | LINSTOR | blockstor stage 1 |
|---|---|---|
| Snapshot | stores the replica's encrypted password with the snapshot | the REST handler copies each source replica's `luksKey` into `Snapshot.spec.luksKeys` when it creates the Snapshot |
| Restore from snapshot | new replica reuses the snapshot's password | target replica on node T gets the `luksKey` of the snapshot whose bytes it receives: node T's own snapshot, or the shipping node's when the bytes come over `zfs send`/`recv` |
| Clone | copies through opened devices, new password for the clone | bytes are copied below LUKS, so the clone's header is the source's: the target replica gets the source replica's `luksKey` |
| Replica added by DRBD resync | own password | own password: a fresh device, first row of section 8 |
| Resize | `cryptsetup resize`, password unchanged | unchanged; `Cryptsetup.Resize` takes the same unwrapped password |
| Backup to a remote, restore from it | source master key recovered from the source passphrase, passwords re-encrypted locally | not implemented in blockstor (501). When it is, the backup metadata carries the wrapped passwords and the source keyring's slots, and restore re-wraps, as LINSTOR does. |

These copies need no master key, which is why the REST handler can do them. A copy that arrives without its key lands in the last row of section 8 and fails loudly with `LUKSKeyMissing`; nothing is formatted.

A byte-level clone also shares the LUKS volume key inside the header with its source, which no password change alters. That is how blockstor clones work today and is outside this design.

**Decision (D10): clone password.** Keep the source's (chosen for stage 1), or re-key the clone's header after the copy as LINSTOR does. Each replica has its own stored copy of the wrapped password, so re-keying a clone later is the `luksChangeKey` operation of section 8 and needs no format change.

## 11. Failure modes

| Failure | Outcome |
|---|---|
| The passphrase Secret is deleted, or changed to a value no slot knows | `locked`. Opened volumes keep running. Restore the value, or enrol the new one with `add-passphrase` authorised by any other slot. |
| Every passphrase is lost while satellites still run | MK is still in satellite memory, but stage 1 offers no way to add a slot without a passphrase. This is the reason to enrol a recovery passphrase on day one; the docs and the `list-passphrases` output say so while only one slot exists. |
| Every passphrase is lost and the nodes rebooted | The data is unrecoverable. This is the property that makes it encryption. |
| The keyring Secret is deleted | Same as losing every passphrase. The keyring is small and contains no plaintext key: the runbook has the operator export it (`kubectl get secret blockstor-master-key -o yaml`) next to the recovery passphrase. The controller does not re-create a keyring while any Resource carries a `luksKey`. |
| Removing the last slot, or the slot the cluster unlocks with | Refused (409). |
| `modify` or the three-step Secret change is interrupted | Every intermediate state has a slot the Secret opens. Repeating the command completes it. |
| Two slot operations race | The second fails on `resourceVersion` and retries against the new keyring. |
| A Resource loses its `luksKey` | That replica cannot be reopened after its mapper closes; other replicas are unaffected and DRBD can resync a new one. Guarded by the CEL rule, the store carry-across and its test. |
| A wrapped password names an unknown `keyID` | `MasterKeyMismatch` on the Resource, naming both IDs. Happens when CRs are restored into a cluster with a different keyring; the fix is restoring the matching keyring. |
| A LUKS volume from the release before this change | `LUKSKeyMissing`; not opened, not formatted (section 8). |
| Master key compromised | Needs master-key rotation: a new MK, every wrapped password re-wrapped (CR writes only), every slot re-enrolled with its passphrase. Not in stage 1. `keyID` on every blob and in the keyring is what makes it possible later without a format change. |

## 12. Where this diverges from PR #192

#192 answered "which hierarchy" with (C) per-volume keys under a KEK, exposed as (B) per-tenant keys, with (D) an external KMS as a provider. This design is (C), and agrees with #192 on what LUKS protects, on never taking a key from a property, and on failing loudly instead of formatting.

| #192 | Here | Why |
|---|---|---|
| Per-tenant custody is the product surface of the first implementation | Stage 1 is cluster-wide: one master key, several passphrases. Per-tenant keys are stage 2. | The maintainer's decision. It also separates the mechanism, which the migration needs now, from the custody question, which needs a tenant model. |
| Existing volumes must not be wedged: the per-RD refusal became a Condition so that RDs working today keep working | Existing blockstor LUKS volumes are explicitly not carried over. | The maintainer's decision: nobody is assumed to run LUKS on blockstor, so compatibility code is not worth its cost. The failure stays loud and non-destructive, which was #192's real requirement. |
| A format-time fingerprint of the key on `Resource.status`, compared on every apply | Not needed. | The replica's own wrapped password is stored, with a key ID. The hazard the fingerprint guarded, a changed cluster passphrase that no longer matches the header, cannot occur when the passphrase is not a disk key. |
| Rotation refused with 409 and a `force` flag | Rotation works; the refusal now on main (`refuseRotationOverLUKSVolumes`) is deleted. | Re-wrapping the master key is the whole point. No client could send `force` anyway, as the review of #192 noted. |
| Repointing `ControllerConfig.spec.passphraseSecretRef` or editing the Secret strands volumes, unguarded | Both lead to `locked`: visible in status, nothing stranded, undone by restoring the value. | Follows from the hierarchy; no admission webhook needed. |
| The passphrase Secret is blockstor's, written by `create-passphrase`, key `passphrase` | The passphrase Secret is the operator's, piraeus-shaped (`MASTER_PASSPHRASE`), read-only. | The maintainer's decision, for migrations from piraeus. |
| "Under LINSTOR rotation re-wraps the per-volume keys" (§3.1) | LINSTOR re-wraps only the master key (`CtrlConfApiCallHandler.java:1833-1841`). | Checked against source. |
| "Unverified that the stored value is wrapped with the master key via LINSTOR's KDF" (§5) | Verified, with the algorithm and file references in section 2.3. | Was the first task #192 asked for. |
| Migration and the hierarchy are one piece of work, to be scoped only after a non-production cluster with encrypted volumes is found | The hierarchy lands first and is useful alone; a rehearsal on a real encrypted LINSTOR cluster is the acceptance gate of the import PR. | The crypto can be pinned with a vector taken from a throw-away LINSTOR controller. |
| `spec.encryption.passphraseSecretRef` on an RD "should be wired" | Stays not implemented in stage 1 and is reserved as the stage 2 selector of a non-default key. #192's `EncryptionKeyIgnored` condition remains correct. | Wiring it now would be the per-tenant model by another name. |
| `vd set-passphrase` answers 501 | Unchanged. | Implementable later with the two-phase re-key. |

What to do with #192 itself is **Decision (D11)**. Part of it is on main in another form since #222 (the rotation refusal, the dropped per-volume property). Chosen: keep its comment corrections about plaintext replication and its `EncryptionKeyIgnored` condition if they are still missing on main, drop its rotation guard, and replace its `docs/byok-design.md` with this document. The hunk-by-hunk triage of #192 against main happens on #192 itself.

## 13. Where stage 2 (external KMS, per-tenant keys) plugs in

Three seams, none of which changes stored volume data:

1. **Slot type.** A slot is "something that yields MK". Stage 1 has `kdf.type: argon2id`. A `kms` slot holds MK encrypted by an external key (OpenBao transit, a cloud KMS) and is unlocked by a call instead of a derivation. Removing the passphrase slots then leaves a cluster that cannot start without the KMS.
2. **Key selection per volume.** Every wrapped password names its `keyID`. A second keyring, for a tenant, is a second key ID; `ResourceDefinition.spec.encryption` selects it. The tenant's key wraps that tenant's volume passwords and nobody else's.
3. **Who unwraps.** With a KMS, nodes should not hold KMS credentials. The controller unwraps and delivers each node its passwords sealed to a per-node key. `pkg/keyring` exposes `Wrap`/`Unwrap` behind an interface so the satellite's caller can be swapped for "read my sealed copy".

In Go:

```go
// pkg/keyring
type Unlocker interface {            // passphrase now, KMS later
    Unlock(ctx context.Context, slot Slot) (MasterKey, error)
}
type VolumeKeys interface {          // local unwrap now, sealed delivery later
    Wrap(password []byte) (WrappedKey, error)
    Unwrap(w WrappedKey) ([]byte, error)
}
```

## 14. Plan and tests

Without a legacy path the switch does not need to be staged across releases: PRs 1 to 4 can land in one release, and it is the breaking one.

| PR | Content | Behaviour change |
|---|---|---|
| 1 `feat(keyring): master key with passphrase slots` | `pkg/keyring`: keyring format, slots, Argon2id, HKDF, wrap/unwrap, framing. This document as `docs/encryption-keys.md`, replacing #192's note. Triage of #192. | none |
| 2 `feat(api): carry a wrapped LUKS password per replica` | `luksKey` on `ResourceVolumeSpec`, `luksKeys` on `SnapshotSpec`, CEL rule, store carry-across; snapshot, restore and clone copy the field. `key` on `PassphraseSecretRef`. Namespaced Secret Role for the satellite. | none (fields empty) |
| 3 `feat(satellite)!: per-volume LUKS passwords under a master key` | Unlock from the piraeus-shaped Secret; keyring bootstrap by the controller; the four cases of section 8; conditions; status from the Secrets; removal of `pkg/passphrase`'s Secret key `passphrase`, of the `LuksPassphrase` wire prop and of the old write path in `create-passphrase`. Release note: existing LUKS volumes are not supported. | **the breaking switch** |
| 4 `feat(encryption): several master passphrases` | create/enter/modify on slots, the generated unlock Secret (D2), the superseded-slot cleanup (D3), `add`/`remove`/`list` in CLI and REST, parity deltas. | rotation becomes possible |
| 5 `feat(linstor-migrate): import LUKS volume passwords` | LINSTOR decryption, re-wrap, keyring output with the LINSTOR passphrase as first slot, runbook step 7 rewritten, #257 rewritten. | LUKS migration without `luksAddKey`, piraeus Secret reused |
| 6 `feat(encryption): change a volume's password` (optional) | Two-phase `luksChangeKey`; `vd set-passphrase`; own password for clones. | header writes, on request |
| 7 stage 2 | KMS slot, per-tenant keyrings, sealed delivery. Its own design. | |

Between PR 3 and PR 4, `create-passphrase` over REST answers 409 under a referenced Secret and is refused without one; if that gap is unwelcome, PR 3 and PR 4 merge together.

**Tests**

- `pkg/keyring` unit: known-answer vectors for slot wrap and volume wrap; wrong passphrase, flipped bit in every field, wrong AAD, wrong key ID all fail; framing round-trip at the block boundaries; slot limit; last-slot and unlock-slot removal refused; passphrase bytes are taken verbatim, trailing newline included.
- Unlock (envtest): Secret with `MASTER_PASSPHRASE` and no keyring creates one with a `secret` slot; two controllers racing create exactly one; Secret that opens no slot gives `locked`; no keyring is created while a Resource carries a `luksKey`; the Secret is never written (a test fails on any write verb against it); the three-step Secret change unlocks at every step; D3's superseded slot is removed once the Secret moves.
- LINSTOR decryption: a vector taken from a throw-away LINSTOR 1.33 controller (create a passphrase, create a LUKS volume, dump the tables, record the password that `cryptsetup open --test-passphrase` accepts), including a non-ASCII passphrase and a `/CTRLCFG` dump; cross-checked with `openssl enc -d -aes-128-cfb`. Wrong passphrase stops before any output.
- Converter golden files: Resources and Snapshots carry `luksKey`; the emitted keyring opens with the LINSTOR passphrase; no plaintext in manifests or report; a dump with LUKS volumes and no passphrase file is refused.
- Store (envtest): `luksKey` survives `Update`, `PatchResourceSpec` and a REST modify of the same Resource; the CEL rule rejects its removal; snapshot, restore (local and shipped) and clone carry the right node's key.
- Satellite unit, with `storage.FakeExec`: each row of the section 8 table; the write-before-format order (a failure injected between the two leaves a stored key and no header); a device with a header and no key is never formatted and never opened; unlock is cached per `resourceVersion`.
- cryptsetup compatibility (needs root, integration tier): a volume formatted the LINSTOR way (password and newline on stdin) opens the blockstor way (`--key-file -`).
- e2e on the stand: a new LUKS volume has exactly one key slot and no master passphrase opens it; change the Secret's passphrase by the three steps, close the mapper, restart the satellite, and the volume opens; add and remove a recovery passphrase; delete the passphrase Secret, observe `locked`, restore it; clone and cross-node restore of a LUKS volume; a piraeus-shaped Secret applied before install gives a working LUKS cluster with no encryption command run; cli-matrix cells and replay files for the three upstream verbs through the linstor python client.
- Migration rehearsal, the acceptance gate of PR 5: a LINSTOR cluster under piraeus with `linstorPassphraseSecret` and LUKS volumes, converted and cut over, opens every volume with the unchanged Secret.
- Mutation checks, as the repository does for guards: each refusal and each ordering constraint reverted makes its named test fail.

## 15. Decisions

| # | Question | Decision |
|---|---|---|
| D1 | Do satellites hold the master key | Yes in stage 1, as in LINSTOR; sealed per-node delivery with stage 2 |
| D2 | Unlock source when no passphrase Secret is referenced | blockstor keeps its own generated unlock Secret; the operator's passphrase lives only in a slot |
| D3 | `modify-passphrase` on the passphrase the Secret supplies | Add the new slot, keep the old one as superseded until the Secret moves, answer 200 with a warning |
| D4 | Password per replica or per volume | Per replica |
| D5 | Where wrapped passwords are stored | `Resource.spec.volumes[].luksKey`, copied into `Snapshot.spec.luksKeys` |
| D6 | Where the keyring is stored | A dedicated Secret, `blockstor-master-key` |
| D7 | KDF for passphrase slots | Argon2id (3, 64 MiB, 4), type stored per slot |
| D8 | Who generates a volume password | The satellite, before formatting |
| D9 | LUKS header KDF for generated passwords | PBKDF2 at minimal cost; can be deferred |
| D10 | Clone password | Same as the source in stage 1 |
| D11 | PR #192 | Keep its comment fixes and Condition, drop its guard, replace its design note |

### Implementation notes

Found while implementing stage 1; each is either a place the design was underspecified or a choice the code makes that the sections above do not.

- **Restore copies the password on the satellite, not in the REST handlers (section 10).** A restore target is placed from several doors (the eager stamp, autoplace for a bare restore, a CSI-prepared target, clone), and the bytes on a node may come from that node's snapshot or, through the snapshot stream, from a peer's. Only the satellite knows which. A replica with no `luksKey` and a LUKS header, restored from `<rd>:<snap>`, tries every password the snapshot recorded for that volume, its own node's first, with `cryptsetup luksOpen --test-passphrase`, stores the one that opens, and then opens. A blank restore target (no header) gets a fresh password like any new volume.
- **`luksKey` is set-once, not only "not removable" (section 4.3).** The CEL rule refuses a change as well as a removal: an overwritten password loses the data as surely as a removed one. The optional re-key (PR 6) has to relax the rule for its own two-phase write.
- **The snapshot copy happens at Snapshot create, in the store.** A replica formatted between the Snapshot CR's create and the take is not covered; its restore fails closed with `LUKSKeyMissing`. `luksKeys` entries also carry `origin`, so a restored replica keeps it.
- **Refusals are Warning Events on the Resource, not conditions (sections 6.1 and 8).** The satellite has no apply-failure condition to extend; it already reports `LUKSDeviceHoldsData` as an Event. `LUKSKeyMissing`, `LUKSKeyMismatch`, `MasterKeyMismatch` and `MasterKeyLocked` follow it. The controller only logs a failed bootstrap (`ErrKeyringLost`); there is no ControllerConfig condition yet.
- **Argon2id bounds.** A slot below the parameters blockstor writes (3, 64 MiB, 4) or above the ceilings (16, 1 GiB, 16) is refused as malformed before Argon2id runs. Raising the defaults later is a slot-by-slot change; lowering them needs a format version.
- **Unlock stops at the first slot that opens.** The time of a successful unlock tells the caller the slot's position, not anything about the passphrase; every failure is the same error after trying every slot.
- **The cached master key is dropped, not cleared, when either Secret changes.** A reconcile may still hold the previous key; it is garbage once nothing references it.
- **`remove-passphrase` takes no passphrase.** The REST API is behind mutual TLS and the CLI already has write access to the keyring Secret; the last slot and the unlock slot are refused either way.
- **`create-passphrase` under a named Secret with no keyring yet answers 409** and says which Secret and key to fill: the controller creates the keyring from it. With a keyring that the given passphrase opens it answers 200, as upstream does for a repeat.
- **The LUKS create gate refuses only `unset`.** A locked keyring lets the definition be created; its replicas wait, as under LINSTOR.
- **`enter-passphrase` repairs a cluster on the generated unlock Secret** by dropping every `generated` slot and adding a fresh one with a new Secret.
- **`-keyring-in` takes the raw `keyring.json`**, the data key of the `blockstor-master-key` Secret, not the Secret manifest.
- **The cozystack chart is not in this repository.** It has to follow the RBAC here: the satellite's Secret access becomes a namespaced `get`, the controller needs `get`, `create`, `update` on Secrets in its namespace, the apiserver `get`, `create`, `update`, and the chart value for the passphrase Secret's name renders into `ControllerConfig.spec.passphraseSecretRef`.
- **`ResourceDefinition.spec.encryption.passphraseSecretRef`'s field comment still says key `passphrase`.** The field is unwired and reserved for stage 2; the comment is left to that change.
- **Not exercised here:** the root-only cryptsetup compatibility test (a LINSTOR-formatted header opened with `--key-file -`), the e2e cases of section 14, a vector from a live LINSTOR controller (non-ASCII passphrase, `/CTRLCFG` dump, CFB segment size), and the #192 triage.
