# Wave 2 — Group 6 — Storage backends (Day2 ops)

Storage-pool CRUD per provider (LVM thick/thin, ZFS thick/thin,
Diskless), pool delete, shared-VG pools, `physical-storage list +
create-device-pool`, pool mixing across providers, and the
`Autoplacer/MaxThroughput` scoring strategy.

Pairs with wave1's `06-storage-backends.md` — Day2 pool-management
scenarios.

[Group index in README.md](README.md).

---

## Pool create

### 6.W01 `sp create lvm <node> <pool> <vg>` — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** UG9 §"Creating storage pools" (lines 610-651) via tests/scenarios/day2-storage-pool-create-lvm.md

Thick LVM. **Critical pre-req:** `/etc/lvm/lvm.conf` `global_filter` must skip `/dev/drbd*` and `/dev/mapper/[lL]instor*` — without this, LVM commands hang on DRBD hosts. Test asserts the filter check before creating the pool.

### 6.W02 `sp create lvmthin <node> <pool> <vg>/<thinpool>` — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** UG9 §"Creating storage pools" (lines 610-651) + storage-providers table at 1997-2029 via tests/scenarios/day2-storage-pool-create-lvm-thin.md

Cross-listed with wave1 6.1. Driver name is `lvmthin` (one word), not `lvm-thin`. Required for snapshot support.

### 6.W03 `sp create zfs <node> <pool> <zpool>` — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** UG9 §"Storage providers" (lines 1997-2029) + §"Creating storage pools" via tests/scenarios/day2-storage-pool-create-zfs.md

Thick ZFS. No snapshot support (use `zfsthin`). `StorDriver/ZfscreateOptions` appends args to `zfs create` (e.g. `-o volblocksize=16k`).

### 6.W04 `sp create zfsthin <node> <pool> <zpool>` — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** UG9 §"Storage providers" (lines 1997-2029) via tests/scenarios/day2-storage-pool-create-zfs-thin.md

Cross-listed with wave1 6.2. Thin ZFS — snapshot support + shipping. `zfs` + `zfsthin` not considered mixing (same extent size, same initial-sync strategy). Most common backend on cozystack stands.

### 6.W05 `sp create diskless <node> <pool>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Storage providers" (lines 1997-2029) + §"DRBD clients" (lines 1686-1699) via tests/scenarios/day2-storage-pool-create-diskless.md

`Driver=DISKLESS`, free/total = 0. Required for tiebreaker reconciler (`AutoAddQuorumTiebreaker`) and for K8s nodes without local storage. Auto-created at node Hello time (see wave1 6.5).

## Pool delete + mixing

### 6.W06 `sp delete <node> <pool>` — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating storage pools by using the physical storage command" (lines 699-731) note via tests/scenarios/day2-storage-pool-delete.md

Removes the LINSTOR record only — underlying VG / zpool stays on host. Refuses if any resource still references the pool; operator must move or remove resources first. UG note: no `physical-storage delete` — host cleanup is manual.

### 6.W07 Mixed-provider RD requires `AllowMixingStoragePoolDriver` — P

- **Priority:** P2  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Mixing storage pools of different storage providers" (lines 2030-2069) via tests/scenarios/day2-storage-pool-mixing.md

Cross-listed with wave1 6.8. LINSTOR ≥ 1.27.0 + DRBD ≥ 9.2.7. Mixed-provider RD is always treated as thick (loses thin space savings). Snapshots may be limited on the mix — test both outcomes and document. `zfs` + `zfsthin` does NOT need the prop.

## Physical-storage helper

### 6.W08 `physical-storage list` enumerates candidate disks — S

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Creating storage pools by using the physical storage command" (lines 699-731) via tests/scenarios/day2-physical-storage-list.md

Cross-listed with wave1 6.6. Filter: > 1 GiB, root device, no FS, no DRBD signature. `pkg/rest/physical_storage_test.go` covers the wire shape.

### 6.W09 `physical-storage create-device-pool` one-shot — P

- **Priority:** P2  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Creating storage pools by using the physical storage command" (lines 699-731) via tests/scenarios/day2-storage-pool-physical-create-device-pool.md

Discover + `pvcreate` + `vgcreate` + `lvcreate --thinpool` + LINSTOR pool register in one call. WARNING: OS-level VG / thin LV are NOT managed by LINSTOR after — `sp delete` does not clean them up.

## Shared pools

### 6.W10 Shared-VG LVM pool via `--shared-space <uuid> --external-locking` — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Sharing storage pools with multiple nodes" (lines 660-697) via tests/scenarios/day2-storage-pool-shared.md

**Out of scope for cozystack.** Shared LVM requires SAN-style multi-attach disks and sanlock / lvmlockd. Cozystack runs HCI with node-local storage; shared VG defeats the whole DRBD-replication premise.

Test stance: `--shared-space` flag accepted in REST handler but returns 501 with `unsupported in blockstor` text. No host-level support.

## Autoplacer throughput strategy

### 6.W11 `Autoplacer/MaxThroughput` on SP + weight on controller — T

- **Priority:** P2  **Target:** unit  **Complexity:** M (implement first)
- **Source:** UG9 §"Storage pool placement" (lines 933-993) via tests/scenarios/day2-storage-pool-set-max-throughput.md

Cross-listed with wave1 2.17 and wave2-02 2.W01. Per-SP `Autoplacer/MaxThroughput` value (bytes/sec) + controller `Autoplacer/Weights/MaxThroughput=N`. Per-volume `sys/fs/blkio_throttle_*` subtracted from pool budget — **depends on QoS** which is out-of-scope per wave1 7.22. P2 unless customer asks.

## LUKS encryption — master-passphrase orchestration

The wave1 LUKS layer (6.13 / 6.14 / 6.15) implements the per-volume encryption primitive. These three scenarios wire the **cluster-wide master passphrase** the controller uses to wrap per-volume LUKS keys and any remote-credential secrets, so a fresh controller restart can re-derive working LUKS keys without an operator stamping passphrases on every replica.

### 6.W12 `linstor encryption create-passphrase` initialises the master key — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Encryption of storage volumes" via tests/scenarios/day2-encryption-create-passphrase.md

POST `/v1/encryption/passphrase` with a fresh passphrase: controller derives a master key, stamps it sealed into a cluster-wide Secret (analogous to upstream's `ENCRYPTION_PASSPHRASE` table), and unlocks itself in-memory. Idempotency: a second POST with the same passphrase succeeds; a different passphrase returns 409. Returns no body on success.

**Unit:** REST handler + store side (sealed-secret round-trip).
**E2E:** create-passphrase on a fresh cluster; LUKS-stack RD becomes provisionable; controller restart with the same passphrase auto-unlocks.

### 6.W13 `linstor encryption enter-passphrase` after controller restart — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Encryption of storage volumes" via tests/scenarios/day2-encryption-enter-passphrase-on-restart.md

PATCH `/v1/encryption/passphrase` re-enters the operator's master passphrase after a controller restart that lost the in-memory key. LUKS-layer resources stay in `Suspended` (controller refuses to provision new) until the passphrase lands. Wrong passphrase → 401 + descriptive error; right passphrase → 200 + Suspended resources resume.

**Unit:** in-memory store with sealed master key; wrong+right paths.
**E2E:** restart controller; assert LUKS provisioning blocked; PATCH passphrase; LUKS provisioning resumes within next reconcile tick.

### 6.W14 `linstor encryption modify-passphrase` rotates the master key — S

- **Priority:** P1  **Target:** unit  **Complexity:** M
- **Source:** UG9 §"Encryption of storage volumes" via tests/scenarios/day2-encryption-modify-passphrase.md

PUT `/v1/encryption/passphrase` rotates: old passphrase decrypts existing wrapped keys, new passphrase re-wraps and rewrites the cluster Secret. Atomic — if any decrypt fails, the whole rotation rolls back. Test pins both happy + rollback paths.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 4 |
| P0 unit + e2e (encryption) | 2 |
| P0 integration | 4 |
| P0 e2e | 1 |
| P1 unit (encryption) | 1 |
| P1 e2e | 1 |
| P2 (any) | 4 |
| T (implement first) | 1 |
| O (out of scope) | 1 |
