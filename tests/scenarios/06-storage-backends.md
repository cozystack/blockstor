# Group 6 — Storage backends, layers, encryption

LVM / ZFS / FILE pool providers and their capability matrices,
the **narrowed** layer stack (DRBD / LUKS / STORAGE — CACHE,
WRITECACHE, NVMe-oF and NVMe-TCP are explicitly **not supported**
in blockstor; see 6.11), encryption (LUKS layer + DRBD shared-secret
+ master passphrase), external DRBD metadata pool, drive
replacement, and fault injection.

Backend tests split sharply: **integration** (real backend on the
CI host with a loop-backed pool) for storage operations,
**e2e** for full provisioning flows.

[Group index in README.md](README.md).

---

## Pool providers — basic CRUD

### 6.1 LVM_THIN provider lifecycle — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** PLAN.md `pkg/storage/lvm`; observability #12 (lvs/vgs/pvs)

**Unit:** `pkg/storage/lvm` with `FakeExec` — CreateVolume, VolumeStatus, CreateSnapshot, DeleteSnapshot, DeleteVolume, PoolStatus. Each assertion is a command-line match.
**Integration:** loop-backed `vgcreate blockstor-lvm /dev/loop0 && lvcreate -T -L 1G blockstor-lvm/thin` → end-to-end CRUD against real LVM.

### 6.2 ZFS / ZFS_THIN provider lifecycle — S

- **Priority:** P0  **Target:** unit + integration  **Complexity:** L
- **Source:** PLAN.md `pkg/storage/zfs/zfs_integration_test.go`

**Unit:** `pkg/storage/zfs` with FakeExec — same matrix as 6.1.
**Integration:** `BLOCKSTOR_ZFS_POOL=blockstor-test go test -tags=integration ./pkg/storage/zfs` against a real 240 MiB loop-backed pool. Already green per PLAN.md.

### 6.3 FILE / FILE_THIN provider — S

- **Priority:** P1  **Target:** unit + integration  **Complexity:** L
- **Source:** PLAN.md `pkg/storage/file`

`fallocate` (thick) / `truncate` (thin) for create, `statfs(2)` for capacity. Snapshots intentionally unsupported (caller routes to LVM/ZFS).

### 6.4 LVM (thick) provider rejects snapshot — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #11; UG9 §"Available storage plugins"

LVM thick supports neither thin nor snapshots. Test: `linstor snapshot create lvm-thick-test snap1` returns clear error "backend does not support snapshots" (not generic 500).

### 6.5 Pool auto-registration via Hello — S

- **Priority:** P0  **Target:** integration + e2e  **Complexity:** L
- **Source:** PLAN.md StoragePool auto-registration 2026-05-08

Satellite ships configured Providers in HelloRequest.Pools → Server.Hello upserts a StoragePool CRD per (node, pool name). 3 satellites with `stand` FILE_THIN pool produce 3 StoragePool CRDs without `linstor storage-pool create`.

### 6.6 `physical-storage` CLI listing — S

- **Priority:** P2  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Creating storage pools by using the physical storage command" (lines 699-733)

`linstor physical-storage list` enumerates raw devices on a satellite. `pkg/rest/physical_storage_test.go`.

---

## Backend capability matrix

### 6.7 Capability flags match actual backend support — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** advanced-config #11

| Backend     | Snapshots | Thin   | LUKS  | Compression |
|-------------|-----------|--------|-------|-------------|
| LVM (thick) | ✗         | ✗      | ✓     | ✗           |
| LVM_THIN    | ✓         | ✓      | ✓     | ✗           |
| ZFS         | ✓         | ✗      | ✓     | ✓ (zfs)     |
| ZFS_THIN    | ✓         | ✓      | ✓     | ✓           |
| FILE        | ✗         | ✗      | ✓     | ✗           |
| FILE_THIN   | ✓ (reflink)| ✓     | ✓     | ✗           |

`StoragePool.Status.SupportsSnapshots` etc. must match the matrix.

**Unit:** per-provider Capabilities() returns the right flags.
**E2E:** each backend instantiated → CRD shows matching flags → unsupported operations return clear errors (not silent no-op or generic 500).

### 6.8 Mixing pools of different providers — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Mixing storage pools of different storage providers" (lines 2031-2071)

RG with `--storage-pool pool1,pool2` where pool1=ZFS_THIN, pool2=LVM_THIN. UG line 2050+ describes prerequisites + consequences. Test: spawn places replicas across the mixed set; subsequent operations honour each provider's semantics.

---

## Layer stack rules

### 6.9 Layer ordering enforced at RD create — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Using LINSTOR without DRBD" (lines 1819-1831); ug9-features 6.4

blockstor supports a **narrower** layer set than upstream LINSTOR.
CACHE, WRITECACHE, and NVME are explicitly **not supported** (see
6.11). The blockstor layer table:

| Layer | Allowed child layer |
|-------|---------------------|
| DRBD | LUKS, STORAGE |
| LUKS | STORAGE |
| STORAGE | (terminal) |

**Unit:** `pkg/rest/resource_groups_test.go` covers both directions:
- `--layer-list drbd,luks,storage` — accepted
- `--layer-list drbd,storage` — accepted
- `--layer-list luks,storage` — accepted (no-DRBD with at-rest encryption)
- `--layer-list storage` — accepted (no-DRBD plain local)
- `--layer-list cache,drbd,storage` — 400 (CACHE not supported)
- `--layer-list drbd,writecache,storage` — 400 (WRITECACHE not supported)
- `--layer-list nvme,storage` — 400 (NVME not supported)
- `--layer-list luks,drbd,storage` — 400 (wrong order; LUKS must be a child of DRBD, not parent)

### 6.10 No-DRBD storage class (single-replica local) — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** drbd-troubleshooting #15; PLAN.md Phase 9 LayerStack

RG with `PlaceCount: 1, LayerStack: ['STORAGE']` → pure local volume for app-level-replicated workloads (Postgres etc.). `linstor v l` DeviceName = `/dev/<vg>/<lv>` (no /dev/drbd*). `linstor node lost` should refuse or warn for single-replica RD (data loss).

### 6.11 CACHE / WRITECACHE / NVMe-oF / NVMe-TCP layers — O

- **Priority:** —  **Target:** unit (rejection pin only)  **Complexity:** L
- **Source:** UG9 §"NVMe-oF/NVMe-TCP LINSTOR layer" (lines 1838-1908), §"Writecache layer" (lines 1909-1944), §"Cache layer" (lines 1945-1978); advanced-config out-of-scope

**Out of scope for cozystack — these layers will not be supported.**
Rationale:

- **NVMe-oF/NVMe-TCP**: Cozystack runs HCI on flat L2 networks where
  DRBD-9's native protocol covers the use case. NVMe-oF adds a target
  / initiator split that duplicates DRBD's diskless attach without
  the replication.
- **CACHE / WRITECACHE**: These wrap dm-cache / dm-writecache between
  DRBD and STORAGE for tiered storage. Cozystack uses homogeneous
  pools (one tier per cluster, NVMe in current generations); the
  caching layer adds complexity without a use case. Operators
  needing tiering can layer it below LINSTOR (e.g., bcache under
  the LVM PV).

**Test stance:** the only test is the rejection pin in 6.9 above —
any `--layer-list` containing `cache`, `writecache`, or `nvme`
returns a clear 400 with `unsupported layer` text. No further
implementation work.

If a customer asks for one of these in the future, the conversation
is: "we don't, here's why, here's the workaround" — not "we'll add it
in a sprint." Reopening this decision needs explicit product approval,
not a code change.

---

## Encryption

### 6.13 LUKS layer encrypts data at rest — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Encrypted volumes" (lines 2273-2324); PLAN.md `pkg/luks` (5 contract tests)

**Unit:** `pkg/luks` Format / Open / Close / DevicePath. Key passes via stdin to keep secrets off argv (Phase 6 detail).
**E2E:** RD with `--layer-list drbd,luks,storage` → backing block device is encrypted (`cryptsetup status` on satellite shows `cipher: aes-xts-plain64`). Pod read/write transparent.

### 6.14 DRBD `shared-secret` for in-transit encryption — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** PLAN.md `POST /v1/resource-definitions/{rd}/encryption-passphrase`

Writes `DrbdOptions/Net/shared-secret` onto RD props. `.res` includes `net { shared-secret "..."; }`. DRBD handshake uses the secret to MAC the connection.

### 6.15 Master passphrase CRUD endpoints — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Encryption commands" (lines 2288-2324); PLAN.md `/v1/encryption/passphrase` POST/PATCH/PUT

`create-passphrase` (idempotent), `enter-passphrase` (re-unlock after restart), `modify-passphrase` (rotate). **Critical for piraeus orchestration** — piraeus reads Secret and calls these endpoints.

**Unit:** `pkg/rest/encryption_test.go` — re-issuing `create-passphrase` returns success (not "already exists"); `enter-passphrase` after a simulated restart re-opens LUKS volumes.

### 6.16 Contract-replay against LINSTOR oracle preserves encryption shape — S

- **Priority:** P1  **Target:** integration  **Complexity:** L
- **Source:** PLAN.md `tests/contract`

Recorded golinstor traces for encrypt CRUD must replay byte-identical against blockstor. Catches wire-shape regressions before piraeus breaks.

### 6.17 Auto-passphrase from env / linstor.toml — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Automatic passphrase" (lines 2326-2351); ug9-features 7.2

**Orchestrated by piraeus, not blockstor.** Piraeus reads Secret → calls `create-passphrase` / `enter-passphrase`. blockstor's job is honour those calls correctly (covered in 6.15).

---

## External DRBD metadata pool

### 6.18 `StorPoolNameDrbdMeta` routes metadata to separate pool — T

- **Priority:** P2  **Target:** unit + integration + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Using external DRBD metadata" (lines 4463-4534); ug9-features 6.3; advanced-config #10

**Status:** Not implemented. `grep StorPoolNameDrbdMeta` → zero matches.

Settable on node / RG / RD / resource / VG / VD (priority increasing). Creates two LVs/ZVOLs per replica: data + metadata.

**Unit (after implement):** conffile renderer emits `meta-disk /dev/<meta-pool>/<rd>_meta` instead of `internal`.
**Integration:** provider creates both volumes; LVs visible in `lvs`.
**E2E:** RD with `StorPoolNameDrbdMeta=meta`; write 1 GiB; verify via `iostat -dx 1` that small metadata I/O hits the meta device and bulk hits the data device.

**Why P2:** cozystack is typically homogeneous storage (one tier). External metadata is more useful in stretched / hybrid setups. Document workaround: just put both data + metadata on the faster pool.

---

## Drive replacement (operator runbook)

### 6.19 Failed disk replacement recovers via satellite reconnect — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** advanced-config #8; KB:replacing-a-failed-drive

**Why:** Cheapest physical-recovery path. Disk dies → replace → recreate LVM/ZFS pool with same name → satellite rediscovers.

**E2E:** RD with 2 diskful replicas on LVM-thin; power-down worker-2, swap disk, power-up; `pvcreate /dev/sdb && vgcreate blockstor-lvm /dev/sdb && lvcreate -T -L 14G blockstor-lvm/thin`; `linstor node reconnect worker-2`; StoragePool reappears; worker-2 replica re-syncs Inconsistent → UpToDate.

**Failure modes:**
- StoragePool stays missing → satellite reconcile didn't pick up new VG
- Replica stuck Inconsistent → metadata-zone reuse → recovery is `linstor r d worker-2 <rd>` + `rd ap` (cross-listed with 5.18)

---

## Fault injection

### 6.20 `dmsetup` error target simulates I/O failures — T

- **Priority:** P2  **Target:** integration  **Complexity:** H (design first)
- **Source:** advanced-config #12

**Why:** Real-disk failures rare in CI; need synthetic injection to test 5.11 (SkipDisk) and observer's `disk:Failed` handling.

**Design TBD:**
- Need a way to point a StoragePool at a dm device (`StoragePool.Spec.DeviceOverride`?)
- Privileged debug pod with `dmsetup` access
- Or: do this only on a non-Talos worker (Talos PSA forbids writable `/sys`)

**Integration (after design):** wrap backing LV with failing dm device; force a read of the bad region; observe DRBD events2 `change device disk:Failed`; verify observer auto-detach per `on-io-error=detach` policy.

---

## Snapshot CRUD + clone — backend semantics

Cross-listed from `04-lifecycle.md` here for the backend-specific assertions.

### 6.21 ZFS snapshot uses `zfs snapshot` — S

- **Priority:** P0  **Target:** integration  **Complexity:** L

Test command line: `zfs snapshot data/<rd>_00000@snap1`. Existing in `pkg/storage/zfs_integration_test.go`.

### 6.22 LVM_THIN snapshot uses `lvcreate -s` — S

- **Priority:** P0  **Target:** integration  **Complexity:** L

`lvcreate -s -n <snap> blockstor-lvm/<lv>`. FakeExec test in `pkg/storage/lvm`.

### 6.23 FILE_THIN snapshot uses reflink — S

- **Priority:** P1  **Target:** integration  **Complexity:** L

`cp --reflink=always source.img snap.img`. Requires XFS or Btrfs backing.

### 6.24 Cross-node ship picks the right tool — S

- **Priority:** P1  **Target:** integration  **Complexity:** L
- **Source:** PLAN.md `Reconciler.ShipSnapshot` (3 contract tests)

ZFS: `zfs send | ssh peer zfs recv`. LVM_THIN: `thin-send-recv`. Dispatched via injectable `ShipExec` so unit tests assert command lines.

---

## Implementation-order recommendation

1. 6.1, 6.2, 6.3 — provider CRUD foundation (existing; integration tests already green for ZFS)
2. 6.5 — pool auto-registration (existing)
3. 6.9 — layer stack ordering enforcement (likely partial — verify)
4. 6.13, 6.14, 6.15 — encryption (existing)
5. 6.21, 6.22, 6.24 — snapshot/ship per-backend (existing)
6. 6.4, 6.7, 6.10, 6.16 — capability matrix + no-DRBD + contract replay (mostly tests)
7. 6.6, 6.8, 6.23 — P2 fill-in
8. 6.11 — out-of-scope pinning (rejection test only)
9. 6.19 — drive replacement (operational runbook)
10. 6.18, 6.20 — external metadata + dmsetup fault injection (implement work)

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 4 |
| P0 integration | 6 |
| P0 e2e | 2 |
| P1 unit | 1 |
| P1 integration | 4 |
| P1 e2e | 3 |
| P2 (any) | 4 |
| T (implement first) | 2 |
| O (out of scope) | 1 (CACHE/WRITECACHE/NVME bundled under 6.11) |
