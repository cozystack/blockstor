# cli-matrix — L6 mandatory operator-CLI e2e

This directory holds the L6 tier of the blockstor test pyramid (see CLAUDE.md → Test tiers). Every user-reported operator-CLI bug must have a cell here that runs the real `linstor` CLI against the stand and asserts Status convergence via observer-stamped Resource.Status + a kernel probe — never via the REST "200 OK".

Why this layer exists: bug-hunt waves v1–v40 caught ~250 REST-handler-level issues via unit tests + `tests/integration/group_*_test.go`. None of those exercise the real `python-linstor → apiserver SSA → satellite cache lag → events2 observer → Status.DiskState` pipeline end-to-end. Bugs 326-330 are the post-mortem of that gap.

## Structure

Each cell is a self-contained shell script `<verb>-<shape>.sh` that:

1. Sources `lib.sh` (which re-sources `../lib.sh`).
2. Calls `linstor_cli_setup` to port-forward the apiserver + build the `LCTL[]` array.
3. Lays down a cluster shape (2r / 2r-tb / 3r / 1r-2d / flip) via `kubectl apply` or by chaining `linstor rd c / vd c / r c` calls.
4. Issues the operator command under test through `"${LCTL[@]}" ...`.
5. Waits for convergence using the helpers in `lib.sh`:
   - `wait_status_state RD NODE EXPECTED [TIMEOUT]` — observer-stamped DiskState.
   - `wait_status_diskless RD NODE [TIMEOUT]` — flag+disk+kernel cross-check.
   - `wait_sync_done RD NODE PEER [TIMEOUT]` — Bug 329: bare `UpToDate` + `Established`, no `(NN%)` suffix.
   - `wait_conns_ok RD NODE PEER [TIMEOUT]` — peer connection Connected/Established.
6. Tears down with `delete_rd` (inherited from parent `lib.sh`) and calls `assert_no_orphans` to verify nothing leaked.

## PASS vs FAIL

A cell counts as **PASS** only when both legs converge inside the bounded timeout:

- **Observer-stamped Status**: `kubectl get resource <rd>.<node> -o json` shows the expected `status.volumes[].diskState`, `status.connections[].message`, `status.role`, or `status.suspended` — whichever the cell under test asserts.
- **Kernel probe** (when applicable): `on_node <node> drbdsetup status <rd>` reports the matching kernel-side state.

A cell counts as **FAIL** if either leg times out, the `linstor` CLI exits non-zero on a path expected to succeed, or `assert_no_orphans` flags residue with `STRICT_ORPHANS=1`.

## Cells

| File | Bug | What it pins |
|---|---|---|
| `ps-cdp-zfs-vdo_enable.sh` | 326 | `linstor ps cdp ... zfs` accepts wire body with `vdo_enable` + sibling VDO/RAID fields without 400. |
| `r-c-on-shape-2r-tb.sh` | 327 | After deleting a diskful replica and re-creating it on a cluster that already carries a TIE_BREAKER witness on another node, the new replica is **diskful** (DRBD,STORAGE layers, UpToDate) — NOT Diskless. |
| `r-d-then-r-c-stuck.sh` | 339 | After `r d <node> <rd>` + bare `r c <node> <rd>` on the SAME node, the recreated replica must converge to UpToDate AND peer connections (both directions) must reach Connected/Established within 90s — no stuck `Connecting` / `StandAlone` / `WFBitMap*`. Standalone catcher for the stuck-state pattern reported as task #532. |
| `r-c-autoplace-3r.sh` | 328 | `linstor r c <rd> --auto-place=3 -s lvm-thin` succeeds on a 3-node cluster with healthy lvm-thin SPs; no "Not enough nodes" string on stderr. |
| `sync-final-uptodate-transition.sh` | 329 | After a 3rd replica is added to a 2-replica RD, the new replica's State converges from `UpToDate(NN%)` to a bare `UpToDate` AND replication state reaches `Established`. |
| `r-td-diskless.sh` | 330 | `linstor r td --diskless <node> <rd>` on a diskful replica flips Spec.Flags + Status.DiskState to Diskless within 30s, and `drbdsetup status` on the satellite confirms `disk:Diskless`. |
| `r-td-diskless-reaps-tiebreaker.sh` | parity | Sibling of `r-d-collapses-tiebreaker` for the toggle path: on a 2-diskful + TIE_BREAKER RD, `linstor r td --diskless <node> <rd>` drops diskful to 1 and the auto-witness is reaped within 30s, settling on exactly 2 rows (1 diskful UpToDate + 1 user-diskless) with no TIE_BREAKER. Pins the upstream-parity contract that no witness is managed below 2 diskful (quorum=off at 1 diskful). |
| `r-l-conns-shapes.sh` | 331 | Conns/State column contract: parses `linstor r l` JSON across (Healthy, Disconnected peer, Diskless, TieBreaker) shapes and pins observer's events2 translation. |
| `snap-restore-snapshotless-node-rejected.sh` | 397 | P0 DATA INTEGRITY. `snapshot resource restore` onto a node NOT holding the snapshot is rejected (no silent empty replica, no orphan RD); restoring onto the snapshot's own nodes converges UpToDate AND every replica holds the real snapshot bytes (marker read per-replica), never a silently-empty UpToDate copy. |
| `rd-clone-vd-data-plane.sh` | 020 | `linstor rd clone <src> <dst>` on a VD-bearing source (plain CLI body, no `use_zfs_clone`) AND the raw-REST `use_zfs_clone=true` body linstor-csi sends both materialise a real clone: 2 replicas UpToDate, marker bytes from the source present on EVERY clone replica (promote each in turn), clone status COMPLETE, internal `clone-<dst>` snapshot visible on the source. Pre-fix: 400 on `use_zfs_clone`, 501 on VD-bearing sources (Bug 114 gate). |
| `encryption-passphrase-luks-rd.sh` | 023 | Secret-only LUKS flow: `linstor encryption create-passphrase` alone (legacy `DrbdOptions/EncryptPassphrase` controller prop asserted ABSENT throughout) unlocks `rd c -l drbd,luks,storage` + autoplace to UpToDate, and the Secret-backed passphrase actually opens the LUKS header on each replica's backing device. Requires the Bug-023 fix (PR #143); pre-fix the rd-create is rejected with "LUKS layer requires DrbdOptions/EncryptPassphrase to be set first". |
| `vd-modify-preserves-drbd-minor.sh` | 433 | A legal in-bounds VD-scoped modify (`vd set-size` grow / `vd set-property`) must NOT change the per-volume DRBDMinor — the /dev/drbd<N> device identity. Creates rd-a (lowest minor Ma) + rd-b (next minor Mb), frees Ma by deleting rd-a, then modifies rd-b: its DRBDMinor AND device path must stay Mb over a settle window. Pre-fix `wireToCRDVD` dropped the minor on the VD-scoped write-back and the allocator re-stamped the freed lower Ma — a permanent device-identity change on a live resized volume. |
| `rg-modify-invalid-layer-rejected.sh` | 434 | `rg modify --layer-list storage,drbd` (STORAGE-before-DRBD, the ordering the create path refuses) must be rejected, the RG must keep its valid `[DRBD,STORAGE]` stack, and an RD inheriting from the RG must get the valid stack — never `[STORAGE,DRBD]`. Pre-fix `rg modify` gated only place_count and `handleRDCreate` validated the explicit layer_list BEFORE the RG inherit, so an invalid stack reached a persisted RD via the modify→inherit chain. |

## Running

On the stand (any worktree with `kubectl` pointing at a healthy 3-worker blockstor cluster):

```sh
# single cell:
make e2e NAME=<cluster> SCENARIO=cli-matrix/r-td-diskless

# whole matrix (sequential):
for cell in tests/e2e/cli-matrix/*.sh; do
    [ "$(basename "$cell")" = "lib.sh" ] && continue
    bash "$cell" .work/<cluster> || true
done
```

The nightly dispatcher (`/tmp/run14-dispatch.sh` on the dev host) is extended to run the `cli-matrix/*` cells on the e2e2 lane alongside the existing scenarios — they share the same stand resources and complete in ~5 min apiece.

## Adding a new cell

1. Pick a `<verb>-<shape>.sh` filename. Keep verbs aligned with the CLI nouns (`r-c`, `r-d`, `r-td`, `ps-cdp`, `sp-c`, `rd-c`, etc.).
2. Start from the boilerplate at the top of any existing cell (shebang, source lib, `require_workers`, `trap delete_rd` + `assert_no_orphans`, `linstor_cli_setup`).
3. Bound every wait with a hard timeout. Use `wait_*` helpers — do NOT add bespoke polling loops unless the contract is genuinely new.
4. Land the cell in the **same commit** that closes the bug it covers. Per the L6 rules: without the L6 cell the bug counts as not closed.
