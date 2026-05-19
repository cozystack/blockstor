# cli-matrix — L6 mandatory operator-CLI e2e

This directory holds the L6 tier of the blockstor test pyramid (see `PLAN.md` § "L6 mandatory: operator-CLI e2e coverage"). Every user-reported operator-CLI bug must have a cell here that runs the real `linstor` CLI against the stand and asserts Status convergence via observer-stamped Resource.Status + a kernel probe — never via the REST "200 OK".

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
| `r-c-autoplace-3r.sh` | 328 | `linstor r c <rd> --auto-place=3 -s lvm-thin` succeeds on a 3-node cluster with healthy lvm-thin SPs; no "Not enough nodes" string on stderr. |
| `sync-final-uptodate-transition.sh` | 329 | After a 3rd replica is added to a 2-replica RD, the new replica's State converges from `UpToDate(NN%)` to a bare `UpToDate` AND replication state reaches `Established`. |
| `r-td-diskless.sh` | 330 | `linstor r td --diskless <node> <rd>` on a diskful replica flips Spec.Flags + Status.DiskState to Diskless within 30s, and `drbdsetup status` on the satellite confirms `disk:Diskless`. |
| `r-l-conns-shapes.sh` | 331 | Conns/State column contract: parses `linstor r l` JSON across (Healthy, Disconnected peer, Diskless, TieBreaker) shapes and pins observer's events2 translation. |

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
4. Land the cell in the **same commit** that closes the bug it covers. Per PLAN.md L6 rules: without the L6 cell the bug counts as not closed.
