# CLI parity audit refresh: blockstor REST vs upstream LINSTOR (piraeus)

Date: 2026-05-19
Refresh-of: `docs/cli-parity-audit-2026-05-14.md`
Driver: `linstor` CLI 1.27.1 (same dev box as the original audit)
Targets:
- **BS**: blockstor apiserver — `linstor controller 1.33.2+; GIT-hash: blockstor`.
- **UP**: piraeus bundled Java controller — `linstor controller 1.32.3; GIT-hash: 6dac06aed233f2c89ac7cc6b1185d6dce9ec74c4`.

Method: read-only re-audit of the F1-F20 fix list against current `main`. For each item the refresh records:
- **CLOSED** — fix has landed; commit hash + date cited.
- **OPEN** — gap still exists; file:line cited where the fix would land.
- **DEFERRED** — explicitly out of scope per `PLAN.md` or memory.
- **SUPERSEDED** — replaced by a different fix path (cited).

The original audit was a one-shot Markdown snapshot. As of `261d9e32f` (2026-05-19) we now have a re-runnable harness — `tests/operator-harness/cli-parity-refresh.sh` — that emits the same kind of table from a live two-controller diff and CI-fails on any non-PARITY row absent from `docs/cli-parity-known-deltas.md`. Future audits should therefore live as harness output, not hand-rolled markdown.

## 1. F1-F20 status table

| ID | Original gap (one-line) | Status | Evidence |
|----|--------------------------|--------|----------|
| F1 | `linstor n l` Addresses column blank because `net_interfaces[].address/port/type/uuid` not serialised. | **CLOSED** | `95e64c84e` (2026-05-14, address+port+type default on read) + `b5e09ff06` (2026-05-14, stable UUID per (node, ifname)). Live code: `pkg/api/v1/node.go:87-138`; `DefaultNetInterfaceFields` runs in both store backends. |
| F2 | Node DTO missing `uuid`, `resource_layers`, `storage_providers`, `unsupported_layers/providers`, `props.NodeUname/CurStltConnName`. | **CLOSED** | `b21c1d872` (2026-05-14, "synthesise Node UUID + capability fields on read"). Live code: `pkg/api/v1/node.go:35-79` declares the full DTO; satellite NodeStatus capability message feeds the slice. |
| F3 | StorPool DTO missing `free_capacity` / `total_capacity` / `static_traits.SupportsSnapshots`; `SharedName` blank; `DfltDisklessStorPool` not auto-created per node. | **CLOSED** | `95e64c84e` (2026-05-14): (a) DfltDisklessStorPool auto-create on Node create (`pkg/rest/nodes.go:459-467`); (b) FILE_THIN reports SupportsSnapshots=true (`pkg/storage/file/file.go`); (c) `free_space_mgr_name` stamped `<node>:<pool>` from InMemory store. Capacity wiring: `pkg/api/v1/storage_pool.go:27-37`. |
| F4 | RG `select_filter.place_count` not nested under `select_filter`. | **CLOSED (pre-audit)** | Wire shape was already correct at audit time — `pkg/api/v1/resource_group.go:50-108` shows `SelectFilter AutoSelectFilter \`json:"select_filter,omitzero"\``. The original audit's row #4 misclassified a glob-rendering test result (audit row #4 was actually labeled "BLOCKSTOR_SUPERSET" not WIRE_SHAPE). No regression. |
| F5 | Default RG named `dfltrscgrp` (lowercase) instead of canonical `DfltRscGrp`. | **CLOSED** | `f2112ca0f` (2026-05-14, "preserve canonical DfltRscGrp case on the wire", Bug 57). Live constant: `pkg/rest/resource_definitions.go` defines `DefaultResourceGroupName = "DfltRscGrp"`. CSI provisioner StorageClass `linstor.csi.linbit.com/resourceGroup` now matches verbatim. |
| F6 | `rd l --resource-definitions <name>` returns ALL RDs (filter ignored). | **CLOSED** | `81f690732` (2026-05-14, "honour resource_definitions filter on RD list", Bug 61). Live code: `pkg/rest/resource_definitions.go:50-56` calls `multiValueQuery(r, "resource_definitions")`. Same fix carried into VD list. |
| F7 | RD `layer_data[]` empty → `linstor rd l` Layers column blank. | **CLOSED** | `80b82f955` (2026-05-14, "populate layer_data on RD/VD/Volume wire shapes", Bug 58). Live: `pkg/api/v1/resource_definition.go:35` declares the field; `annotateRDLayerData` at REST edge derives from LayerStack. |
| F8 | `GET /v1/view/volume-definitions` returned `[]` → `linstor vd l` empty rows. | **CLOSED** | `80b82f955` (2026-05-14, Bug 58). Live: `pkg/rest/volume_definitions.go:86-87` registers `GET /v1/view/volume-definitions → handleVDView`. |
| F9 | `volumes[].storage_pool_name` and `minor_number` missing → `linstor v l` shows `StoragePool=None`. | **CLOSED** | `pkg/api/v1/resource.go:159` defines `StoragePool string \`json:"storage_pool_name,omitempty"\`` on `Volume`. Bug 58 wired the DRBD-layer `minor_number` per-volume via `LayerDataList`. |
| F10 | `layer_object.drbd.{node_id,peer_slots,may_promote,...}` and `layer_object.children[].storage.storage_volumes[]` missing → no `drbdtop`-style observability. | **CLOSED (partial)** | Recursive layer tree present: `pkg/api/v1/resource_definition.go:91-189` declares `ResourceLayer.Children`, `DrbdResourceLayer.{TCPPorts,Connections,DrbdVolumes}`, `StorageResourceLayer.StorageVolumes`. Note: `may_promote`, `promotion_score`, `node_id`, `al_size`, `al_stripes` are NOT yet populated — they are derivable from `drbdsetup events2`, currently observed but not surfaced through the DRBD-layer DTO. **Residual gap:** see "Still open" section below. |
| F11 | `GET /v1/physical-storage` returned empty → `linstor ps l` blank table. | **CLOSED** | `b85f46656` (2026-05-10, "GET /v1/physical-storage[/{node}] from PhysicalDevice CRDs", Phase 10.7). Live: `pkg/rest/physical_storage.go:73-80` wires the LIST + per-node + POST routes. Bug 326 (2026-05-19, `84276ad63`) closed the strict-decoder VDO-field reject on the CDP POST path. |
| F12 | `GET /v1/error-reports` returned empty → `linstor err l` blank. | **CLOSED** | `176310994` (2026-05-14, "err l ring buffer", Bug 62). Live: `pkg/rest/error_reports.go:175-176` wires LIST + GET; reconcilers push via `RecordErrorReport` (in-memory ring, cap 1000). |
| F13 | Autoplace shortfall returned terse string; upstream returns `cause`/`correction`/`details` envelope with criteria list. | **CLOSED** | `0d0ddc7a5` (2026-05-14, "structured ApiCallRc envelope for autoplace shortfall", F13). Live: `pkg/rest/autoplace.go:512-700` builds full criteria bullet list + "Replica count: N" block. |
| F14 | `s d <rd> nonexistent` returned `SUCCESS` envelope; upstream returns `WARNING` (mask `0x4000_0000`). | **CLOSED** | `176310994` (2026-05-14, Bug 62 row #33). Live: `pkg/rest/snapshots.go:871-879` returns `warnSnapshotNotFound` on `ErrNotFound`. Wire body now matches upstream `Snapshot definition <snap> of resource <rd> not found.` shape. |
| F15 | `r d <rd> <node>` of non-existent pair returned 500+ERROR; upstream returns 200+WARNING. **CSI-blocking.** | **CLOSED** | `156a45c9e`/`dc48620df` (2026-05-14, "idempotent ResourceDelete folds unknown (rd, node) into 200 + WARN"). Live: `pkg/rest/delete_toctou.go:84-167` runs the Bug-174 framework; `warnRscNotFound`/`warnRDNotFound` constants in `pkg/rest/api_call_rc.go`. |
| F16 | `rg modify --place-count N` did not re-run autoplace on dependent RDs. | **CLOSED** | Three-commit wave: `e9c219fd4` (rebalance-pending annotation), `fb5b8b1f5` (`RGRebalanceReconciler`), `6fffaaaef` (rebalance-scheduled count in reply). All 2026-05-14, all Bug 60. Live: `pkg/rest/resource_groups.go:191-234` checks `rgNeedsRebalance`, enqueues annotation; `internal/controller/` runs `RGRebalanceReconciler`. |
| F17 | `n c` reply was single-line, no UUID, no "not connected" warning. | **CLOSED** | `176310994` (2026-05-14, Bug 62 row #40). Live: `pkg/rest/nodes.go:565-600` `buildNodeCreateEnvelope` emits SUCCESS+UUID+WARNING-no-active-connection apiCallRc array. |
| F18 | Snapshot reports `State=Incomplete` even after satellite failure; upstream surfaces `Failed`. | **CLOSED** | `19237d07c` (2026-05-14, "stamp Status.Flags=FAILED on terminal CreateSnapshot errors", F18). Live: `pkg/rest/snapshots.go:362-378` recognises `FAILED`/`FailedDeployment`/`FailedDisconnect` terminal flags; satellite snapshot reconciler stamps `Status.Flags=FAILED` on terminal error. |
| F19 | TieBreaker rendered `Layers=DRBD` only; upstream shows `DRBD,STORAGE`. | **CLOSED** | `27d64e411` (2026-05-14, "keep STORAGE child layer on diskless / tiebreaker", F19). Live: `pkg/rest/resources.go:200-287` `ensureVolumesForView` synthesises diskless storage child; `StorageResourceLayer.ProviderKind=DISKLESS` carried through. |
| F20 | Snapshot DTO missing `flags`, `snapshots[].snapshot_volumes[]`, `snapshot_definition_props`, `resource_definition_props`. | **CLOSED** | `bab584d19`/`4ab465a14` (2026-05-14, "snapshot DTO carries inherited props + per-node snapshot_volumes", F20). Live: `pkg/api/v1/snapshot.go:38-90` declares full DTO; backup/restore tooling can round-trip the shape. |

### Totals

- **CLOSED**: 19 of 20 (F1, F2, F3, F4, F5, F6, F7, F8, F9, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20).
- **CLOSED (partial)**: 1 (F10 — recursive layer tree present, but `may_promote`/`promotion_score`/`node_id` not stamped on the wire).
- **OPEN**: 0 of the original F1-F20 in the strict sense. F10's residual is tracked below.
- **DEFERRED**: 0 (`PLAN.md` and `docs/cli-parity-known-deltas.md` defer separate items — see section 4).
- **SUPERSEDED**: 0.

## 2. Closed since 2026-05-14

Chronological by author date:

- **2026-05-14 00:23** `81f690732` — F6 (RD filter ignored, Bug 61).
- **2026-05-14 00:25** `f2112ca0f` — F5 (DfltRscGrp CamelCase, Bug 57).
- **2026-05-14 00:26** `156a45c9e` — F15 (resource delete idempotent + WARN envelope).
- **2026-05-14 08:13** `95e64c84e` — F1 (partial: address/port/type) + F3 (partial: DfltDisklessStorPool + FILE_THIN snapshot capability + SharedName).
- **2026-05-14 08:17** `80b82f955` — F7 + F8 + F9 (layer_data on RD/VD/Volume).
- **2026-05-14 08:20** `e9c219fd4` / `fb5b8b1f5` / `6fffaaaef` — F16 (rg modify re-autoplace via RGRebalanceReconciler, Bug 60 wave).
- **2026-05-14 08:27** `176310994` — F12 (err l) + F14 (snapshot delete WARN) + F17 (node create envelope) — all Bug 62.
- **2026-05-14 09:20** `b5e09ff06` — F1 (UUID synthesis on NetInterface).
- **2026-05-14 09:22** `0d0ddc7a5` — F13 (autoplace shortfall envelope).
- **2026-05-14 09:24** `bab584d19` — F20 (Snapshot DTO).
- **2026-05-14 09:27** `27d64e411` — F19 (TieBreaker STORAGE child layer).
- **2026-05-14 09:31** `b21c1d872` — F2 (Node UUID + capability fields).
- **2026-05-14 09:35** `19237d07c` — F18 (snapshot Status.Flags=FAILED).
- **2026-05-19 09:59** `84276ad63` — Bug 326: F11-adjacent strict-decoder reject on `ps cdp` (cli-parity-orthogonal but unblocks the F11 write path).

F11/F12 had landed before the audit was written (`b85f46656` 2026-05-10 and `1eeb29a32` 2026-05-08 respectively), but the audit caught a measurement bug — the GET endpoints existed but returned `[]` because the in-memory ring/cache was empty in the test cluster. The "auto-write on first error" + "PhysicalDevice CRD discovery loop" landed in the Phase 10.7 wave; Bug 62/`176310994` made `err l` start emitting entries.

## 3. Still open as of 2026-05-19

Pure F1-F20 leftovers: only the F10 sub-fields (DRBD layer richness).

| Priority | Gap | File:line | Recommendation |
|----------|-----|-----------|----------------|
| P3 | `layer_object.drbd.{node_id, peer_slots, may_promote, promotion_score, al_size, al_stripes, al_stripe_size_kib}` not serialised. | `pkg/api/v1/resource_definition.go:142-161` (`DrbdResourceLayer`) | Extend `DrbdResourceLayer`; satellite events2 observer already records DRBD-id and may_promote on Status. Drives `drbdtop` / advanced monitoring; not CSI-blocking. |
| P3 | `layer_object.luks` data carriers (passphrase-key-id etc.) | `pkg/api/v1/resource_definition.go:91` (`ResourceLayer.Data`) | Currently a `map[string]any` opaque blob; the CLI reads it through `layer_data.luks` to render the State suffix. Not urgent — LUKS layer detection works through `LayerStack` already (see `stampSuspendedOnLUKS`). |

Everything else from F1-F20 is closed. Open items in `docs/cli-parity-known-deltas.md` mostly track *new* divergences uncovered by the L6/L7 wave (see section 5).

## 4. Recurring bugs that should have been caught by F*

Cross-reference of Bug 326-335 (the wave that triggered this refresh) against F1-F20:

| Bug | One-liner | Should F* have caught it? | Why F* missed it |
|-----|-----------|---------------------------|------------------|
| 326 | `linstor ps cdp` body carries `vdo_enable` and seven sibling VDO/RAID/SED knobs; strict decoder rejected unknown fields. | **No, not in scope.** F11 covered the GET side. The audit's row #16 only exercised `linstor ps l`, never `ps cdp` POST. | F* is a read-path audit. POST envelopes weren't surveyed. **Lesson:** L7 catalogue must include at least one write per resource type with the *full* CLI body (not just minimal happy path). |
| 327 | `linstor r c <node> <rd>` (no `--diskless` flag) brought up the new replica as Diskless because pool resolution from parent RG was skipped. | **Partial.** Would have been caught by an L6 cell on `r c` followed by `linstor r l` (would show Diskless flag). F* row #12 (`v l`) exercised the post-state but only for an autoplaced RD. | Resource-create wire-body was treated as a write whose effect was sampled through unrelated reads. **Lesson:** L7 replay YAMLs must pin `r c` -> `r l` round-trips per flag combination. |
| 328 | Autoplace `--storage-pool=A,B` picked only A on a 3-node fleet because the candidate set was pre-filtered. | **No.** F13 surfaces *failed* autoplace; this bug had a *successful* but wrong placement. | F* never measured placement *correctness*, only error-envelope shape. **Lesson:** L7 must include multi-pool autoplace assertions. |
| 329 | `r l` Conns column stuck on "(NN%)" after sync finished — satellite cached stale `out_of_sync_kib`. | **No.** F9/F10 surfaced the DTO, but neither audits dynamic state transitions. | F* was static-shape. **Lesson:** L6 must run a "state-transition" cell per Conns/State value. |
| 330 | `r td --diskless` returned SUCCESS but DRBD kept disk attached. | **No.** Same as 329 — F* never exercised post-mutation state convergence. | Read-only audit. **Lesson:** every CLI verb that mutates DRBD state must have an L7 replay step that asserts the DRBD kernel slot reaches the requested state. |
| 332 | Late `vd c` after RD had reached UpToDate left new volumes Diskless (regression of Bug 79). | **No** — multi-volume RDs weren't in the audit at all. | The seed in the audit used a single-volume `parity-rd`. **Lesson:** L7 fuzz must vary VD cardinality. |
| 333 | LUKS layer operator-CLI cells (test-only). | Test-only. | n/a |
| 334+335 | `r c <rd> --auto-place=2 -l STORAGE` created 2 unrelated diskful volumes + a TieBreaker — STORAGE-only multi-place is meaningless. | **No.** F13 surfaces *insufficient candidate* envelopes; F19 surfaces tie-breaker layer reporting. Neither asserts that STORAGE-only multi-place should fail. | Audit measured shapes, not safety invariants. **Lesson:** L7 invariants must include "no silent split-brain". |

**Summary — the audit-of-audit conclusion:**

F* (the one-shot 2026-05-14 audit) was a *static read-path wire-shape* survey. It correctly identified 20 wire-shape and error-envelope deltas, all of which landed in the following week. It did **not** survey:

1. Write-side body envelopes (Bug 326).
2. Post-mutation state convergence (Bugs 327, 329, 330, 332).
3. Safety invariants on placement decisions (Bugs 328, 334+335).

This is exactly the gap the L7 harness was created to close: `cli-parity-refresh.sh` continues the static survey on a CI cadence; replay YAMLs catch dynamic state convergence; `operator-fuzz.sh` (L8 skeleton, follow-up) catches placement-correctness fuzz. The 9-commit wave Bug 326-335 (2026-05-19) is the post-mortem that proved the gap; the L6 + L7 mandates in `PLAN.md` are the structural response.

## 5. Recommendations

1. **Delete** the per-row "open list" in `docs/cli-parity-known-deltas.md` for items that F1-F20 closed (sections 5/12/13/17/40/42 of the open list). Update made in this refresh.
2. **Keep open** the rows that track items the harness can re-discover (#04 BLOCKSTOR_SUPERSET, #18 git_hash, #19 controller list-properties subset, #21/22 advise, #52 exos, #53 backup, #54 schedule).
3. **Promote** the F10 residual (DRBD layer richness) into a Phase-tracked task: not CSI-blocking, but `drbdtop`-style operator UX wants it.
4. **Future audits**: do not write another one-shot Markdown like F1-F20. Use `cli-parity-refresh.sh`'s output instead and append L7 replay YAMLs for each new verb. Bug 326-335 wave is exactly the failure mode the L7 harness is built to prevent recurring.
