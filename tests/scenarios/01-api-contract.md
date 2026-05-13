# Group 1 — API & CLI contract

REST wire shape, list-endpoint envelopes, error/ApiCallRc shapes,
pagination, case-insensitive name lookup, and the cheat-sheet's
operator-command completeness audit. The cheapest layer to test —
most of it lives in `pkg/rest/*_test.go` against an in-memory store.

[Group index in README.md](README.md).

---

## Read-only list endpoints (CLI table renderers)

### 1.1 `linstor node list` renders satellite-backed nodes — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #1

**Why:** Operator's first command. NodeType / Addresses / State columns must populate so subsequent narrowing has data to walk.

**Unit:** `Server.handleNodes` against an in-memory store with 3 SATELLITE entries returns the expected `[]Node` shape (NodeType, NetInterfaces, ConnectionStatus).
**E2E:** fresh 3-worker stand → `linstor n l` exit 0, 3 rows, State=Online, Addresses non-empty.

### 1.2 `linstor storage-pool list` enumerates per-node pools — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #2, observability #1

**Why:** Capacity planning. Includes synthesised `DfltDisklessStorPool` per node.

**Unit:** view-store handler synthesises diskless rows even when the store has none persisted.
**E2E:** 3 workers × 3 pools (`stand`, `lvm-thin`, `zfs-thin`) + 3 diskless = 12 rows. `CanSnapshots` matches the matrix in `06-storage-backends.md`.

**Failure mode:** `__WORKER_1__` placeholder leak (raw `kubectl apply` instead of `install-pools.sh`).

### 1.3 `linstor resource-definition list` shows RDs with port + RG — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** linstor-cli #3

`Server.handleResourceDefinitions` returns name, port, RG, state. Port allocated by the controller's allocator.

### 1.4 `linstor volume-definition list -r <rd>` shows inline VDs — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #4

**Why:** Wire-shape regression caught this session — CLI iterates `lstmsg.resource_definitions[i].volume_definitions[j]`. Flat `[]VolumeDefinition` renders empty.

**Unit:** assert handler returns `[]rdWithVDs` envelope shape, not flat slice.
**E2E:** `linstor vd l -r test1` shows 1 row, size 10 GiB, minor allocated.

### 1.5 `linstor resource list -r <rd>` shows per-node replicas — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #5

**Why:** Same port for all rows; per-row state (UpToDate / TieBreaker / Diskless) distinguished. **Connections cleanup** (the destroy-event fix this session) — no ghost StandAlone rows for removed peers.

### 1.6 `linstor volume list -r <rd>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #6

Per-replica view. DeviceName = `/dev/drbd<N>`, Allocated reflects thin usage, tiebreaker excluded.

### 1.7 `linstor resource-group list` renders SelectFilter — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #7

PlaceCount / StoragePool / DisklessOnRemaining / LayerStack fields must round-trip through `rg modify`.

### 1.8 `linstor volume-group list <rg>` — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** linstor-cli #8

---

## Wire-shape envelopes (regression guards)

These pin the exact JSON shapes golinstor's decoders expect. Each one was a real csi-sanity failure this session.

### 1.9 KV store endpoint returns `[]KV` (single-element array) — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** PLAN.md csi-sanity gap closure

**Why:** linstor-csi's snapshot-lookup decoder expects an array. Bare object broke `ListSnapshots check presence`.

**Test:** `pkg/rest/kv_store_test.go` — `GET /v1/key-value-store/{instance}` returns `[]KV` with one entry.

### 1.10 KV PUT/DELETE persist in process-local bag — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

**Why:** no-op writes broke `CreateSnapshot from source volume` (CSI writes the snapshot's pvName into KV, then reads it back).

### 1.11 RD clone returns `ResourceDefinitionCloneStarted` envelope — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** PLAN.md, csi-sanity

Was `[]ApiCallRc` — broke `CreateVolume from source`. Now wraps `cloneStartedResponse{Location, SourceName, CloneName, Messages}`.

### 1.12 Snapshot synthesises `Snapshots[]` per node at create — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

`Spec.Nodes` → `Status.Snapshots[]` synthesised in REST handler at Create time so the satellite-reconcile delay doesn't hide the snapshot from `ListSnapshots`.

### 1.13 CreateSnapshot idempotent; DeleteSnapshot folds NotFound — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

Both endpoints must be safe to retry. csi-sanity's AfterEach calls Delete twice on cleanup.

### 1.14 `/v1/view/{snapshots,resources}` honour `offset` + `limit` — S

- **Priority:** P1  **Target:** unit  **Complexity:** L

CSI paginates list calls. Test exercises offset=0/limit=1 → returns first; offset=1/limit=1 → returns second; offset=>count → empty.

### 1.15 `/v1/remotes/{type}` returns bare `[]` — S

- **Priority:** P1  **Target:** unit  **Complexity:** L

Typed list returns bare array; only `/v1/remotes` (untyped) returns envelope. Fixes a csi-sanity wire-shape mismatch.

### 1.16 NodeModify merges instead of clobber-writing — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

`PUT /v1/nodes/{node}` decodes `apiv1.NodeModify` (with `GenericPropsModify`) and merges into existing — clobber would wipe Aux/ props on every CSI call.

### 1.17 `r d` cleans up Connections for removed peer — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #13, drbd-troubleshooting

**Why:** This session's bug — observer didn't handle `destroy connection` events → ghost StandAlone entries forever.

**Unit:** `pkg/satellite/controllers/observer_test.go` — feed `change connection ... action:destroy` → `translateEvent` emits `connectionObservation{Removed: true}` → `mergeConnections` deletes the peer.
**E2E:** delete a replica; verify peer's `Status.Connections[]` shrinks within 10s.

---

## Case-insensitive name handling

### 1.18 Mixed-case RG/RD names normalise to lowercase — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** linstor-cli #23

**Why:** Upstream LINSTOR is case-insensitive. `DfltRscGrp` and `dfltrscgrp` address the same object.

**Unit:** `pkg/store/k8s/crdname_test.go` — `Name("DfltRscGrp") == "dfltrscgrp"`; `SetOriginalName` skips annotation on case-only differences (`TestSetOriginalName_CaseOnlyDifference`).
**E2E:** `linstor rg c MyMixedCaseRG` → `linstor rg l` shows `mymixedcaserg`; `linstor rg modify MYMIXEDCASERG ...` finds it.

### 1.19 Non-RFC1123 names slugify deterministically — S

- **Priority:** P1  **Target:** unit  **Complexity:** L

`needs/slugify` → `<8hex>-needs-slugify` so CRD apply doesn't fail validation. Annotation `blockstor.io/original-name` preserves the original for round-trip.

---

## Error / ApiCallRc envelope

### 1.20 ApiCallRc has int64 `ret_code` and uppercase satellite_encryption_type — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** PLAN.md csi-sanity gap closure

ret_code as int (not string), and CSI-specific normalisations from this session's wire-shape work.

### 1.21 Override-props passthrough on RG/RD/spawn payloads — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

`override_props` in spawn payload → propagates to spawned RD's props. csi-sanity's `CreateVolume with parameters` exercises this.

### 1.22 Lenient JSON decoder matches LINSTOR semantics — S

- **Priority:** P0  **Target:** unit  **Complexity:** L

Trailing junk / extra fields don't 400. Upstream LINSTOR accepts them; rejecting breaks linstor-csi.

---

## Cheat-sheet command completeness audit

These are smoke tests — each command must work end-to-end against blockstor.

### 1.23 Level-1 CSI commands (kubectl describe pvc / volumeattachments / piraeus-csi logs) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** observability #10

Smoke: each `kubectl logs piraeus-csi-controller -c {csi-attacher,csi-provisioner,csi-resizer}` returns non-empty.

### 1.24 Level-2 LINSTOR-CLI commands work against blockstor REST — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** observability #11

Matrix: `n l`, `sp l`, `rd l`, `vd l`, `r l`, `v l`, `r c [--diskless]`, `r d`. Each must exit 0 against a fresh stand. Covered by 1.1–1.6 individually; this is the integration smoke.

### 1.25 Level-3 satellite-container utilities — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** observability #12

`drbdadm status / down / up / primary --force / secondary / disconnect / connect [--discard-my-data] / adjust`, `cat /etc/drbd.d/<rd>.res`, `dmesg | grep drbd`, `lsblk`, `lvs / vgs / pvs`, `zfs list / zpool list`.

`tests/e2e/satellite-utils-smoke.sh` enters each satellite pod and runs each command — no "command not found".

**Failure modes caught:**
- `lvs` missing → LVM_THIN debugging blind
- `dmesg` permission-denied → satellite pod missing `CAP_SYSLOG`
- `.res` not at `/etc/drbd.d/` (upstream uses `/var/lib/linstor.d/`) → either symlink in image or document the delta

### 1.26 Pod / deployment / namespace naming deltas vs upstream — P

- **Priority:** P2  **Target:** e2e  **Complexity:** L
- **Source:** observability §B-C

Upstream cheat sheet says `kubectl exec -ti linstor-controller -- bash`. blockstor says `kubectl exec -n blockstor-system -ti deploy/blockstor-apiserver -- bash`. Either rename for parity or document — test asserts the **blockstor-specific** command works.

---

## Pagination, query params, view endpoints

### 1.27 view/resources + view/snapshots paginate identically — S

- **Priority:** P1  **Target:** unit  **Complexity:** L

CSI relies on consistent paging behaviour. `pkg/rest/resources_test.go` + `snapshots_test.go` cover offset+limit boundaries.

### 1.28 Spawn endpoint autoplaces per RG SelectFilter — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** PLAN.md, linstor-cli #9, recovery skill

**Why:** `rg spawn` used to be definitional-only — `pkg/rest/spawn.go:spawnAutoplace` added this session.

**Unit:** spawn handler against in-memory store with RG PlaceCount=2 produces RD + 2 Resources + tiebreaker.
**E2E:** `linstor rg spawn default test 1G` → 3 rows in `linstor r l` within 30s.

---

## Test harness skeleton

```bash
# tests/e2e/linstor-cli-<scenario>.sh
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"
source "$(dirname "$0")/lib.sh"

kubectl port-forward -n blockstor-system svc/blockstor-controller \
    3370:3370 >/tmp/pf-blockstor.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; cleanup' EXIT
sleep 2

LINSTOR="linstor --controllers=127.0.0.1:3370"
# ... per-scenario commands + asserts ...
```

Each script: independently re-runnable, dumps stderr + CRD YAML on failure, cleans up via trap.

---

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 16 |
| P0 e2e (or hybrid) | 10 |
| P1 unit | 5 |
| P1 e2e | 4 |
| P2 e2e | 1 |

Implementation effort: zero new code (all S). Pure regression-guard
group — the cheapest 28 tests to land first.
