# LINSTOR REST API surface — CSI MVP scope

This is the minimum subset of the LINSTOR REST contract that blockstor must
implement before any real Kubernetes client can drive it. It is derived from
two surveys:

1. Every `golinstor.Client` method invoked by `linstor-csi`
   (`pkg/client/`, `pkg/topology/`, `cmd/`).
2. Every `golinstor.Client` method invoked by `piraeus-operator`
   (`internal/controller/`, `pkg/`).

If a client we listed in `PLAN.md` calls something that is not on this list,
the list is wrong — please add it. Out-of-scope endpoints (backup shipping,
schedules, S3 / EBS, SPDK, NVMe-oF, OpenFlex, Exos) MUST return
`501 Not Implemented` with a clear message rather than silently 404.

## Status legend

- ✅ implemented and tested
- 🟡 stubbed (returns valid placeholder, no real backing)
- ⬜ not yet implemented
- ⛔ explicitly out of scope (return 501)

---

## Controller-level

| Endpoint | Method | Status | Notes |
|---|---|---|---|
| `/v1/controller/version` | GET | ✅ | Phase 1; pinned by `pkg/rest/server_test.go` |
| `/v1/controller/config` | GET | ⬜ | |
| `/v1/controller/properties` | GET | ⬜ | |
| `/v1/controller/properties` | POST/DELETE | ⬜ | |
| `/v1/controller/properties/{key}` | DELETE | ⬜ | |
| `/v1/controller/properties/info` | GET | ⬜ | piraeus uses for prop discovery |
| `/v1/controller/properties/info/all` | GET | ⬜ | |
| `/v1/healthz` | GET | ✅ | blockstor-only readiness probe |

## Nodes

| Endpoint | Method | Status |
|---|---|---|
| `/v1/nodes` | GET | ✅ |
| `/v1/nodes` | POST | ✅ |
| `/v1/nodes/{node}` | GET | ✅ |
| `/v1/nodes/{node}` | PUT (modify) | ✅ |
| `/v1/nodes/{node}` | DELETE | ✅ |
| `/v1/nodes/{node}/lost` | DELETE | ⬜ |
| `/v1/nodes/{node}/restore` | PUT | ⬜ |
| `/v1/nodes/{node}/evacuate` | PUT | ⬜ |
| `/v1/nodes/{node}/net-interfaces` | GET / POST | ⬜ |
| `/v1/nodes/{node}/net-interfaces/{nif}` | GET / PUT / DELETE | ⬜ |
| `/v1/nodes/{node}/storage-pools` | GET | ✅ |
| `/v1/nodes/{node}/storage-pools` | POST | ⬜ |
| `/v1/nodes/{node}/storage-pools/{pool}` | GET | ✅ |
| `/v1/nodes/{node}/storage-pools/{pool}` | PUT / DELETE | ⬜ |
| `/v1/physical-storage` | GET | ⬜ |
| `/v1/physical-storage/{node}` | GET / POST | ⬜ |

## Resource definitions

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-definitions` | GET / POST | ⬜ |
| `/v1/resource-definitions/{rd}` | GET / PUT / DELETE | ⬜ |
| `/v1/resource-definitions/{rd}/volume-definitions` | GET / POST | ⬜ |
| `/v1/resource-definitions/{rd}/volume-definitions/{vn}` | GET / PUT / DELETE | ⬜ |
| `/v1/resource-definitions/{rd}/clone` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/clone/{target}` | GET (status) | ⬜ |
| `/v1/resource-definitions/{rd}/sync-status` | GET | ⬜ |
| `/v1/resource-definitions/{rd}/snapshot-restore-resource/{snap}` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshot-restore-volume-definition/{snap}` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshot-rollback/{snap}` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshots` | GET / POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshots/{snap}` | GET / DELETE | ⬜ |

## Resources

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-definitions/{rd}/resources` | GET / POST | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}` | GET / PUT / DELETE | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/volumes` | GET | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/volumes/{vn}` | GET / PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/migrate-disk/{from}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/migrate-disk/{from}/{pool}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful/{pool}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless/{pool}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/make-available` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/activate` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/deactivate` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/autoplace` | POST | ⬜ |

## Resource groups

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-groups` | GET | ✅ |
| `/v1/resource-groups` | POST | ✅ |
| `/v1/resource-groups/{rg}` | GET | ✅ |
| `/v1/resource-groups/{rg}` | PUT | ✅ |
| `/v1/resource-groups/{rg}` | DELETE | ✅ |
| `/v1/resource-groups/{rg}/spawn` | POST | ⬜ |
| `/v1/resource-groups/{rg}/adjust` | PUT | ⬜ |
| `/v1/resource-groups/adjustall` | PUT | ⬜ |
| `/v1/resource-groups/{rg}/query-size-info` | POST | ⬜ |
| `/v1/resource-groups/{rg}/query-max-volume-size` | POST | ⬜ |
| `/v1/resource-groups/{rg}/volume-groups` | GET / POST | ⬜ |
| `/v1/resource-groups/{rg}/volume-groups/{vn}` | GET / PUT / DELETE | ⬜ |

## Storage pool definitions

| Endpoint | Method | Status |
|---|---|---|
| `/v1/storage-pool-definitions` | GET / POST | ⬜ |
| `/v1/storage-pool-definitions/{spd}` | GET / PUT / DELETE | ⬜ |

## Aggregated views (used heavily by linstor-csi for listing)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/view/resources` | GET | ⬜ |
| `/v1/view/snapshots` | GET | ⬜ |
| `/v1/view/storage-pools` | GET | ✅ |
| `/v1/query-max-volume-size` | POST | ⬜ |

## KeyValueStore (used by linstor-csi for its own bookkeeping)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/key-value-store` | GET | ⬜ |
| `/v1/key-value-store/{instance}` | GET / PUT / DELETE | ⬜ |

## Connections (piraeus-operator drives these via LinstorNodeConnection)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/node-connections` | GET | ⬜ |
| `/v1/node-connections/{a}/{b}` | GET / PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resource-connections` | GET | ⬜ |
| `/v1/resource-definitions/{rd}/resource-connections/{a}/{b}` | GET / PUT | ⬜ |

## Encryption (LUKS — in scope but not MVP)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/encryption/passphrase` | POST / PUT / PATCH | ⬜ |

## Out of scope — return 501

These are listed so we explicitly handle them. The error body should look
like the upstream `ApiCallRc` so golinstor decodes it cleanly.

| Endpoint group | Why |
|---|---|
| `/v1/remotes/...`, `/v1/remotes/{r}/backups/...` | cross-cluster shipping; out of scope |
| `/v1/schedules/...` | cron-driven backups; out of scope |
| `/v1/view/backup/queue`, `/v1/view/schedules-by-resource` | shipping observability |
| `/v1/stats/...` | nice-to-have, deferred to Phase 7 |
| `/v1/controller/backup/db` | controller DB backup; CRDs are the DB now |
| `/v1/sos-report`, `/v1/sos-report/download` | deferred to Phase 7 |
| `/v1/error-reports[*/...]` | deferred to Phase 7 |
| `/v1/files[/{name}/check/{node}]` | external files; deferred to Phase 6 |
| `/v1/resource-definitions/{r}/files/{name}` | external files; deferred to Phase 6 |
| `/v1/resource-definitions/{r}/drbd-proxy[/...]` | DRBD proxy; deferred to Phase 6 |
| `/v1/events/drbd/promotion`, `/v1/events/nodes` | SSE event streams; consider in Phase 4 |

---

## How this drives implementation

Every endpoint in this file gets, in order:

1. A row in this table moved from ⬜ to 🟡, and then to ✅, as work lands.
2. A test in `pkg/rest/<group>_test.go` that fixes the contract: happy path,
   unhappy paths (not found, conflict, validation), and the JSON shape (per
   the per-endpoint TDD policy in `PLAN.md`).
3. Implementation in `pkg/rest/<group>.go` until the tests go green.
4. A contract-diff entry against the Java oracle once that suite is wired
   (Phase 5).
