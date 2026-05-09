# LINSTOR REST API surface — CSI MVP scope

This is the minimum subset of the LINSTOR REST contract that blockstor must
implement before any real Kubernetes client can drive it. It is derived from
two surveys:

1. Every `golinstor.Client` method invoked by `linstor-csi`
   (`pkg/client/`, `pkg/topology/`, `cmd/`).
2. Every `golinstor.Client` method invoked by `piraeus-operator`
   (`internal/controller/`, `pkg/`).

If a client we listed in `PLAN.md` calls something that is not on this list,
the list is wrong — please add it. Out-of-scope endpoints (cross-cluster
backup shipping, schedules, S3 / EBS, SPDK, NVMe-oF, OpenFlex, Exos) MUST
return `501 Not Implemented` with a clear message rather than silently 404.

## Status legend

- ✅ implemented and tested
- 🟡 stubbed (returns valid placeholder, no real backing)
- ⬜ not yet implemented
- ⛔ explicitly out of scope (return 501)

Last sync against handler list: 2026-05-09 (Phase 8.5 close).

---

## Controller-level

| Endpoint | Method | Status | Notes |
|---|---|---|---|
| `/v1/controller/version` | GET | ✅ | `pkg/rest/server_test.go` |
| `/v1/controller/config` | GET | ⬜ | |
| `/v1/controller/properties` | GET | ✅ | KV-store-backed (`ControllerProps` instance) |
| `/v1/controller/properties` | POST | ✅ | `override_props` payload |
| `/v1/controller/properties/{key}` | DELETE | ⬜ | use POST with empty value as workaround |
| `/v1/controller/properties/info` | GET | 🟡 | empty list — autocomplete catalogue ports later |
| `/v1/controller/properties/info/all` | GET | ⬜ | |
| `/v1/healthz` | GET | ✅ | blockstor-only readiness probe |
| `/v1/stats` | GET | ✅ | cluster-wide counters |

## Encryption (cluster passphrase + per-RD)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/encryption/passphrase` | POST | ✅ (seed) |
| `/v1/encryption/passphrase` | PATCH | ✅ (unlock) |
| `/v1/encryption/passphrase` | PUT | ✅ (rotate) |
| `/v1/resource-definitions/{rd}/encryption-passphrase` | POST | ✅ |

## Nodes

| Endpoint | Method | Status |
|---|---|---|
| `/v1/nodes` | GET | ✅ |
| `/v1/nodes` | POST | ✅ |
| `/v1/nodes/{node}` | GET | ✅ |
| `/v1/nodes/{node}` | PUT (modify) | ✅ |
| `/v1/nodes/{node}` | DELETE | ✅ |
| `/v1/nodes/{node}/lost` | POST | ✅ |
| `/v1/nodes/{node}/restore` | POST | ✅ |
| `/v1/nodes/{node}/evacuate` | POST | ✅ |
| `/v1/nodes/{node}/net-interfaces` | GET | ⬜ |
| `/v1/nodes/{node}/net-interfaces` | POST | ✅ |
| `/v1/nodes/{node}/net-interfaces/{nif}` | GET | ⬜ |
| `/v1/nodes/{node}/net-interfaces/{nif}` | PUT | ✅ |
| `/v1/nodes/{node}/net-interfaces/{nif}` | DELETE | ✅ |
| `/v1/nodes/{node}/storage-pools` | GET | ✅ |
| `/v1/nodes/{node}/storage-pools/{pool}` | GET | ✅ |
| `/v1/nodes/{node}/storage-pools` | POST / PUT / DELETE | ⬜ (StoragePools owned by satellite Hello today) |
| `/v1/nodes/{node}/physical-storage` | GET | 🟡 (always `[]`; cozystack provisions via Talos extensions) |
| `/v1/physical-storage` | GET | 🟡 (always `[]`) |
| `/v1/physical-storage/{node}` | POST | 🟡 (501 with rationale) |

## Resource definitions

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-definitions` | GET | ✅ |
| `/v1/resource-definitions` | POST | ✅ |
| `/v1/resource-definitions/{rd}` | GET | ✅ |
| `/v1/resource-definitions/{rd}` | PUT | ✅ |
| `/v1/resource-definitions/{rd}` | DELETE | ✅ |
| `/v1/resource-definitions/{rd}/volume-definitions` | GET / POST | ✅ |
| `/v1/resource-definitions/{rd}/volume-definitions/{vn}` | GET / PUT / DELETE | ✅ |
| `/v1/resource-definitions/{rd}/clone` | POST | 🟡 (delegates to snapshot-restore-resource) |
| `/v1/resource-definitions/{rd}/clone/{target}` | GET (status) | ⬜ |
| `/v1/resource-definitions/{rd}/sync-status` | GET | ⬜ |
| `/v1/resource-definitions/{rd}/snapshot-restore-resource` | POST | ✅ |
| `/v1/resource-definitions/{rd}/snapshot-restore-volume-definition/{snap}` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshot-rollback/{snap}` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/snapshots` | GET / POST | ✅ |
| `/v1/resource-definitions/{rd}/snapshots/{snap}` | GET / DELETE | ✅ |
| `/v1/resource-definitions/{rd}/adjust` | POST | ✅ |
| `/v1/resource-definitions/{rd}/advise` | GET | ✅ |
| `/v1/resource-definitions/{rd}/encryption-passphrase` | POST | ✅ |

## Resources

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-definitions/{rd}/resources` | GET | ✅ |
| `/v1/resource-definitions/{rd}/resources` | POST | ✅ |
| `/v1/resource-definitions/{rd}/resources/{node}` | GET | ✅ |
| `/v1/resource-definitions/{rd}/resources/{node}` | DELETE | ✅ |
| `/v1/resource-definitions/{rd}/resources/{node}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/volumes` | GET / PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/migrate-disk/...` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/toggle-disk/...` | PUT | ⬜ (auto-diskful covers the common case) |
| `/v1/resource-definitions/{rd}/resources/{node}/make-available` | POST | ⬜ |
| `/v1/resource-definitions/{rd}/resources/{node}/activate` | POST | ✅ |
| `/v1/resource-definitions/{rd}/resources/{node}/deactivate` | POST | ✅ |
| `/v1/resource-definitions/{rd}/resources/{node}/adjust` | POST | ✅ |
| `/v1/resource-definitions/{rd}/autoplace` | POST | ✅ |

## Resource groups

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-groups` | GET / POST | ✅ |
| `/v1/resource-groups/{rg}` | GET / PUT / DELETE | ✅ |
| `/v1/resource-groups/{rg}/spawn` | POST | ✅ |
| `/v1/resource-groups/{rg}/adjust` | PUT | ⬜ |
| `/v1/resource-groups/adjustall` | PUT | ⬜ |
| `/v1/resource-groups/{rg}/query-size-info` | POST | ✅ |
| `/v1/resource-groups/{rg}/query-max-volume-size` | POST | ⬜ (use query-size-info) |
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
| `/v1/view/resources` | GET | ✅ |
| `/v1/view/snapshots` | GET | ✅ |
| `/v1/view/storage-pools` | GET | ✅ |
| `/v1/view/advise/resources` | GET | ✅ |
| `/v1/query-all-size-info` | POST | ✅ |
| `/v1/query-max-volume-size` | POST | ⬜ (use query-all-size-info) |

## KeyValueStore (used by linstor-csi for its own bookkeeping)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/key-value-store` | GET | ✅ |
| `/v1/key-value-store/{instance}` | GET | ✅ |
| `/v1/key-value-store/{instance}` | POST / PUT | ✅ |
| `/v1/key-value-store/{instance}` | DELETE | ✅ |

## Connections (piraeus-operator drives these via LinstorNodeConnection)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/node-connections` | GET | 🟡 (empty list) |
| `/v1/node-connections/{a}/{b}` | GET | 🟡 (empty list) |
| `/v1/node-connections/{a}/{b}` | PUT | ⬜ |
| `/v1/resource-definitions/{rd}/resource-connections` | GET | ⬜ |
| `/v1/resource-definitions/{rd}/resource-connections/{a}/{b}` | GET / PUT | ⬜ |

## Multi-snapshot actions

| Endpoint | Method | Status |
|---|---|---|
| `/v1/actions/snapshot/multi` | POST | 🟡 (501) |

## DRBD proxy (cozystack uses flat L2; included for `linstor drbd-proxy *`)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/resource-definitions/{rd}/drbd-proxy` | PUT | 🟡 (501) |
| `/v1/resource-definitions/{rd}/drbd-proxy/enable/{a}/{b}` | POST | 🟡 (501) |
| `/v1/resource-definitions/{rd}/drbd-proxy/disable/{a}/{b}` | POST | 🟡 (501) |

## External files (cozystack handles via Talos extensions)

| Endpoint | Method | Status |
|---|---|---|
| `/v1/files` | GET | 🟡 (empty list) |
| `/v1/files/{path...}` | GET | 🟡 (404 with rationale) |

## Error reports / SOS

| Endpoint | Method | Status |
|---|---|---|
| `/v1/error-reports` | GET | 🟡 (empty list) |
| `/v1/error-reports/{id}` | GET | 🟡 (404) |
| `/v1/sos-report` | GET | ⬜ (Phase 7 follow-up) |

## Out of scope — return 501

These are listed so we explicitly handle them. The error body should look
like the upstream `ApiCallRc` so golinstor decodes it cleanly.

| Endpoint group | Why |
|---|---|
| `/v1/remotes/...`, `/v1/remotes/{r}/backups/...` | cross-cluster shipping; 🟡 GET returns `[]`, mutations ⛔ 501 |
| `/v1/schedules/...` | cron-driven backups; ⛔ |
| `/v1/view/backup/queue`, `/v1/view/schedules-by-resource` | shipping observability; ⛔ |
| `/v1/controller/backup/db` | controller DB backup; CRDs are the DB now; ⛔ |
| `/v1/events/drbd/promotion`, `/v1/events/nodes` | SSE event streams; deferred (consumers use kube-watch on Resource CRDs) |

---

## How this drives implementation

Every endpoint in this file gets, in order:

1. A row in this table moved from ⬜ to 🟡, and then to ✅, as work lands.
2. A test in `pkg/rest/<group>_test.go` that fixes the contract: happy path,
   unhappy paths (not found, conflict, validation), and the JSON shape.
3. Implementation in `pkg/rest/<group>.go` until the tests go green.
4. A contract-diff entry against the Java oracle once that suite is wired
   (Phase 5).
