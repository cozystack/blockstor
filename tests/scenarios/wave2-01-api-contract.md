# Wave 2 — Group 1 — API & CLI contract (Day2 ops)

Additional REST/CLI contract scenarios harvested from UG9 Day2 ops:
property CRUD across object scopes, Aux/ property shorthand,
RD-name conflict path, the `query-size-info` preview endpoint, and
the basic `cluster-state` smoke (list `n l` + `sp l` + `r l`).

Pairs with wave1's `01-api-contract.md` — these are read/write
property surfaces that the wire-shape regression guards in wave1
don't cover yet.

[Group index in README.md](README.md).

---

### 1.W01 `list-properties` for every object scope — S

- **Priority:** P0  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Verifying options for LINSTOR objects" (lines 3399-3413) via tests/scenarios/day2-property-list.md

`linstor <obj> list-properties …` for controller / node / RG / RD / VG / VD / SP / resource / resource-connection / node-connection.

**Unit:** each scope returns a `{Key, Value}` map; unknown scope returns 404, empty scope returns empty map (not nil); known namespaces (`DrbdOptions/`, `Aux/`, `FileSystem/`, `StorDriver/`) round-trip without normalisation drift.

### 1.W02 Aux property set / unset via `--aux` shorthand — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Constraining automatic resource placement by using auxiliary node properties" (lines 1006-1097) via tests/scenarios/day2-properties-aux-set-unset.md

`linstor node set-property --aux <node> rack-id rack-7` stores as `Aux/rack-id`; passing no value deletes. Foundational for `--replicas-on-same/different` (see wave2-09).

**Unit:** REST handler maps `--aux foo bar` → key `Aux/foo`.
**E2E:** set, list-properties greps the key, then spawn an RG with `--replicas-on-same Aux/rack-id` observes the value.

### 1.W03 RD create with existing name returns Conflict — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Creating and deploying resources and volumes" (lines 823-862) via tests/scenarios/day2-resource-create-existing-name-conflict.md

Cross-listed with wave1 4.2. Idempotent automation needs the actionable error envelope (`already exists` + offending name) rather than generic 500. Also covers `(node, rd)` pair conflict on `resource create`.

### 1.W04 `resource-group query-size-info` preview — P

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 lines 3480-3498 (example output) + over-provisioning sections via tests/scenarios/day2-query-size-info.md

Dry-run sizing endpoint. Returns `MaxVolumeSize`, `AvailableSize`, `Capacity`, `Next Spawn Result`. Cross-listed with wave1 7.19 — the over-subscription gates feed this preview.

**Unit:** `pkg/rest/query_size_info_test.go` — pool 10 GiB free + ratio=2 → `MaxVolumeSize=20 GiB`; constraint-impossible RG → `Next Spawn Result` clearly explains why.
**E2E:** spawn after consulting → spawn size matches preview.

### 1.W05 `cluster-state` smoke (`n l` + `sp l --groupby` + `r l`) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** UG9 §"Checking cluster state" (lines 2352-2362) via tests/scenarios/day2-cluster-state-check.md

Three-command smoke against a fresh stand. Validates `--groupby` on `sp l`; `--resource`/`--nodes` filters on `r l`; `--machine-readable` on any list.

**E2E:** healthy 3-worker stand → all rows `Online` / `UpToDate`; one filter-narrow run per command returns the same data as the unfiltered run minus the filter set.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 2 |
| P1 unit | 1 |
| P1 e2e | 2 |
