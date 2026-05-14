# Wave 2 — Group 9 — Resource group (Day2 ops)

RG full surface: create / delete-with-rds / modify-place-count /
spawn (happy + impossible-placement), all placement constraint
flavours (`replicas-on-same`, `replicas-on-different`,
`x-replicas-on-different`, `do-not-place-with`, `layer-list`,
`providers`), unset-placement-property, RG DRBD options, RG
filesystem-on-spawn.

Pairs with wave1's `02-placement.md` (autoplacer engine) and
`04-lifecycle.md` (RG basic CRUD). This group focuses on the **RG
modify knob surface** that wave1 only sketches.

[Group index in README.md](README.md).

---

## RG CRUD

### 9.W01 `rg create <name> --place-count N --storage-pool <pool>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Using resource groups to deploy LINSTOR provisioned volumes" (lines 744-808) via tests/scenarios/day2-rg-create.md

Foundation of RG-driven deployment. POST `/v1/resource-groups` + `POST /v1/resource-groups/{rg}/volume-groups`. Settings on RG (DRBD options, layer list, FS type) propagate to spawned RDs.

### 9.W02 `rg delete` refused if RDs exist — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Deleting a resource group" (lines 1403-1438) via tests/scenarios/day2-rg-delete-with-rds.md

Cross-listed with wave1 4.5. Clear error text `Cannot delete resource group '<name>' because it has existing resource definitions.` No `--force`; operator must clear or reassign RDs (see 4.W14).

### 9.W03 `rg modify --place-count N` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Automatically maintaining resource group placement count" (lines 885-907) + §"Placement count" (lines 909-931) via tests/scenarios/day2-rg-modify-place-count.md

Cross-listed with wave1 4.4. New spawns honour immediately; existing RDs converge via periodic balance task (depends on `BalanceResourcesEnabled` — wave2-02 2.W02). If new count impossible, future spawn fails with `Not enough available nodes`.

### 9.W04 `rg spawn <rg> <rd> <size>` autoplaces — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Using resource groups to deploy LINSTOR provisioned volumes" (lines 744-808) via tests/scenarios/day2-rg-spawn-resource.md

Cross-listed with wave1 1.28 / 2.3. Creates RD + VD + Resources in one call; spawn-time `.res` includes the RG's DRBD options. Existing surface — pin happy path.

### 9.W05 `rg spawn` impossible-placement returns actionable error — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating a resource group with impossible placement constraints" (lines 916-931 WARNING) via tests/scenarios/day2-rg-spawn-impossible-placement.md

Cross-listed with wave1 2.19. `place-count > nodes` → spawn fails with `Not enough available nodes`; NO partial state (no orphan RD / VD / Resource). RG itself is allowed to exist with impossible constraints; only spawn refuses.

## Placement constraints

### 9.W06 `--replicas-on-same Aux/<key>=<val>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Constraining automatic resource placement by using auxiliary node properties" (lines 1006-1076) via tests/scenarios/day2-rg-replicas-on-same.md

Cross-listed with wave1 2.5. Operator-set `Aux/<key>` (1.W02). Spawn picks nodes whose Aux value matches; nodes WITHOUT the property are excluded (UG line 1006-1076).

### 9.W07 `--replicas-on-different Aux/<key>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Avoiding colocating resources when automatically placing a resource" + §"Constraining automatic resource placement..." (lines 995-1076) via tests/scenarios/day2-rg-replicas-on-different.md

Cross-listed with wave1 2.6. Inverse of 9.W06. **Gotcha:** nodes WITHOUT the property are treated as "different from any value" — they will satisfy "different from zone=a" even with no zone label. Test pins this so operators don't get surprised.

### 9.W08 `--x-replicas-on-different <key> N --place-count M` — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M (implement first)
- **Source:** UG9 §"Ensuring automatic resource placement on different nodes for disaster recovery" (lines 1098-1199) via tests/scenarios/day2-rg-x-replicas-on-different.md

Cross-listed with wave1 2.8. Up to N replicas per same-value bucket (e.g. 2 per site, 1 elsewhere for stretched-DR). `--x-replicas-on-different site 1` equivalent to `--replicas-on-different site`. Status: not implemented — autoplacer doesn't have the bucket-cap logic.

### 9.W09 `--do-not-place-with <rd>` anti-affinity — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (implement first)
- **Source:** UG9 §"Avoiding colocating resources when automatically placing a resource" (lines 995-1005) via tests/scenarios/day2-rg-do-not-place-with.md

Cross-listed with wave1 2.10. Autoplacer skips nodes already hosting `<rd>`'s replicas. Constraint enforced at PLACEMENT time only — later toggle-disk onto a now-shared node not prevented. Status: implemented in `pkg/placer/placer.go` via `applyNotPlaceWith` over `AutoSelectFilter.NotPlaceWithRsc` (verbatim slice) — see `TestPlaceNotPlaceWithRscExactExcludesNamedHosts` and `TestPlaceNotPlaceWithRscExactIgnoresSelf`. `--do-not-place-with-regex` covers the wave1 2.11 variant via `NotPlaceWithRscRegex`.

### 9.W10 `--layer-list drbd,luks,storage` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Constraining automatic resource placement by LINSTOR layers or storage pool providers" (lines 1201-1230) + layer table at 1819-1831 via tests/scenarios/day2-rg-layer-list.md

Cross-listed with wave1 6.9. Allowed orderings: `drbd,storage`, `drbd,luks,storage`, `luks,storage`, `storage`. Rejects: any stack with CACHE / WRITECACHE / NVME (wave1 6.11). LUKS-under-DRBD requires master passphrase already set (out-of-scope here — piraeus orchestrates per wave1 6.17).

### 9.W11 `--providers LVM,LVM_THIN` — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Constraining automatic resource placement by LINSTOR layers or storage pool providers" (lines 1201-1230) via tests/scenarios/day2-rg-providers.md

Cross-listed with wave1 2.12. CSV list; autoplacer only considers matching SPs. CSV is set-membership, not priority — ranking within the set uses free-space / throughput / count strategies. `--providers ZFS` on an LVM-only cluster → `Not enough available nodes`.

### 9.W12 Unset placement property via empty-string modify — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Unsetting autoplacement properties" (lines 1076-1097) via tests/scenarios/day2-rg-unset-placement-property.md

`linstor rg modify <rg> --replicas-on-same ""` clears the constraint. Same for `--replicas-on-different`, `--do-not-place-with`, `--do-not-place-with-regex`, `--layer-list`, `--providers`. Existing replicas stay; balance task may move them on next interval if new constraints permit.

## RG-level options propagation

### 9.W13 RG `drbd-options --protocol C --verify-alg crc32c` propagate to spawn — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Using resource groups to deploy LINSTOR provisioned volumes" (lines 768-808) via tests/scenarios/day2-resource-group-drbd-options.md

PATCH RG props → next spawn's `.res` has matching `net { protocol C; verify-alg crc32c; }`. Hierarchy: RD > RG (RD-level overrides).

### 9.W14 RG `FileSystem/Type=xfs` auto-mkfs on first deploy — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Creating a file system on a storage volume" (lines 1280-1340) via tests/scenarios/day2-resource-group-filesystem.md

Set `FileSystem/Type`, `FileSystem/User`, `FileSystem/Group`, `FileSystem/MkfsParams` on RG. Satellite runs `mkfs.<type>` on the DRBD device of the primary replica AFTER creation. FS created once on first deploy — NOT recreated on toggle/migrate. Defaults user/group = root.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 5 |
| P0 e2e | 5 |
| P1 unit | 3 |
| P1 e2e | 4 |
| T (implement first) | 2 |
