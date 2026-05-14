# Wave 2 — Group 5 — DRBD state observation & recovery (Day2 ops)

DRBD options at every scope (RD / RG / resource / node-connection /
resource-connection / unset), external DRBD metadata pool,
disk-failure replacement (internal/external metadata), split-brain
manual recovery, quorum-loss recovery, SkipDisk auto-set and manual
toggle, transient secondary-node failure, manual disconnect /
reconnect (operator-driven), and the Prometheus alertmanager smoke.

Pairs with wave1's `05-drbd-state-recovery.md` — Day2 recipes that
the satellite reconciler must NOT fight, plus property surface for
DRBD tuning.

[Group index in README.md](README.md).

---

## DRBD options surface

### 5.W01 `rd drbd-options --protocol C` (RD scope) — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Setting DRBD options for LINSTOR objects" (lines 3300-3328) via tests/scenarios/day2-drbd-options-rd.md

PATCH `rd <rd>` props → `.res` `net { protocol C; }` on every replica. Hierarchy: RD > RG > controller. Direct edits in `/etc/drbd.d/global_common.conf` are IGNORED.

**Unit:** REST writes prop; `conffile.go` renders `protocol C`.
**E2E:** `linstor rd drbd-options --protocol C backups` → `grep protocol /var/lib/linstor.d/backups.res` shows `C`.

### 5.W02 `--unset-<option>` removes DRBD option — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Removing DRBD options from LINSTOR objects" (lines 3414-3434) via tests/scenarios/day2-drbd-options-unset.md

`linstor rd drbd-options --unset-protocol backups` → prop deleted; next adjust returns option to DRBD default. Same `--unset-` syntax for `drbd-options` and `drbd-peer-options`.

### 5.W03 `node-connection drbd-peer-options --ping-timeout` — S ✓

- **Priority:** P2  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Setting DRBD options for node connections" (lines 3386-3398) via tests/scenarios/day2-drbd-peer-options-node-connection.md

Per (nodeA, nodeB) pair, applies to every RD's connection between them. Resource-level / RD-level options take precedence. DRBD time encoding is 1/10 of a second (`--ping-timeout 299` = 29.9s).

**Unit:** `pkg/satellite.TestApplyRendersPingTimeoutIntoNetBlock` pins the renderer-side contract — a `DesiredResource.DrbdOptions["DrbdOptions/Net/ping-timeout"]="500"` on a 2-replica RD (n1↔n2) flows through `splitDRBDOptions` into the `net { … }` block of the rendered `.res` as a verbatim `ping-timeout 500;` line, with the `DrbdOptions/Net/` prefix stripped (the form drbdadm parses). The hand-off from the controller-side effective-props resolver — which folds controller-scope, RD-scope, and node-connection-scope keys into one flat bag before dispatch — is scope-agnostic by construction, so one renderer test covers all three set-property paths.

### 5.W04 `resource-connection drbd-peer-options --max-buffers` — S

- **Priority:** P2  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Setting DRBD peer options for LINSTOR resources or resource connections" (lines 3328-3385) via tests/scenarios/day2-drbd-peer-options-resource-connection.md

Per (nodeA, nodeB, rd) tuple. `.res` net block updated only for the matching connection block; other connections unchanged. `resource drbd-peer-options` and `resource-connection drbd-peer-options` are aliases.

## External metadata pool

### 5.W05 `StorPoolNameDrbdMeta` routes DRBD metadata to a separate pool — T

- **Priority:** P2  **Target:** unit + integration + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Using external DRBD metadata" (lines 4462-4534) via tests/scenarios/day2-external-drbd-metadata.md

Cross-listed with wave1 6.18. Settable on node / RG / RD / resource / VG / VD (priority increasing). Two LVs/ZVOLs per replica: data + metadata. UG note: only affects NEW resources, never migrates existing.

**Unit (after implement):** conffile renderer emits `meta-disk /dev/<meta-pool>/<rd>_meta` instead of `internal`.
**E2E:** RD with `StorPoolNameDrbdMeta=meta`; `iostat -dx 1` shows small metadata I/O on meta device.

## Disk failure / replacement

### 5.W06 `drbdadm detach` on I/O error → SkipDisk auto-set — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"SkipDisk" (lines 4427-4460) + drbd-troubleshooting §"Dealing with hard disk failure" (lines 21-80) via tests/scenarios/day2-drbd-detach-on-io-error.md

Cross-listed with wave1 5.11. DRBD reports UpToDate → Failed → Diskless → observer detects transition → controller writes `DrbdOptions/SkipDisk=True` → satellite passes `--skip-disk` to `drbdadm adjust`. `linstor r l` shows `(R)` marker.

### 5.W07 Manual `DrbdOptions/SkipDisk=True` for scheduled maintenance — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"SkipDisk" (lines 4427-4460) via tests/scenarios/day2-drbd-toggle-skipdisk-via-property.md

Same prop, manual set/unset. Marker `(R)` = resource scope; `(R, N)` = resource AND node scope. Test pins both indicators.

### 5.W08 Clear `SkipDisk` after disk repair — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** UG9 §"SkipDisk" (lines 4427-4460) via tests/scenarios/day2-skipdisk-clear.md

`linstor resource set-property <node> <rd> DrbdOptions/SkipDisk` (no value) deletes prop → next adjust attaches → Inconsistent → SyncTarget → UpToDate. **Reverse failure mode:** clearing before disk repaired → re-detach loop. Document the workflow.

### 5.W09 Replace failed disk (internal metadata) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting §"Replacing a failed disk when using internal metadata" (lines 82-106) via tests/scenarios/day2-drbd-replace-failed-disk-internal-metadata.md

LINSTOR-managed equivalent: `linstor r d <node> <rd>` + `linstor rd ap <rd>`. Pin: doing the raw `drbdadm create-md + attach` outside LINSTOR also works but breaks the controller's view — assert reconciler picks up state within 10s without overwriting `.res`.

### 5.W10 Replace failed disk (external metadata) — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting §"Replacing a failed disk when using external metadata" (lines 108-133) via tests/scenarios/day2-drbd-replace-failed-disk-external-metadata.md

Requires 5.W05 first. Adds `drbdadm invalidate` step (WARNING: run only on side WITHOUT good data). LINSTOR-managed: `r td <node> <rd> --diskless` then re-toggle to diskful.

### 5.W11 Permanent node failure: replace with new hardware — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting §"Dealing with permanent node failure" (lines 192-216) via tests/scenarios/day2-drbd-permanent-node-failure-replace.md

`node lost` (4.W04) + fresh OS + LINSTOR satellite install + `node create` + `sp c` + `rd ap` for each affected RD. New disks must be ≥ original size (DRBD refuses smaller). Full resyncs because no metadata preserved.

## Recovery recipes (operator-driven, reconciler must stay quiet)

### 5.W12 Split-brain manual recovery — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting §"Manual split brain recovery" (lines 217-289) via tests/scenarios/day2-drbd-split-brain-manual-recovery.md

Cross-listed with wave1 5.14. Recipe: on VICTIM `disconnect → secondary → connect --discard-my-data`; on SURVIVOR (if also StandAlone) `disconnect → connect`. **Reconciler-survival:** blockstor must NOT re-render `.res` mid-recipe and break side selection.

**E2E:** `tests/e2e/split-brain-recovery.sh` pins the recipe contract — runs both VICTIM and SURVIVOR commands verbatim against a reconciler-managed RD; asserts both peers converge to Established + UpToDate within 30 s, `.res` sha256 unchanged on both sides across the recipe window, and the original Primary never loses Primary-ship. Distinct from wave1 5.14's `tests/e2e/recovery-discard-my-data.sh` (data-marker md5 round-trip) — 5.W12 is the command-contract guard, no operator-driven REST endpoint exposed.

### 5.W13 Quorum-loss recovery (suspend-io → force secondary) — S

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** drbd-troubleshooting §"Recovering a primary node that lost quorum" (lines 290-347) via tests/scenarios/day2-drbd-recover-quorum-loss.md

Cross-listed with wave1 5.20 / wave1 7.5. Recipe: `drbdadm secondary --force` (DRBD 9.1.7+) → suspended/new I/O fails → unmount → reconnect heals. Quorum-off prop persists through satellite restart.

### 5.W14 Operator `drbdadm disconnect` survives ≥30s without reconciler fight — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** drbd-troubleshooting (manual disconnect/connect) via tests/scenarios/day2-drbd-reconnect-after-disconnect.md

Cross-listed with wave1 5.29. Operator runs `drbdadm disconnect` from satellite shell → reconciler does NOT auto-reconnect for ≥30s. Open design question: per-resource `Aux/operator-managed=true` to gate the reconciler?

### 5.W15 Temporary secondary-node failure auto-recovers — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** drbd-troubleshooting §"Dealing with temporary secondary node failure" (lines 148-170) via tests/scenarios/day2-drbd-temporary-secondary-failure.md

Cross-listed with wave1 5.8 / 5.15. Surviving primary records changes in dirty bitmap; recovered secondary auto-syncs from bitmap on reconnect. Test: power-off worker-2 mid-write → power-on after 60s → SyncTarget → UpToDate without manual intervention.

## Observability smoke

### 5.W16 Prometheus alertmanager smoke: drbd-disconnect → alert fires — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** linstor-kubernetes.adoc §"Verifying the Prometheus Alertmanager web console deployment" (lines 3177-3239) via tests/scenarios/day2-drbd-disconnect-alertmanager-test.md

**Out of scope.** Alertmanager rules + ServiceMonitor / PrometheusRule live in cozystack's platform monitoring stack, not blockstor. blockstor's role stops at `/metrics` + the drbd-reactor sidecar (wave2-07 §7.W08). See `out-of-scope.md` → "Prometheus Alertmanager smoke".

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 e2e | 3 |
| P1 unit | 2 |
| P1 e2e | 6 |
| P2 unit | 2 |
| P2 e2e | 1 |
| T (implement first) | 2 |
