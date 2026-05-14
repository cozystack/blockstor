# Wave 2 — Group 7 — Quorum, observability, capacity (Day2 ops)

Day2 tuning surface for: auto-evict, auto-quorum disable, auto-diskful,
error-reports + sos-report, controller log level + logback rotation,
the K8s Prometheus stack, and the three over-subscription gates.

Pairs with wave1's `07-quorum-observability.md` — Day2 knobs that
operators flip during incident response or capacity planning.

[Group index in README.md](README.md).

---

## Quorum / auto-* controllers

### 7.W01 `auto-quorum=disabled` then manual quorum/on-no-quorum — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Auto-quorum policies" (lines 4233-4279) via tests/scenarios/day2-auto-quorum-disable.md

Cross-listed with wave1 7.3. Set on RG: `DrbdOptions/auto-quorum=disabled`, then `DrbdOptions/Resource/quorum=majority`, `DrbdOptions/Resource/on-no-quorum=suspend-io`. LINSTOR no longer auto-adjusts quorum as replica count changes. Acceptable `auto-quorum` values: `disabled`, `suspend-io`, `io-error`.

### 7.W02 `DrbdOptions/AutoEvict*` tuning — T

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Auto-evict" (lines 4281-4348) via tests/scenarios/day2-auto-evict-tuning.md

Cross-listed with wave1 4.24. Four controller props: `AutoEvictAfterTime` (default 60min), `AutoEvictMinReplicaCount`, `AutoEvictMaxDisconnectedNodes` (default 34%), `AutoEvictAllowEviction` (per-node opt-out). Setting `MaxDisconnectedNodes=0` disables globally.

**Integration:** envtest with mocked clock + offline satellite > `AfterTime` → reconciler triggers eviction.
**E2E:** 4-node cluster, set props, drop one satellite → eviction within configured window; resources auto-place on the 4th.

### 7.W03 `DrbdOptions/auto-diskful=<minutes>` — S

- **Priority:** P1  **Target:** integration + e2e  **Complexity:** M
- **Source:** UG9 §"Auto-diskful and related options" (lines 4349-4425) via tests/scenarios/day2-auto-diskful.md

Cross-listed with wave1 4.25, 4.26. Hierarchy: RD > RG > controller. Auto-diskful + `auto-diskful-allow-cleanup` (default true) → after Diskless held Primary > N minutes, toggle to Diskful + remove excess Secondary.

## Observability

### 7.W04 `error-reports list` filter by node + since + limit — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Generating SOS reports" (lines 4653-4677) via tests/scenarios/day2-error-reports-list.md

Cross-listed with wave1 5.38 / 7.17. GET `/v1/error-reports?node=...&since=...&limit=...` filters; `show <id>` returns full stack-trace + context. Prometheus `/metrics` exposes a count.

### 7.W05 `sos-report create | download` bundles diagnostics — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Generating SOS reports" (lines 4653-4677) via tests/scenarios/day2-sos-report.md

**Out of scope.** K8s log aggregation (`kubectl logs`, Loki/Promtail, cozystack support flows) covers the LINSTOR sos-report use case. See `out-of-scope.md` → "SOS / diagnostic bundle".

### 7.W06 `controller set-log-level DEBUG` (runtime) — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Logging" (lines 3926+) via tests/scenarios/day2-logging-set-level.md

PATCH controller props → effective log level without restart. Supported: TRACE / DEBUG / INFO / WARN / ERROR. For blockstor (K8s deployment): `kubectl logs deploy/blockstor-apiserver` reflects the new level immediately.

### 7.W07 `logback.xml` persistent rotation config — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** UG9 §"Logging" (lines 3926+) via tests/scenarios/day2-controller-set-log-rotation-via-config.md

**Out of scope.** blockstor is Go, not Java — no logback. K8s handles log rotation at kubelet / fluent-bit level. Log level changes go through `--log-level` already pinned in wave1 1.x. See `out-of-scope.md` → "Logback rotation config".

### 7.W08 K8s Prometheus + Alertmanager + Grafana stack — P

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Configuring monitoring with Prometheus in Operator v2 deployments" (lines 3050-3265) via tests/scenarios/day2-monitoring-prometheus-kubernetes.md

ServiceMonitor (controller) + PodMonitor (per-satellite DRBD metrics) + PrometheusRule. blockstor exposes `/metrics` on the apiserver; satellites need a `drbd-reactor` sidecar for per-resource metrics. Test:

**E2E:** deploy kube-prometheus-stack + LINBIT monitoring manifests; `kubectl get prometheusrule -A | grep linbit-sds` returns rules; Prometheus query `linstor_info` returns data; force-disconnect a Secondary → `drbdResourceSuspended` alert fires within 1min. Pairs with wave2-05 5.W16.

## Capacity gates

### 7.W09 `MaxFreeCapacityOversubscriptionRatio` (default 20) — P

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Configuring a maximum free capacity over provisioning ratio" (lines 3453-3499) via tests/scenarios/day2-over-provisioning-free-capacity.md

Cross-listed with wave1 7.19. Per-pool or controller-wide. `MaxVolumeSize = ratio * free_capacity`. Pool-level overrides controller. Combined with 7.W10: LINSTOR uses the LOWER.

**Unit:** `pkg/rest/query_size_info_test.go` — ZFS_THIN 10 GiB free + ratio=2 → MaxVolumeSize=20 GiB.
**E2E:** flip ratio, `rg spawn` exceeds it → rejected with "exceeds oversubscription" text.

### 7.W10 `MaxTotalCapacityOversubscriptionRatio` (default 20) — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (after 7.W09)
- **Source:** UG9 §"Configuring a maximum total capacity over provisioning ratio" (lines 3500-3528) via tests/scenarios/day2-over-provisioning-total-capacity.md

Cross-listed with wave1 7.20. Caps against total pool capacity (not free) — relaxes as volumes fill (total doesn't change). Pool-level overrides controller.

### 7.W11 `MaxOversubscriptionRatio` master backstop — T

- **Priority:** P1  **Target:** unit  **Complexity:** L (after 7.W09)
- **Source:** UG9 §"Configuring a maximum over subscription ratio for over provisioning" (lines 3530-3548) + §"The effects of setting values on multiple over provisioning properties" (lines 3550-3607) via tests/scenarios/day2-over-provisioning-max-oversubscription.md

Cross-listed with wave1 7.21. Used when more-specific (`MaxFree*` / `MaxTotal*`) are unset. Pool-level always wins for the SAME prop; per-prop fallback to controller. Default 20.

---

## Group summary

| Tag | Count |
|-----|------:|
| P1 unit | 4 |
| P1 e2e | 5 |
| P2 (any) | 2 |
| T (implement first) | 3 |
