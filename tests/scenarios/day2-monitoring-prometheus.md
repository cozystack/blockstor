# day2-monitoring-prometheus

## Scenario

Scrape the LINSTOR Prometheus `/metrics` endpoint for cluster health monitoring.

## Steps

1. From a Prometheus host, configure a scrape target pointing at the controller's HTTP port: `<controller-ip>:3370/metrics`.
2. (Optional) Reduce the dataset using GET parameters: `/metrics?error_reports=false` or `?resource=false` or `?storage_pools=false`.
3. Reload Prometheus and confirm the targets are up.
4. (Optional) Use the `/health` endpoint for basic liveness checks (returns HTTP 200 / 500).

## Expected outcome

- Metrics like `linstor_info`, `drbd_version`, `linstor_resource_state`, `linstor_storage_pool_capacity_bytes` are scraped.
- `/health` returns 200 when the controller can reach its DB and all services.

## Validations

- `curl http://<controller>:3370/metrics` returns Prometheus exposition format.
- `curl -o /dev/null -w '%{http_code}\n' http://<controller>:3370/health` returns `200`.

## Doc reference

linstor-administration.adoc: `=== Monitoring` (lines 4030-4048).

## Notes

- LINSTOR has had `/metrics` since 1.8.0.
- DRBD-resource-level metrics are exposed via the `drbd-reactor` sidecar on each satellite (Kubernetes Operator handles this; on bare metal it must be deployed separately).
- See `day2-monitoring-prometheus-kubernetes.md` for the Operator v2 setup.
