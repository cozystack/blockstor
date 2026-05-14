# day2-monitoring-prometheus-kubernetes

## Scenario

Deploy Prometheus + Alertmanager + Grafana for LINSTOR via the Operator v2 chart, including PodMonitor and PrometheusRule manifests.

## Steps

1. Deploy the kube-prometheus-stack:
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install --create-namespace -n monitoring prometheus prometheus-community/kube-prometheus-stack \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false
```
2. Apply the LINBIT-provided monitoring manifests:
```
kubectl apply -k "https://github.com/linbit/linstor-operator-builder//config/monitoring?ref=v2"
```
(or `monitoring-with-api-tls` if API TLS is enabled).
3. Verify the resources appear: `kubectl get servicemonitor,podmonitor,prometheusrule -A | grep linstor`.
4. Port-forward to inspect the consoles:
```
kubectl port-forward -n monitoring services/prometheus-kube-prometheus-prometheus 9090:9090
kubectl port-forward -n monitoring services/prometheus-kube-prometheus-alertmanager 9093:9093
kubectl port-forward -n monitoring services/prometheus-grafana 3000:http-web
```

## Expected outcome

- Prometheus scrapes both LINSTOR controller (ServiceMonitor) and DRBD per-node metrics (PodMonitor).
- A Grafana dashboard "LINBIT SDS" is available.
- Alerts fire on LINSTOR / DRBD anomalies.

## Validations

- In Prometheus, `linstor_info` and `drbd_version` queries return data.
- `kubectl get prometheusrule -A | grep linbit-sds` returns one rule resource.
- A test event (e.g. `kubectl exec ds/linstor-satellite.<node> -- drbdadm disconnect --force <rsc>`) triggers a `drbdResourceSuspended` alert.

## Doc reference

linstor-kubernetes.adoc: `==== Configuring monitoring with Prometheus in Operator v2 deployments` (lines 3050-3265).

## Notes

- For Operator v1, use the SatelliteSet's bundled monitoring sidecar instead (`monitoringImage`).
- LINSTOR is usually in `linbit-sds`; default `kube-prometheus-stack` settings only watch `kube-system` - the `*NilUsesHelmValues=false` settings fix that.
- Cross-link: `day2-monitoring-prometheus.md` for the bare-metal endpoint.
