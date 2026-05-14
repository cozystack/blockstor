# day2-drbd-disconnect-alertmanager-test

## Scenario

Validate that DRBD-related Prometheus alerts fire when a connection is intentionally torn down.

## Steps

1. Pick a SECONDARY-role replica of a non-critical resource: `linstor r l --resource my-res` and confirm Usage is Unused (not Primary).
2. Force-disconnect the connection from a satellite: `kubectl exec -it -n linbit-sds kube-0 -- drbdadm disconnect --force my-res` (k8s) or `drbdadm disconnect --force my-res` (bare metal).
3. Wait < 1 minute for Prometheus / Alertmanager to evaluate.
4. Confirm alerts in the Alertmanager UI (e.g. `drbdResourceSuspended`, `DrbdConnectionNotConnected`).
5. Restore: `drbdadm connect my-res`.
6. Confirm alerts auto-resolve.

## Expected outcome

- An alert with severity warning/critical appears in Alertmanager.
- Resolves to `inactive` after reconnect.

## Validations

- `kubectl exec -it -n linbit-sds kube-0 -- drbdadm status my-res` shows `Connected` after step 5.
- Alertmanager UI shows no active alerts after resolution.

## Doc reference

linstor-kubernetes.adoc: `====== Verifying the Prometheus Alertmanager web console deployment` (lines 3177-3239).

## Notes

- USE A SECONDARY REPLICA. Disconnecting a Primary in production can cause I/O hang or split-brain.
- After the test, run a `drbdadm status` to confirm `peer-disk:UpToDate` on the reconnected peer before declaring the test passed.
- Cross-link: `day2-monitoring-prometheus-kubernetes.md` for setting up the Alertmanager.
