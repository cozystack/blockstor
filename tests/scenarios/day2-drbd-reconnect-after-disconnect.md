# day2-drbd-reconnect-after-disconnect

## Scenario

A peer was manually disconnected (`drbdadm disconnect`) - reconnect it without restarting the satellite.

## Steps

1. Verify state: on the disconnected side, `drbdadm status <rsc>` shows `connection:StandAlone`.
2. Reconnect: `drbdadm connect <rsc>`.
3. Watch for `Connected` and sync progress.

## Expected outcome

- The connection state transitions `StandAlone` → `Connecting` → `Connected`.
- Any bitmap-tracked changes resync.

## Validations

- `drbdadm status <rsc>` returns `connection:Connected` and `peer-disk:UpToDate`.
- No data corruption (provided no split-brain).

## Doc reference

drbd-troubleshooting.adoc / `=== Manual split brain recovery` (line 270) for the survivor-side `connect` command and general DRBD docs.

## Notes

- If `connect` fails due to split-brain, follow `day2-drbd-split-brain-manual-recovery.md`.
- In Kubernetes Operator v2, you can do this via `kubectl exec ds/linstor-satellite.<node> -- drbdadm connect <rsc>`.
