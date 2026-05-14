# day2-resource-toggle-disk-stuck

## Scenario

A `resource toggle-disk` command got stuck (typically due to backend disk error or satellite disconnect). Recover by retrying or cancelling.

## Steps

1. Identify the stuck resource: `linstor r l` shows the replica in a transient state (toggling).
2. Resolve the underlying cause first (replace failed disk, reconnect the satellite, etc.).
3. To retry, re-issue the original command, e.g. `linstor resource toggle-disk alpha backups --storage-pool pool_ssd`.
4. To cancel, run the inverse: if the stuck command was `--storage-pool ...`, run `--diskless`. If it was `--diskless`, run `--storage-pool <pool>`.

## Expected outcome

- Retry: LINSTOR re-runs the toggle once the underlying issue is resolved and the resource reaches the intended state.
- Cancel: LINSTOR reverses course, the replica returns to the previous (pre-toggle) state.

## Validations

- After retry: `linstor r l --resource <rd> --node <node>` shows the intended `State` (Diskless or UpToDate).
- After cancel: state matches what it was before the original toggle was issued.

## Doc reference

linstor-administration.adoc: `==== Recovering stuck resources from failed toggle disk operations` (lines 3631-3641). Requires LINSTOR server >= 1.34.0.

## Notes

- Pre-1.34.0 there was no clean recovery path - operators had to manually edit the LINSTOR DB.
- ALWAYS resolve the root cause first; retrying with an unhealthy backend just re-creates the stuck state.
