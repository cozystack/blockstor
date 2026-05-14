# day2-auto-diskful

## Scenario

Configure LINSTOR to automatically convert a Diskless DRBD replica to Diskful if the node has been Primary for the resource for more than X minutes.

## Steps

1. Set the property on the RD: `linstor resource-definition set-property myres DrbdOptions/auto-diskful 5` (minutes).
2. (Optional) Also at RG or controller scope: `linstor resource-group set-property <rg> DrbdOptions/auto-diskful 5` / `linstor controller set-property DrbdOptions/auto-diskful 5`.
3. Trigger by making a Diskless node Primary (mount the DRBD device).
4. After 5 minutes, observe LINSTOR run `resource toggle-disk`.

## Expected outcome

- After the threshold, LINSTOR converts the node from Diskless to Diskful by toggling its disk and syncing data in.
- If `auto-diskful-allow-cleanup` is true (default), LINSTOR also removes a now-superfluous Secondary replica elsewhere.

## Validations

- Before: `linstor r l --node <node> --resource myres | grep State` shows `Diskless`.
- After threshold: same query shows `SyncTarget`, then eventually `UpToDate`.

## Doc reference

linstor-administration.adoc: `==== Auto-diskful and related options` (lines 4349-4425).

## Notes

- Priority: RD > RG > controller.
- Unset with `linstor <object> set-property DrbdOptions/auto-diskful` (no value).
- Helpful in integration with platforms (OpenStack, OpenNebula) that move workloads unpredictably.
