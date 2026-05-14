# day2-auto-quorum-disable

## Scenario

Disable the LINSTOR auto-quorum policies on a resource group so you can manually configure quorum behaviour.

## Steps

1. Disable the LINSTOR automatism: `linstor resource-group set-property my_ssd_group DrbdOptions/auto-quorum disabled`.
2. Set the DRBD quorum policy by hand:
```
linstor resource-group set-property my_ssd_group DrbdOptions/Resource/quorum majority
linstor resource-group set-property my_ssd_group DrbdOptions/Resource/on-no-quorum suspend-io
```
3. Spawn or re-deploy a resource and verify the new quorum policy.

## Expected outcome

- Auto-quorum is disabled for this RG; spawned resources inherit the manual settings exactly as configured.
- LINSTOR no longer adds/removes quorum properties automatically as replica count changes.

## Validations

- `linstor rg list-properties my_ssd_group` shows `auto-quorum=disabled` and the manual `quorum=majority`, `on-no-quorum=suspend-io`.
- On the satellite, `<rd>.res` shows `quorum majority; on-no-quorum suspend-io;` in the `options` block.

## Doc reference

linstor-administration.adoc: `==== Auto-quorum policies` (lines 4233-4279).

## Notes

- Acceptable values for `DrbdOptions/auto-quorum`: `disabled`, `suspend-io`, `io-error`.
- To completely turn off DRBD quorum on the resource: after `auto-quorum=disabled`, set `DrbdOptions/Resource/quorum=off` and unset `on-no-quorum` (by setting it to empty).
