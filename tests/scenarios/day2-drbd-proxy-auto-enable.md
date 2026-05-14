# day2-drbd-proxy-auto-enable

## Scenario

Have LINSTOR automatically enable DRBD Proxy on connections that cross site boundaries.

## Steps

1. Label each node with its site: `linstor node set-property <node> Site <site-id>`.
2. Set the controller-level toggle: `linstor controller set-property DrbdProxy/AutoEnable true`.
3. Create new resources - LINSTOR will check site values and enable Proxy on connections that cross sites.

## Expected outcome

- New resources whose replicas span two sites get DRBD Proxy automatically enabled on the cross-site connections.
- Same-site connections continue without Proxy.

## Validations

- `linstor controller list-properties | grep DrbdProxy/AutoEnable` returns `true`.
- For a newly created cross-site resource, `linstor drbd-proxy list <rsc>` returns Proxy enabled rows.

## Doc reference

linstor-administration.adoc: `==== Automatically enabling DRBD Proxy` (lines 3690-3715).

## Notes

- `DrbdProxy/AutoEnable` can be set at controller / node / RD / resource / resource-connection levels (left-to-right is increasing priority).
- Already-deployed resources are not retroactively updated; rerun your placement workflow if needed.
