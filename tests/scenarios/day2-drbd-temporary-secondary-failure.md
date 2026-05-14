# day2-drbd-temporary-secondary-failure

## Scenario

A secondary-role node fails temporarily (reboot, memory swap, brief network glitch). After it comes back, DRBD must catch up without operator intervention.

## Steps

1. Note the issue: peer node powers off / network drops.
2. On the surviving primary, observe: `drbdadm status` shows `connection:Connecting` for the lost peer.
3. Fix the underlying issue (replace RAM, restore network, etc.) and bring the node back up.
4. DRBD on both sides re-establishes the connection.
5. Surviving primary sends the modifications recorded in its dirty bitmap to the recovered secondary.

## Expected outcome

- The recovered secondary transitions through `SyncTarget` to `UpToDate`.
- No data loss; no manual intervention needed.

## Validations

- Before recovery: `drbdadm status` on the primary shows the peer as `Connecting`.
- After recovery: both nodes are `Connected` and `UpToDate`.

## Doc reference

drbd-troubleshooting.adoc: `==== Dealing with temporary secondary node failure` (lines 148-170).

## Notes

- During the resync window (post-failure), the secondary cannot be promoted - it's briefly Inconsistent.
- With three or more replicas (DRBD 9), a single failing secondary still leaves quorum and another secondary available for failover.
- This scenario relies on `--disconnect` not being explicitly invoked; if you needed to force a disconnect, see `day2-drbd-reconnect-after-disconnect.md`.
