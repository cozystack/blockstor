# day2-drbd-recover-quorum-loss

## Scenario

A primary node lost DRBD quorum with `on-no-quorum=suspend-io`. I/O is suspended. Recover gracefully without losing data on the other side.

## Steps

1. Confirm I/O is suspended: `dmesg | grep -i quorum` shows the loss event; consumer I/O is hanging.
2. (Optional, if a file system idler process is stuck) `fuser -k /mountpoint` to kill non-I/O openers.
3. Force the node to secondary: `drbdadm secondary --force <rsc>` (requires DRBD 9.1.7+ and drbd-utils 9.21+).
4. All suspended and newly submitted I/O on this node terminates with I/O errors.
5. Unmount the file system on this node.
6. Reconnect: the node rejoins the cluster partition that has the more recent data and resyncs.

## Expected outcome

- The suspended I/O resolves (with errors).
- The node demotes to Secondary cleanly and resyncs from the survivors.
- Cluster heals once the network heals.

## Validations

- `drbdadm role <rsc>` returns `Secondary` after step 3.
- `drbdadm status <rsc>` eventually reports `connection:Connected` and `disk:UpToDate` on this node.

## Doc reference

drbd-troubleshooting.adoc: `=== Recovering a primary node that lost quorum` (lines 290-347).

## Notes

- For automatic recovery in the same scenario, configure `on-suspended-primary-outdated=force-secondary` and `rr-conflict=retry-connect` in the resource's DRBD config (LINSTOR exposes both via `drbd-options` / `drbd-peer-options`).
- The opposite policy (`on-no-quorum=io-error`) makes this scenario unnecessary - I/O fails immediately instead of suspending.
