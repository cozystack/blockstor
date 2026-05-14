# day2-drbd-proxy-enable

## Scenario

Enable DRBD Proxy (long-distance replication buffer) on connections between local nodes and a remote-site node.

## Steps

1. Confirm DRBD Proxy is installed on every node involved (it must run on the SAME nodes as DRBD).
2. Enable on each local-to-remote connection:
```
linstor drbd-proxy enable alpha charlie backups
linstor drbd-proxy enable bravo charlie backups
```
3. Tune memlimit / compression:
```
linstor drbd-proxy options backups --memlimit 100000000
linstor drbd-proxy compression zlib backups --level 9
```
4. Switch to async protocol for the long-haul connections:
```
linstor resource-connection drbd-peer-options alpha charlie backups --protocol A
linstor resource-connection drbd-peer-options bravo charlie backups --protocol A
```

## Expected outcome

- The local-to-remote DRBD connections are routed via DRBD Proxy, with the configured buffer / compression.
- Protocol A async means writes complete locally before they're replicated remotely.

## Validations

- `linstor drbd-proxy list backups` shows the configured options.
- DRBD Proxy log on each involved node shows established connections.

## Doc reference

linstor-administration.adoc: `=== Configuring DRBD Proxy by using LINSTOR` (lines 3658-3688).

## Notes

- DRBD Proxy is a separately-licensed LINBIT product.
- For auto-enable based on `Site` aux property, see `day2-drbd-proxy-auto-enable.md`.
