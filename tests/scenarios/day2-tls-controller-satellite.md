# day2-tls-controller-satellite

## Scenario

Enable TLS between the LINSTOR controller and its satellites (mTLS).

## Steps

1. Generate keystores per node (controller + each satellite) per the doc's keytool recipe.
2. Cross-import: each satellite trusts the controller; the controller trusts each satellite.
3. Copy keystores to `/etc/linstor/ssl/` on each host.
4. Update `linstor_satellite.toml` on each satellite to point at the keystores and use `type="ssl"` on port 3367.
5. Recreate (or update) each LINSTOR node with `--communication-type SSL`.
6. Restart controller and satellites.

## Expected outcome

- Controller-satellite traffic flows over TLS on port 3367 instead of plain TCP on 3366.
- `linstor node list` shows `Online (SSL)`.

## Validations

- `linstor node list` shows the new addressing on port 3367.
- `tcpdump -i any port 3367` shows TLS-encrypted traffic.

## Doc reference

linstor-administration.adoc: `=== Secure satellite connections` (lines 4050-4138).

## Notes

- For Kubernetes Operator v2, cert-manager is the supported automation path; see `linstor-kubernetes.adoc` lines 190-380.
- Mixing plain and SSL satellites is supported but discouraged; pick one mode cluster-wide.
