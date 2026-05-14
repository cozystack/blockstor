# day2-controller-tcp-port-range

## Scenario

Restrict the TCP port range LINSTOR uses for dynamic DRBD port assignments (default 7000-7999).

## Steps

1. Pick a desired range, e.g. `7900-7999`.
2. Set the property: `linstor controller set-property TcpPortAutoRange 7900-7999`.
3. Verify: `linstor controller list-properties | grep TcpPortAutoRange`.
4. Create a new resource and verify it gets a port within the range.

## Expected outcome

- New DRBD resources are assigned ports in the new range only.
- Existing resources keep their previously-assigned ports (no auto-renumber).

## Validations

- `linstor controller list-properties | grep TcpPortAutoRange` shows `7900-7999`.
- A freshly created RD has ports in the new range when inspected via `linstor rd l --show-props` or via the `.res` file.

## Doc reference

linstor-administration.adoc: NOTE block at lines 2224-2231 in `=== Managing network interface cards` (TcpPortAutoRange documented as a controller property).

## Notes

- Shrinking the range below the current resource count will eventually cause port exhaustion when new resources are created.
- To free a stuck port, see `day2-drbd-port-stuck.md` (forbidden actions for force-stripping CRD finalizers).
