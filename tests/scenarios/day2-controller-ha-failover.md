# day2-controller-ha-failover

## Scenario

Use a highly available LINSTOR controller setup (DRBD-replicated state + Pacemaker / drbd-reactor / k8s leader-election). Validate that failover preserves cluster state.

## Steps

1. Confirm the HA controller is currently active on one node: `linstor controller list-properties | head -5` from a client, or check the VIP.
2. Trigger failover (stop the running controller pod / service).
3. The HA layer (Pacemaker / drbd-reactor / Operator) starts the controller on a secondary node, attaching to the DRBD-replicated DB.
4. Run `linstor node list` from a client - must return the same state as before failover.

## Expected outcome

- Brief unavailability (seconds) while the controller migrates.
- Existing satellites stay Online.
- All previously-defined resources, properties and remote configs are intact.

## Validations

- `linstor node list` works against the new active controller and returns the pre-failover state.
- Existing PVCs / DRBD resources continue to serve I/O throughout.

## Doc reference

linstor-administration.adoc: `=== Creating a highly available LINSTOR cluster` (lines 1521-...) and Kubernetes section `==== High-availability deployment in Operator v1` (lines 1375-1421).

## Notes

- HA database storage is itself a LINSTOR resource; chicken-and-egg bootstrapping is documented in the HA setup section.
- Without HA: a controller outage means no CRUD operations until restored, but existing DRBD I/O still works.
