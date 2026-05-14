# day2-physical-storage-list

## Scenario

Discover blank backing disks on satellites that are eligible for storage-pool creation.

## Steps

1. Run `linstor physical-storage list`.
2. Inspect the output: grouped by size and rotational status, only showing disks that:
   - Are larger than 1 GiB
   - Are root devices (`/dev/vda`, `/dev/sda`, ...)
   - Have no filesystem signature (no `blkid` markers)
   - Are not DRBD devices

## Expected outcome

- A list of candidate disks per node. Missing disks mean they failed one of the four filters above.

## Validations

- `linstor physical-storage list | grep <node>` returns one entry per eligible disk.
- A disk with a known filesystem does NOT appear in the list.

## Doc reference

linstor-administration.adoc: `==== Creating storage pools by using the physical storage command` (lines 699-731).

## Notes

- If a candidate disk is missing, run `wipefs -a /dev/<dev>` (DESTRUCTIVE) to remove residual signatures, then re-list.
- Pair with `physical-storage create-device-pool` to immediately turn a candidate into a managed pool.
