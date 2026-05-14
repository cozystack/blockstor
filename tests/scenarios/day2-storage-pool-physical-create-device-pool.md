# day2-storage-pool-physical-create-device-pool

## Scenario

Use the LINSTOR `physical-storage` helper to discover a blank disk on a satellite and create an LVM-thin pool plus the matching LINSTOR storage pool in one step.

## Steps

1. List candidate disks on the satellite: `linstor physical-storage list`.
2. Pick a disk row from the output (must be >1 GiB, root device, no FS, no DRBD signature).
3. Run `linstor physical-storage create-device-pool --pool-name lv_my_pool LVMTHIN <node> /dev/vdc --storage-pool newpool`.
4. Verify the storage pool appears: `linstor storage-pool list`.

## Expected outcome

- LINSTOR runs `pvcreate` / `vgcreate` / `lvcreate --thinpool` on the satellite over the given device.
- A LINSTOR storage pool named `newpool` of type `LVM_THIN` appears.
- The OS-level VG / thin LV are not managed by LINSTOR afterwards: deleting the pool from LINSTOR will NOT clean them up.

## Validations

- `linstor physical-storage list | grep /dev/vdc` shows the disk before step 3.
- After step 3, `linstor sp l --node <node> --storage-pool newpool` returns a row with `Driver=LVM_THIN`.
- On the satellite, `lvs` shows the new thin pool LV.

## Doc reference

linstor-administration.adoc: `==== Creating storage pools by using the physical storage command` (lines 699-731).

## Notes

- `linstor physical-storage list` filters out disks that already have a filesystem, are not root devices, or already host DRBD; if your disk is missing, run `wipefs -a /dev/<dev>` first.
- There is no `physical-storage delete` - clean-up after deletion is manual (`linstor storage-pool delete` only removes the LINSTOR record).
