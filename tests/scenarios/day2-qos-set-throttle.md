# day2-qos-set-throttle

## Scenario

Apply per-volume QoS (block I/O throughput / IOPS limits) via LINSTOR sysfs properties.

## Steps

1. Decide the limit (bytes/sec or IOPS).
2. Set a property on a VG / RG so all spawned volumes inherit it, for example:
```
linstor volume-group set-property qos_limited 0 sys/fs/blkio_throttle_write 1048576
```
3. Spawn (or modify) resources from the group and verify on a satellite: `cat /sys/fs/cgroup/blkio/blkio.throttle.write_bps_device`.

## Expected outcome

- Each spawned volume's backing block device has a write throttle of 1 MiB/s (or whatever was set).
- The sysfs file shows `<major>:<minor> 1048576`.

## Validations

- On the satellite, `cat /sys/fs/cgroup/blkio/blkio.throttle.write_bps_device | grep <devmajor>:<devminor>` shows the configured value.
- Benchmark inside the consumer confirms throttling.

## Doc reference

linstor-administration.adoc: `=== QoS settings` (lines 4536-4609). Property keys are `sys/fs/blkio_throttle_read`, `sys/fs/blkio_throttle_write`, `sys/fs/blkio_throttle_read_iops`, `sys/fs/blkio_throttle_write_iops`.

## Notes

- Setting at group/definition level affects BOTH existing and new resources (in contrast to many LINSTOR properties).
- For a DRBD volume with external metadata, the throttle applies only to the local data backing device, NOT the metadata device.
- For NVMe target/initiator, the throttle is applied to both the target's LVM/ZFS backing and the initiator's connected nvme-device.
