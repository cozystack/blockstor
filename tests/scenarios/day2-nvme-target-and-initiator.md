# day2-nvme-target-and-initiator

## Scenario

Use the LINSTOR NVMe-oF / NVMe-TCP layer (NO DRBD) to access remote storage on diskless nodes.

## Steps

1. Confirm `nvme-cli` is installed on every satellite that will be a target or initiator.
2. Create the RD with NVMe-only layer list: `linstor resource-definition create nvmedata -l nvme,storage`.
3. (Optional) Switch from NVMe-oF (RDMA) to NVMe-TCP: `linstor resource-definition set-property nvmedata NVMe/TRType tcp`.
4. Create the volume: `linstor volume-definition create nvmedata 500G`.
5. Create the diskful target: `linstor resource create alpha nvmedata --storage-pool pool_ssd`.
6. Create the diskless initiator on a peer node: `linstor resource create beta nvmedata --nvme-initiator`.

## Expected outcome

- `alpha` exports the volume via NVMe; `beta` connects and sees a block device that maps to alpha's storage.
- I/O on `beta`'s block device traverses the NVMe transport to `alpha`.
- NO replication (single copy on `alpha`).

## Validations

- On `alpha`, `nvmetcli ls` (or `nvmet` sysfs) shows the exported namespace.
- On `beta`, `nvme list` shows a connected device.
- `linstor r l --resource nvmedata` shows `alpha=UpToDate` and `beta=Diskless` with `Layers=NVME,STORAGE`.

## Doc reference

linstor-administration.adoc: `==== NVMe-oF/NVMe-TCP LINSTOR layer` (lines 1838-1906).

## Notes

- NVMe-oF needs RDMA-capable networking. NVMe-TCP works on plain Ethernet.
- If nodes have multiple NICs, route NVMe traffic explicitly (`ip route`) to avoid path flapping.
- Cozystack/blockstor explicitly DOES NOT support NVMe layers (DRBD-only).
