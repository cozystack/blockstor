# day2-node-lost

## Scenario

Remove a permanently unreachable / destroyed node from the LINSTOR cluster, discarding everything that was on it.

## Steps

1. Confirm the node truly cannot be brought back (hardware destroyed, irreversibly reinstalled, etc.).
2. From any controller-aware host run `linstor node lost <node>`.
3. Verify the cluster's resources, ports and storage pools no longer reference the dead node.

## Expected outcome

- The node is removed from `linstor node list`.
- All resources, snapshots and storage pools that lived on the node are dropped from the LINSTOR database.
- Resources that had replicas on the dead node now show one fewer replica unless an autoplace task placed a new one elsewhere.

## Validations

- `linstor node list | grep <node>` returns nothing.
- `linstor resource list | grep <node>` returns nothing.
- For each previously-shared RD, `linstor resource list --resource <rd>` shows replicas only on the survivors.
- DRBD on survivors shows the dead node as `Connecting` only briefly before the controller removes the peer from the .res file.

## Doc reference

linstor-administration.adoc: `==== Auto-evict` (lines 4281-4348) - `node lost` is the manual counterpart to auto-evict.

## Notes

- `node lost` is destructive: never run it on a node you intend to bring back. Use `node delete` (after evacuation) or wait for auto-restore otherwise.
- Differs from `node delete`: `node delete` refuses if resources remain; `node lost` does not.
- If you want LINSTOR to repopulate the missing replicas automatically, ensure the resource groups have a `--place-count` greater than the surviving replica count and that there is capacity on the survivors.
