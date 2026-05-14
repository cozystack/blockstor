# day2-property-list

## Scenario

Inspect properties set on any LINSTOR object (controller, node, RG, RD, VG, VD, storage-pool, resource).

## Steps

1. Pick the object scope. Use the appropriate `list-properties` subcommand:
   - Controller: `linstor controller list-properties`
   - Node: `linstor node list-properties <node>`
   - RG: `linstor resource-group list-properties <rg>`
   - RD: `linstor resource-definition list-properties <rd>`
   - VG / VD: `linstor volume-group list-properties <rg>` / `linstor volume-definition list-properties <rd> <vlmnr>`
   - Storage pool: `linstor storage-pool list-properties <node> <pool>`
   - Resource: `linstor resource list-properties <node> <rd>`
   - Resource connection: `linstor resource-connection list-properties <node-a> <node-b> <rd>`
   - Node connection: `linstor node-connection list-properties <node-a> <node-b>`

## Expected outcome

- A two-column table (`Key`, `Value`) of properties is returned for the chosen object.

## Validations

- For each scope, `list-properties` returns a non-empty list when properties are set; empty otherwise.
- Property keys conform to known namespaces (DrbdOptions/, Aux/, FileSystem/, StorDriver/, etc.).

## Doc reference

linstor-administration.adoc: `==== Verifying options for LINSTOR objects` (lines 3399-3413) and per-object set/unset sections throughout the doc.

## Notes

- Setting a property without a value typically DELETES it.
- For wildcard scope listing (controller-wide), only the controller `list-properties` exists; inheritance is computed at evaluation time.
