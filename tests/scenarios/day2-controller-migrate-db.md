# day2-controller-migrate-db

## Scenario

Convert the LINSTOR controller database from one backend to another (for example H2 file -> PostgreSQL, or internal -> etcd) using `linstor-database`.

## Steps

1. Export the current DB: `/usr/share/linstor-server/bin/linstor-database export-db /tmp/dump.json` (controller MUST be stopped).
2. Update `/etc/linstor/linstor.toml` `[db]` section to the new backend (PostgreSQL/MariaDB/etcd) with connection URL and credentials.
3. (Optional) Create the target schema/user on the new DB.
4. Import: `/usr/share/linstor-server/bin/linstor-database import-db /tmp/dump.json`.
5. Start the controller and verify state with `linstor node list` etc.

## Expected outcome

- The controller comes up using the new backend, with all previous LINSTOR objects intact.

## Validations

- `linstor node list`, `linstor rd l`, `linstor sp l` show the same data as before.
- Connections from the controller to the new DB are visible in the new DB's process list.

## Doc reference

linstor-administration.adoc: `==== Converting databases` (lines 1502-1516) and `=== External database providers` (lines 3716-3784).

## Notes

- Controller MUST be stopped for both export and import; otherwise data corruption.
- Backup the OLD DB before starting; if anything fails you need to roll back.
- MariaDB requires the schema name `LINSTOR` (case-sensitive in some configs).
