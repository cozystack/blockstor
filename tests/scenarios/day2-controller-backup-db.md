# day2-controller-backup-db

## Scenario

Back up the LINSTOR controller database for disaster recovery using the bundled `linstor-database` tool.

## Steps

1. On the controller, run: `/usr/share/linstor-server/bin/linstor-database export-db /path/to/backup.json` (optionally pass `--config-directory /etc/linstor`).
2. Copy the resulting JSON file off-host.
3. To restore: `/usr/share/linstor-server/bin/linstor-database import-db /path/to/backup.json` on a fresh controller install.

## Expected outcome

- A portable JSON file containing the full LINSTOR DB state is produced.
- Import on a new controller restores nodes, RDs, RGs, resources, properties.

## Validations

- File exists and is valid JSON; size > 0.
- After `import-db` + controller start, `linstor node list` / `linstor rd l` shows the previous state.

## Doc reference

linstor-administration.adoc: `=== Backup and restore database` (lines 1439-1517).

## Notes

- Tool available since LINSTOR 1.24.0.
- `--config-directory` is needed if `linstor.toml` lives in a non-default location.
- Also use this to migrate between database backends (`day2-controller-migrate-db.md`).
