# day2-remote-modify

## Scenario

Update an existing remote's credentials, endpoint, or path-style flag.

## Steps

1. List remotes: `linstor remote list`.
2. Modify in place (interactive for password fields): `linstor remote modify <name> [--access-key NEW] [--secret-key NEW] [--endpoint NEW] [--use-path-style] ...`.
3. Verify with `linstor remote list`.

## Expected outcome

- Subsequent `backup` / `ship` operations using the remote use the new credentials/endpoint.

## Validations

- `linstor remote list | grep <name>` shows the updated configuration where visible.
- A test `linstor backup list <name>` succeeds with the new credentials.

## Doc reference

linstor-administration.adoc: `===== Listing, modifying, and deleting remotes` (lines 2599-2604).

## Notes

- For LINSTOR remotes, you can update the `--cluster-id` or `--passphrase` after the fact.
- Encryption must remain enabled while a remote exists.
