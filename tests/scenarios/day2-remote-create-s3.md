# day2-remote-create-s3

## Scenario

Register an S3-compatible bucket as a LINSTOR remote, so snapshots can be shipped to it as backups.

## Steps

1. Make sure LINSTOR encryption is enabled (passphrase created) - remotes hold sensitive credentials and refuse to be created without it. See `day2-encryption-create-passphrase.md`.
2. Create the remote: `linstor remote create s3 myRemote s3.us-west-2.amazonaws.com my-bucket us-west-2 admin password`.
3. Verify: `linstor remote list`.

## Expected outcome

- A remote named `myRemote` is registered.
- `linstor backup create myRemote <rsc>` will be able to upload a backup of the resource.

## Validations

- `linstor remote list | grep myRemote` returns one row with type `S3`.
- A subsequent `linstor backup list myRemote` returns a (possibly empty) list without error.

## Doc reference

linstor-administration.adoc: `===== Creating an S3 remote` (lines 2562-2580).

## Notes

- Default URL style is virtual-hosted (`my-bucket.s3.endpoint`). For setups requiring path-style (`endpoint/my-bucket`), pass `--use-path-style`.
- The secret-key may be passed interactively if omitted from the command line.
- Cross-link: `day2-encryption-create-passphrase.md` for the prerequisite encryption setup.
