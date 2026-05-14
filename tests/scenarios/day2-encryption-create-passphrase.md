# day2-encryption-create-passphrase

## Scenario

Initialize LINSTOR's master encryption passphrase so that LUKS-layered resources, remotes, and backups can be used.

## Steps

1. Confirm `cryptsetup` is installed on every satellite that will host LUKS volumes.
2. Create the master passphrase: `linstor encryption create-passphrase` (interactive; prompts twice).
3. (Optional) Configure auto-passphrase via `linstor.toml`:
```
[encrypt]
passphrase="example"
```
4. After every controller restart, re-enter the passphrase: `linstor encryption enter-passphrase` (unless auto-passphrase is configured).

## Expected outcome

- LINSTOR is now able to encrypt/decrypt LUKS volumes and store sensitive remote credentials.
- `linstor remote create ...` no longer refuses with "encryption required".

## Validations

- `linstor encryption enter-passphrase` returns success.
- Creating a LUKS-layered resource succeeds.

## Doc reference

linstor-administration.adoc: `=== Encrypted volumes` (lines 2272-2351).

## Notes

- LINSTOR doesn't store the master passphrase - it's only kept in memory.
- An automatically configured passphrase in `linstor.toml` MUST match the previously-created one; otherwise the controller behaves as if the wrong passphrase was entered.
- `linstor encryption modify-passphrase` lets you rotate the passphrase.
