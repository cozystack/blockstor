# day2-encryption-modify-passphrase

## Scenario

Rotate the LINSTOR master passphrase used to protect LUKS keys and remote credentials.

## Steps

1. Make sure the current passphrase has been entered: `linstor encryption enter-passphrase`.
2. Rotate: `linstor encryption modify-passphrase` (interactive; prompts for old and new).
3. Update the auto-passphrase config (`linstor.toml`) to the new value if you use that path.
4. Restart the controller and run `linstor encryption enter-passphrase` with the new passphrase.

## Expected outcome

- All stored encryption secrets are re-encrypted under the new master passphrase.
- Existing LUKS volumes and remotes continue to work without re-creation.

## Validations

- After the controller restart, `linstor encryption enter-passphrase` with the new passphrase unlocks LINSTOR.
- A LUKS-layered resource is still mountable.

## Doc reference

linstor-administration.adoc: lines 2300-2305 `linstor encryption modify-passphrase`.

## Notes

- Backup the LINSTOR database before rotating in production.
- If `linstor.toml` still has the old passphrase, the controller refuses on next restart.
