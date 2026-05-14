# day2-encryption-enter-passphrase-on-restart

## Scenario

Re-enter the LINSTOR master passphrase after a controller restart so that encryption-dependent operations (LUKS resources, remotes) work again.

## Steps

1. After the controller starts, run `linstor encryption enter-passphrase` (interactive prompt).
2. Confirm by attempting an encryption-dependent operation, for example listing remotes: `linstor remote list`.

## Expected outcome

- LINSTOR unlocks the master key in memory.
- LUKS resources are mountable again; backup ship / restore commands no longer fail with "encryption not entered".

## Validations

- `linstor encryption enter-passphrase` returns success.
- A LUKS-layered resource that was previously inaccessible can be activated (`drbdadm up <rd>`).

## Doc reference

linstor-administration.adoc: `==== Encryption commands` lines 2314-2323.

## Notes

- Forgetting to re-enter the passphrase typically manifests as remotes appearing "broken" or LUKS volumes refusing to open.
- For automated re-entry, see `day2-encryption-create-passphrase.md` (auto-passphrase via `linstor.toml`).
