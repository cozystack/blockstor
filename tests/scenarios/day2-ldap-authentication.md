# day2-ldap-authentication

## Scenario

Restrict LINSTOR controller access to LDAP-authenticated users.

## Steps

1. Edit `/etc/linstor/linstor.toml` to add an `[ldap]` section:
```
[ldap]
  enabled = true
  allow_public_access = false
  uri = "ldaps://ldap.example.com"
  dn = "uid={user}"
  search_base = "dc=example,dc=com"
  search_filter = "(&(uid={user})(memberof=ou=storage-services,dc=example,dc=com))"
```
2. Restart the controller: `systemctl restart linstor-controller`.
3. Issue commands as an authenticated user: `linstor --user alice node list` (prompts for password).
4. (If HTTPS REST API is not configured) add `--allow-insecure-auth` (NOT recommended).

## Expected outcome

- Unauthenticated requests are denied (except the trivial `--version`).
- Members of the configured LDAP group can list / modify cluster state.

## Validations

- A user OUTSIDE the search filter receives an auth error.
- A user INSIDE the filter successfully runs `linstor node list`.

## Doc reference

linstor-administration.adoc: `=== Configuring LDAP authentication` (lines 4140-4226).

## Notes

- Always pair with HTTPS REST API (see relevant section in linstor-administration.adoc, lines 3812-3848) - otherwise credentials traverse the network in plaintext.
- Misconfigured `search_filter` can lock everyone out; keep an emergency shell on the controller.
