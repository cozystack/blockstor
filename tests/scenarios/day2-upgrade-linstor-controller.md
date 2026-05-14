# day2-upgrade-linstor-controller

## Scenario

Upgrade the LINSTOR controller package (and clients) to a new version while preserving the database.

## Steps

1. BACK UP the controller DB first: see `day2-controller-backup-db.md`.
2. Stop the controller: `systemctl stop linstor-controller`.
3. Upgrade the package (RPM: `dnf upgrade linstor-controller linstor-client`; DEB: `apt install --only-upgrade linstor-controller linstor-client`).
4. Start: `systemctl start linstor-controller`.
5. Verify the new version: `linstor controller version`.
6. Repeat for satellites (`linstor-satellite` package) one at a time, rolling. Each satellite restart briefly disconnects DRBD; cluster stays available.

## Expected outcome

- Controller comes up on the new version with the same data.
- Satellites reconnect after upgrade; DRBD resyncs any bitmap deltas accumulated during the disconnect.

## Validations

- `linstor controller version` shows the new version string.
- `linstor node list` shows all satellites `Online`.
- DRBD resources return to `UpToDate` after rolling satellite upgrades.

## Doc reference

linstor-administration.adoc: `=== Upgrading LINSTOR` (lines 173-260) plus `==== Verifying a LINSTOR upgrade` (lines 248-260).

## Notes

- Satellite-only nodes can be upgraded individually; controller-combined nodes need the controller stopped briefly.
- Always upgrade controller BEFORE satellites; controller is backward-compatible with older satellites but not vice-versa.
- LINSTOR's online resources keep serving I/O during the rolling upgrade (DRBD survives satellite restarts).
