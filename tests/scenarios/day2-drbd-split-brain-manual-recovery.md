# day2-drbd-split-brain-manual-recovery

## Scenario

Resolve a DRBD split-brain manually: choose the victim, discard its modifications, reconnect.

## Steps

1. Identify the split-brain: `dmesg | grep -i 'Split-Brain detected'` or `drbdadm status <rsc>` shows `connection:StandAlone` on one side.
2. Decide which node will be the SURVIVOR (the one with the data you want to keep) and which is the VICTIM.
3. On the VICTIM:
```
drbdadm disconnect <resource>
drbdadm secondary <resource>
drbdadm connect --discard-my-data <resource>
```
4. On the SURVIVOR (if its connection state is also `StandAlone`):
```
drbdadm disconnect <resource>
drbdadm connect <resource>
```
5. Watch the resync.

## Expected outcome

- The VICTIM transitions to `SyncTarget`, overwrites its modifications with data from the SURVIVOR.
- After sync, both nodes are `UpToDate`.

## Validations

- `drbdadm status <rsc>` on both nodes settles to `connection:Connected`, `disk:UpToDate`.
- Data on both nodes matches the SURVIVOR's pre-recovery state.

## Doc reference

drbd-troubleshooting.adoc: `=== Manual split brain recovery` (lines 217-289).

## Notes

- The victim is not subject to a FULL device sync; only modifications since the divergence are reverted.
- VERIFY which side has the good data BEFORE running `--discard-my-data`. Wrong choice = data loss.
- LINSTOR can prevent split-brain by enabling auto-quorum policies (see `day2-auto-quorum-disable.md` for the inverse).
