# day2-auto-evict-tuning

## Scenario

Tune auto-eviction behaviour - how long a satellite can be offline before its resources are reassigned, and whether eviction is allowed at all.

## Steps

1. Set the global eviction timeout (minutes): `linstor controller set-property DrbdOptions/AutoEvictAfterTime 90`.
2. Set the minimum replicas always required: `linstor controller set-property DrbdOptions/AutoEvictMinReplicaCount 2`.
3. Set the disconnect-percentage threshold (0=disabled, 100=always-evict): `linstor controller set-property DrbdOptions/AutoEvictMaxDisconnectedNodes 34`.
4. (Optional, per-node) Prevent a single node from being evicted while you're working on it: `linstor node set-property <node> DrbdOptions/AutoEvictAllowEviction false`.

## Expected outcome

- After a satellite is offline for `AutoEvictAfterTime` minutes AND fewer than `AutoEvictMaxDisconnectedNodes`% of nodes are disconnected, LINSTOR marks it `EVICTED` and triggers autoplace to replace its resources, preserving `AutoEvictMinReplicaCount`.
- Setting `AutoEvictAllowEviction=false` on a node makes it immune.

## Validations

- `linstor controller list-properties | grep AutoEvict` returns the configured values.
- Simulating an offline node: after the configured timeout, `linstor node list` shows it `EVICTED` and resources reappear on other nodes.

## Doc reference

linstor-administration.adoc: `==== Auto-evict` (lines 4281-4348).

## Notes

- Default `AutoEvictAfterTime` is 60 min; `AutoEvictMaxDisconnectedNodes` is 34%.
- Setting `AutoEvictMaxDisconnectedNodes=0` disables auto-evict globally; `=100` always evicts regardless of cluster health.
- To bring an evicted node back, use `linstor node restore` (see `day2-node-evacuate-restore.md`).
