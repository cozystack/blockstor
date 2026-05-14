# day2-drbd-peer-options-node-connection

## Scenario

Apply a DRBD option across ALL resources at a node-connection level (for example all traffic from node-0 to node-1 gets a longer ping-timeout).

## Steps

1. Set the option: `linstor node-connection drbd-peer-options --ping-timeout 299 node-0 node-1`.
2. Verify: `linstor node-connection list-properties node-0 node-1`.

## Expected outcome

- The option is applied to every resource's connection between those two nodes.
- Effective DRBD `ping-timeout` is 29.9 seconds.

## Validations

- `linstor node-connection list-properties node-0 node-1 | grep ping-timeout` returns `299`.
- On both satellites, every `<rd>.res` shows the option in the alpha-bravo connection block.

## Doc reference

linstor-administration.adoc: `==== Setting DRBD options for node connections` (lines 3386-3398).

## Notes

- DRBD encodes time as 1/10 of a second; `--ping-timeout 299` = 29.9s.
- Resource-level / RD-level options take precedence over node-connection options.
