---
title: "Testing"
linkTitle: "Testing"
weight: 20
description: "The test tiers and how to run each of them."
---

Each tier answers a different question, and a change is expected to come with tests at the lowest tier that can catch the bug.

| Tier | What it covers | Where |
|---|---|---|
| L1 unit | Logic, run on every commit | `*_test.go` next to the code |
| L2 contract, golden | Recorded `golinstor` responses replayed against the server, byte diff | `tests/contract/` |
| L3 contract, oracle | The same requests to a real LINSTOR and to blockstor, JSON diff | `tests/contract/` |
| Integration | Controllers and REST against a real Kubernetes API server (envtest), satellites faked | `tests/integration/` |
| L4 integration on DRBD | `make smoke` on the stand | `tests/smoke*.sh` |
| L5 e2e | csi-sanity and piraeus-operator e2e on the stand | `tests/e2e/` |
| L6 operator CLI | The real CLI, through REST and the satellite, down to the DRBD kernel state | `tests/e2e/cli-matrix/` |
| L7 replay | Operator workflows replayed, and the CLI parity diff against upstream | `tests/operator-harness/` |

## Unit tests and lint

```sh
go test ./...
make lint
```

Tests that need envtest skip themselves when `KUBEBUILDER_ASSETS` is not set. `make test` downloads the envtest binaries and runs the suite with them. `make lint` must be clean before a push.

## Integration tests

The integration suite boots an in-process Kubernetes API server and etcd with envtest, applies the CRDs, and runs every reconciler plus the REST server against it. Satellites are replaced with fakes that write Status the way a real satellite would. The tests carry the `integration` build tag:

```sh
make setup-envtest
go test -tags integration ./tests/integration/...
```

## e2e on the stand

```sh
make e2e-list
make e2e NAME=alice SCENARIO=tiebreaker
```

Each scenario is a shell script in `tests/e2e/`. New scenarios register their cleanup with `register_strict_cleanup` from `tests/e2e/lib.sh`: it deletes the named definitions, waits for terminating satellite pods, and checks that the cluster is back to its baseline. A scenario that leaves the cluster dirty is turned into a failure, so the next one is not blamed for it.

## The operator CLI matrix

`tests/e2e/cli-matrix/` holds one script per operator-reported CLI bug. A cell passes only when the replica's Status, written by the satellite's observer, and a `drbdsetup status` probe on the node both reach the expected state. A `200 OK` from the API is not enough. See the [cell README](https://github.com/cozystack/blockstor/blob/main/tests/e2e/cli-matrix/README.md).

## Replay and parity

```sh
# One replay workflow
BS_URL=http://127.0.0.1:3370 \
    tests/operator-harness/replay-runner.sh <cluster> tests/operator-harness/replay/pvc-lifecycle.yaml

# CLI parity against an upstream LINSTOR
BS_URL=http://127.0.0.1:3370 UP_URL=http://127.0.0.1:3371 \
    tests/operator-harness/cli-parity-refresh.sh /tmp/cli-parity
```

You set up the port-forwards yourself. The parity run fails on any difference from upstream that is not listed in [`docs/cli-parity-known-deltas.md`](https://github.com/cozystack/blockstor/blob/main/docs/cli-parity-known-deltas.md). Add a row there only for an intended difference; a regression gets fixed instead.
