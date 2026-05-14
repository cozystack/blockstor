# Tier 3 — drbd-utils contract tests

Tier 3 of the test strategy. Exec real `drbdmeta` / `drbdadm` binaries inside a thin Alpine container against loopback files, and assert exit codes / stderr substrings / dump-md fields. Catches the Bug-81 class: unit tests pass because `FakeExec` accepts any argv, but real drbd-utils rejects the flag combo. See `docs/test-strategy.md` "Tier 3" section.

## Build tag

All Tier 3 files use `//go:build contract`. Plain `go test ./tests/contract/...` runs the pre-existing oracle/replay/normalize tests; `go test -tags=contract ./tests/contract/...` adds the drbd-utils contract tests.

## Run locally

Requires Docker. The harness builds the image (`blockstor-drbd-contract:local`, ~14 MB) lazily on first test via `sync.Once`:

```sh
go test -tags=contract -count=1 -v ./tests/contract/...
```

## Linux-only

The harness calls `t.Skip` on non-Linux platforms: Docker Desktop on macOS converts the `-v <file>:/dev/loop0` bind-mount into a directory, which drbdmeta rejects with "Is a directory". Tests pass `SKIP` on macOS and the CI job pinned to `ubuntu-latest` runs them for real.

## Files

- `Dockerfile` — Alpine 3.19 + `apk add drbd-utils`. Build target tag is fixed (`blockstor-drbd-contract:local`).
- `harness.go` — `EnsureImage`, `RunDrbdmeta`, `RunDrbdadm`, `RunDocker`, plus the per-test 64 MiB loopback allocator.
- `drbdmeta_test.go` — five tests pinning create-md, set-gi (Bug 81), dump-md, and `drbdadm dump` against a Builder-rendered .res.

## CI

`.github/workflows/contract.yml` builds the image once and runs `go test -tags=contract -count=1 ./tests/contract/...` on every PR.
