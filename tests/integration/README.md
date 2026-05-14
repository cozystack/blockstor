# tests/integration — Tier 2

Tier 2 of the test strategy (`docs/test-strategy.md`). Boots an in-process apiserver+etcd via `envtest`, the full controller-runtime manager, the REST server, and a satellite mock, then drives the whole stack with the native `linstor` CLI plus in-process CSI gRPC.

## Layout

```
tests/integration/
├── README.md            (this file — read FIRST)
├── harness/
│   ├── envtest.go       envtest.Environment bootstrap, CRD loading
│   ├── manager.go       controller-runtime Manager + REST server wiring
│   ├── satellite.go     in-process satellite mock (FakeExec → Status writes)
│   ├── linstor.go       exec wrapper for the linstor CLI, JSON parsing,
│   │                    python-traceback guard
│   ├── csi.go           in-process CSI gRPC server + client
│   ├── fixtures.go      pre-seeded Nodes/StoragePools/RG
│   ├── asserts.go       Eventually, MustList, MustGet, WaitForDRBDState
│   └── concurrent.go    RunParallel goroutine-storm helper
├── smoke_test.go        canonical first test — proves harness works
└── group_<a..l>_test.go one file per group (Phase 1 agents)
```

## Build tag

All files in this directory use `//go:build integration`. They do NOT run with a plain `go test ./...`; CI uses `-tags=integration`.

## Running locally

```
# One-time: install envtest binaries
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
export KUBEBUILDER_ASSETS="$(setup-envtest use --print path 1.34.x)"

# One-time: install upstream linstor CLI
apt-get install -y linstor-client python3-linstor   # Debian/Ubuntu

# Run the suite
go test -tags=integration -count=1 ./tests/integration/...

# Run one group
go test -tags=integration -count=1 ./tests/integration/... -run '^TestGroupA'
```

## For agents

Read `docs/agent-playbook.md` and `docs/test-strategy.md` (your group's table) before writing anything. The harness exists so you do NOT touch it — if you think you need a harness change, stop and ask the launcher.
