//go:build integration

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package integration is the Tier 2 test suite scaffold. Tests in
// this package use the `harness` sub-package to drive an envtest
// stack with every reconciler wired, then exercise the LINSTOR REST
// surface via the upstream `linstor` CLI binary.
//
// Phase 0 ships exactly one test (TestSmokeNodeList) that proves
// envtest + manager + REST + CLI end-to-end. Phase 1 agents add
// per-group files (group_<a..l>_test.go).
package integration

import (
	"sort"
	"testing"

	"github.com/cozystack/blockstor/tests/integration/harness"
)

// TestSmokeNodeList is the canonical Phase 0 smoke. It validates
// every harness component in one round-trip:
//
//   - envtest + manager bootstrap (harness.StartStack)
//   - fixture seeding (harness.SeedThreeNodeCluster)
//   - REST server is reachable on the picked port
//   - the upstream `linstor` CLI can parse our /v1/nodes response
//   - the response contains exactly the three fixture nodes
//
// If this passes, Phase 1 group agents can extend the suite with
// confidence that the scaffold is functional.
func TestSmokeNodeList(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	out := cli.JSON(t, "node", "list")

	names := make([]string, 0, len(out))
	for _, row := range out {
		if n, ok := row["name"].(string); ok {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	want := []string{"worker-1", "worker-2", "worker-3"}
	if len(names) != len(want) {
		t.Fatalf("expected %d nodes, got %d (%v)", len(want), len(names), names)
	}

	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("node[%d] = %q, want %q (full list: %v)", i, names[i], want[i], names)
		}
	}
}
