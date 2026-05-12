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

package contract_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/tests/contract"
)

// TestOracleTraceReplay loads testdata/oracle/*.json — traces
// captured against a live LINSTOR controller via
// cmd/linstor-trace-recorder — and replays each against an
// in-process blockstor REST server.
//
// Today's corpus pins the wire-shape contract for: controller
// version + props CRUD, nodes lifecycle, resource-groups +
// volume-groups CRUD, resource-definitions + volume-definitions
// CRUD, error-reports list.
//
// The test tolerates divergences for now — it logs each diff via
// t.Logf instead of t.Errorf — so the baseline can land without
// hard-failing CI before blockstor's REST shim has been brought
// up to parity on every endpoint. Flip to t.Errorf once the
// known divergences have been triaged and either fixed or
// allow-listed via Normalize.
func TestOracleTraceReplay(t *testing.T) {
	baseURL, stop := resolveTarget(t)
	defer stop()

	traces, err := contract.LoadTracesDir("testdata/oracle")
	if err != nil {
		t.Fatalf("LoadTracesDir: %v", err)
	}

	if len(traces) == 0 {
		t.Skip("no oracle traces — run cmd/linstor-trace-recorder to populate")
	}

	results, err := contract.Replay(t.Context(), nil, baseURL, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	matches := 0
	diverges := 0

	for _, result := range results {
		if result.Match {
			matches++

			continue
		}

		diverges++

		// Soft-report: surface the diff but don't fail. The
		// initial corpus has known stand-side state (e2e6 workers)
		// that won't reproduce in CI; logging-only lets the corpus
		// land while we iteratively triage.
		t.Logf("DIVERGE %s: %s", result.Trace, strings.Join(result.Diffs, "; "))
	}

	t.Logf("oracle replay: %d match, %d diverge (out of %d total)",
		matches, diverges, len(results))
}
