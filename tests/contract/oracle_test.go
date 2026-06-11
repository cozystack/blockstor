// SPDX-License-Identifier: Apache-2.0

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

//go:build contract

// Oracle trace replay is part of the dedicated contract test tier
// (Dockerised drbd-utils + recorded LINSTOR traces). Gated behind
// the `contract` build tag so plain `go test ./...` in the unit job
// does NOT pick it up. Deliberate wire-shape supersets the shim
// emits over the recorded oracle (e.g. RD layer_data, Bug 349) are
// scrubbed per-field in Normalize and documented as rows in
// docs/cli-parity-known-deltas.md (#81).

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
// All known divergences have been triaged: either fixed in the
// REST shim (POST /v1/controller/properties 201, DELETE
// /v1/controller/properties/{key} route, VolumeDefinitions POST
// 200) or allow-listed in Normalize (stand-default property keys,
// piraeus-operator topology / last-applied props, ApiCallRc
// pipeline noise, RD layer_data — known-delta #81 in
// docs/cli-parity-known-deltas.md, Bug 349). Any new diff here
// means the contract regressed.
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

		t.Errorf("DIVERGE %s: %s", result.Trace, strings.Join(result.Diffs, "; "))
	}

	t.Logf("oracle replay: %d match, %d diverge (out of %d total)",
		matches, diverges, len(results))
}
