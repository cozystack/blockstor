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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 262 (P2) — stand-caught on dev-kvaps. `linstor rd lp <new-rd>
// | grep quorum` reported `DrbdOptions/Resource/quorum off` on a
// freshly-created RD: Bug 152's RG-modify auto-quorum default never
// fired on RG CREATE, so RDs spawned from a fresh RG inherited the
// absent default → `quorum off`. Combined with the auto-tiebreaker
// reconciler stamping a witness on 2-diskful RDs, the cluster
// silently runs the 3-voter set with quorum off — defeating the
// purpose of the witness.
//
// The fix seeds `DrbdOptions/auto-quorum=majority` plus the
// companion `DrbdOptions/Resource/on-no-quorum=suspend-io` on every
// fresh RD if not explicitly set by the operator. We stamp on the
// RD layer (the universal write point: every RD path lands through
// handleRDCreate, including the rd-spawn-from-rg path) rather than
// only on the RG-create surface — that way an RD created against an
// older RG (no auto-quorum prop on the parent) still gets the
// invariant.

// TestBug262RDCreateSeedsAutoQuorumMajority pins the default-seed:
// `POST /v1/resource-definitions` with a bare body must persist an
// RD whose Props contain `DrbdOptions/auto-quorum=majority`.
func TestBug262RDCreateSeedsAutoQuorumMajority(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "pvc-bug262"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (RD create); body=%s",
			resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-bug262")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got.Props["DrbdOptions/auto-quorum"] != "majority" {
		t.Errorf("Bug 262: DrbdOptions/auto-quorum: got %q, want %q (default seed missing); props=%v",
			got.Props["DrbdOptions/auto-quorum"], "majority", got.Props)
	}

	if got.Props["DrbdOptions/Resource/on-no-quorum"] != "suspend-io" {
		t.Errorf("Bug 262: DrbdOptions/Resource/on-no-quorum: got %q, want %q (companion seed missing); props=%v",
			got.Props["DrbdOptions/Resource/on-no-quorum"], "suspend-io", got.Props)
	}
}

// TestBug262RDCreateHonoursExplicitAutoQuorum pins the "operator
// wins" rule: if the wire body explicitly sets
// `DrbdOptions/auto-quorum`, the seed must NOT clobber it. The
// operator's policy choice (e.g. `disabled` for manual control,
// `io-error` for fail-fast) is load-bearing — silently overriding
// it would re-introduce the pre-Bug-262 silent-quorum surface from
// the other direction.
func TestBug262RDCreateHonoursExplicitAutoQuorum(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name: "pvc-bug262-explicit",
			Props: map[string]string{
				"DrbdOptions/auto-quorum": "disabled",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-bug262-explicit")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got.Props["DrbdOptions/auto-quorum"] != "disabled" {
		t.Errorf("operator's explicit auto-quorum=disabled was clobbered: got %q",
			got.Props["DrbdOptions/auto-quorum"])
	}
}

// TestBug262RDCreateHonoursExplicitOnNoQuorum: same for the
// companion `on-no-quorum` knob.
func TestBug262RDCreateHonoursExplicitOnNoQuorum(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name: "pvc-bug262-onnq",
			Props: map[string]string{
				"DrbdOptions/Resource/on-no-quorum": "io-error",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201; body=%s", resp.StatusCode, readAll(t, resp))
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-bug262-onnq")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got.Props["DrbdOptions/Resource/on-no-quorum"] != "io-error" {
		t.Errorf("operator's explicit on-no-quorum=io-error was clobbered: got %q",
			got.Props["DrbdOptions/Resource/on-no-quorum"])
	}
}

// readAll is a tiny helper for surfacing body content in failure
// messages without pulling in a per-test "decode response" boilerplate.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()

	var buf [1024]byte

	n, _ := resp.Body.Read(buf[:])

	return string(buf[:n])
}
