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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRGAdjustAllReturns200WithApiCallRc pins Bug 223: the upstream
// LINSTOR endpoint `POST /v1/resource-groups/adjustall` re-runs autoplace
// across every child RD of every RG. The Go ServeMux matched the literal
// segment `adjustall` against the wildcard `{rg}` route registered
// below it, so the request landed on a per-RG handler that 404'd on
// the missing RG named "adjustall" — and in the wired-but-wrong-verb
// case the route inventory surfaced a misleading 405.
//
// The fix wires a dedicated static route that runs BEFORE the
// `{rg}/...` wildcards so the literal `adjustall` segment is matched
// first. The response shape mirrors upstream's `Flux<ApiCallRc>` /
// `mapToMonoResponse(...)`: one ApiCallRc entry per RD touched, with
// a MASK_INFO success ret_code.
func TestRGAdjustAllReturns200WithApiCallRc(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Two RGs × two RDs each — adjustall walks every RG and re-runs
	// the placer on every child RD, so the response envelope must
	// carry one entry per RD (= 4).
	for _, rg := range []string{"rg-1", "rg-2"} {
		if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         rg,
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1},
		}); err != nil {
			t.Fatalf("seed rg %q: %v", rg, err)
		}
	}

	rds := map[string]string{
		"pvc-a": "rg-1",
		"pvc-b": "rg-1",
		"pvc-c": "rg-2",
		"pvc-d": "rg-2",
	}

	for name, rg := range rds {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              name,
			ResourceGroupName: rg,
		}); err != nil {
			t.Fatalf("seed rd %q: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/adjustall", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 "+
			"(Bug 223: static `adjustall` route must be registered "+
			"BEFORE the `{rg}/...` wildcard)", resp.StatusCode)
	}

	var reply []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}

	if len(reply) != len(rds) {
		t.Fatalf("envelope count: got %d, want %d "+
			"(one APICallRc per child RD across every RG); reply=%+v",
			len(reply), len(rds), reply)
	}

	seen := map[string]struct{}{}

	for i := range reply {
		if reply[i].RetCode&maskInfo == 0 {
			t.Errorf("entry %d ret_code %#x: MASK_INFO bit not set", i, reply[i].RetCode)
		}

		if reply[i].Message == "" {
			t.Errorf("entry %d message: empty (python-linstor will hide the line)", i)
		}

		// Each RD that got adjusted must be named in exactly one
		// entry — otherwise the operator can't tell which RD had
		// what outcome.
		for name := range rds {
			if strings.Contains(reply[i].Message, name) {
				if _, dup := seen[name]; dup {
					t.Errorf("RD %q named in multiple entries", name)
				}

				seen[name] = struct{}{}
			}
		}
	}

	for name := range rds {
		if _, ok := seen[name]; !ok {
			t.Errorf("RD %q missing from reply envelope; reply=%+v", name, reply)
		}
	}
}

// TestRGAdjustAllNot405 is the explicit negative companion to
// TestRGAdjustAllReturns200WithApiCallRc: independently of whether the
// handler returns 200 (success path) the request MUST NOT 405. A 405
// is the visible operator symptom Bug 223 was filed against — Go
// ServeMux matched `adjustall` as a `{rg}` path value and surfaced
// the wired-but-wrong-verb shape via the with404Envelope wrapper.
//
// Splitting this assertion out lets a future refactor that changes
// the success envelope (or even returns 5xx because the placer broke)
// continue to flag the routing regression separately from the body
// shape change.
func TestRGAdjustAllNot405(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/adjustall", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusMethodNotAllowed {
		t.Fatalf("status 405: route registration order is wrong — "+
			"the static `adjustall` segment is being matched against "+
			"the `{rg}` wildcard (Bug 223). Got Allow=%q",
			resp.Header.Get("Allow"))
	}

	// Adjacent guard: the empty-store happy path on the new route
	// also must not 404. An empty `[]ApiCallRc` envelope is allowed
	// when there are no RDs to walk.
	if resp.StatusCode == http.StatusNotFound {
		t.Fatalf("status 404 on adjustall: the static route is "+
			"not wired (Bug 223). Header=%v", resp.Header)
	}
}
