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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 144 — P0 regression of Bug 98.
//
// `POST /v1/resource-definitions/{bogus-rd}/resources` with a body that
// only carries `nodeName` happily persists a Resource CRD even though
// no parent RD exists. The satellite reconciler then allocates a DRBD
// minor + port for a row that has no parent — effectively a port leak
// under any tooling that exercises this path (csi retries, operator
// dry-runs, third-party clients).
//
// Bug 98 fixed `s rollback` on a missing RD; the regression is in a
// neighbouring handler (`handleResourceCreate` / `createResources`)
// that never probed the RD store before staging the Resource. Same
// gate shape as Bug 94 (unknown node on r c), Bug 118 (unknown pool
// on r c), Bug 133 (unknown node on node-connection), Bug 134
// (unknown RG on rd c), Bug 143 (unknown RD on r lp): look up the
// parent first, return 404 + LINSTOR envelope, refuse BEFORE any
// downstream allocation.

// TestBug144ResourceCreateOnBogusRDReturns404 is the primary repro:
// POST a Resource against an RD that doesn't exist. The handler must
// 404 with a LINSTOR envelope naming the missing RD, and the Resource
// store must NOT carry a (rd, node) entry afterwards — no DRBD minor /
// port can be allocated on a phantom row.
func TestBug144ResourceCreateOnBogusRDReturns404(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed the target node; the RD is intentionally absent.
	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bogus-rd/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 144: bogus RD must be refused before staging Resource). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-rd") {
		t.Errorf("envelope missing offending RD name: %s", got)
	}

	// LINSTOR-shaped envelope, not a bare error object.
	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// No phantom Resource CRD persisted — no DRBD minor / port leak.
	_, err := st.Resources().Get(ctx, "bogus-rd", "n1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("phantom Resource bogus-rd.n1 persisted despite 404: err=%v", err)
	}

	// And the RD itself must still be absent (we didn't accidentally
	// auto-create one as part of the resource path).
	_, err = st.ResourceDefinitions().Get(ctx, "bogus-rd")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("RD bogus-rd accidentally materialized: err=%v", err)
	}
}

// TestBug144ResourceCreateOnValidRDStillWorks is the happy-path guard:
// the new RD-existence gate must not regress the normal `r c` flow.
func TestBug144ResourceCreateOnValidRDStillWorks(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "ok-rd"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/ok-rd/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (happy-path). Body: %s",
			resp.StatusCode, got)
	}

	res, err := st.Resources().Get(ctx, "ok-rd", "n1")
	if err != nil {
		t.Fatalf("Resource ok-rd.n1 not persisted: %v", err)
	}

	if res.Name != "ok-rd" || res.NodeName != "n1" {
		t.Errorf("Resource shape: got %+v, want name=ok-rd node=n1", res)
	}
}

// TestBug144AutoplaceOnBogusRDReturns404 is the symmetric autoplace
// gate. `POST /v1/resource-definitions/{bogus-rd}/autoplace` must
// refuse with the same 404 + LINSTOR envelope before any placer /
// minor / port allocation runs. (Today's `handleAutoplace` already
// probes the RD via getRDWithCacheRetry; this test pins that contract
// so a future refactor can't silently re-introduce the leak.)
func TestBug144AutoplaceOnBogusRDReturns404(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bogus-rd/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 144 twin: bogus RD must be refused on autoplace). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-rd") {
		t.Errorf("envelope missing offending RD name: %s", got)
	}

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// And no phantom Resource on any node afterwards.
	all, err := st.Resources().ListByDefinition(ctx, "bogus-rd")
	if err != nil {
		t.Fatalf("ListByDefinition: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("phantom Resources persisted despite 404: %+v", all)
	}
}
