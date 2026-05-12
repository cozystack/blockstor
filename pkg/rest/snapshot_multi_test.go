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
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestSnapshotCreateMultiFansOut pins the upstream LINSTOR
// `linstor snapshot create-multiple` contract: one POST against
// /v1/actions/snapshot/multi takes per-RD snapshots and returns one
// ApiCallRc per entry. The CLI uses this for consistency-group-
// style operator flows; the per-RD POST loop replaces the previous
// 501 stub.
func TestSnapshotCreateMultiFansOut(t *testing.T) {
	st := store.NewInMemory()
	ctx := context.Background()

	// Two RDs, one diskful replica each — the multi-create batch
	// will fan out one snapshot per RD.
	for _, rd := range []string{"pvc-a", "pvc-b"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
			t.Fatalf("seed rd %s: %v", rd, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "n1"}); err != nil {
			t.Fatalf("seed resource %s: %v", rd, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"snapshots":[
		{"resource_name":"pvc-a","name":"snap-a"},
		{"resource_name":"pvc-b","name":"snap-b"}
	]}`)

	resp := httpPost(t, base+"/v1/actions/snapshot/multi", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status: got %d, want 201", resp.StatusCode)
	}

	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json prefix", got)
	}

	var rcs []apiv1.APICallRc

	err := json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("ApiCallRc count: got %d, want 2", len(rcs))
	}

	for i, rc := range rcs {
		if rc.RetCode&maskInfo == 0 {
			t.Errorf("rc[%d] not info-marked: %#v", i, rc)
		}
	}

	// Per-RD store side-effect: both snapshots ended up in the
	// store. Pin both since the handler must hit both entries.
	for _, rd := range []string{"pvc-a", "pvc-b"} {
		snaps, sErr := st.Snapshots().ListByDefinition(ctx, rd)
		if sErr != nil {
			t.Fatalf("list snapshots %s: %v", rd, sErr)
		}

		if len(snaps) != 1 {
			t.Errorf("snapshots for %s: got %d, want 1", rd, len(snaps))
		}
	}
}

// TestSnapshotCreateMultiPartialFailure pins the best-effort semantic:
// an invalid entry returns an error rc in its slot but doesn't abort
// the valid sibling. Matches upstream's per-action ApiCallRc
// accumulation.
func TestSnapshotCreateMultiPartialFailure(t *testing.T) {
	st := store.NewInMemory()
	ctx := context.Background()

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-ok"})
	if err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-ok", NodeName: "n1"})
	if err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Entry 1 has empty name (invalid); entry 2 is good.
	body := []byte(`{"snapshots":[
		{"resource_name":"pvc-ok","name":""},
		{"resource_name":"pvc-ok","name":"snap-ok"}
	]}`)

	resp := httpPost(t, base+"/v1/actions/snapshot/multi", body)
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc

	err = json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("ApiCallRc count: got %d, want 2", len(rcs))
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("rc[0] should be error-marked; got %#v", rcs[0])
	}

	if rcs[1].RetCode&maskInfo == 0 {
		t.Errorf("rc[1] should be info-marked; got %#v", rcs[1])
	}
}
