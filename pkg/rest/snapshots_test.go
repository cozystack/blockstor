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

// TestSnapshotsViewEmpty: aggregate is empty until something gets created.
func TestSnapshotsViewEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestSnapshotsCreateRoundTrip: create through REST, see it via Get/View/List.
func TestSnapshotsCreateRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{
		Name:  "snap-1",
		Nodes: []string{"n1", "n2"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	// POST now returns an `[]ApiCallRc` envelope (upstream LINSTOR
	// convention; the Python CLI parses replies[0].ret_code). The
	// snapshot itself comes back via the View endpoint a few lines
	// below — assert its observable state, not the create echo.
	var rc []apiv1.APICallRc
	if jErr := json.NewDecoder(resp.Body).Decode(&rc); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if len(rc) == 0 || rc[0].RetCode <= 0 {
		t.Errorf("ApiCallRc envelope: got %+v", rc)
	}

	// View aggregate must contain it.
	viewResp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = viewResp.Body.Close() }()

	var view []apiv1.Snapshot
	if jErr := json.NewDecoder(viewResp.Body).Decode(&view); jErr != nil {
		t.Fatalf("decode view: %v", jErr)
	}

	if len(view) != 1 {
		t.Errorf("view len: got %d, want 1", len(view))
	}
}

// TestSnapshotsListMissingRD: 404 on missing RD.
func TestSnapshotsListMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost/snapshots")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSnapshotsListPerRD pins the per-RD list happy path: a GET on
// /v1/resource-definitions/{rd}/snapshots returns 200 + a bare JSON
// slice scoped to that RD only. linstor-csi calls this endpoint when
// reconciling a tenant's VolumeSnapshot lifecycle; if the handler ever
// regressed to leak snapshots from sibling RDs, two unrelated PVCs
// would see each other's snapshot metadata in the CSI driver — a
// silent multi-tenant boundary break.
func TestSnapshotsListPerRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD pvc-1: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-2"}); err != nil {
		t.Fatalf("seed RD pvc-2: %v", err)
	}

	for _, sn := range []apiv1.Snapshot{
		{Name: "s1", ResourceName: "pvc-1"},
		{Name: "s2", ResourceName: "pvc-1"},
		{Name: "s1", ResourceName: "pvc-2"},
	} {
		if err := st.Snapshots().Create(ctx, &sn); err != nil {
			t.Fatalf("seed snap: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (only pvc-1's snapshots); %+v", len(got), got)
	}

	for _, s := range got {
		if s.ResourceName != "pvc-1" {
			t.Errorf("leak: got snapshot %s/%s in pvc-1 list", s.ResourceName, s.Name)
		}
	}
}

// TestSnapshotsDeleteThenGet: delete then 404.
func TestSnapshotsDeleteThenGet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	delResp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete: got %d, want 200", delResp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/snapshots/s1")
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("post-delete: got %d, want 404", getResp.StatusCode)
	}
}

// TestSnapshotsViewFilters pins ?resources= and ?snapshots= on the
// cross-RD aggregate. linstor-csi's snapshot-existence poll arrives
// scoped to one RD + name; without filtering we'd return the whole
// cluster's snapshot list and force the client to scan.
func TestSnapshotsViewFilters(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sn := range []apiv1.Snapshot{
		{Name: "s1", ResourceName: "pvc-1"},
		{Name: "s2", ResourceName: "pvc-1"},
		{Name: "s1", ResourceName: "pvc-2"},
	} {
		if err := st.Snapshots().Create(ctx, &sn); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	cases := []struct {
		query string
		want  int
	}{
		{"resources=pvc-1", 2},
		{"snapshots=s1", 2},
		{"resources=pvc-1&snapshots=S2", 1},
	}

	for _, tc := range cases {
		resp := httpGet(t, base+"/v1/view/snapshots?"+tc.query)

		var got []apiv1.Snapshot

		err := json.NewDecoder(resp.Body).Decode(&got)
		_ = resp.Body.Close()

		if err != nil {
			t.Errorf("%s decode: %v", tc.query, err)

			continue
		}

		if len(got) != tc.want {
			t.Errorf("%s: got %d entries, want %d (%v)", tc.query, len(got), tc.want, got)
		}
	}
}

// TestSnapshotsCreateBadJSON: malformed body → 400.
func TestSnapshotsCreateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSnapshotsCreateMissingName: empty snap name → 400. linstor-csi
// derives the snap name from a VolumeSnapshot UID — a regression
// that allowed nameless snapshots would orphan rows that no later
// reconcile can address.
func TestSnapshotsCreateMissingName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Nodes: []string{"n1"}}) // Name omitted
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSnapshotsDeleteMissing: DELETE on a non-existent (RD, snap)
// → 404 from writeStoreError. Pins the idempotent-delete contract
// linstor-csi relies on during VolumeSnapshot deletion.
func TestSnapshotsDeleteMissing(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost/snapshots/s1")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
