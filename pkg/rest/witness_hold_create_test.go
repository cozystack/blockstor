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

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// holdObservingStore records, at every replica create, whether the
// definition holds the witness for a placement in progress.
type holdObservingStore struct {
	store.Store

	mu   sync.Mutex
	held []bool
}

func (s *holdObservingStore) Resources() store.ResourceStore {
	return holdObservingResources{ResourceStore: s.Store.Resources(), st: s}
}

type holdObservingResources struct {
	store.ResourceStore

	st *holdObservingStore
}

func (r holdObservingResources) Create(ctx context.Context, res *apiv1.Resource) error {
	rd, err := r.st.ResourceDefinitions().Get(ctx, res.Name)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	_, held := apiv1.PlacementInProgressUntil(rd.Annotations, time.Now())

	r.st.mu.Lock()
	r.st.held = append(r.st.held, held)
	r.st.mu.Unlock()

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

// `linstor r c n1 n2 n3 rd` writes its replicas one at a time just like
// a placement, and two replicas in the definition looks like the
// even-diskful tie a witness breaks: the tiebreaker reconciler would
// put one on n3, which n1 and n2 set up as a diskless peer before the
// third create takes it over as diskful. LINSTOR writes the whole
// request in one transaction. The handler holds the witness for as long
// as it writes more than one diskful replica, and a request for one
// diskful replica, or for diskless ones next to it, passes through no
// such shape and leaves the definition alone.
func TestResourceCreateHoldsTheWitnessWhileItWritesReplicas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		nodes []string
		flags [][]string
		held  bool
	}{
		{name: "three diskful", nodes: []string{"n1", "n2", "n3"}, flags: [][]string{nil, nil, nil}, held: true},
		{name: "one diskful", nodes: []string{"n1"}, flags: [][]string{nil}, held: false},
		{
			name:  "one diskful and a diskless one",
			nodes: []string{"n1", "n2"},
			flags: [][]string{nil, {apiv1.ResourceFlagDiskless}},
			held:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := store.NewInMemory()
			seedDefaultGroup(t, inner)

			for _, n := range []string{"n1", "n2", "n3"} {
				seedOnlineNodeWithPool(t, inner, n)
			}

			err := inner.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1", ResourceGroupName: "DfltRscGrp"})
			if err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			st := &holdObservingStore{Store: inner}

			base, stop := startServerWithStore(t, st)
			defer stop()

			envelopes := make([]apiv1.ResourceCreate, 0, len(tc.nodes))
			for i, n := range tc.nodes {
				res := apiv1.Resource{NodeName: n, Flags: tc.flags[i]}
				if len(tc.flags[i]) == 0 {
					res.Props = map[string]string{"StorPoolName": "zfs-thin"}
				}

				envelopes = append(envelopes, apiv1.ResourceCreate{Resource: res})
			}

			body, err := json.Marshal(envelopes)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources", body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("status %d, want 201", resp.StatusCode)
			}

			st.mu.Lock()
			held := append([]bool(nil), st.held...)
			st.mu.Unlock()

			if len(held) != len(tc.nodes) {
				t.Fatalf("observed %d creates, want %d", len(held), len(tc.nodes))
			}

			for i, h := range held {
				if h != tc.held {
					t.Errorf("create %d: witness held %v, want %v", i+1, h, tc.held)
				}
			}

			rd, err := inner.ResourceDefinitions().Get(t.Context(), "pvc-1")
			if err != nil {
				t.Fatalf("get definition: %v", err)
			}

			if v, ok := rd.Annotations[apiv1.PlacementInProgressUntilAnnotation]; ok {
				t.Errorf("request done, the definition still carries %s=%q", apiv1.PlacementInProgressUntilAnnotation, v)
			}
		})
	}
}
