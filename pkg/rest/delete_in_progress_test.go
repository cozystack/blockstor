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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// heldReplicas accepts a replica delete and keeps the replica, the way the
// API server keeps a Resource whose satellite finalizer is still on it.
type heldReplicas struct{ store.ResourceStore }

func (heldReplicas) Delete(context.Context, string, string) error { return nil }

type heldReplicaStore struct{ store.Store }

func (h heldReplicaStore) Resources() store.ResourceStore { return heldReplicas{h.Store.Resources()} }

// A delete whose replica is still held when the answer is due says so: the
// replica is not removed early to make the answer look finished, and the
// client is told the deletion is still in progress.
func TestDeleteAnswersInProgressWhileTheSatelliteHoldsTheReplica(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/v1/resource-definitions/rd-held/resources/node-a",
		"/v1/resource-definitions/rd-held",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-held"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "rd-held", NodeName: "node-a"}); err != nil {
				t.Fatalf("seed replica: %v", err)
			}

			base, stop := startServerWithStore(t, heldReplicaStore{backend})
			defer stop()

			resp := httpDelete(t, base+path)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want 200", resp.StatusCode)
			}

			var answer []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
				t.Fatalf("decode: %v", err)
			}

			found := false

			for _, rc := range answer {
				if rc.RetCode == warnDeleteInProgress {
					found = true
				}
			}

			if !found {
				t.Errorf("answer %+v has no in-progress warning", answer)
			}

			if _, err := backend.Resources().Get(ctx, "rd-held", "node-a"); err != nil {
				t.Errorf("the held replica was removed: %v", err)
			}
		})
	}
}
