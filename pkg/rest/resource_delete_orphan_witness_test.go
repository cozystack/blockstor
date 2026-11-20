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
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedOrphanWitnessRD seeds an RD with the given replicas.
func seedOrphanWitnessRD(t *testing.T, st store.Store, rd string, replicas []apiv1.Resource) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for i := range replicas {
		replicas[i].Name = rd
		if err := st.Resources().Create(ctx, &replicas[i]); err != nil {
			t.Fatalf("seed replica %s: %v", replicas[i].NodeName, err)
		}
	}
}

func deleteReplica(t *testing.T, base, rd, node string) {
	t.Helper()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/"+node)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete %s/%s: got %d, want 200", rd, node, resp.StatusCode)
	}
}

// undeletedFromView returns the nodes of rd's replicas that the
// resource view lists without the DELETE flag: the exact predicate
// linstor-csi applies before it deletes the resource definition.
func undeletedFromView(t *testing.T, base, rd string) []string {
	t.Helper()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	var view []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode view: %v", err)
	}

	var out []string

	for i := range view {
		if view[i].Name == rd && !slices.Contains(view[i].Flags, apiv1.ResourceFlagDelete) {
			out = append(out, view[i].NodeName)
		}
	}

	return out
}

// TestResourceDeleteLastDiskfulReapsWitness replays linstor-csi's
// DeleteVolume on a volume whose attach-time diskless replica was
// replaced by an auto witness while csi was deleting: the witness was
// stamped after csi listed the replicas, so csi deletes only the two
// diskful ones and then lists again. The witness must be gone (or
// flagged DELETE) by then, or csi skips the resource-definition
// delete for good.
func TestResourceDeleteLastDiskfulReapsWitness(t *testing.T) {
	st := store.NewInMemory()
	seedOrphanWitnessRD(t, st, "pvc-orphan", []apiv1.Resource{
		{NodeName: "n1"},
		{NodeName: "n2"},
		{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	deleteReplica(t, base, "pvc-orphan", "n1")

	// One diskful left: the witness decision stays with the RD
	// reconciler, the delete handler leaves it alone.
	if got := undeletedFromView(t, base, "pvc-orphan"); !slices.Equal(got, []string{"n2", "n3"}) {
		t.Fatalf("after first diskful delete: undeleted %v, want [n2 n3]", got)
	}

	deleteReplica(t, base, "pvc-orphan", "n2")

	if got := undeletedFromView(t, base, "pvc-orphan"); len(got) != 0 {
		t.Fatalf("after last diskful delete: undeleted %v, want none (linstor-csi would keep the RD)", got)
	}
}

// TestResourceDeleteLastDiskfulKeepsUserDiskless pins the scope of the
// reap: only auto-managed TIE_BREAKER witnesses go. A plain DISKLESS
// replica is something a user or csi created and deletes itself.
func TestResourceDeleteLastDiskfulKeepsUserDiskless(t *testing.T) {
	st := store.NewInMemory()
	seedOrphanWitnessRD(t, st, "pvc-client", []apiv1.Resource{
		{NodeName: "n1"},
		{NodeName: "n2", Flags: []string{apiv1.ResourceFlagDiskless}},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	deleteReplica(t, base, "pvc-client", "n1")

	if got := undeletedFromView(t, base, "pvc-client"); !slices.Equal(got, []string{"n2"}) {
		t.Fatalf("undeleted %v, want [n2]", got)
	}
}
