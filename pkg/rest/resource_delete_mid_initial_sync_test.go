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
	"errors"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/storetest"
)

// seedInitialSyncVolume lays out a three-replica volume deleted a few
// seconds after creation: node-a is the source of the initial sync,
// node-b already reached UpToDate, node-c is still Inconsistent and its
// connection to node-b is Established (DRBD sees nothing to resync).
func seedInitialSyncVolume(t *testing.T, st store.Store, rd string) {
	t.Helper()

	seedU130RD(t, st, rd)

	inconsistent := diskfulReplica(rd, "node-c", drbdDiskStateInconsistent)
	inconsistent.Volumes[0].State.ReplicationStates = map[string]apiv1.ReplicationState{
		"node-b": {ReplicationState: "Established"},
	}

	for _, res := range []*apiv1.Resource{
		diskfulReplica(rd, "node-a", drbdDiskStateUpToDate),
		diskfulReplica(rd, "node-b", drbdDiskStateUpToDate),
		inconsistent,
	} {
		if err := st.Resources().Create(t.Context(), res); err != nil {
			t.Fatalf("seed %s: %v", res.NodeName, err)
		}
	}
}

func deleteReplicaStatus(t *testing.T, base, rd, node string) int {
	t.Helper()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/"+node)
	_ = resp.Body.Close()

	return resp.StatusCode
}

// linstor-csi's DeleteVolume, call for call: switch balancing off on the
// definition, then delete the replicas one by one in listing order. The
// last complete copy is the second delete, while the Inconsistent replica
// is still there; refusing it left the volume undeletable, with the
// driver retrying every five minutes for good. LINSTOR answers all three
// deletes, and so must blockstor.
func TestCSIDeleteVolumeDuringInitialSyncDeletesEveryReplica(t *testing.T) {
	st := store.NewInMemory()
	rd := "pvc-initial-sync"

	seedInitialSyncVolume(t, st, rd)

	base, stop := startServerWithStore(t, st)
	defer stop()

	put := httpPut(t, base+"/v1/resource-definitions/"+rd,
		[]byte(`{"override_props":{"BalanceResourcesEnabled":"False"}}`))
	_ = put.Body.Close()

	if put.StatusCode != http.StatusOK {
		t.Fatalf("modify: got %d, want 200", put.StatusCode)
	}

	for _, node := range []string{"node-a", "node-b", "node-c"} {
		if code := deleteReplicaStatus(t, base, rd, node); code != http.StatusOK {
			t.Fatalf("delete %s: got %d, want 200", node, code)
		}
	}

	left, err := st.Resources().ListByDefinition(t.Context(), rd)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("list: %v", err)
	}

	if len(left) != 0 {
		t.Errorf("%d replica(s) survived DeleteVolume: %+v", len(left), left)
	}
}

// The same replica delete on a volume that stays is still refused: the
// guard is lifted for a definition being deleted, not for every delete.
func TestSingleReplicaDeleteDuringInitialSyncStaysGuarded(t *testing.T) {
	st := store.NewInMemory()
	rd := "pvc-initial-sync-live"

	seedInitialSyncVolume(t, st, rd)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := deleteReplicaStatus(t, base, rd, "node-a"); code != http.StatusOK {
		t.Fatalf("delete node-a: got %d, want 200 (node-b survives)", code)
	}

	if code := deleteReplicaStatus(t, base, rd, "node-b"); code != http.StatusConflict {
		t.Fatalf("delete node-b: got %d, want 409 (last complete copy)", code)
	}
}

// A definition flagged DELETE (rd d marks it before walking the replicas)
// is being deleted as a whole too.
func TestReplicaDeleteOfDefinitionFlaggedDeleteIsNotGuarded(t *testing.T) {
	st := store.NewInMemory()
	rd := "pvc-initial-sync-rdd"

	seedInitialSyncVolume(t, st, rd)

	if err := st.Resources().Delete(t.Context(), rd, "node-a"); err != nil {
		t.Fatalf("drop node-a: %v", err)
	}

	if err := store.MarkDefinitionDeleting(t.Context(), st, rd); err != nil {
		t.Fatalf("mark: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := deleteReplicaStatus(t, base, rd, "node-b"); code != http.StatusOK {
		t.Fatalf("delete node-b: got %d, want 200", code)
	}
}

// rd d flags the definition DELETE before the first replica goes, so the
// RD reconciler keeps its hands off the tiebreaker for the whole walk.
func TestRDDeleteFlagsDefinitionBeforeItsReplicas(t *testing.T) {
	backend := store.NewInMemory()
	rd := "pvc-initial-sync-flag"

	seedInitialSyncVolume(t, backend, rd)

	rec := storetest.NewDeleteRecorder(backend)

	base, stop := startServerWithStore(t, rec)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rd d: got %d, want 200", resp.StatusCode)
	}

	deletes := rec.Deletes()
	if len(deletes) != 3 {
		t.Errorf("deletes = %+v, want all three replicas", deletes)
	}

	for _, d := range deletes {
		if !d.DefinitionFlagged {
			t.Errorf("replica on %s deleted before the definition was flagged DELETE", d.Node)
		}
	}
}

// U130 on a definition whose replicas are placed by hand: balancing is
// switched off, a diskless Primary runs on it, a second diskful replica is
// still syncing, and the original is deleted. The property alone does not
// make this a teardown; DeleteVolume only deletes once nothing is InUse.
func TestReplicaDeleteWithBalancingOffAndAPrimaryStaysGuarded(t *testing.T) {
	st := store.NewInMemory()
	rd := "pvc-hand-placed"

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  rd,
		Props: map[string]string{apiv1.PropBalanceResourcesEnabled: "False"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	inUse := true
	primary := disklessReplica(rd, "node-p", apiv1.ResourceFlagDiskless)
	primary.State.InUse = &inUse

	for _, res := range []*apiv1.Resource{
		diskfulReplica(rd, "node-a", drbdDiskStateUpToDate),
		diskfulReplica(rd, "node-b", drbdDiskStateInconsistent),
		primary,
	} {
		if err := st.Resources().Create(t.Context(), res); err != nil {
			t.Fatalf("seed %s: %v", res.NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := deleteReplicaStatus(t, base, rd, "node-a"); code != http.StatusConflict {
		t.Fatalf("delete node-a: got %d, want 409 (the Primary's last complete copy)", code)
	}
}

// The guard reads the definition to tell a teardown from a single delete.
// A read it cannot make refuses the delete instead of letting it through.
func TestReplicaDeleteRefusedWhenTheDefinitionCannotBeRead(t *testing.T) {
	backend := store.NewInMemory()
	rd := "pvc-unreadable"

	seedInitialSyncVolume(t, backend, rd)

	base, stop := startServerWithStore(t, storetest.FailingDefinitionGet{Store: backend})
	defer stop()

	if code := deleteReplicaStatus(t, base, rd, "node-a"); code == http.StatusOK {
		t.Fatal("delete went through although the definition could not be read")
	}

	if _, err := backend.Resources().Get(t.Context(), rd, "node-a"); err != nil {
		t.Errorf("node-a was deleted: %v", err)
	}
}
