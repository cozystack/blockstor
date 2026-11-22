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

func deleteReplica(t *testing.T, base, rd, node string) int {
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
		if code := deleteReplica(t, base, rd, node); code != http.StatusOK {
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

	if code := deleteReplica(t, base, rd, "node-a"); code != http.StatusOK {
		t.Fatalf("delete node-a: got %d, want 200 (node-b survives)", code)
	}

	if code := deleteReplica(t, base, rd, "node-b"); code != http.StatusConflict {
		t.Fatalf("delete node-b: got %d, want 409 (last complete copy)", code)
	}
}

