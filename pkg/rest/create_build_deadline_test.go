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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedReplicaCreatedAgo writes a built replica of rdName on node that
// was placed ago before now, the way an earlier call left it.
func seedReplicaCreatedAgo(t *testing.T, st store.Store, rdName, node string, ago time.Duration) {
	t.Helper()

	rsc := apiv1.Resource{
		Name:            rdName,
		NodeName:        node,
		Props:           map[string]string{"StorPoolName": "pool"},
		CreateTimestamp: time.Now().Add(-ago).UnixMilli(),
	}
	markReady(&rsc, []string{node})

	if err := st.Resources().Create(t.Context(), &rsc); err != nil {
		t.Fatalf("seed replica %s: %v", node, err)
	}
}

// TestSecondPlacementOfACreateSharesTheBuildBound reproduces the second
// of linstor-csi's two autoplace calls per CreateVolume: the replica on
// the preferred node was placed by the first call, and this one places
// the rest. Its build wait ends the deploy bound after the first
// replica was placed, not a whole bound after this call started: two
// whole bounds plus the ready wait ran past csi-provisioner's minute
// whenever the satellites were slow, and the cancelled create had
// linstor-csi delete the volume it was making.
func TestSecondPlacementOfACreateSharesTheBuildBound(t *testing.T) {
	const deployWait = 2 * time.Second

	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-second", "n1", "n2")
	seedReplicaCreatedAgo(t, st, "pvc-second", "n1", 1500*time.Millisecond)

	base := startDeployWaitServer(t, st, deployWait, 10*time.Second)

	rcs, elapsed := autoplace(t, base, "pvc-second", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	if elapsed >= 1500*time.Millisecond {
		t.Errorf("second placement waited %s for its build, want what is left of the create's %s bound (about 500ms)",
			elapsed, deployWait)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n2"] != maskWarn {
		t.Errorf("want one not-ready warning, for n2, got %v", w)
	}
}

// TestPlacementOnAnOldDefinitionGetsTheWholeBuildBound pins the other
// side: a replica placed more than one CreateVolume ago belongs to no
// create in flight, so a placement next to it waits the whole bound.
func TestPlacementOnAnOldDefinitionGetsTheWholeBuildBound(t *testing.T) {
	const deployWait = 500 * time.Millisecond

	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-old", "n1", "n2")
	seedReplicaCreatedAgo(t, st, "pvc-old", "n1", 2*createVolumeWindow)

	base := startDeployWaitServer(t, st, deployWait, 10*time.Second)

	_, elapsed := autoplace(t, base, "pvc-old", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	if elapsed < deployWait {
		t.Errorf("placement next to an old replica answered after %s, before the %s bound", elapsed, deployWait)
	}
}

// TestMakeAvailableRightAfterCreateGetsTheWholeBuildBound pins that the
// bound is shared only within a create: make-available is linstor-csi's
// ControllerPublishVolume, which has a minute of its own, and a pod
// attaching remotely right after its volume was created must still get
// a built replica back.
func TestMakeAvailableRightAfterCreateGetsTheWholeBuildBound(t *testing.T) {
	const deployWait = time.Second

	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-attach", "n1", "n2")
	seedReplicaCreatedAgo(t, st, "pvc-attach", "n1", 800*time.Millisecond)

	base := startDeployWaitServer(t, st, deployWait, 10*time.Second)

	body, err := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_, elapsed := postForRcs(t, base+"/v1/resource-definitions/pvc-attach/resources/n2/make-available", body, http.StatusOK)

	if elapsed < deployWait {
		t.Errorf("make-available answered after %s, before the %s bound", elapsed, deployWait)
	}
}
