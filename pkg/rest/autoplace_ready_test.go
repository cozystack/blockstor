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
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedReadyWaitDefinition creates a one-volume definition, and an
// ONLINE node with a pool for each of nodes: the shape linstor-csi's
// CreateVolume places into.
func seedReadyWaitDefinition(t *testing.T, st store.Store, rdName string, nodes ...string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, n := range nodes {
		seedReadyWaitNode(t, st, n, apiv1.NodeTypeOnline)
	}
}

// seedReadyWaitNode creates a satellite node in the given connection
// state with a pool on it.
func seedReadyWaitNode(t *testing.T, st store.Store, node, status string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:             node,
		Type:             apiv1.NodeTypeSatellite,
		ConnectionStatus: status,
	}); err != nil {
		t.Fatalf("seed node %s: %v", node, err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    100 << 20,
		TotalCapacity:   100 << 20,
	}); err != nil {
		t.Fatalf("seed pool %s: %v", node, err)
	}
}

// seedReplica writes a replica of rdName on node, ready or not.
func seedReplica(t *testing.T, st store.Store, rdName, node string, ready bool, peers []string, flags ...string) {
	t.Helper()

	rsc := apiv1.Resource{
		Name:     rdName,
		NodeName: node,
		Props:    map[string]string{"StorPoolName": "pool"},
		Flags:    flags,
	}

	if ready {
		markReady(&rsc, peers)
	}

	if err := st.Resources().Create(t.Context(), &rsc); err != nil {
		t.Fatalf("seed replica %s: %v", node, err)
	}
}

// markReady gives rsc what a satellite reports once its DRBD resource
// is up: a device path and UpToDate data for volume 0, and a live
// connection to each of peers.
func markReady(rsc *apiv1.Resource, peers []string) {
	diskState := diskStateUpToDate
	devicePath := "/dev/drbd1000"

	if isDisklessReplica(rsc) {
		diskState = "Diskless"
		devicePath = ""
	}

	rsc.Volumes = []apiv1.Volume{{
		VolumeNumber: 0,
		DevicePath:   devicePath,
		State:        apiv1.VolumeState{DiskState: diskState},
	}}

	conns := map[string]apiv1.DrbdConnection{}

	for _, p := range peers {
		if p != rsc.NodeName {
			conns[p] = apiv1.DrbdConnection{Connected: true, Message: "Connected"}
		}
	}

	rsc.LayerObject = &apiv1.ResourceLayer{
		Type: apiv1.LayerKindDRBD,
		Drbd: &apiv1.DrbdResourceLayer{Connections: conns},
	}
}

// startReadyWaitServer starts a REST server that waits up to wait for
// deployed replicas, polling the store every few milliseconds.
func startReadyWaitServer(t *testing.T, st store.Store, wait time.Duration) string {
	t.Helper()

	shortenReadyPoll(t)

	base, stop := startServerCustom(t, &Server{
		Addr:              pickFreeAddr(t),
		Store:             st,
		Client:            newFakeRESTClient(t),
		Namespace:         testRESTNamespace,
		ResourceReadyWait: wait,
	})
	t.Cleanup(stop)

	return base
}

func shortenReadyPoll(t *testing.T) {
	t.Helper()

	prev := resourceReadyPollInterval
	resourceReadyPollInterval = 10 * time.Millisecond

	t.Cleanup(func() { resourceReadyPollInterval = prev })
}

// satelliteStandIn plays the satellites: once want replicas of rdName
// exist, it waits, as bringing a replica up takes, and then reports
// the replica on each of nodes ready and connected to every other
// replica. When
// pathFirst is set, it reports the device paths at once and the data
// and connections only after the delay, the order in which a blockstor
// satellite reports them.
func satelliteStandIn(ctx context.Context, st store.Store, rdName string, want int, nodes []string, pathFirst bool) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- runSatelliteStandIn(ctx, st, rdName, want, nodes, pathFirst)
	}()

	return done
}

func runSatelliteStandIn(ctx context.Context, st store.Store, rdName string, want int, nodes []string, pathFirst bool) error {
	var replicas []apiv1.Resource

	for {
		var err error

		replicas, err = st.Resources().ListByDefinition(ctx, rdName)
		if err != nil {
			return errors.Wrap(err, "list replicas")
		}

		if len(replicas) >= want {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "wait for replicas")
		case <-time.After(5 * time.Millisecond):
		}
	}

	if pathFirst {
		for i := range replicas {
			replicas[i].Volumes = []apiv1.Volume{{VolumeNumber: 0, DevicePath: fmt.Sprintf("/dev/drbd%d", 1000+i)}}

			if err := st.Resources().Update(ctx, &replicas[i]); err != nil {
				return errors.Wrap(err, "report device path")
			}
		}
	}

	time.Sleep(300 * time.Millisecond)

	all := make([]string, 0, len(replicas))
	for i := range replicas {
		all = append(all, replicas[i].NodeName)
	}

	for i := range replicas {
		if !slices.Contains(nodes, replicas[i].NodeName) {
			continue
		}

		live, err := st.Resources().Get(ctx, rdName, replicas[i].NodeName)
		if err != nil {
			return errors.Wrap(err, "get replica")
		}

		markReady(&live, all)

		if err := st.Resources().Update(ctx, &live); err != nil {
			return errors.Wrap(err, "report ready")
		}
	}

	return nil
}

// autoplace posts an autoplace and returns its answer and how long it
// took.
func autoplace(t *testing.T, base, rdName string, filter apiv1.AutoSelectFilter) ([]apiv1.APICallRc, time.Duration) {
	t.Helper()

	body, err := json.Marshal(apiv1.AutoPlaceRequest{SelectFilter: filter})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return postForRcs(t, base+"/v1/resource-definitions/"+rdName+"/autoplace", body, http.StatusOK)
}

func postForRcs(t *testing.T, url string, body []byte, wantStatus int) ([]apiv1.APICallRc, time.Duration) {
	t.Helper()

	start := time.Now()
	resp := httpPost(t, url, body)
	elapsed := time.Since(start)

	var rcs []apiv1.APICallRc

	decodeErr := json.NewDecoder(resp.Body).Decode(&rcs)
	_ = resp.Body.Close()

	if resp.StatusCode != wantStatus {
		t.Fatalf("status: got %d, want %d (%+v)", resp.StatusCode, wantStatus, rcs)
	}

	if decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	return rcs, elapsed
}

func warningsFor(rcs []apiv1.APICallRc) map[string]int64 {
	out := map[string]int64{}

	for i := range rcs {
		if rcs[i].RetCode&maskWarn != 0 {
			out[rcs[i].ObjRefs["Node"]] = rcs[i].RetCode
		}
	}

	return out
}

// requireReadyReplicas fails unless every replica on nodes reports what
// markReady stamps: the answer must not have come before that.
func requireReadyReplicas(t *testing.T, st store.Store, rdName string, nodes []string) {
	t.Helper()

	replicas, err := st.Resources().ListByDefinition(t.Context(), rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range replicas {
		rsc := &replicas[i]
		if !slices.Contains(nodes, rsc.NodeName) {
			continue
		}

		if len(rsc.Volumes) == 0 || rsc.Volumes[0].State.DiskState == "" ||
			rsc.LayerObject == nil || rsc.LayerObject.Drbd == nil || len(rsc.LayerObject.Drbd.Connections) == 0 {
			t.Errorf("answered before the replica on %s was ready: %+v", rsc.NodeName, rsc.Volumes)
		}
	}
}

// TestAutoplaceAnswersAfterReplicasAreReady pins the LINSTOR contract
// linstor-csi depends on: once the autoplace of CreateVolume returned,
// the replicas are up and a volume read on any of them carries a device
// path. Without it ControllerPublishVolume fails with "could not
// determine device path" and the attacher's exponential retry decides
// when the volume attaches.
func TestAutoplaceAnswersAfterReplicasAreReady(t *testing.T) {
	st := store.NewInMemory()
	nodes := []string{"n1", "n2"}

	seedReadyWaitDefinition(t, st, "pvc-ready", nodes...)

	base := startReadyWaitServer(t, st, 10*time.Second)
	standIn := satelliteStandIn(t.Context(), st, "pvc-ready", 2, nodes, false)

	rcs, _ := autoplace(t, base, "pvc-ready", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	requireReadyReplicas(t, st, "pvc-ready", nodes)

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for replicas that became ready: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestAutoplaceWaitsForConnectionsNotJustDevicePaths pins LINSTOR's
// readiness (ResourceState.isReady): UpToDate data and a connection to
// every diskful peer. A blockstor satellite reports the device path as
// soon as it applied the resource, before DRBD connected; answering on
// the path alone answers earlier than LINSTOR does.
func TestAutoplaceWaitsForConnectionsNotJustDevicePaths(t *testing.T) {
	st := store.NewInMemory()
	nodes := []string{"n1", "n2"}

	seedReadyWaitDefinition(t, st, "pvc-conn", nodes...)

	base := startReadyWaitServer(t, st, 10*time.Second)
	standIn := satelliteStandIn(t.Context(), st, "pvc-conn", 2, nodes, true)

	autoplace(t, base, "pvc-conn", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	requireReadyReplicas(t, st, "pvc-conn", nodes)

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestAutoplaceWarnsWhenReplicaIsNotReadyInTime pins the bound: a
// replica that never becomes ready does not hold the answer past the
// wait, the placement stands, and its node gets the warning LINSTOR
// gives for a resource that did not become ready.
func TestAutoplaceWarnsWhenReplicaIsNotReadyInTime(t *testing.T) {
	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-slow", "n1", "n2")

	base := startReadyWaitServer(t, st, time.Second)
	standIn := satelliteStandIn(t.Context(), st, "pvc-slow", 2, []string{"n1"}, false)

	rcs, elapsed := autoplace(t, base, "pvc-slow", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	if elapsed > 5*time.Second {
		t.Errorf("autoplace took %s, the wait is bounded at 1s", elapsed)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), "pvc-slow")
	if err != nil || len(replicas) != 2 {
		t.Fatalf("placement did not stand: %d replicas, err %v", len(replicas), err)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n2"] != maskWarn {
		t.Errorf("want one not-ready warning, for n2, got %v", w)
	}

	<-standIn
}

// TestAutoplaceThatPlacesNothingDoesNotWait pins LINSTOR's "already
// deployed, skipping": an autoplace whose target the definition
// already meets deploys nothing and waits for nothing, even when an
// existing replica is degraded. Waiting on it held every CreateVolume
// retry on such a volume for the whole bound.
func TestAutoplaceThatPlacesNothingDoesNotWait(t *testing.T) {
	st := store.NewInMemory()
	nodes := []string{"n1", "n2"}

	seedReadyWaitDefinition(t, st, "pvc-done", nodes...)
	seedReplica(t, st, "pvc-done", "n1", true, nodes)
	seedReplica(t, st, "pvc-done", "n2", false, nil)

	base := startReadyWaitServer(t, st, 3*time.Second)

	rcs, elapsed := autoplace(t, base, "pvc-done", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	if elapsed > time.Second {
		t.Errorf("autoplace that placed nothing took %s", elapsed)
	}

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("warnings for replicas this call did not create: %v", w)
	}
}

// TestAutoplaceWaitsOnlyForReplicasItCreated pins that the wait covers
// the replicas the call deployed and not the ones already there: a
// degraded existing replica neither holds the answer nor earns a
// warning.
func TestAutoplaceWaitsOnlyForReplicasItCreated(t *testing.T) {
	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-plus", "n1", "n2", "n3")
	seedReplica(t, st, "pvc-plus", "n1", true, []string{"n1", "n2"})
	seedReplica(t, st, "pvc-plus", "n2", false, nil)

	base := startReadyWaitServer(t, st, 3*time.Second)
	standIn := satelliteStandIn(t.Context(), st, "pvc-plus", 3, []string{"n3"}, false)

	rcs, elapsed := autoplace(t, base, "pvc-plus", apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "pool"})

	if elapsed > 2*time.Second {
		t.Errorf("autoplace waited %s, on a replica it did not create", elapsed)
	}

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestResourceCreateOnNodeAnswersAfterReplicaIsReady pins the same wait
// on the explicit create linstor-csi's Manual scheduler posts: LINSTOR
// answers it through the same deployResources.
func TestResourceCreateOnNodeAnswersAfterReplicaIsReady(t *testing.T) {
	st := store.NewInMemory()
	nodes := []string{"n1", "n2"}

	seedReadyWaitDefinition(t, st, "pvc-manual", nodes...)
	seedReplica(t, st, "pvc-manual", "n1", true, nil)

	base := startReadyWaitServer(t, st, 10*time.Second)
	standIn := satelliteStandIn(t.Context(), st, "pvc-manual", 2, []string{"n2"}, false)

	rcs, _ := postForRcs(t, base+"/v1/resource-definitions/pvc-manual/resources/n2", nil, http.StatusCreated)

	requireReadyReplicas(t, st, "pvc-manual", []string{"n2"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestWaitDeployedReplicasFollowsLinstorPredicate pins which deployed
// replicas LINSTOR's waitResourcesReady waits for, and how it answers
// for the others, with a deployed replica that never becomes ready.
func TestWaitDeployedReplicasFollowsLinstorPredicate(t *testing.T) {
	const wait = 300 * time.Millisecond

	cases := []struct {
		name string
		// seed builds the definition "rd"; the deployed replica is on n2.
		seed func(t *testing.T, st store.Store)
		// waits: the call holds for the whole bound.
		waits bool
		// warn is the RetCode expected for n2, 0 for none.
		warn int64
	}{
		{
			name: "single replica is not awaited",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n2")
				seedReplica(t, st, "rd", "n2", false, nil)
			},
		},
		{
			name: "definition without DRBD is not awaited",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n1", "n2")
				setLayerStack(t, st, "rd", apiv1.LayerKindStorage)
				seedReplica(t, st, "rd", "n1", true, nil)
				seedReplica(t, st, "rd", "n2", false, nil)
			},
		},
		{
			name: "INACTIVE replica is not awaited",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n1", "n2")
				seedReplica(t, st, "rd", "n1", true, nil)
				seedReplica(t, st, "rd", "n2", false, nil, apiv1.ResourceFlagInactive)
			},
		},
		{
			name: "replica on a node that is not ONLINE gets the not-connected warning at once",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n1")
				seedReadyWaitNode(t, st, "n2", apiv1.NodeTypeConnecting)
				seedReplica(t, st, "rd", "n1", true, nil)
				seedReplica(t, st, "rd", "n2", false, nil)
			},
			warn: warnNotConnected,
		},
		{
			name: "deployed diskless replica is awaited",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n1", "n2")
				seedReplica(t, st, "rd", "n1", true, nil)
				seedReplica(t, st, "rd", "n2", false, nil, apiv1.ResourceFlagDiskless)
			},
			waits: true,
			warn:  maskWarn,
		},
		{
			name: "diskful replica without its peer connection is awaited",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedReadyWaitDefinition(t, st, "rd", "n1", "n2")
				seedReplica(t, st, "rd", "n1", true, nil)
				seedReplica(t, st, "rd", "n2", true, nil)
			},
			waits: true,
			warn:  maskWarn,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shortenReadyPoll(t)

			st := store.NewInMemory()
			tc.seed(t, st)

			srv := &Server{Store: st, ResourceReadyWait: wait}

			start := time.Now()
			rcs := srv.waitDeployedReplicasReady(t.Context(), "rd", []string{"n2"})
			elapsed := time.Since(start)

			if tc.waits && elapsed < wait {
				t.Errorf("answered after %s, before the %s bound", elapsed, wait)
			}

			if !tc.waits && elapsed >= wait {
				t.Errorf("waited %s for a replica LINSTOR does not wait for", elapsed)
			}

			got := warningsFor(rcs)["n2"]
			if got != tc.warn {
				t.Errorf("warning for n2: got %#x, want %#x (%+v)", got, tc.warn, rcs)
			}
		})
	}
}

// TestWaitDeployedDisklessReadyThroughUpToDatePeer pins how a diskless
// replica becomes ready: connected to every diskful peer, one of which
// holds UpToDate data.
func TestWaitDeployedDisklessReadyThroughUpToDatePeer(t *testing.T) {
	shortenReadyPoll(t)

	st := store.NewInMemory()
	seedReadyWaitDefinition(t, st, "rd", "n1", "n2")
	seedReplica(t, st, "rd", "n1", true, []string{"n1", "n2"})
	seedReplica(t, st, "rd", "n2", true, []string{"n1", "n2"}, apiv1.ResourceFlagDiskless)

	srv := &Server{Store: st, ResourceReadyWait: time.Second}

	start := time.Now()
	rcs := srv.waitDeployedReplicasReady(t.Context(), "rd", []string{"n2"})

	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Errorf("waited %s for a diskless replica that was ready", elapsed)
	}

	if len(rcs) != 0 {
		t.Errorf("unexpected answer entries: %+v", rcs)
	}
}

func setLayerStack(t *testing.T, st store.Store, rdName string, stack ...string) {
	t.Helper()

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), rdName, func(rd *apiv1.ResourceDefinition) error {
		rd.LayerStack = stack

		return nil
	})
	if err != nil {
		t.Fatalf("set layer stack: %v", err)
	}
}
