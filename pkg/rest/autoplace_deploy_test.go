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
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// startDeployWaitServer starts a REST server that waits up to deploy
// for the satellites to build the deployed replicas and then up to
// ready for them to become ready.
func startDeployWaitServer(t *testing.T, st store.Store, deploy, ready time.Duration) string {
	t.Helper()

	shortenReadyPoll(t)

	base, stop := startServerCustom(t, &Server{
		Addr:               pickFreeAddr(t),
		Store:              st,
		Client:             newFakeRESTClient(t),
		Namespace:          testRESTNamespace,
		ResourceReadyWait:  ready,
		ResourceDeployWait: deploy,
	})
	t.Cleanup(stop)

	return base
}

// deviceStandIn plays a satellite that builds a replica without DRBD:
// once want replicas of rdName exist, it waits delay and then reports a
// device path, and nothing else, for the replica on each of nodes.
func deviceStandIn(ctx context.Context, st store.Store, rdName string, want int, nodes []string, delay time.Duration) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- runDeviceStandIn(ctx, st, rdName, want, nodes, delay)
	}()

	return done
}

func runDeviceStandIn(ctx context.Context, st store.Store, rdName string, want int, nodes []string, delay time.Duration) error {
	for {
		replicas, err := st.Resources().ListByDefinition(ctx, rdName)
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

	time.Sleep(delay)

	for _, node := range nodes {
		live, err := st.Resources().Get(ctx, rdName, node)
		if err != nil {
			return errors.Wrap(err, "get replica")
		}

		live.Volumes = []apiv1.Volume{{VolumeNumber: 0, DevicePath: "/dev/zvol/pool/" + rdName + "_00000"}}

		if err := st.Resources().Update(ctx, &live); err != nil {
			return errors.Wrap(err, "report device path")
		}
	}

	return nil
}

// requireDevicePaths fails unless the replica on each of nodes carries
// a device path: what ControllerPublishVolume reads right after
// CreateVolume.
func requireDevicePaths(t *testing.T, st store.Store, rdName string, nodes []string) {
	t.Helper()

	replicas, err := st.Resources().ListByDefinition(t.Context(), rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range replicas {
		if !slices.Contains(nodes, replicas[i].NodeName) {
			continue
		}

		if len(replicas[i].Volumes) == 0 || replicas[i].Volumes[0].DevicePath == "" {
			t.Errorf("answered before the satellite built the replica on %s: %+v", replicas[i].NodeName, replicas[i].Volumes)
		}
	}
}

// TestAutoplaceOfLocalVolumeAnswersOnceItsDeviceExists pins LINSTOR's
// deployResources for a definition without DRBD: the answer comes after
// the satellite built the volume, although there is no DRBD resource to
// become ready. linstor-csi refills its cached resource view right
// after CreateVolume, so an earlier answer left ControllerPublishVolume
// reading a view without the device path until that cache expired.
func TestAutoplaceOfLocalVolumeAnswersOnceItsDeviceExists(t *testing.T) {
	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-local", "n1")
	setLayerStack(t, st, "pvc-local", apiv1.LayerKindStorage)

	base := startDeployWaitServer(t, st, 10*time.Second, 10*time.Second)
	standIn := deviceStandIn(t.Context(), st, "pvc-local", 1, []string{"n1"}, 300*time.Millisecond)

	rcs, _ := autoplace(t, base, "pvc-local", apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"})

	requireDevicePaths(t, st, "pvc-local", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a replica that was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestAutoplaceWaitsForTheBuildBeforeTheReadyBound pins the order of
// LINSTOR's two waits: its 15 second ready bound starts once the
// satellites answered the update, not at placement. When allocating
// and building the replicas takes longer than the ready bound, the
// answer still comes after the device paths exist.
func TestAutoplaceWaitsForTheBuildBeforeTheReadyBound(t *testing.T) {
	st := store.NewInMemory()
	nodes := []string{"n1", "n2"}

	seedReadyWaitDefinition(t, st, "pvc-slowbuild", nodes...)

	base := startDeployWaitServer(t, st, 10*time.Second, 100*time.Millisecond)
	standIn := satelliteStandIn(t.Context(), st, "pvc-slowbuild", 2, nodes, false)

	rcs, _ := autoplace(t, base, "pvc-slowbuild", apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"})

	requireDevicePaths(t, st, "pvc-slowbuild", nodes)

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for replicas that were built and became ready: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestResourceCreateOfLocalVolumeAnswersOnceItsDeviceExists pins the
// same wait on the explicit create linstor-csi posts for a volume
// placed on the node its consumer was scheduled to.
func TestResourceCreateOfLocalVolumeAnswersOnceItsDeviceExists(t *testing.T) {
	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-wffc", "n1")
	setLayerStack(t, st, "pvc-wffc", apiv1.LayerKindStorage)

	base := startDeployWaitServer(t, st, 10*time.Second, 10*time.Second)
	standIn := deviceStandIn(t.Context(), st, "pvc-wffc", 1, []string{"n1"}, 300*time.Millisecond)

	postForRcs(t, base+"/v1/resource-definitions/pvc-wffc/resources/n1", nil, 201)

	requireDevicePaths(t, st, "pvc-wffc", []string{"n1"})

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestAutoplaceWarnsWhenReplicaIsNotBuiltInTime pins the bound blockstor
// adds where LINSTOR relies on its satellites answering every update: a
// replica that is never built does not hold the answer past the deploy
// bound, the placement stands, and the node gets the not-ready warning.
func TestAutoplaceWarnsWhenReplicaIsNotBuiltInTime(t *testing.T) {
	st := store.NewInMemory()

	seedReadyWaitDefinition(t, st, "pvc-nobuild", "n1")
	setLayerStack(t, st, "pvc-nobuild", apiv1.LayerKindStorage)

	base := startDeployWaitServer(t, st, 500*time.Millisecond, 10*time.Second)

	rcs, elapsed := autoplace(t, base, "pvc-nobuild", apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"})

	if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("autoplace took %s, want the 500ms deploy bound", elapsed)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), "pvc-nobuild")
	if err != nil || len(replicas) != 1 {
		t.Fatalf("placement did not stand: %d replicas, err %v", len(replicas), err)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != maskWarn {
		t.Errorf("want one not-ready warning, for n1, got %v", w)
	}
}

// TestReplicaBuilt pins what counts as built: a device path on every
// defined volume, for a diskless replica as for a diskful one. A
// diskless replica's disk state alone is not enough: linstor-csi reads
// the path right after make-available returned.
func TestReplicaBuilt(t *testing.T) {
	volumes := map[int32]int64{0: 1 << 20, 1: 1 << 20}

	both := func(path, disk string) []apiv1.Volume {
		return []apiv1.Volume{
			{VolumeNumber: 0, DevicePath: path, State: apiv1.VolumeState{DiskState: disk}},
			{VolumeNumber: 1, DevicePath: path, State: apiv1.VolumeState{DiskState: disk}},
		}
	}

	cases := []struct {
		name  string
		rsc   apiv1.Resource
		built bool
	}{
		{name: "diskful, nothing reported", rsc: apiv1.Resource{}},
		{name: "diskful, one volume without a path", rsc: apiv1.Resource{Volumes: both("/dev/drbd1000", "")[:1]}},
		{name: "diskful, disk state but no path", rsc: apiv1.Resource{Volumes: both("", diskStateUpToDate)}},
		{name: "diskful, every volume has a path", rsc: apiv1.Resource{Volumes: both("/dev/drbd1000", "")}, built: true},
		{name: "diskless, nothing reported", rsc: apiv1.Resource{Flags: []string{apiv1.ResourceFlagDiskless}}},
		{
			name: "diskless, disk state but no path",
			rsc:  apiv1.Resource{Flags: []string{apiv1.ResourceFlagDiskless}, Volumes: both("", "Diskless")},
		},
		{
			name:  "diskless, every volume has its DRBD device",
			rsc:   apiv1.Resource{Flags: []string{apiv1.ResourceFlagDiskless}, Volumes: both("/dev/drbd1000", "Diskless")},
			built: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicaBuilt(&tc.rsc, volumes); got != tc.built {
				t.Errorf("replicaBuilt: got %v, want %v", got, tc.built)
			}
		})
	}
}
