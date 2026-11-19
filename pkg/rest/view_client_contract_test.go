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
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// These tests read /v1/view/resources through golinstor, the client
// linstor-csi and linstor-scheduler-extender are built on, against the
// Kubernetes-backed store the apiserver runs in production, and then
// ask the question each of those clients asks of the answer.

const contractRD = "pvc-contract"

func contractNodeNames() []string { return []string{"node-a", "node-b", "node-c"} }

// seedThreeReplicaRD creates a one-volume RD with a diskful replica on
// each contract node and no satellite-reported state: the state a new
// volume is in between placement and the satellites building it.
func seedThreeReplicaRD(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	for _, n := range contractNodeNames() {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: contractRD}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, contractRD, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, n := range contractNodeNames() {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     contractRD,
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "data"},
		}); err != nil {
			t.Fatalf("seed resource on %s: %v", n, err)
		}
	}
}

func contractResourceView(t *testing.T, base string) []lapi.ResourceWithVolumes {
	t.Helper()

	view, err := newClient(t, base).Resources.GetResourceView(t.Context(), &lapi.ListOpts{
		Resource: []string{contractRD},
	})
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if len(view) != len(contractNodeNames()) {
		t.Fatalf("view: got %d replicas, want %d", len(view), len(contractNodeNames()))
	}

	return view
}

func drbdLayer(layer *lapi.ResourceLayer) *lapi.DrbdResource {
	if layer == nil {
		return nil
	}

	if layer.Type == devicelayerkind.Drbd {
		return layer.Drbd
	}

	for i := range layer.Children {
		if drbd := drbdLayer(&layer.Children[i]); drbd != nil {
			return drbd
		}
	}

	return nil
}

func hasDRBDLayer(layer *lapi.ResourceLayer) bool {
	if layer == nil {
		return false
	}

	if layer.Type == devicelayerkind.Drbd {
		return true
	}

	for i := range layer.Children {
		if hasDRBDLayer(&layer.Children[i]) {
			return true
		}
	}

	return false
}

// TestViewUnbuiltReplicasListEveryVolume: linstor-scheduler-extender's
// InspectVolume reads `volumes[0].state.disk_state` of every replica
// with a DRBD layer, relying on LINSTOR listing one volume per volume
// definition from the moment the replica exists. blockstor answered
// `volumes: []` until the satellite reported, and the extender
// panicked with index out of range on every filter call for a pod
// whose volume was still being built.
func TestViewUnbuiltReplicasListEveryVolume(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
	seedThreeReplicaRD(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, r := range contractResourceView(t, base) {
		if !hasDRBDLayer(r.LayerObject) {
			t.Fatalf("%s: no DRBD layer in %+v", r.NodeName, r.LayerObject)
		}

		if len(r.Volumes) != 1 {
			t.Fatalf("%s: got %d volumes, want 1 per volume definition (the extender indexes volumes[0])", r.NodeName, len(r.Volumes))
		}

		vol := r.Volumes[0]
		if vol.VolumeNumber != 0 || vol.StoragePoolName != "data" {
			t.Errorf("%s: volume = number %d pool %q, want 0 and the replica's pool", r.NodeName, vol.VolumeNumber, vol.StoragePoolName)
		}

		// Not built yet, so not UpToDate: the extender must skip
		// the node rather than offer it.
		if vol.State.DiskState != "" {
			t.Errorf("%s: disk_state = %q before any report, want empty", r.NodeName, vol.State.DiskState)
		}
	}
}

// reportHealthyReplicas writes onto every contract replica the Status
// the satellite observer records for a healthy, connected, UpToDate
// Secondary.
func reportHealthyReplicas(t *testing.T, cli ctrlclient.Client) {
	t.Helper()

	ctx := t.Context()
	score := int32(10102)
	mayPromote := true

	for _, n := range contractNodeNames() {
		var crd crdv1alpha1.Resource
		if err := cli.Get(ctx, ctrlclient.ObjectKey{Name: k8s.ResourceCRName(contractRD, n)}, &crd); err != nil {
			t.Fatalf("get replica on %s: %v", n, err)
		}

		crd.Status.Role = "Secondary"
		crd.Status.DrbdState = "UpToDate"
		crd.Status.PromotionScore = &score
		crd.Status.MayPromote = &mayPromote
		crd.Status.Volumes = []crdv1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0,
			StoragePool:  "data",
			DevicePath:   "/dev/drbd1000",
			DiskState:    "UpToDate",
		}}

		for _, peer := range contractNodeNames() {
			if peer != n {
				crd.Status.Connections = append(crd.Status.Connections, crdv1alpha1.ResourceConnectionStatus{
					PeerNodeName:     peer,
					Connected:        true,
					Message:          "Connected",
					ReplicationState: "Established",
				})
			}
		}

		if err := cli.Status().Update(ctx, &crd); err != nil {
			t.Fatalf("report replica on %s: %v", n, err)
		}
	}
}

// TestViewHealthyReplicasPassTheCSIVolumeCondition: linstor-csi's
// ControllerGetVolume and ListVolumes report a volume abnormal on
// every replica with no `state` or with a DRBD layer whose
// `promotion_score` is 0. LINSTOR publishes the kernel's promotion
// score there; blockstor never did, so every replica of every volume
// was reported as a node with issues on each health-monitor pass while
// the same view showed it UpToDate and connected.
func TestViewHealthyReplicasPassTheCSIVolumeCondition(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(crScheme(t)).
		WithStatusSubresource(&crdv1alpha1.Resource{}).Build()
	st := k8s.New(cli)
	seedThreeReplicaRD(t, st)
	reportHealthyReplicas(t, cli)

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, r := range contractResourceView(t, base) {
		if r.State == nil {
			t.Errorf("%s: no state, linstor-csi reports the node as having issues", r.NodeName)

			continue
		}

		drbd := drbdLayer(r.LayerObject)
		if drbd == nil {
			t.Fatalf("%s: no DRBD layer in %+v", r.NodeName, r.LayerObject)
		}

		if drbd.PromotionScore == 0 {
			t.Errorf("%s: layer_object.drbd.promotion_score = 0 on a healthy replica, linstor-csi reports the node as having issues", r.NodeName)
		}

		if !drbd.MayPromote {
			t.Errorf("%s: layer_object.drbd.may_promote = false, want the observed true", r.NodeName)
		}
	}
}

// TestViewPlaceholderNeedsAPool: a diskful replica with no pool on
// its spec gets no placeholder volume. A row with an empty
// `storage_pool_name` is what crashes the Python CLI's `n describe`
// on a pool lookup returning None (the Bug 137 crash class), so the
// replica keeps the list the satellite observed.
func TestViewPlaceholderNeedsAPool(t *testing.T) {
	t.Parallel()

	rsc := &apiv1.Resource{Name: contractRD, NodeName: "node-a"}

	got := ensureVolumesForView(nil, rsc, map[int32]int64{0: 1024 * 1024})
	if got == nil {
		t.Fatal("volumes = nil, want an empty list so the key stays present")
	}

	for _, v := range got {
		if v.StoragePool == "" {
			t.Errorf("volume %d has an empty storage pool, want no placeholder without a pool", v.VolumeNumber)
		}
	}
}
