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

package k8s

import (
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestVolumesFromStatus pins the CRD → wire projection that gives
// the Python CLI a non-Unknown rsc_state. Without per-volume
// disk_state, the CLI hides the Conns column and `--faulty` cannot
// see broken peer connections — verified end-to-end on e2e1.
func TestVolumesFromStatus(t *testing.T) {
	if got := volumesFromStatus(nil); got != nil {
		t.Errorf("nil input: got %v, want nil", got)
	}

	if got := volumesFromStatus([]crdv1alpha1.ResourceVolumeStatus{}); got != nil {
		t.Errorf("empty input: got %v, want nil", got)
	}

	in := []crdv1alpha1.ResourceVolumeStatus{
		{
			VolumeNumber: 0,
			StoragePool:  "stand",
			DevicePath:   "/dev/drbd1000",
			AllocatedKib: 1024,
			UsableKib:    1024,
			DiskState:    "UpToDate",
			CurrentGi:    "1234ABCD",
		},
		{
			VolumeNumber: 1,
			DiskState:    "Inconsistent", // the bit the CLI keys --faulty off
		},
	}

	got := volumesFromStatus(in)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	if got[0].VolumeNumber != 0 || got[0].State.DiskState != "UpToDate" ||
		got[0].DevicePath != "/dev/drbd1000" || got[0].StoragePool != "stand" {
		t.Errorf("vol[0] wrong: %+v", got[0])
	}

	if got[0].State.CurrentGi != "1234ABCD" {
		t.Errorf("vol[0] CurrentGi: got %q, want 1234ABCD", got[0].State.CurrentGi)
	}

	if got[1].VolumeNumber != 1 || got[1].State.DiskState != "Inconsistent" {
		t.Errorf("vol[1] wrong: %+v", got[1])
	}

	// Python CLI gates State-column trust on layer_data_list[0].type ==
	// DRBD. Without this, even a UpToDate disk_state shows as "Created".
	if len(got[0].LayerDataList) == 0 || got[0].LayerDataList[0].Type != apiv1.LayerKindDRBD {
		t.Errorf("vol[0] layer_data_list: %+v (expected first entry DRBD)", got[0].LayerDataList)
	}
}

// TestDrbdLayerFromStatus pins the DRBD-layer projection. Empty
// status → nil (the wire `drbd` key is omitted via omitempty; the
// Python CLI tolerates a missing key but crashes on a half-populated
// one). A populated status emits TCPPorts + Connections.
func TestDrbdLayerFromStatus(t *testing.T) {
	if got := drbdLayerFromStatus(&crdv1alpha1.ResourceStatus{}); got != nil {
		t.Errorf("empty status: got %+v, want nil", got)
	}

	port := int32(7000)

	got := drbdLayerFromStatus(&crdv1alpha1.ResourceStatus{
		DRBDPort: &port,
		Connections: []crdv1alpha1.ResourceConnectionStatus{
			{PeerNodeName: "n2", Connected: true, Message: "Connected"},
			{PeerNodeName: "n3", Connected: false, Message: "BrokenPipe"},
		},
	})

	if got == nil {
		t.Fatal("populated status: got nil")
	}

	if len(got.TCPPorts) != 1 || got.TCPPorts[0] != 7000 {
		t.Errorf("TCPPorts: got %v, want [7000]", got.TCPPorts)
	}

	if len(got.Connections) != 2 {
		t.Fatalf("Connections len = %d, want 2: %+v", len(got.Connections), got.Connections)
	}

	if c := got.Connections["n2"]; !c.Connected || c.Message != "Connected" {
		t.Errorf("n2 wrong: %+v", c)
	}

	if c := got.Connections["n3"]; c.Connected || c.Message != "BrokenPipe" {
		t.Errorf("n3 wrong: %+v", c)
	}
}

// TestDrbdLayerFromStatusPortOnly pins the partial-data case: when
// only the port is set (no connection observations yet), we still
// emit a non-nil layer so the CLI's `r list` shows the DRBD port
// column. Otherwise drbd-just-attached resources look as if they
// have no port.
func TestDrbdLayerFromStatusPortOnly(t *testing.T) {
	port := int32(7000)

	got := drbdLayerFromStatus(&crdv1alpha1.ResourceStatus{DRBDPort: &port})
	if got == nil {
		t.Fatal("port-only status: got nil")
	}

	if len(got.TCPPorts) != 1 || got.TCPPorts[0] != 7000 {
		t.Errorf("TCPPorts: got %v, want [7000]", got.TCPPorts)
	}

	if got.Connections != nil {
		t.Errorf("Connections: got %v, want nil (no observations yet)", got.Connections)
	}
}

// TestLayerObjectFromStackDiskless pins the DISKLESS / TIE_BREAKER
// rule: a diskless resource must NOT advertise a STORAGE child
// (there's no backing disk on this node). The Python CLI renders
// the Layers column from the children chain, so getting this wrong
// shows a fake STORAGE leaf for tiebreakers.
func TestLayerObjectFromStackDiskless(t *testing.T) {
	cases := []struct {
		name      string
		flags     []string
		wantStack []string
	}{
		{"default diskful", nil, []string{"DRBD", "STORAGE"}},
		{"explicit STORAGE child", []string{}, []string{"DRBD", "STORAGE"}},
		{"DISKLESS strips STORAGE", []string{"DISKLESS"}, []string{"DRBD"}},
		{"TIE_BREAKER strips STORAGE", []string{"TIE_BREAKER"}, []string{"DRBD"}},
		{"unrelated flag does not strip", []string{"INACTIVE"}, []string{"DRBD", "STORAGE"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			top := layerObjectFromStack(nil, tc.flags)
			gotStack := walkLayerStack(top)

			if !equalStrSlice(gotStack, tc.wantStack) {
				t.Errorf("stack: got %v, want %v", gotStack, tc.wantStack)
			}
		})
	}
}

// TestLayerObjectFromCRD wraps the stack derivation with the
// status-side DRBD enrichment. The fix here is end-to-end:
// `linstor r list --faulty` requires `layer_object.drbd.connections`
// to be populated on diskful peers.
func TestLayerObjectFromCRD(t *testing.T) {
	port := int32(7000)

	crd := &crdv1alpha1.Resource{
		Spec: crdv1alpha1.ResourceSpec{},
		Status: crdv1alpha1.ResourceStatus{
			DRBDPort: &port,
			Connections: []crdv1alpha1.ResourceConnectionStatus{
				{PeerNodeName: "n2", Connected: true, Message: "Connected"},
			},
		},
	}

	got := layerObjectFromCRD(crd)
	if got == nil {
		t.Fatal("got nil")
	}

	if got.Type != apiv1.LayerKindDRBD {
		t.Errorf("Type: got %q, want %q", got.Type, apiv1.LayerKindDRBD)
	}

	if got.Drbd == nil {
		t.Fatal("Drbd: got nil — Conns column would silently disappear")
	}

	if len(got.Drbd.Connections) != 1 || !got.Drbd.Connections["n2"].Connected {
		t.Errorf("Drbd.Connections wrong: %+v", got.Drbd.Connections)
	}
}

// TestLayerObjectFromCRDDisklessSkipsDrbdEnrichment guards a subtle
// case: a diskless replica is still on the DRBD layer (it just
// has no STORAGE child), and must still surface peer connections.
// Otherwise `linstor r list --faulty` cannot see broken peers from
// a witness node's perspective.
func TestLayerObjectFromCRDDisklessSkipsDrbdEnrichment(t *testing.T) {
	port := int32(7000)

	crd := &crdv1alpha1.Resource{
		Spec: crdv1alpha1.ResourceSpec{Flags: []string{"DISKLESS", "TIE_BREAKER"}},
		Status: crdv1alpha1.ResourceStatus{
			DRBDPort: &port,
			Connections: []crdv1alpha1.ResourceConnectionStatus{
				{PeerNodeName: "n1", Connected: true, Message: "Connected"},
				{PeerNodeName: "n2", Connected: false, Message: "Connecting"},
			},
		},
	}

	got := layerObjectFromCRD(crd)
	if got == nil {
		t.Fatal("got nil")
	}

	// Stack collapses to just DRBD (no STORAGE child).
	if len(got.Children) != 0 {
		t.Errorf("diskless: children = %v, want none", got.Children)
	}

	if got.Drbd == nil || len(got.Drbd.Connections) != 2 {
		t.Errorf("Drbd not populated on diskless: %+v", got.Drbd)
	}
}

func walkLayerStack(top *apiv1.ResourceLayer) []string {
	if top == nil {
		return nil
	}

	out := []string{top.Type}
	cursor := top

	for len(cursor.Children) > 0 {
		cursor = &cursor.Children[0]
		out = append(out, cursor.Type)
	}

	return out
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// Order is meaningful for the layer stack.
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// TestResourceCRDNameUsesRDDotNodeOrder pins the canonical Resource
// name encoding: `<rd>.<node>`. Matches the CRD-level CEL rule on
// the Resource type and the cluster-wide naming convention every
// other node-bound CRD in the project follows. Flipping the order
// silently breaks the CEL rule on Create — k8s rejects the write
// with a 422 and the wire-side error is hard to trace back to the
// wrong helper.
func TestResourceCRDNameUsesRDDotNodeOrder(t *testing.T) {
	t.Parallel()

	got := resourceCRDName("pvc-1", "w1")
	want := "pvc-1.w1"

	if got != want {
		t.Errorf("resourceCRDName: got %q, want %q (must be <rd>.<node>)",
			got, want)
	}
}

// TestWireToCRDResourceProducesCanonicalName pins the wire→CRD
// converter for Resource to the canonical `<rd>.<node>` shape that
// the CRD's CEL rule enforces. The store's `wireToCRDResource`
// builds the metadata.Name from (Name, NodeName); a regression here
// would only surface against a real cluster (apiserver 422 from
// CEL), this test catches it pre-flight.
func TestWireToCRDResourceProducesCanonicalName(t *testing.T) {
	t.Parallel()

	in := apiv1.Resource{Name: "pvc-x", NodeName: "w1"}
	crd := wireToCRDResource(&in)

	want := "pvc-x.w1"
	if crd.Name != want {
		t.Errorf("CRD metadata.name: got %q, want %q", crd.Name, want)
	}

	// Replicate the CEL invariant so a future converter rewrite that
	// drifts from the rule fails this test rather than a much-harder
	// -to-trace apiserver 422 on Create.
	if crd.Name != crd.Spec.ResourceDefinitionName+"."+crd.Spec.NodeName {
		t.Errorf("CEL invariant broken: name=%q, expected %q",
			crd.Name, crd.Spec.ResourceDefinitionName+"."+crd.Spec.NodeName)
	}
}
