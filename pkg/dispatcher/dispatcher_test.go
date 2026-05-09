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

package dispatcher_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/store/k8s"
	"google.golang.org/grpc"
)

// fakeDialer captures the endpoint a Dispatcher dialled and returns a
// stub SatelliteClient that records ApplyResources requests.
type fakeDialer struct {
	endpoint string
	stub     *fakeSatelliteClient
}

type fakeSatelliteClient struct {
	satellitepb.SatelliteClient

	last *satellitepb.ApplyResourcesRequest
	resp *satellitepb.ApplyResourcesResponse
	err  error
}

func (f *fakeDialer) Dial(_ context.Context, endpoint string) (satellitepb.SatelliteClient, func() error, error) {
	f.endpoint = endpoint
	return f.stub, func() error { return nil }, nil
}

func (f *fakeSatelliteClient) ApplyResources(_ context.Context, req *satellitepb.ApplyResourcesRequest, _ ...grpc.CallOption) (*satellitepb.ApplyResourcesResponse, error) {
	f.last = req
	return f.resp, f.err
}

// TestApplyDialsTargetSatellite: the dispatcher uses the target Node's
// SatelliteEndpoint to pick where to dial.
func TestApplyDialsTargetSatellite(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", NodeName: "n1", Ok: true}},
		},
	}
	dialer := &fakeDialer{stub: stub}
	d := dispatcher.New(dialer)

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.244.1.5:7000"},
		},
	}}

	result, err := d.Apply(t.Context(), target, nil, nodes, nil, dispatcher.ApplyOptions{})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if dialer.endpoint != "10.244.1.5:7000" {
		t.Errorf("dialed %q, want 10.244.1.5:7000", dialer.endpoint)
	}

	if !result.GetOk() {
		t.Errorf("expected ok; got %v", result)
	}
}

// TestApplyMissingEndpoint: no SatelliteEndpoint prop → error before dial.
func TestApplyMissingEndpoint(t *testing.T) {
	d := dispatcher.New(&fakeDialer{stub: &fakeSatelliteClient{}})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}}

	_, err := d.Apply(t.Context(), target, nil, nodes, nil, dispatcher.ApplyOptions{})
	if err == nil {
		t.Fatalf("expected error when endpoint missing")
	}
}

// TestApplyBuildsPeers: a 2-replica RD pushes the other node's name into
// the Peers slice and into the per-peer drbd_options keys.
func TestApplyBuildsPeers(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", Ok: true}},
		},
	}
	d := dispatcher.New(&fakeDialer{stub: stub})

	id0 := int32(0)
	id1 := int32(1)

	target := &blockstoriov1alpha1.Resource{
		Spec:   blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id0},
	}

	peers := []blockstoriov1alpha1.Resource{
		{
			Spec:   blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
			Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id0},
		},
		{
			Spec:   blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n2"},
			Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id1},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	_, err := d.Apply(t.Context(), target, peers, nodes, nil, dispatcher.ApplyOptions{})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got := stub.last.GetResources()[0]
	if !slices.Contains(got.GetPeers(), "n2") {
		t.Errorf("expected n2 in Peers; got %v", got.GetPeers())
	}

	for _, key := range []string{"peer.n2.port", "peer.n2.node-id", "peer.n2.address"} {
		if got.GetDrbdOptions()[key] == "" {
			t.Errorf("missing drbd option %q in %v", key, got.GetDrbdOptions())
		}
	}
}

// TestApplyDialError surfaces transport-level failures as errors.
func TestApplyDialError(t *testing.T) {
	stub := &fakeSatelliteClient{}
	d := dispatcher.New(&errDialer{err: errFakeDial})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	_, err := d.Apply(t.Context(), target, nil, nodes, nil, dispatcher.ApplyOptions{})
	if err == nil {
		t.Fatalf("expected dial error")
	}

	_ = stub
}

// TestApplyMatchesSlugifiedNode: when the LINSTOR-side node name was
// slugified at write time, the CRD's metadata.Name differs from
// target.Spec.NodeName. Apply must still resolve the SatelliteEndpoint
// by reading the original-name annotation rather than metadata.Name.
func TestApplyMatchesSlugifiedNode(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", NodeName: "MixedCaseNode", Ok: true}},
		},
	}
	dialer := &fakeDialer{stub: stub}
	d := dispatcher.New(dialer)

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "MixedCaseNode",
		},
	}

	meta := metav1.ObjectMeta{
		Name:        "abcd1234-mixedcasenode",
		Annotations: map[string]string{k8s.AnnotationLinstorName: "MixedCaseNode"},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: meta,
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.244.1.7:7000"},
		},
	}}

	_, err := d.Apply(t.Context(), target, nil, nodes, nil, dispatcher.ApplyOptions{})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if dialer.endpoint != "10.244.1.7:7000" {
		t.Errorf("dialed %q, want 10.244.1.7:7000", dialer.endpoint)
	}
}

// TestApplyDRBDOptionsFromEffectiveProps pins the option-hierarchy
// flow: ApplyOptions.EffectiveProps lands on the satellite's
// drbd_options bag (DrbdOptions/... keys) while non-DRBD entries
// flow through as Props. The dispatcher itself doesn't merge —
// the controller does that via drbd.ResolveOptions; this test only
// asserts the in-out wiring is right.
func TestApplyDRBDOptionsFromEffectiveProps(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", Ok: true}},
		},
	}
	d := dispatcher.New(&fakeDialer{stub: stub})

	id0 := int32(0)
	target := &blockstoriov1alpha1.Resource{
		Spec:   blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id0},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	effective := map[string]string{
		"DrbdOptions/Net/protocol":    "C",
		"DrbdOptions/Net/max-buffers": "8192",
		"StorPoolName":                "pool",
	}

	_, err := d.Apply(t.Context(), target, nil, nodes, nil, dispatcher.ApplyOptions{EffectiveProps: effective})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got := stub.last.GetResources()[0]
	if got.GetDrbdOptions()["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("protocol missing from DrbdOptions: %v", got.GetDrbdOptions())
	}

	if got.GetDrbdOptions()["DrbdOptions/Net/max-buffers"] != "8192" {
		t.Errorf("max-buffers missing: %v", got.GetDrbdOptions())
	}

	if got.GetProps()["StorPoolName"] != "pool" {
		t.Errorf("StorPoolName must flow through as Props, not DrbdOptions; props=%v drbd=%v",
			got.GetProps(), got.GetDrbdOptions())
	}

	if _, leaked := got.GetProps()["DrbdOptions/Net/protocol"]; leaked {
		t.Errorf("DRBD option leaked into Props: %v", got.GetProps())
	}
}

// errDialer always fails — used to assert the dialErr path.
type errDialer struct{ err error }

func (e *errDialer) Dial(_ context.Context, _ string) (satellitepb.SatelliteClient, func() error, error) {
	return nil, nil, e.err
}

// errFakeDial is the canned transport-level error errDialer surfaces.
// Using a package-level sentinel keeps err113 happy.
var errFakeDial = errors.New("dispatcher_test: connection refused")

// nodeMeta is sugar for setting the only ObjectMeta field this package
// touches (Name) without dragging the whole metav1 boilerplate into
// every table entry.
func nodeMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name}
}
