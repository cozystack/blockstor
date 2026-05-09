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

	// DeleteResource state, populated when the test exercises that
	// path (Apply tests leave these zero-valued).
	delLast *satellitepb.DeleteResourceRequest
	delResp *satellitepb.DeleteResourceResponse

	// CreateSnapshot state.
	snapLast []*satellitepb.CreateSnapshotRequest
	snapResp *satellitepb.CreateSnapshotResponse
}

func (f *fakeDialer) Dial(_ context.Context, endpoint string) (satellitepb.SatelliteClient, func() error, error) {
	f.endpoint = endpoint
	return f.stub, func() error { return nil }, nil
}

func (f *fakeSatelliteClient) ApplyResources(_ context.Context, req *satellitepb.ApplyResourcesRequest, _ ...grpc.CallOption) (*satellitepb.ApplyResourcesResponse, error) {
	f.last = req
	return f.resp, f.err
}

func (f *fakeSatelliteClient) DeleteResource(_ context.Context, req *satellitepb.DeleteResourceRequest, _ ...grpc.CallOption) (*satellitepb.DeleteResourceResponse, error) {
	f.delLast = req
	if f.delResp == nil {
		return &satellitepb.DeleteResourceResponse{Ok: true}, nil
	}
	return f.delResp, nil
}

func (f *fakeSatelliteClient) CreateSnapshot(_ context.Context, req *satellitepb.CreateSnapshotRequest, _ ...grpc.CallOption) (*satellitepb.CreateSnapshotResponse, error) {
	f.snapLast = append(f.snapLast, req)
	if f.snapResp == nil {
		return &satellitepb.CreateSnapshotResponse{Ok: true}, nil
	}
	return f.snapResp, nil
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

// TestPeerAddress: lookup → host extraction + fallback contract.
// peerAddress must:
//   - return just the host part of "host:port"
//   - return the whole string when there's no port separator
//   - fall back to the 0.0.0.0 placeholder when the node is unknown
//     or hasn't published a SatelliteEndpoint yet
//
// The placeholder is what the .res renderer drops in until the
// satellite re-registers with its real endpoint — without the
// fallback, peerAddress would emit empty strings into the .res
// `address` field and drbdadm would refuse to parse the resource.
func TestPeerAddress(t *testing.T) {
	t.Parallel()

	nodes := []blockstoriov1alpha1.Node{
		{
			ObjectMeta: nodeMeta("with-port"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "10.244.1.5:7000"},
			},
		},
		{
			ObjectMeta: nodeMeta("no-port"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "no-colon-here"},
			},
		},
		{
			ObjectMeta: nodeMeta("ipv6"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "[fe80::1]:7000"},
			},
		},
	}

	cases := []struct {
		name string
		want string
	}{
		{"with-port", "10.244.1.5"},
		{"no-port", "no-colon-here"},
		// IPv6 has multiple colons; LastIndex picks the right one.
		{"ipv6", "[fe80::1]"},
		{"ghost-node", dispatcher.DrbdAddrAny}, // unregistered → placeholder
	}

	for _, c := range cases {
		got := dispatcher.PeerAddress(c.name, nodes)
		if got != c.want {
			t.Errorf("peerAddress(%q): got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestDeriveExports: the public DerivePort / DeriveMinor wrappers must
// match their unexported implementations. Used by ad-hoc tooling
// (drbdadm-show-replicas helpers) that imports the dispatcher package
// without re-implementing the hash. Different RDs must hash differently;
// the same RD must be deterministic across calls.
func TestDeriveExports(t *testing.T) {
	port := dispatcher.DerivePort("pvc-1")
	if port < 7000 || port >= 8000 {
		t.Errorf("DerivePort: got %d, want in [7000, 8000)", port)
	}

	if dispatcher.DerivePort("pvc-1") != port {
		t.Errorf("DerivePort non-deterministic")
	}

	if dispatcher.DerivePort("pvc-1") == dispatcher.DerivePort("pvc-2") {
		t.Errorf("DerivePort hash collision on different RDs")
	}

	minor := dispatcher.DeriveMinor("pvc-1")
	if minor < 1000 || minor >= 10000 {
		t.Errorf("DeriveMinor: got %d, want in [1000, 10000)", minor)
	}
}

// TestDeleteResourceDispatchesToTarget: DeleteResource dials the target
// satellite's endpoint and forwards (rd_name, storage_pool, volume_numbers)
// over the gRPC contract. Pins the per-replica teardown path the
// Resource controller drives on RD deletion.
func TestDeleteResourceDispatchesToTarget(t *testing.T) {
	stub := &fakeSatelliteClient{}
	dialer := &fakeDialer{stub: stub}
	d := dispatcher.New(dialer)

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-del",
			NodeName:               "n1",
			Props:                  map[string]string{"StorPoolName": "thin"},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
				{VolumeNumber: 1, SizeKib: 1024 * 1024},
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.244.1.5:7000"},
		},
	}}

	resp, err := d.DeleteResource(t.Context(), target, rd, nodes)
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if !resp.GetOk() {
		t.Errorf("expected Ok=true; got %+v", resp)
	}

	if dialer.endpoint != "10.244.1.5:7000" {
		t.Errorf("endpoint: got %q, want 10.244.1.5:7000", dialer.endpoint)
	}

	if stub.delLast.GetName() != "pvc-del" {
		t.Errorf("Name: got %q, want pvc-del", stub.delLast.GetName())
	}

	if stub.delLast.GetStoragePool() != "thin" {
		t.Errorf("StoragePool: got %q, want thin", stub.delLast.GetStoragePool())
	}

	if !slices.Equal(stub.delLast.GetVolumeNumbers(), []int32{0, 1}) {
		t.Errorf("VolumeNumbers: got %v, want [0 1]", stub.delLast.GetVolumeNumbers())
	}
}

// TestDeleteResourceFallsBackToRDStorPool: when the Resource itself
// doesn't carry a StorPoolName prop, the dispatcher reaches up to the
// RD's prop. Mirrors the same fallback Apply does so a teardown after
// an RD-level pool change still hits the right pool.
func TestDeleteResourceFallsBackToRDStorPool(t *testing.T) {
	stub := &fakeSatelliteClient{}
	d := dispatcher.New(&fakeDialer{stub: stub})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-del",
			NodeName:               "n1",
			// StorPoolName intentionally absent on the Resource.
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{"StorPoolName": "rd-pool"},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.244.1.5:7000"},
		},
	}}

	_, err := d.DeleteResource(t.Context(), target, rd, nodes)
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	if stub.delLast.GetStoragePool() != "rd-pool" {
		t.Errorf("StoragePool fallback: got %q, want rd-pool", stub.delLast.GetStoragePool())
	}
}

// TestDeleteResourceMissingEndpoint: no SatelliteEndpoint on the
// target node → error before dial. Caller (Resource controller) is
// expected to retry once the Node CRD catches up.
func TestDeleteResourceMissingEndpoint(t *testing.T) {
	d := dispatcher.New(&fakeDialer{stub: &fakeSatelliteClient{}})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-del", NodeName: "n1"},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}}

	_, err := d.DeleteResource(t.Context(), target, nil, nodes)
	if err == nil {
		t.Errorf("expected error when SatelliteEndpoint missing; got nil")
	}
}

// TestCreateSnapshotFanout: CreateSnapshot dials every diskful
// replica's satellite, skipping DISKLESS replicas (they have no LV
// to snapshot). Pins the snapshot fan-out path used by the Snapshot
// CRD reconciler.
func TestCreateSnapshotFanout(t *testing.T) {
	stub := &fakeSatelliteClient{}
	d := dispatcher.New(&fakeDialer{stub: stub})

	replicas := []blockstoriov1alpha1.Resource{
		{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-snap",
				NodeName:               "n1",
			},
		},
		{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-snap",
				NodeName:               "n2",
			},
		},
		{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-snap",
				NodeName:               "n3",
				Flags:                  []string{"DISKLESS"},
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{
		{
			ObjectMeta: nodeMeta("n1"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "10.244.1.5:7000"},
			},
		},
		{
			ObjectMeta: nodeMeta("n2"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "10.244.1.6:7000"},
			},
		},
		{
			ObjectMeta: nodeMeta("n3"),
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"SatelliteEndpoint": "10.244.1.7:7000"},
			},
		},
	}

	results, err := d.CreateSnapshot(t.Context(), "pvc-snap", "snap-1", replicas, nodes)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("results: got %d, want 2 (DISKLESS replica must be skipped)", len(results))
	}

	if len(stub.snapLast) != 2 {
		t.Errorf("RPCs sent: got %d, want 2", len(stub.snapLast))
	}

	for _, req := range stub.snapLast {
		if req.GetResourceName() != "pvc-snap" || req.GetSnapshotName() != "snap-1" {
			t.Errorf("RPC payload: got rd=%q snap=%q, want pvc-snap/snap-1",
				req.GetResourceName(), req.GetSnapshotName())
		}
	}
}

// TestApplyDisklessOmitsVolumes: a target with the DISKLESS flag must
// not push any DesiredVolume entries on the wire — diskless replicas
// don't allocate local storage. The RD's VolumeDefinitions are still
// available, but buildVolumes short-circuits.
func TestApplyDisklessOmitsVolumes(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", Ok: true}},
		},
	}
	d := dispatcher.New(&fakeDialer{stub: stub})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			Flags:                  []string{"DISKLESS"},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	_, err := d.Apply(t.Context(), target, nil, nodes, rd, dispatcher.ApplyOptions{})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	desired := stub.last.GetResources()[0]
	if len(desired.GetVolumes()) != 0 {
		t.Errorf("DISKLESS replica must push 0 volumes; got %d (%+v)",
			len(desired.GetVolumes()), desired.GetVolumes())
	}
}

// TestApplyLiftsLuksPassphrase: when the effective DRBD options
// carry the upstream LINSTOR `DrbdOptions/Encryption/passphrase`
// prop, the dispatcher must lift it onto the wire as
// `LuksPassphrase` so the satellite's LUKS layer can read it via
// `dr.GetProps()["LuksPassphrase"]`. Pins the cross-name handover —
// keeps the upstream prop key for `linstor rd set-property` parity
// while letting the satellite use a less-cluttered key.
func TestApplyLiftsLuksPassphrase(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", Ok: true}},
		},
	}
	d := dispatcher.New(&fakeDialer{stub: stub})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			Props:                  map[string]string{"StorPoolName": "thin"},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	// EffectiveProps carries the upstream Encryption prop.
	opts := dispatcher.ApplyOptions{
		EffectiveProps: map[string]string{
			"DrbdOptions/Encryption/passphrase": "topsecret",
		},
	}

	_, err := d.Apply(t.Context(), target, nil, nodes, rd, opts)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	desired := stub.last.GetResources()[0]

	// Wire-side LuksPassphrase must be populated.
	if desired.GetProps()["LuksPassphrase"] != "topsecret" {
		t.Errorf("LuksPassphrase: got %q, want topsecret (props=%v)",
			desired.GetProps()["LuksPassphrase"], desired.GetProps())
	}

	// Layer stack must round-trip onto the wire so the satellite
	// knows to wire the LUKS layer.
	if len(desired.GetLayerStack()) != 3 {
		t.Errorf("LayerStack: got %v, want 3 entries", desired.GetLayerStack())
	}
}

// TestApplyStorPoolFallsBackToRD: a Resource without StorPoolName on
// its own Spec.Props must inherit it from the RD's Spec.Props. This
// is what makes `linstor rd set-property pool` propagate to existing
// Resources without re-applying every Resource CRD.
func TestApplyStorPoolFallsBackToRD(t *testing.T) {
	stub := &fakeSatelliteClient{
		resp: &satellitepb.ApplyResourcesResponse{
			Results: []*satellitepb.ResourceApplyResult{{Name: "pvc-1", Ok: true}},
		},
	}
	d := dispatcher.New(&fakeDialer{stub: stub})

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			// StorPoolName intentionally absent on the Resource.
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{"StorPoolName": "rd-pool"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n1"),
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}}

	_, err := d.Apply(t.Context(), target, nil, nodes, rd, dispatcher.ApplyOptions{})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	desired := stub.last.GetResources()[0]
	if len(desired.GetVolumes()) != 1 {
		t.Fatalf("volumes: got %d, want 1", len(desired.GetVolumes()))
	}

	if desired.GetVolumes()[0].GetStoragePool() != "rd-pool" {
		t.Errorf("StoragePool fallback: got %q, want rd-pool",
			desired.GetVolumes()[0].GetStoragePool())
	}
}

// TestCreateSnapshotMissingEndpointRecorded: a replica whose Node has
// no SatelliteEndpoint must surface as an Ok=false result rather than
// failing the whole fan-out (other replicas can still take their snap).
func TestCreateSnapshotMissingEndpointRecorded(t *testing.T) {
	stub := &fakeSatelliteClient{}
	d := dispatcher.New(&fakeDialer{stub: stub})

	replicas := []blockstoriov1alpha1.Resource{
		{
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "pvc-snap",
				NodeName:               "n-missing",
			},
		},
	}

	nodes := []blockstoriov1alpha1.Node{{
		ObjectMeta: nodeMeta("n-missing"),
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}}

	results, err := d.CreateSnapshot(t.Context(), "pvc-snap", "snap-1", replicas, nodes)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Errorf("missing endpoint must surface as Ok=false; got %+v", results[0])
	}
}
