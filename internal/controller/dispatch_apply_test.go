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

package controller_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/store"
)

// applyOkFalseDialer returns a satellite client whose ApplyResources
// always responds with Ok=false body-level (e.g. "unknown storage
// pool"). The Reconciler's dispatchApply must log + return Result{}
// — never propagate Ok=false as a transport error.
type applyOkFalseDialer struct{}

func (applyOkFalseDialer) Dial(_ context.Context, _ string) (satellitepb.SatelliteClient, func() error, error) {
	return &applyOkFalseClient{}, func() error { return nil }, nil
}

type applyOkFalseClient struct {
	satellitepb.SatelliteClient
}

func (*applyOkFalseClient) ApplyResources(_ context.Context, req *satellitepb.ApplyResourcesRequest, _ ...grpc.CallOption) (*satellitepb.ApplyResourcesResponse, error) {
	results := make([]*satellitepb.ResourceApplyResult, 0, len(req.GetResources()))
	for _, r := range req.GetResources() {
		results = append(results, &satellitepb.ResourceApplyResult{
			Name:     r.GetName(),
			NodeName: r.GetNodeName(),
			Ok:       false,
			Message:  "unknown storage pool 'ghost'",
		})
	}
	return &satellitepb.ApplyResourcesResponse{Results: results}, nil
}

// TestDispatchApplyOkFalseDoesNotRequeue: when the satellite returns
// Ok=false body-level (e.g. unknown pool, missing passphrase), the
// Reconciler must log and return a clean Result{} — NOT requeue. A
// requeue would burn CPU on a misconfiguration that needs operator
// action, not retry. The operator-visible signal is the per-replica
// log line.
func TestDispatchApplyOkFalseDoesNotRequeue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	// Pre-populate the Status with DRBD ids so the Reconcile doesn't
	// take the requeue-after-allocation path on its first pass.
	id := int32(0)
	port := int32(7000)
	minor := int32(1000)

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-okfalse.n1",
			Finalizers: []string{"blockstor.io.blockstor.io/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-okfalse",
			NodeName:               "n1",
			Props:                  map[string]string{"StorPoolName": "ghost"},
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &id,
			DRBDPort:   &port,
			DRBDMinor:  &minor,
		},
	}

	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-okfalse"},
	}

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD, rdCRD, nodeCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(applyOkFalseDialer{}),
		Store:      store.NewInMemory(),
	}

	got, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-okfalse.n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("Ok=false body-level must NOT requeue (operator action needed, not retry); got %+v", got)
	}
}
