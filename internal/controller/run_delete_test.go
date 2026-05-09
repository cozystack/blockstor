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
	"slices"
	"testing"
	"time"

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

// happyDialer returns a stub SatelliteClient whose DeleteResource
// just ACKs everything. Lets tests exercise runDelete's success
// path without spinning up a real satellite gRPC server.
type happyDialer struct{}

func (happyDialer) Dial(_ context.Context, _ string) (satellitepb.SatelliteClient, func() error, error) {
	return &happySatelliteClient{}, func() error { return nil }, nil
}

type happySatelliteClient struct {
	satellitepb.SatelliteClient
}

func (*happySatelliteClient) DeleteResource(_ context.Context, _ *satellitepb.DeleteResourceRequest, _ ...grpc.CallOption) (*satellitepb.DeleteResourceResponse, error) {
	return &satellitepb.DeleteResourceResponse{Ok: true}, nil
}

// TestRunDeleteRequeuesOnRPCError: a resource with DeletionTimestamp
// set + finalizer + unreachable satellite must NOT have the finalizer
// stripped — the runDelete path requeues with a 10 s back-off so the
// resource isn't stuck half-gone if a satellite is briefly down.
//
// Pins the failure-then-retry contract: stripping the finalizer
// prematurely would let kube-apiserver complete the delete before
// the satellite finished tearing down, leaking storage on the
// satellite side.
func TestRunDeleteRequeuesOnRPCError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	now := metav1.Now()
	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-del.n1",
			Finalizers:        []string{"blockstor.io.blockstor.io/resource"},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-del",
			NodeName:               "n1",
		},
	}

	// Node CRD must exist so DeleteResource can resolve the
	// SatelliteEndpoint — but the dialer always fails, so the RPC
	// errors out and the reconciler requeues.
	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(resCRD, nodeCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(noopDialer{}),
		Store:      store.NewInMemory(),
	}

	got, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-del.n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 10*time.Second {
		t.Errorf("RequeueAfter: got %v, want 10s (RPC error must trigger back-off)",
			got.RequeueAfter)
	}

	// Finalizer MUST still be present — we haven't confirmed the
	// satellite actually torn down.
	post := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-del.n1"}, post); err != nil {
		t.Fatalf("get post-reconcile: %v", err)
	}

	if len(post.Finalizers) != 1 || post.Finalizers[0] != "blockstor.io.blockstor.io/resource" {
		t.Errorf("finalizer stripped despite RPC error: got %v",
			post.Finalizers)
	}
}

// (no-op-without-finalizer branch isn't testable via the fake client:
// it refuses to seed an object with DeletionTimestamp + no finalizers,
// matching real kube-apiserver semantics — the apiserver would have
// already finished the delete before our hook ever sees it. The
// branch is dead-on-arrival defensive code; production exercise is
// implicit.)

// TestRunDeleteHappyPathStripsFinalizer: with a successful satellite
// RPC, the runDelete path strips the finalizer so kube-apiserver can
// complete the delete. Pins the contract: the storage-side teardown
// happens BEFORE the finalizer comes off, so the satellite has
// finished its work by the time the apiserver finalizes.
func TestRunDeleteHappyPathStripsFinalizer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	now := metav1.Now()
	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-del-ok.n1",
			Finalizers:        []string{"blockstor.io.blockstor.io/resource"},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-del-ok",
			NodeName:               "n1",
		},
	}

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"SatelliteEndpoint": "10.0.0.1:7000"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(resCRD, nodeCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(happyDialer{}),
		Store:      store.NewInMemory(),
	}

	got, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-del-ok.n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 0 || got.Requeue {
		t.Errorf("Result: got %+v, want zero (success means no requeue)", got)
	}

	// After Reconcile the fake client has either deleted the
	// object (apiserver-style) or stripped the finalizer. Either
	// way, our finalizer must NOT be present on a fetched copy.
	post := &blockstoriov1alpha1.Resource{}
	getErr := cli.Get(ctx, types.NamespacedName{Name: "pvc-del-ok.n1"}, post)

	if getErr == nil && slices.Contains(post.Finalizers, "blockstor.io.blockstor.io/resource") {
		t.Errorf("finalizer still present after happy-path delete: %v", post.Finalizers)
	}
}
