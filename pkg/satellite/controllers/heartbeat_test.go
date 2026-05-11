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

package controllers_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// TestHeartbeatRunnable_Stamps: one tick of the runnable updates
// LastHeartbeatTime and stamps a Ready=True condition on the
// satellite's own Node CRD. Pins the satellite half of the
// kubelet-style liveness contract — the controller-side watchdog
// reads what this runnable writes.
func TestHeartbeatRunnable_Stamps(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	hb := &controllers.HeartbeatRunnable{
		Client:   cli,
		NodeName: "worker-1",
		Period:   100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := hb.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("Get post-stamp: %v", err)
	}

	if got.Status.LastHeartbeatTime == nil {
		t.Fatal("LastHeartbeatTime not stamped")
	}

	var ready *metav1.Condition

	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == blockstoriov1alpha1.NodeConditionReady {
			ready = &got.Status.Conditions[i]
		}
	}

	if ready == nil {
		t.Fatal("Ready condition not stamped")
	}

	if ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready status: got %v, want True", ready.Status)
	}

	if ready.Reason != "SatelliteHeartbeat" {
		t.Errorf("Reason: got %q, want SatelliteHeartbeat", ready.Reason)
	}
}

// TestHeartbeatRunnable_NodeMissing: stamping against a Node CRD
// that doesn't exist is a no-op (the operator hasn't bootstrapped
// the node yet). The runnable must keep ticking — it shouldn't
// take the satellite out of service.
func TestHeartbeatRunnable_NodeMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	hb := &controllers.HeartbeatRunnable{
		Client:   cli,
		NodeName: "ghost",
		Period:   100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	// Should not error even though the Node CRD doesn't exist.
	err := hb.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
}
