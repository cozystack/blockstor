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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// TestNodeHeartbeat_StaleFlipsToUnknown: a Node whose LastHeartbeatTime
// is older than NodeMonitorGracePeriod gets Ready=Unknown + OFFLINE.
// Pins the kubelet-style node-monitor algorithm: nodes that haven't
// reported within the grace period are no longer trusted.
func TestNodeHeartbeat_StaleFlipsToUnknown(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	stale := time.Now().Add(-2 * controller.NodeMonitorGracePeriod)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		Status: blockstoriov1alpha1.NodeStatus{
			LastHeartbeatTime: &metav1.Time{Time: stale},
			ConnectionStatus:  blockstoriov1alpha1.NodeConnectionStatusOnline,
			Conditions: []metav1.Condition{
				{
					Type:               blockstoriov1alpha1.NodeConditionReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Time{Time: stale},
				},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	rec := &controller.NodeHeartbeatReconciler{Client: cli, Now: time.Now}

	res, err := rec.Reconcile(t.Context(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "worker-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != controller.NodeMonitorPeriod {
		t.Errorf("RequeueAfter: got %v, want %v", res.RequeueAfter, controller.NodeMonitorPeriod)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("Get post-reconcile: %v", err)
	}

	if got.Status.ConnectionStatus != blockstoriov1alpha1.NodeConnectionStatusOffline {
		t.Errorf("ConnectionStatus: got %q, want %q",
			got.Status.ConnectionStatus, blockstoriov1alpha1.NodeConnectionStatusOffline)
	}

	cond := findReady(&got)
	if cond == nil {
		t.Fatal("Ready condition not found")
	}

	if cond.Status != metav1.ConditionUnknown {
		t.Errorf("Ready status: got %v, want Unknown", cond.Status)
	}
}

// TestNodeHeartbeat_FreshStaysConnected: a Node with a fresh heartbeat
// keeps Ready=True / ONLINE — the watchdog must not overwrite the
// satellite's happy-path state.
func TestNodeHeartbeat_FreshStaysConnected(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	fresh := time.Now().Add(-5 * time.Second)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-2"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		Status: blockstoriov1alpha1.NodeStatus{
			LastHeartbeatTime: &metav1.Time{Time: fresh},
			ConnectionStatus:  blockstoriov1alpha1.NodeConnectionStatusOnline,
			Conditions: []metav1.Condition{
				{
					Type:               blockstoriov1alpha1.NodeConditionReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Time{Time: fresh},
				},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	rec := &controller.NodeHeartbeatReconciler{Client: cli, Now: time.Now}

	_, err := rec.Reconcile(t.Context(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "worker-2"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-2"}, &got); err != nil {
		t.Fatalf("Get post-reconcile: %v", err)
	}

	if got.Status.ConnectionStatus != blockstoriov1alpha1.NodeConnectionStatusOnline {
		t.Errorf("ConnectionStatus drift: got %q, want %q",
			got.Status.ConnectionStatus, blockstoriov1alpha1.NodeConnectionStatusOnline)
	}

	cond := findReady(&got)
	if cond == nil {
		t.Fatal("Ready condition not found")
	}

	if cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready status: got %v, want True", cond.Status)
	}
}

// TestNodeHeartbeat_NilHeartbeatIsStale: a Node whose satellite has
// never stamped LastHeartbeatTime is treated as stale immediately —
// matches kubelet's "no NodeStatus yet" semantics.
func TestNodeHeartbeat_NilHeartbeatIsStale(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-3"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		// No LastHeartbeatTime — satellite never stamped.
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	rec := &controller.NodeHeartbeatReconciler{Client: cli, Now: time.Now}

	_, err := rec.Reconcile(t.Context(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "worker-3"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-3"}, &got); err != nil {
		t.Fatalf("Get post-reconcile: %v", err)
	}

	cond := findReady(&got)
	if cond == nil {
		t.Fatal("Ready condition not found")
	}

	if cond.Status != metav1.ConditionUnknown {
		t.Errorf("never-reported node should be Unknown, got %v", cond.Status)
	}
}

func findReady(node *blockstoriov1alpha1.Node) *metav1.Condition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == blockstoriov1alpha1.NodeConditionReady {
			return &node.Status.Conditions[i]
		}
	}

	return nil
}
