// SPDX-License-Identifier: Apache-2.0

//go:build integration

package harness

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// SimulateNodeOffline has to produce a shape NodeHeartbeatReconciler agrees
// with, not just the field a test reads.
//
// The watchdog re-evaluates every Node every NodeMonitorPeriod and decides on
// LastHeartbeatTime, not on ConnectionStatus. Stamping OFFLINE beside a FRESH
// heartbeat is a state no real satellite produces, and it made the two writers
// trade the field: the watchdog read the beat as fresh and wrote ONLINE back,
// the mock wrote OFFLINE again on its next tick, and a test acting on the node
// in between got whichever had gone last. That is what refused `n lost`
// against a node the cascade test had just watched go offline, and it failed
// the way an oscillation does — rarely, on a loaded runner, with a message
// about something else.
//
// Asserted here rather than by holding a live node, because the failure is a
// race: a hold catches it only when it samples inside the window the watchdog
// owns the field, which is neither quick nor reliable. The shape is the cause,
// and it is exact.
func TestSimulateNodeOfflineProducesAShapeTheWatchdogAgreesWith(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("core scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor scheme: %v", err)
	}

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(node).
		Build()

	sat := NewSatellite(cli)
	sat.SimulateNodeOffline("worker-1")
	sat.reconcileNodes(context.Background())

	var got blockstoriov1alpha1.Node
	if err := cli.Get(context.Background(),
		types.NamespacedName{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("read the node back: %v", err)
	}

	if got.Status.ConnectionStatus != blockstoriov1alpha1.NodeConnectionStatusOffline {
		t.Errorf("ConnectionStatus = %q, want OFFLINE", got.Status.ConnectionStatus)
	}

	// A nil heartbeat is stale by the watchdog's own reading, so it is the one
	// other shape that agrees. Anything inside the grace period is the bug.
	if got.Status.LastHeartbeatTime != nil {
		age := time.Since(got.Status.LastHeartbeatTime.Time)
		if age <= controller.NodeMonitorGracePeriod {
			t.Errorf("OFFLINE node carries a heartbeat %s old, inside the %s grace "+
				"period: the watchdog reads that as a live satellite and writes "+
				"ONLINE back", age, controller.NodeMonitorGracePeriod)
		}
	}

	for i := range got.Status.Conditions {
		c := got.Status.Conditions[i]
		if c.Type == blockstoriov1alpha1.NodeConditionReady && c.Status == metav1.ConditionTrue {
			t.Errorf("OFFLINE node carries Ready=True; the watchdog and the mock " +
				"disagree about it and will trade the field")
		}
	}
}

// The control: a node nobody took offline still gets the healthy shape, so the
// assertions above are about the simulation and not about reconcileNodes
// having stopped writing.
func TestReconcileNodesStillStampsAHealthyNodeOnline(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("core scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor scheme: %v", err)
	}

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-2"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(node).
		Build()

	sat := NewSatellite(cli)
	sat.reconcileNodes(context.Background())

	var got blockstoriov1alpha1.Node
	if err := cli.Get(context.Background(),
		types.NamespacedName{Name: "worker-2"}, &got); err != nil {
		t.Fatalf("read the node back: %v", err)
	}

	if got.Status.ConnectionStatus != blockstoriov1alpha1.NodeConnectionStatusOnline {
		t.Errorf("ConnectionStatus = %q, want ONLINE", got.Status.ConnectionStatus)
	}

	if got.Status.LastHeartbeatTime == nil ||
		time.Since(got.Status.LastHeartbeatTime.Time) > controller.NodeMonitorGracePeriod {
		t.Errorf("healthy node has no fresh heartbeat; the watchdog would flip it OFFLINE")
	}
}
