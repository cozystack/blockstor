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
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// Scenario 3.W05: cluster-scope `TcpPortAutoRange` on the
// ControllerConfig singleton must constrain the controller's
// `allocateDRBDFields` port allocator for any Node that doesn't
// carry a per-node override. The four pin-points below capture
// the contract described in tests/scenarios/wave2-03-networking.md
// §3.W05 and the implementation pointer in
// tests/advanced-config-scenarios.md §5:
//
//  1. The cluster-scope range overrides the compiled-in 7000-7999
//     default for a vanilla node.
//  2. The per-node range still wins when both are set — cluster
//     scope is a default, not an override.
//  3. A malformed cluster-scope value surfaces as an actionable
//     error so the operator notices the typo (not silent
//     fallback).
//  4. Once the cluster-scope range is fully consumed, the
//     allocator returns `ErrPortPoolExhausted` rather than panic
//     or invent an out-of-range port — the operator-facing
//     "widen the range" signal.

// TestTcpPortAutoRangeConstrainsAllocator pins fact 1: an
// otherwise-bare cluster (no per-Node overrides) draws ports
// from the cluster-scope `TcpPortAutoRange` rather than the
// hard-coded 7000-7999 default.
func TestTcpPortAutoRangeConstrainsAllocator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"TcpPortAutoRange": "7900-7999"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(cfg).
		Build()

	rd := "pvc-cluster-range"
	create(ctx, t, cli, rd, "n1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	res := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n1"}, res); err != nil {
		t.Fatalf("get res: %v", err)
	}

	if res.Status.DRBDPort == nil {
		t.Fatalf("DRBDPort not allocated")
	}

	port := *res.Status.DRBDPort
	if port < 7900 || port > 7999 {
		t.Errorf("port %d outside cluster TcpPortAutoRange [7900,7999]", port)
	}
}

// TestTcpPortAutoRangePerNodeOverrideWins pins fact 2: a Node
// with its own `DrbdOptions/TcpPortRange` MUST keep its local
// range even when the cluster scope sets `TcpPortAutoRange`.
// Cluster scope is a default for vanilla nodes, never a
// silent rewrite of explicit per-node operator intent.
func TestTcpPortAutoRangePerNodeOverrideWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"TcpPortAutoRange": "7900-7999"},
		},
	}

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{"DrbdOptions/TcpPortRange": "9000-9000"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(cfg, node).
		Build()

	rd := "pvc-pernode-wins"
	create(ctx, t, cli, rd, "n1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	res := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n1"}, res); err != nil {
		t.Fatalf("get res: %v", err)
	}

	if got := *res.Status.DRBDPort; got != 9000 {
		t.Errorf("per-node range must win: got %d, want 9000", got)
	}
}

// TestTcpPortAutoRangeMalformedSurfaces pins fact 3: a typo
// such as "7900-bogus" in the cluster-scope prop must NOT
// silently fall back to defaults; the operator needs to see
// the misconfig as an error.
func TestTcpPortAutoRangeMalformedSurfaces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"TcpPortAutoRange": "7900-bogus"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cfg).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	_, _, err := rec.RangePropWithClusterFallback(ctx, "ghost-node",
		"DrbdOptions/TcpPortRange", "TcpPortAutoRange",
		drbd.DefaultPortMin, drbd.DefaultPortMax)
	if err == nil {
		t.Fatalf("malformed cluster-scope range must surface as error; got nil")
	}
}

// TestTcpPortAutoRangeExhaustedActionable pins fact 4: when
// every port in a narrow cluster-scope range is already taken
// the allocator MUST return `drbd.ErrPortPoolExhausted`
// (operator-facing, sentinel-checkable) rather than panic or
// hand out an out-of-range port.
func TestTcpPortAutoRangeExhaustedActionable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"TcpPortAutoRange": "7900-7900"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(cfg).
		Build()

	// First RD consumes the only slot (7900).
	create(ctx, t, cli, "pvc-fills-pool", "n1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, "pvc-fills-pool")

	// Sanity: the first replica picked 7900.
	first := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-fills-pool.n1"}, first); err != nil {
		t.Fatalf("get first: %v", err)
	}

	if first.Status.DRBDPort == nil || *first.Status.DRBDPort != 7900 {
		t.Fatalf("first port: got %v, want 7900", first.Status.DRBDPort)
	}

	// Second RD on the same node must hit the pool-exhausted path.
	create(ctx, t, cli, "pvc-overflows", "n1")

	second := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-overflows.n1"}, second); err != nil {
		t.Fatalf("get second: %v", err)
	}

	_, err := rec.EnsureDRBDIDsForTest(ctx, second, []blockstoriov1alpha1.Resource{*first, *second})
	if err == nil {
		t.Fatalf("exhausted pool must surface error; got nil")
	}

	if !errors.Is(err, drbd.ErrPortPoolExhausted) {
		t.Errorf("got %v, want ErrPortPoolExhausted (operator-actionable sentinel)", err)
	}
}
