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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestRangePropMissingNodeFallsBackToDefaults: a port allocator
// looking up a node that hasn't yet registered (Hello hasn't run)
// must use the controller-wide defaults rather than failing the
// allocation. Otherwise a satellite-restart race would deadlock
// the first replica's reconcile.
func TestRangePropMissingNodeFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	low, high, err := rec.RangeProp(context.Background(),
		"ghost-node", "DrbdOptions/TcpPortRange", 7000, 8000)
	if err != nil {
		t.Fatalf("RangeProp on missing node: got %v, want defaults", err)
	}

	if low != 7000 || high != 8000 {
		t.Errorf("missing node fallback: got [%d,%d], want [7000,8000]", low, high)
	}
}

// TestRangePropMissingPropFallsBackToDefaults: node exists but the
// range prop isn't set (vanilla cluster) → defaults.
func TestRangePropMissingPropFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
			// No DrbdOptions/TcpPortRange prop.
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeCRD).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	low, high, err := rec.RangeProp(context.Background(),
		"n1", "DrbdOptions/TcpPortRange", 7000, 8000)
	if err != nil {
		t.Fatalf("RangeProp without prop: %v", err)
	}

	if low != 7000 || high != 8000 {
		t.Errorf("missing prop fallback: got [%d,%d], want [7000,8000]", low, high)
	}
}

// TestRangePropParsesValidValue: an explicit "min-max" prop
// overrides the defaults — the path piraeus-operator uses to give
// each node its own port pool (cozystack runs multiple satellite
// pods per host, each on a distinct range).
func TestRangePropParsesValidValue(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
			Props: map[string]string{
				"DrbdOptions/TcpPortRange": "9000-9999",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeCRD).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	low, high, err := rec.RangeProp(context.Background(),
		"n1", "DrbdOptions/TcpPortRange", 7000, 8000)
	if err != nil {
		t.Fatalf("RangeProp: %v", err)
	}

	if low != 9000 || high != 9999 {
		t.Errorf("got [%d,%d], want [9000,9999]", low, high)
	}
}

// TestRangePropMalformedValueReturnsError: a typo in the range
// (e.g. "9000-foo") must surface as an error, NOT silently fall
// through to defaults — operator needs to see the misconfig.
func TestRangePropMalformedValueReturnsError(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
			Props: map[string]string{
				"DrbdOptions/TcpPortRange": "9000-foo",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeCRD).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	_, _, err := rec.RangeProp(context.Background(),
		"n1", "DrbdOptions/TcpPortRange", 7000, 8000)
	if err == nil {
		t.Errorf("malformed range must surface as error; got nil")
	}
}
