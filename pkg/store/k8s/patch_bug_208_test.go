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

package k8s_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 208 (pokeV16): operator-set typed Node spec fields
// (Spec.DRBDPortRange, Spec.DRBDMinorRange) live on the Node CRD but
// have NO counterpart on the wire `apiv1.Node` shape. Both
// `nodes.Update` and `nodes.PatchNodeSpec` (Bug 205 helper) do
// `existing.Spec = wireToCRDNodeSpec(in)` — which produces a spec
// missing these typed pointers. A routine REST `linstor n modify
// --property X=Y` therefore silently wipes operator-configured
// per-node port/minor pinning, dropping the resource reconciler back
// to the cluster-wide defaults (7000-7999 / 1000-1099) on the next
// resource Reconcile tick. New DRBD resources will then collide with
// whatever ports/minors the operator was holding aside for hardware-
// firewalled satellites.
//
// Same root-cause class as Bug 206 (Spec.Volumes carry-across), but
// for a different sibling store that the Bug 206 commit explicitly
// audited and dismissed as "out of scope".

func TestBug208_PatchNodeSpecWipesTypedPortRange(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	// Node CRD with an operator-pinned DRBD port range.
	seed := crdv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec: crdv1alpha1.NodeSpec{
			Type: "SATELLITE",
			DRBDPortRange: &crdv1alpha1.PortRange{
				Min: 7500,
				Max: 7599,
			},
			DRBDMinorRange: &crdv1alpha1.PortRange{
				Min: 1100,
				Max: 1199,
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Node{}).
		Build()

	s := k8s.New(cli)

	// REST handler does a routine `linstor n set-property`.
	err := s.Nodes().PatchNodeSpec(context.Background(), "node-a", func(n *apiv1.Node) error {
		if n.Props == nil {
			n.Props = map[string]string{}
		}
		n.Props["Aux/zone"] = "rack1"
		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got crdv1alpha1.Node
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "node-a"}, &got); err != nil {
		t.Fatalf("get post-patch: %v", err)
	}

	if got.Spec.DRBDPortRange == nil {
		t.Fatalf("Spec.DRBDPortRange was WIPED by the routine prop bump (Bug 208); operator's port pin lost")
	}
	if got.Spec.DRBDPortRange.Min != 7500 || got.Spec.DRBDPortRange.Max != 7599 {
		t.Fatalf("Spec.DRBDPortRange clobbered: got [%d,%d] want [7500,7599]",
			got.Spec.DRBDPortRange.Min, got.Spec.DRBDPortRange.Max)
	}
	if got.Spec.DRBDMinorRange == nil {
		t.Fatalf("Spec.DRBDMinorRange was WIPED by the routine prop bump (Bug 208); operator's minor pin lost")
	}
}

func TestBug208_UpdateWipesTypedPortRange(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
		Spec: crdv1alpha1.NodeSpec{
			Type: "SATELLITE",
			DRBDPortRange: &crdv1alpha1.PortRange{
				Min: 7500,
				Max: 7599,
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Node{}).
		Build()

	s := k8s.New(cli)

	cur, err := s.Nodes().Get(context.Background(), "node-b")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if cur.Props == nil {
		cur.Props = map[string]string{}
	}
	cur.Props["Aux/zone"] = "rack1"

	if err := s.Nodes().Update(context.Background(), &cur); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.Node
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "node-b"}, &got); err != nil {
		t.Fatalf("get post-update: %v", err)
	}

	if got.Spec.DRBDPortRange == nil {
		t.Fatalf("Spec.DRBDPortRange was WIPED by the routine Update (Bug 208); operator's port pin lost")
	}
}
