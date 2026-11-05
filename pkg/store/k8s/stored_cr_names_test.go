// SPDX-License-Identifier: Apache-2.0

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
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// mixedCaseParents is a fake API server holding a definition registered as
// Pvc-Mix and a node registered as Node-1, both under their folded CR names.
func mixedCaseParents(t *testing.T) ctrlclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Mix")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Mix")

	node := crdv1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Node-1")}}
	k8s.SetOriginalName(&node.ObjectMeta, "Node-1")

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rd, &node).Build()
}

// A replica of a mixed-case definition stores the definition's and the
// node's CR names in spec, whatever spelling it was created in: the
// controller and the satellite Get the definition and the node by those
// values and compare them with CR names, and the satellite names the DRBD
// resource and its volumes after the definition's. The wire still shows
// the registered spellings, and a later wire write keeps both.
func TestReplicaStoresTheCRNamesOfItsParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cli := mixedCaseParents(t)
	s := k8s.New(cli).Resources()

	if err := s.Create(ctx, &apiv1.Resource{Name: "PVC-MIX", NodeName: "NODE-1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	assertSpec := func(stage string) {
		t.Helper()

		var list crdv1alpha1.ResourceList
		if err := cli.List(ctx, &list); err != nil || len(list.Items) != 1 {
			t.Fatalf("%s: list replicas = %v, %v; want one", stage, list.Items, err)
		}

		spec := list.Items[0].Spec
		if spec.ResourceDefinitionName != "pvc-mix" || spec.NodeName != "node-1" {
			t.Errorf("%s: spec names %q/%q, want the CR names pvc-mix/node-1",
				stage, spec.ResourceDefinitionName, spec.NodeName)
		}
	}

	assertSpec("create")

	got, err := s.Get(ctx, "pvc-mix", "node-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "Pvc-Mix" || got.NodeName != "Node-1" {
		t.Errorf("wire names %q/%q, want the registered Pvc-Mix/Node-1", got.Name, got.NodeName)
	}

	if _, leaked := got.Annotations[k8s.AnnotationLinstorRDName]; leaked {
		t.Errorf("wire annotations %v carry the store-internal display spelling", got.Annotations)
	}

	// A wire that carries any annotation replaces the user keys: the
	// display spellings are store-internal and must survive it.
	got.Props = map[string]string{"Aux/k": "v"}
	got.Annotations = map[string]string{"example.com/user": "u"}

	if err := s.Update(ctx, &got); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertSpec("update")
	assertShown := func(stage string) {
		t.Helper()

		shown, err := s.Get(ctx, "pvc-mix", "node-1")
		if err != nil || shown.Name != "Pvc-Mix" || shown.NodeName != "Node-1" {
			t.Errorf("%s: shown as %q/%q, %v; want Pvc-Mix/Node-1", stage, shown.Name, shown.NodeName, err)
		}
	}

	assertShown("update")

	if err := s.PatchResourceSpec(ctx, "Pvc-Mix", "Node-1", func(r *apiv1.Resource) error {
		r.Props["Aux/k"] = "w"
		r.Annotations = map[string]string{"example.com/user": "p"}

		return nil
	}); err != nil {
		t.Fatalf("PatchResourceSpec: %v", err)
	}

	assertSpec("patch")
	assertShown("patch")

	again, err := s.Get(ctx, "Pvc-Mix", "Node-1")
	if err != nil || again.Name != "Pvc-Mix" || again.NodeName != "Node-1" {
		t.Errorf("after the writes: %q/%q, %v; want Pvc-Mix/Node-1", again.Name, again.NodeName, err)
	}
}

// A snapshot of a mixed-case definition stores the definition's CR name the
// same way and shows the registered spelling.
func TestSnapshotStoresTheCRNameOfItsDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cli := mixedCaseParents(t)
	s := k8s.New(cli).Snapshots()

	if err := s.Create(ctx, &apiv1.Snapshot{Name: "snap-1", ResourceName: "PVC-MIX", Nodes: []string{"node-1"}}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	stored := func(stage string) {
		t.Helper()

		var crd crdv1alpha1.Snapshot
		if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-mix.snap-1"}, &crd); err != nil {
			t.Fatalf("%s: get the snapshot CR: %v", stage, err)
		}

		if crd.Spec.ResourceDefinitionName != "pvc-mix" {
			t.Errorf("%s: spec definition %q, want the CR name pvc-mix", stage, crd.Spec.ResourceDefinitionName)
		}
	}

	stored("create")

	got, err := s.Get(ctx, "pvc-mix", "snap-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ResourceName != "Pvc-Mix" {
		t.Errorf("wire definition %q, want the registered Pvc-Mix", got.ResourceName)
	}

	got.Props = map[string]string{"Aux/k": "v"}
	got.Annotations = map[string]string{"example.com/user": "u"}

	if err := s.Update(ctx, &got); err != nil {
		t.Fatalf("Update: %v", err)
	}

	stored("update")

	shown, err := s.Get(ctx, "pvc-mix", "snap-1")
	if err != nil || shown.ResourceName != "Pvc-Mix" {
		t.Errorf("after an update carrying an annotation: shown as %q, %v; want Pvc-Mix", shown.ResourceName, err)
	}
}
