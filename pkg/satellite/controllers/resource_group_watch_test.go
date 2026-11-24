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

package controllers

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A volume group's disk options, block-size above all, render into the
// .res of every replica of the group's definitions. An edit of the
// group re-renders this node's replicas of those definitions, and only
// those: a replica on another node, or of a definition in another
// group, is not this satellite's to wake.
func TestAResourceGroupEditReachesTheLocalReplicasOfItsDefinitions(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rg := &blockstoriov1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: "rg-1"}}
	inGroup := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a"},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: "Rg-1"},
	}
	otherGroup := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-b"},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: "rg-2"},
	}
	local := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-a", NodeName: "node-1"},
	}
	remote := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a.node-2"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-a", NodeName: "node-2"},
	}
	unrelated := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-b.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-b", NodeName: "node-1"},
	}

	r := &ResourceReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(rg, inGroup, otherGroup, local, remote, unrelated).Build(),
		Config: Config{NodeName: "node-1"},
	}

	reqs := r.enqueueResourcesForRG(t.Context(), rg)

	names := make([]string, 0, len(reqs))
	for _, req := range reqs {
		names = append(names, req.Name)
	}

	if !slices.Equal(names, []string{local.Name}) {
		t.Errorf("an edit of rg-1 enqueued %v, want only %s", names, local.Name)
	}
}
