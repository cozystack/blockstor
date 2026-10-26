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

package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

func watchGroup(stack ...string) *blockstoriov1alpha1.ResourceGroup {
	return &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-watch"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{LayerStack: stack},
		},
	}
}

// Only a change to a group's layer stack re-checks its RDs.
func TestGroupLayerStackChangedFiresOnlyOnAStackChange(t *testing.T) {
	t.Parallel()

	pred := groupLayerStackChanged()

	if !pred.Update(event.UpdateEvent{ObjectOld: watchGroup("DRBD", "STORAGE"), ObjectNew: watchGroup("DRBD", "LUKS", "STORAGE")}) {
		t.Error("a group that gained LUKS did not re-check its RDs")
	}

	if pred.Update(event.UpdateEvent{ObjectOld: watchGroup("DRBD", "STORAGE"), ObjectNew: watchGroup("DRBD", "STORAGE")}) {
		t.Error("a group update that left the stack alone re-checked its RDs")
	}
}

// The group's RDs are enqueued under any spelling of its name, and no other.
func TestEnqueueRDsInGroupFindsItsRDsOnly(t *testing.T) {
	t.Parallel()

	rd := func(name, group string) *blockstoriov1alpha1.ResourceDefinition {
		return &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: group},
		}
	}

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).
		WithObjects(rd("pvc-a", "rg-watch"), rd("pvc-b", "RG-Watch"), rd("pvc-c", "rg-other"), rd("pvc-d", "")).Build()
	r := &ResourceDefinitionReconciler{Client: cli}

	got := map[string]bool{}
	for _, req := range r.enqueueRDsInGroup(context.Background(), watchGroup()) {
		got[req.Name] = true
	}

	if len(got) != 2 || !got["pvc-a"] || !got["pvc-b"] {
		t.Errorf("enqueued %v, want pvc-a and pvc-b", got)
	}
}
