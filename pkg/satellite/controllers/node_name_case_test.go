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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A replica written under its node's registered spelling, Node-1, belongs
// to the satellite started as node-1: the store keeps one Node for both.
func TestSatelliteTakesItsReplicaInAnySpelling(t *testing.T) {
	t.Parallel()

	pred := nodeNamePredicate("node-1")

	for spelling, want := range map[string]bool{"node-1": true, "Node-1": true, "NODE-1": true, "node-2": false} {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc.node-1"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: spelling},
		}

		if got := pred.Generic(event.GenericEvent{Object: res}); got != want {
			t.Errorf("replica on %q taken by node-1 = %v, want %v", spelling, got, want)
		}
	}
}
