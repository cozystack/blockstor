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

package k8s

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// The reads compare the satellite running on a node with the one that
// reported each replica, so both incarnations must reach them.
func TestCrdToWireCarriesSatelliteIncarnation(t *testing.T) {
	t.Parallel()

	node := crdToWireNode(&crdv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Status:     crdv1alpha1.NodeStatus{SatelliteIncarnation: "inc-2"},
	})
	if node.SatelliteIncarnation != "inc-2" {
		t.Errorf("node incarnation = %q, want inc-2", node.SatelliteIncarnation)
	}

	rsc := crdToWireResource(&crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-x.worker-1"},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "worker-1"},
		Status:     crdv1alpha1.ResourceStatus{SatelliteIncarnation: "inc-1"},
	})
	if rsc.SatelliteIncarnation != "inc-1" {
		t.Errorf("replica incarnation = %q, want inc-1", rsc.SatelliteIncarnation)
	}
}
