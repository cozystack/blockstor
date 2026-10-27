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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

func mixedCaseReplica(rd, node string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(rd) + "." + node},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
	}
}

// A definition registered as Pvc-Mix is stored under pvc-mix, and its
// replicas keep the registered spelling. The controller counts them all,
// whichever spelling each was written with, and nothing else.
func TestControllerSeesReplicasOfAMixedCaseDefinition(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).WithObjects(
		mixedCaseReplica("Pvc-Mix", "n1"), mixedCaseReplica("pvc-mix", "n2"), mixedCaseReplica("pvc-other", "n1"),
	).Build()

	r := &ResourceDefinitionReconciler{Client: cli, APIReader: cli}

	got, err := r.listReplicasDirect(context.Background(), k8s.Name("Pvc-Mix"))
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("replicas of Pvc-Mix = %v, want the two on n1 and n2", got)
	}
}

// A replica event enqueues the definition under the name its CR has.
func TestReplicaEventEnqueuesTheFoldedDefinition(t *testing.T) {
	t.Parallel()

	r := &ResourceDefinitionReconciler{}

	reqs := r.enqueueRDForResource(context.Background(), mixedCaseReplica("Pvc-Mix", "n1"))
	if len(reqs) != 1 || reqs[0].Name != k8s.Name("Pvc-Mix") {
		t.Errorf("enqueued %v, want %s", reqs, k8s.Name("Pvc-Mix"))
	}
}
