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

package controller_test

import (
	"context"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rd d flags the definition DELETE before it walks the replicas, and the
// definition object itself only goes after them. Between two replica
// deletes the reconciler sees two diskful replicas and no witness; on a
// definition being deleted it must not add one (nor remove one), or every
// pass of the delete adds a tiebreaker and takes it away again.
func TestEnsureTiebreakerLeavesDefinitionFlaggedDeleteAlone(t *testing.T) {
	t.Parallel()

	for name, seed := range map[string][]*apiv1.Resource{
		"no witness is added": {
			{Name: "pvc-going", NodeName: "n1"},
			{Name: "pvc-going", NodeName: "n2"},
		},
		"the witness is not removed": {
			{Name: "pvc-going", NodeName: "n1"},
			{
				Name: "pvc-going", NodeName: "n3",
				Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedNodes(t, ctx, st, "n1", "n2", "n3")

			err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "pvc-going", Flags: []string{apiv1.ResourceFlagDelete},
			})
			if err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			for _, res := range seed {
				if err := st.Resources().Create(ctx, res); err != nil {
					t.Fatalf("seed %s: %v", res.NodeName, err)
				}
			}

			rdCRD := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-going"},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Flags: []string{apiv1.ResourceFlagDelete},
				},
			}

			rec := &controllerpkg.ResourceDefinitionReconciler{
				Client: fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(rdCRD).Build(),
				Scheme: newScheme(t),
				Store:  st,
			}

			if err := rec.EnsureTiebreaker(ctx, rdCRD); err != nil {
				t.Fatalf("ensureTiebreaker: %v", err)
			}

			got, err := st.Resources().ListByDefinition(ctx, "pvc-going")
			if err != nil {
				t.Fatalf("list: %v", err)
			}

			nodes := make([]string, 0, len(got))
			for i := range got {
				nodes = append(nodes, got[i].NodeName)
			}

			want := make([]string, 0, len(seed))
			for _, res := range seed {
				want = append(want, res.NodeName)
			}

			slices.Sort(nodes)
			slices.Sort(want)

			if !slices.Equal(nodes, want) {
				t.Errorf("replicas = %v, want them untouched: %v", nodes, want)
			}
		})
	}
}
