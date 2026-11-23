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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// teardownFinalizer stands in for the satellite's finalizer, which
// holds a deleted replica's CRD until DRBD and the backing volume are
// torn down on the node.
const teardownFinalizer = "test.blockstor.org/teardown"

// holdAndDelete deletes the replica of rd on node while a finalizer
// keeps its CRD around, the window of a slow teardown or a down
// satellite.
func holdAndDelete(ctx context.Context, t *testing.T, cli client.Client, rd, node string) {
	t.Helper()

	var crd blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, types.NamespacedName{Name: naming.Key(rd + "." + node)}, &crd); err != nil {
		t.Fatalf("get replica %s: %v", node, err)
	}

	crd.Finalizers = append(crd.Finalizers, teardownFinalizer)
	if err := cli.Update(ctx, &crd); err != nil {
		t.Fatalf("hold replica %s: %v", node, err)
	}

	if err := cli.Delete(ctx, &crd); err != nil {
		t.Fatalf("delete replica %s: %v", node, err)
	}
}

// Both diskful replicas of a two-diskful definition are deleted while
// their satellites still hold them, and the witness has already gone.
// Upstream counts no replica flagged DELETE, so it sees nothing to
// break a tie between and creates no witness. A reconciler that
// counted the terminating replicas saw "2 diskful, no witness" and
// grew a fresh TIE_BREAKER, which linstor-csi's final listing then
// found undeleted, so the definition was never removed.
func TestEnsureTiebreakerIgnoresReplicasBeingDeleted(t *testing.T) {
	t.Parallel()

	for _, direct := range []bool{true, false} {
		name := "store"
		if direct {
			name = "api-reader"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			scheme := newScheme(t)
			cli := fake.NewClientBuilder().WithScheme(scheme).Build()
			st := k8s.New(cli)
			ctx := context.Background()

			for _, n := range []string{"n1", "n2", "n3", "n4"} {
				if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
					t.Fatalf("seed node %s: %v", n, err)
				}
			}

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-going"}); err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			for _, n := range []string{"n1", "n2"} {
				if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-going", NodeName: n}); err != nil {
					t.Fatalf("seed diskful on %s: %v", n, err)
				}

				holdAndDelete(ctx, t, cli, "pvc-going", n)
			}

			var rd blockstoriov1alpha1.ResourceDefinition
			if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-going"}, &rd); err != nil {
				t.Fatalf("get definition: %v", err)
			}

			rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}
			if direct {
				rec.APIReader = cli
			}

			if err := rec.EnsureTiebreaker(ctx, &rd); err != nil {
				t.Fatalf("EnsureTiebreaker: %v", err)
			}

			all, err := st.Resources().ListByDefinition(ctx, "pvc-going")
			if err != nil {
				t.Fatalf("list: %v", err)
			}

			for i := range all {
				if !slices.Contains(all[i].Flags, apiv1.ResourceFlagDelete) {
					t.Errorf("replica on %s is not being deleted (flags %v); a definition whose replicas are all going must not grow a new one",
						all[i].NodeName, all[i].Flags)
				}
			}
		})
	}
}
