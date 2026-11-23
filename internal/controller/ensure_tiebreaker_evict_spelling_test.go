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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A witness on an EVICTED node is moved to a healthy spare when the node
// is registered as Node-1: the replica stores the node's CR name, node-1,
// while the node list shows Node-1, and the two are one node.
func TestAWitnessOnAnEvictedMixedCaseNodeIsReplaced(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := k8s.New(cli)
	ctx := context.Background()

	for _, n := range []apiv1.Node{
		{Name: "Node-1", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagEvicted}},
		{Name: "Node-2", Type: apiv1.NodeTypeSatellite},
		{Name: "Node-3", Type: apiv1.NodeTypeSatellite},
		{Name: "Node-4", Type: apiv1.NodeTypeSatellite},
	} {
		if err := st.Nodes().Create(ctx, &n); err != nil {
			t.Fatalf("seed node %s: %v", n.Name, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-ev"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for _, n := range []string{"Node-2", "Node-3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-ev", NodeName: n}); err != nil {
			t.Fatalf("seed diskful on %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-ev", NodeName: "Node-1",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-ev"}, &rd); err != nil {
		t.Fatalf("get definition: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}
	if err := rec.EnsureTiebreaker(ctx, &rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	all, err := st.Resources().ListByDefinition(ctx, "pvc-ev")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	var witnesses []string

	for i := range all {
		if slices.Contains(all[i].Flags, apiv1.ResourceFlagTieBreaker) {
			witnesses = append(witnesses, all[i].NodeName)
		}
	}

	if !slices.Equal(witnesses, []string{"Node-4"}) {
		t.Errorf("witnesses on %v, want only the spare Node-4; replicas %v", witnesses, all)
	}
}
