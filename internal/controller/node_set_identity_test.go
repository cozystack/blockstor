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
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The direct node probe finds a node by the CR name its wire name maps to:
// My_Node by its own spelling and by its CR name, never by my_node, which
// hashes to another CR.
func TestNodeProbeKeepsTwoNodesOfOneLowercaseApart(t *testing.T) {
	t.Parallel()

	node := &blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: naming.Key("My_Node")}}
	k8s.SetOriginalName(&node.ObjectMeta, "My_Node")

	r := &ResourceDefinitionReconciler{APIReader: fake.NewClientBuilder().WithScheme(bug048Scheme(t)).WithObjects(node).Build()}

	for name, want := range map[string]bool{"My_Node": true, naming.Key("My_Node"): true, "my_node": false} {
		probe, err := r.probeNodeDirect(context.Background(), name)
		if err != nil || probe.found != want {
			t.Errorf("probe %q = %+v, %v; want found=%v", name, probe, err, want)
		}
	}
}

// A replica on a node the store lists is live without a direct probe: the
// replica names the node by its CR name, the list by the registered
// spelling, and the two are one node.
func TestAReplicaOnAListedMixedCaseNodeIsNotProbed(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).Build()
	st := k8s.New(cli)

	if err := st.Nodes().Create(context.Background(), &apiv1.Node{Name: "Node-2", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	var probes atomic.Int32

	r := &ResourceDefinitionReconciler{Store: st, APIReader: interceptor.NewClient(cli, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			probes.Add(1)

			return c.List(ctx, list, opts...)
		},
	})}

	_, known, err := r.nodeSets(context.Background())
	if err != nil {
		t.Fatalf("nodeSets: %v", err)
	}

	replicas := []apiv1.Resource{{Name: "pvc-x", NodeName: "node-2"}, {Name: "pvc-y", NodeName: "Node-2"}}

	live, ghosts, err := r.confirmGhostReplicas(context.Background(), replicas, known)
	if err != nil || len(live) != 2 || len(ghosts) != 0 {
		t.Fatalf("live %v, ghosts %v, %v; want both replicas live", live, ghosts, err)
	}

	if n := probes.Load(); n != 0 {
		t.Errorf("a replica on a listed node took %d direct probes", n)
	}
}

// The witness selector never picks a node that hosts a diskful replica of
// the definition, whichever spelling the replica listing shows for it.
func TestTheWitnessSelectorExcludesADiskfulMixedCaseNode(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).Build()
	st := k8s.New(cli)
	ctx := context.Background()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-2", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "Node-2"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	r := &ResourceDefinitionReconciler{Store: st}

	got, err := r.pickTiebreakerNodeForRD(ctx, "pvc-x", nil)
	if err != nil || got != "" {
		t.Errorf("witness node = %q, %v; want none, Node-2 hosts a diskful replica", got, err)
	}
}

// The caller's hosting set excludes its nodes whichever spelling it uses.
func TestTheWitnessSelectorExcludesAHostingNodeInAnySpelling(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(bug048Scheme(t)).Build())

	if err := st.Nodes().Create(context.Background(), &apiv1.Node{Name: "Node-2", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	r := &ResourceDefinitionReconciler{Store: st}

	got, err := r.pickTiebreakerNode(context.Background(), map[string]bool{"NODE-2": true})
	if err != nil || got != "" {
		t.Errorf("witness node = %q, %v; want none, NODE-2 already hosts a replica", got, err)
	}
}

// Auto-diskful drops a replica on an EVICTED node whichever spelling the
// replica shows for it: an adopted row stores the node's CR name, node-1,
// with no display annotation, while the node list shows Node-1.
func TestAutoDiskfulDropsAReplicaOnAnEvictedMixedCaseNode(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).Build()
	st := k8s.New(cli)
	ctx := context.Background()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-1", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagEvicted}}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := cli.Create(ctx, &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-x.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "node-1"},
	}); err != nil {
		t.Fatalf("seed adopted replica: %v", err)
	}

	r := &AutoDiskfulReconciler{Store: st}

	disabled, err := r.disabledNodeSet(ctx)
	if err != nil {
		t.Fatalf("disabledNodeSet: %v", err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-x")
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas %v, %v", replicas, err)
	}

	replicas = append(replicas, apiv1.Resource{Name: "pvc-y", NodeName: "Node-1"})

	if diskful, _ := splitDiskfulAndCandidates(replicas, disabled); len(diskful) != 0 {
		t.Errorf("diskful %v; want both replicas on the evicted node dropped", diskful)
	}
}
