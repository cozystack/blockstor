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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// preSeed creates a Resource and stamps its Status.DRBDMinor directly
// so the allocator sees it as a "taken minor" on the named node.
// Used to construct the per-node skew that produced Bug 268 on the
// live stand: when each node's pre-existing taken set has a different
// shape, the per-node allocator picks divergent minors for a new RD.
func preSeed(ctx context.Context, t *testing.T, cli client.Client, rdName, node string, minor int32) {
	t.Helper()

	r := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
		},
	}
	if err := cli.Create(ctx, r); err != nil {
		t.Fatalf("preSeed create %s.%s: %v", rdName, node, err)
	}

	zero := int32(0)
	port := int32(7000)

	r.Status = blockstoriov1alpha1.ResourceStatus{
		DRBDNodeID: &zero,
		DRBDPort:   &port,
		DRBDMinor:  &minor,
	}
	if err := cli.Status().Update(ctx, r); err != nil {
		t.Fatalf("preSeed status update %s.%s: %v", rdName, node, err)
	}
}

// Bug 268 (CRITICAL, data-correctness): the DRBD minor MUST be the
// same on every replica of an RD. The satellite's `.res` renderer
// uses ONE minor for every `on <node>` block in the file — the local
// satellite reads its own Status.DRBDMinor and writes that value
// across all hosts. When two satellites pick different minors for the
// same RD, their .res files diverge → drbdadm adjust on the peers
// rejects "minor mismatch" → connection state hangs at
// Connecting:Connecting forever → initial sync never starts.
//
// Pre-existing damage observed on the dev-kvaps live stand: testq.res
// had w1 minor 1002 and w2 minor 1001 for the SAME RD.
//
// Root cause: the historical allocator ran at per-node scope and each
// replica picked the lowest free local minor with no coordination
// across peers. The fix moves allocation to RD scope: ONE minor per
// RD, all replicas inherit it.
//
// This test pins the invariant. It MUST fail before the per-RD
// allocator lands.
func TestBug268DRBDMinorSameAcrossPeersOfOneRD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Pre-seed sibling RDs so each node's "taken minors" set looks
	// different — this is the exact production shape that produced
	// the live-stand bug. With per-node allocation w1 would pick
	// minor 1003 (skipping 1000-1002), w2 picks minor 1002 (skipping
	// 1000-1001) — divergent. Per-RD allocation must coordinate and
	// pick ONE value that's free on every node.
	preSeed(ctx, t, cli, "sibling-w1-vol0", "w1", 1000)
	preSeed(ctx, t, cli, "sibling-w1-vol1", "w1", 1001)
	preSeed(ctx, t, cli, "sibling-w1-vol2", "w1", 1002)
	preSeed(ctx, t, cli, "sibling-w2-vol0", "w2", 1000)
	preSeed(ctx, t, cli, "sibling-w2-vol1", "w2", 1001)

	rd := "pvc-bug268-minor-coherence"

	// Create the parent RD so the allocator can persist
	// Status.DRBDMinor / Status.DRBDPort on it.
	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rd
	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"w1", "w2"} {
		create(ctx, t, cli, rd, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	var (
		seen     *int32
		seenNode string
	)

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rd {
			continue
		}

		got := list.Items[i].Status.DRBDMinor
		if got == nil {
			t.Fatalf("%s: minor not allocated", list.Items[i].Name)
		}

		if seen == nil {
			seen = got
			seenNode = list.Items[i].Spec.NodeName

			continue
		}

		if *got != *seen {
			t.Errorf("minor diverges across peers of RD %q: %s=%d vs %s=%d "+
				"(per-RD scope contract violated — divergent minors break drbdadm adjust)",
				rd, seenNode, *seen, list.Items[i].Spec.NodeName, *got)
		}
	}
}

// TestBug268DRBDMinorRecordedOnRD asserts the chosen minor is
// persisted on the parent RD CRD too — that's the single source of
// truth the per-RD allocator publishes once, and every Resource
// reads from on subsequent reconciles. Without this, the per-node
// shadow allocators could still race and pick divergent values.
func TestBug268DRBDMinorRecordedOnRD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	rdName := "pvc-bug268-rd-persist"

	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rdName

	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"w1", "w2"} {
		create(ctx, t, cli, rdName, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdName)

	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if gotRD.Status.DRBDMinor == nil {
		t.Fatalf("RD.Status.DRBDMinor not stamped after per-RD allocation; " +
			"the allocator must persist the chosen value on the parent RD " +
			"so every Resource inherits the SAME minor")
	}

	wantMinor := *gotRD.Status.DRBDMinor

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		got := list.Items[i].Status.DRBDMinor
		if got == nil || *got != wantMinor {
			t.Errorf("replica %q minor=%v, want %d (must equal RD's allocated minor)",
				list.Items[i].Name, got, wantMinor)
		}
	}
}
