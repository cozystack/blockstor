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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// Bug 266 (HIGH): same pathology as Bug 268 but for the DRBD TCP
// port. Allocating ports at per-node scope lets one RD silently
// re-use a port already taken by a sibling RD on a DIFFERENT node
// — and because the satellite's .res file writes the local port
// across all peer blocks, the two RDs end up advertising the same
// port AND conflicting node-ids on the wire. drbdadm adjust
// rejects, the resource never connects, the failure leaks into
// every neighbouring RD that shares the port number.
//
// Fix shape: same as Bug 268. Allocate ONE port per RD (cluster
// scope), persist on the parent RD CRD, every Resource inherits it.
// Two distinct RDs MUST land on distinct ports.
func TestBug266DRBDPortPerRDUniqueAcrossCluster(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Two RDs, two nodes each. Pre-seed RD-A on w1 with the lowest
	// free port (7000) so when RD-B's allocator runs it sees:
	//   w1 taken={7000}  → would pick 7001
	//   w3 taken={}      → would pick 7000
	// Per-node allocation hands RD-B port 7001 on w1 but 7000 on w3,
	// AND that 7000 collides cluster-wide with RD-A's port.
	// Per-RD allocation must pick ONE port per RD, both RDs distinct.
	rdA := &blockstoriov1alpha1.ResourceDefinition{}
	rdA.Name = "pvc-bug266-rdA"

	if err := cli.Create(ctx, rdA); err != nil {
		t.Fatalf("create rdA: %v", err)
	}

	rdB := &blockstoriov1alpha1.ResourceDefinition{}
	rdB.Name = "pvc-bug266-rdB"

	if err := cli.Create(ctx, rdB); err != nil {
		t.Fatalf("create rdB: %v", err)
	}

	// Seed RD-A's replicas with the lowest port to bias the per-node
	// taken set differently across nodes.
	preSeedPort(ctx, t, cli, rdA.Name, "w1", 7000)
	preSeedPort(ctx, t, cli, rdA.Name, "w2", 7000)

	// Stamp RD-A's port onto its RD Status so the per-RD allocator
	// sees it as a cluster-taken value when computing RD-B's port.
	rdA.Status.DRBDPort = int32Ptr(7000)
	if err := cli.Status().Update(ctx, rdA); err != nil {
		t.Fatalf("stamp rdA status: %v", err)
	}

	// RD-B lands on w1 and w3; w3 has no other replicas → naive
	// per-node allocator picks 7000 on w3, which collides cluster-
	// wide with RD-A's 7000.
	for _, node := range []string{"w1", "w3"} {
		create(ctx, t, cli, rdB.Name, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdB.Name)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	var (
		bPort     *int32
		bPortNode string
	)

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdB.Name {
			continue
		}

		got := list.Items[i].Status.DRBDPort
		if got == nil {
			t.Fatalf("%s: port not allocated", list.Items[i].Name)
		}

		// All RD-B replicas must share the same port.
		if bPort == nil {
			bPort = got
			bPortNode = list.Items[i].Spec.NodeName

			continue
		}

		if *got != *bPort {
			t.Errorf("RD-B port diverges across peers: %s=%d vs %s=%d "+
				"(per-RD scope violated — divergent ports break drbdadm adjust)",
				bPortNode, *bPort, list.Items[i].Spec.NodeName, *got)
		}
	}

	// RD-B's port MUST differ from RD-A's port (cluster-wide
	// uniqueness across RDs). RD-A's status.drbdPort == 7000.
	if bPort != nil && *bPort == 7000 {
		t.Errorf("RD-B picked port 7000, colliding with RD-A's cluster-wide "+
			"port (RD-A.Status.DRBDPort=7000). Got bPort=%d", *bPort)
	}
}

// TestBug266DRBDPortRecordedOnRD pins the persistence-on-RD half of
// the contract: the allocator must stamp the chosen port on the
// parent RD's Status so subsequent Resources inherit it without
// re-running the cluster scan.
func TestBug266DRBDPortRecordedOnRD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	rdName := "pvc-bug266-rd-persist"

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

	if gotRD.Status.DRBDPort == nil {
		t.Fatalf("RD.Status.DRBDPort not stamped after per-RD allocation; " +
			"the allocator must persist the chosen value on the parent RD")
	}

	wantPort := *gotRD.Status.DRBDPort

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		got := list.Items[i].Status.DRBDPort
		if got == nil || *got != wantPort {
			t.Errorf("replica %q port=%v, want %d (must equal RD's allocated port)",
				list.Items[i].Name, got, wantPort)
		}
	}
}

// preSeedPort creates a Resource with a stamped Status.DRBDPort so
// the allocator sees it as a port-taken on the named node.
func preSeedPort(ctx context.Context, t *testing.T, cli client.Client, rdName, node string, port int32) {
	t.Helper()

	r := &blockstoriov1alpha1.Resource{}
	r.Name = rdName + "." + node
	r.Spec.ResourceDefinitionName = rdName
	r.Spec.NodeName = node

	if err := cli.Create(ctx, r); err != nil {
		t.Fatalf("preSeedPort create %s.%s: %v", rdName, node, err)
	}

	zero := int32(0)
	minor := int32(1000)

	r.Status.DRBDNodeID = &zero
	r.Status.DRBDPort = &port
	r.Status.DRBDMinor = &minor

	if err := cli.Status().Update(ctx, r); err != nil {
		t.Fatalf("preSeedPort status update %s.%s: %v", rdName, node, err)
	}
}

func int32Ptr(v int32) *int32 { return &v }
