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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

func replica(rd, node string, flags ...string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd,
			NodeName:               node,
			Flags:                  flags,
		},
	}
}

func autoSnapshotFixture(t *testing.T, objs ...client.Object) (*controllerpkg.AutoSnapshotRunnable, client.Client) {
	t.Helper()

	cli := fake.NewClientBuilder().
		WithScheme(newSnapshotControllerScheme(t)).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(objs...).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}

	return &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}, cli
}

func tick(t *testing.T, r *controllerpkg.AutoSnapshotRunnable) {
	t.Helper()

	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
}

// LINSTOR takes an auto-snapshot like any other: on every diskful replica
// of the definition. The snapshot lists those nodes, as a REST create
// defaults them, so the orchestration has nodes to take it on: the
// diskless witness and an INACTIVE replica are left out.
func TestAutoSnapshotTargetsEveryActiveDiskfulReplica(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-targets", 15, "")

	r, cli := autoSnapshotFixture(t, rd,
		replica(rd.Name, "n2"),
		replica(rd.Name, "n1"),
		replica(rd.Name, "n3", apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker),
		replica(rd.Name, "n4", apiv1.ResourceFlagInactive),
		replica("pvc-other", "n5"),
	)

	tick(t, r)

	snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snaps))
	}

	if got := snaps[0].Spec.Nodes; !slices.Equal(got, []string{"n1", "n2"}) {
		t.Errorf("auto-snapshot nodes = %v, want [n1 n2]", got)
	}

	if !snaps[0].Spec.SuspendIO {
		t.Errorf("auto-snapshot not created suspended, unlike a REST create")
	}
}

// The auto-snapshot goes through the same suspend, take and resume as a
// REST one, and ends SUCCESSFUL, which linstor-csi waits for before it
// restores from it.
func TestAutoSnapshotIsTakenToSuccessful(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-taken", 15, "")

	runnable, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1"), replica(rd.Name, "n2"))

	tick(t, runnable)

	snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snaps))
	}

	name := snaps[0].Name
	r := &controllerpkg.SnapshotReconciler{Client: cli, Scheme: cli.Scheme()}

	// What every satellite reports at each phase, standing in for them.
	stamp := func(acked, ready bool) {
		t.Helper()

		snap := getSnap(t, cli, name)
		snap.Status.NodeStatus = nil

		for _, node := range snap.Spec.Nodes {
			snap.Status.NodeStatus = append(snap.Status.NodeStatus, blockstoriov1alpha1.SnapshotPerNodeStatus{
				NodeName: node, SuspendIOAcked: acked, Ready: ready, CreateTimestamp: 1,
			})
		}

		if err := cli.Status().Update(context.Background(), snap); err != nil {
			t.Fatalf("stamp per-node status: %v", err)
		}
	}

	stamp(true, false)
	reconcileSnapshot(t, r, name)

	if !getSnap(t, cli, name).Spec.TakeSnapshot {
		t.Fatalf("no take ordered once every node suspended: %+v", getSnap(t, cli, name).Spec)
	}

	stamp(true, true)
	reconcileSnapshot(t, r, name)
	stamp(false, true)
	reconcileSnapshot(t, r, name)

	if !isMarkedSuccessful(t, cli, name) {
		t.Fatalf("auto-snapshot not SUCCESSFUL after a completed take: %+v", getSnap(t, cli, name))
	}
}

// LINSTOR skips the auto-snapshot of a definition one of whose replicas
// sits on an offline node: the take could only suspend I/O on the
// reachable replicas until it timed out. Nothing is booked either, so
// the next tick tries again.
func TestAutoSnapshotSkipsWhileAReplicaNodeIsOffline(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-offline", 15, "")

	offline := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n2"},
		Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOffline},
	}

	r, cli := autoSnapshotFixture(t, rd, offline, replica(rd.Name, "n1"), replica(rd.Name, "n2"))

	tick(t, r)

	if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 0 {
		t.Fatalf("auto-snapshot taken with a replica's node offline: %d", len(snaps))
	}

	var updated blockstoriov1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), types.NamespacedName{Name: rd.Name}, &updated); err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got := updated.Spec.Props[controllerpkg.PropAutoSnapshotNextID]; got != "" {
		t.Errorf("NextAutoId booked for a skipped auto-snapshot: %q", got)
	}
}

// With no replica that holds data there is nothing to snapshot; LINSTOR
// fails the create, and no snapshot that could never complete is left.
func TestAutoSnapshotSkipsWithoutADiskfulReplica(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-diskless", 15, "")

	r, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1", apiv1.ResourceFlagDiskless))

	tick(t, r)

	if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 0 {
		t.Fatalf("auto-snapshot created with no diskful replica: %+v", snaps[0].Spec)
	}
}
