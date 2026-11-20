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
	"bytes"
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
// witness, an INACTIVE replica and one being evacuated are left out.
func TestAutoSnapshotTargetsEveryActiveDiskfulReplica(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-targets", 15, "")

	r, cli := autoSnapshotFixture(t, rd,
		replica(rd.Name, "n2"),
		replica(rd.Name, "n1"),
		replica(rd.Name, "n3", apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker),
		replica(rd.Name, "n4", apiv1.ResourceFlagInactive),
		replica(rd.Name, "n6", apiv1.ResourceFlagTieBreaker),
		replica(rd.Name, "n7", "EVACUATE"),
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

// A satellite refuses to take a snapshot that does not record its
// replica's LUKS password, as the snapshot's bytes could never be read.
// The auto-snapshot copies the passwords of the replicas it is taken on,
// as the store does for a REST create, and only theirs.
func TestAutoSnapshotRecordsTheReplicasLUKSPasswords(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-luks", 15, "")

	key := func(fill byte) *blockstoriov1alpha1.LUKSKey {
		return &blockstoriov1alpha1.LUKSKey{
			KeyID:   "c7324c99567678f7",
			Wrapped: bytes.Repeat([]byte{fill}, 12+64+16),
			Origin:  blockstoriov1alpha1.LUKSKeyOriginGenerated,
		}
	}

	encrypted := func(node string, fill byte, flags ...string) *blockstoriov1alpha1.Resource {
		res := replica(rd.Name, node, flags...)
		res.Spec.Volumes = []blockstoriov1alpha1.ResourceVolumeSpec{{VolumeNumber: 0, LUKSKey: key(fill)}}

		return res
	}

	r, cli := autoSnapshotFixture(t, rd, encrypted("n1", 0xa1), encrypted("n2", 0xa2),
		encrypted("n3", 0xa3, apiv1.ResourceFlagInactive))

	tick(t, r)

	snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snaps))
	}

	got := map[string]byte{}
	for _, k := range snaps[0].Spec.LUKSKeys {
		got[k.NodeName] = k.Wrapped[0]
	}

	if len(got) != 2 || got["n1"] != 0xa1 || got["n2"] != 0xa2 {
		t.Errorf("auto-snapshot LUKS passwords by node = %#v, want n1 and n2 only", got)
	}
}

// LINSTOR refuses a snapshot on a pool whose provider cannot take one,
// as the REST create does. Taken anyway, a thick LVM pool would freeze
// I/O each tick and pile up copy-on-write overlays until the volume
// group fills, so the auto-snapshot is skipped.
func TestAutoSnapshotSkipsAPoolWithoutSnapshots(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-thick", 15, "")

	onPool := func(node, pool string) *blockstoriov1alpha1.Resource {
		res := replica(rd.Name, node)
		res.Spec.StoragePool = pool

		return res
	}

	pool := func(node, name string, snapshots bool) *blockstoriov1alpha1.StoragePool {
		return &blockstoriov1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: node + "." + name},
			Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: node, PoolName: name},
			Status:     blockstoriov1alpha1.StoragePoolStatus{SupportsSnapshots: snapshots},
		}
	}

	r, cli := autoSnapshotFixture(t, rd,
		onPool("n1", "thin"), onPool("n2", "thick"),
		pool("n1", "thin", true), pool("n2", "thick", false))

	tick(t, r)

	if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 0 {
		t.Fatalf("auto-snapshot taken on a pool without snapshots: %+v", snaps[0].Spec)
	}
}

// A definition whose delete is under way gets no new auto-snapshot.
func TestAutoSnapshotSkipsADefinitionBeingDeleted(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-delete-flag", 15, "")
	rd.Spec.Flags = []string{"DELETE"}

	r, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1"))

	tick(t, r)

	if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 0 {
		t.Fatalf("auto-snapshot taken of a definition flagged DELETE")
	}
}

// The replicas are read through the API reader when one is set: a LUKS
// password a fresh replica was just given may not be in the cache yet,
// and a snapshot without it would be refused by the satellite.
func TestAutoSnapshotReadsTheReplicasThroughTheAPIReader(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-reader", 15, "")

	fresh := replica(rd.Name, "n1")
	fresh.Spec.Volumes = []blockstoriov1alpha1.ResourceVolumeSpec{{VolumeNumber: 0, LUKSKey: &blockstoriov1alpha1.LUKSKey{
		KeyID: "c7324c99567678f7", Wrapped: bytes.Repeat([]byte{0xa1}, 12+64+16), Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated,
	}}}

	r, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1"))
	r.APIReader = fake.NewClientBuilder().WithScheme(newSnapshotControllerScheme(t)).WithObjects(fresh).Build()

	tick(t, r)

	snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
	if len(snaps) != 1 || len(snaps[0].Spec.LUKSKeys) != 1 {
		t.Fatalf("auto-snapshot does not record the password the API server holds: %+v", snaps)
	}
}

// autoSnapshotAt is an auto-snapshot of rd created at the given time.
func autoSnapshotAt(rd, name string, created time.Time) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rd + "." + name,
			CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{
				"blockstor.org/resource-definition": rd,
				"blockstor.org/snapshot-name":       name,
				controllerpkg.LabelAutoSnapshot:     "true",
			},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rd,
			SnapshotName:           name,
			Nodes:                  []string{"n1"},
		},
	}
}

// LINSTOR prunes old auto-snapshots only once the take finished. A
// snapshot deleted mid-take is never resumed, as the orchestration stops
// at the deletion, and its replicas stay frozen. A take still ordered
// suspended, one with no outcome yet, and one whose node has not
// confirmed the resume are kept past the budget; a finished one goes.
func TestAutoSnapshotPruneKeepsATakeNotOver(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 5, 14, 11, 0, 0, 0, time.UTC)
	acked := []blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", SuspendIOAcked: true}}
	successful := []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}

	cases := map[string]func(*blockstoriov1alpha1.Snapshot){
		"suspended": func(s *blockstoriov1alpha1.Snapshot) {
			s.Spec.SuspendIO = true
			s.Status.NodeStatus = acked
		},
		"taking": func(s *blockstoriov1alpha1.Snapshot) {
			s.Spec.SuspendIO, s.Spec.TakeSnapshot = true, true
		},
		"no outcome": func(*blockstoriov1alpha1.Snapshot) {},
		"aborted, resume unconfirmed": func(s *blockstoriov1alpha1.Snapshot) {
			s.Spec.AbortReason = "snapshot aborted: test"
			s.Status.NodeStatus = acked
		},
	}

	for name, shape := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rd := makeRDWithAutoSnapshot(t, "pvc-prune", 15, "1")
			rd.Annotations = map[string]string{
				controllerpkg.AnnotationAutoSnapshotLastAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			}

			inFlight := autoSnapshotAt(rd.Name, "auto-snap-00001", base)
			shape(inFlight)

			done := autoSnapshotAt(rd.Name, "auto-snap-00002", base.Add(time.Minute))
			done.Status.Flags = successful

			r, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1"), inFlight, done)

			tick(t, r)

			if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 2 {
				t.Fatalf("a take not over was pruned, or a newer snapshot in its place: %d left", len(snaps))
			}

			inFlight.Spec.SuspendIO, inFlight.Spec.TakeSnapshot = false, false
			inFlight.Spec.AbortReason = ""
			inFlight.Status = blockstoriov1alpha1.SnapshotStatus{Flags: successful}

			live := getSnap(t, cli, inFlight.Name)
			live.Spec = inFlight.Spec

			if err := cli.Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}

			live.Status = inFlight.Status
			if err := cli.Status().Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}

			tick(t, r)

			snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
			if len(snaps) != 1 || snaps[0].Name != done.Name {
				t.Fatalf("the finished oldest take was not pruned: %d left", len(snaps))
			}
		})
	}
}

// Two takes of one definition would suspend the same replicas, and
// whichever resumed first would unfreeze the other in the middle of its
// take. A tick that falls due while any snapshot of the definition,
// automatic or not, is still being taken books nothing and waits.
func TestAutoSnapshotWaitsForATakeUnderWay(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-busy", 15, "")

	rest := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rd.Name + ".csi-snap",
			Labels: map[string]string{"blockstor.org/resource-definition": rd.Name},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rd.Name,
			SnapshotName:           "csi-snap",
			Nodes:                  []string{"n1"},
			SuspendIO:              true,
		},
	}

	r, cli := autoSnapshotFixture(t, rd, replica(rd.Name, "n1"), rest)

	tick(t, r)

	if snaps := listAutoSnapshotsByRD(t, cli, rd.Name); len(snaps) != 0 {
		t.Fatalf("auto-snapshot started while another take of the definition is under way: %d", len(snaps))
	}

	var updated blockstoriov1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), types.NamespacedName{Name: rd.Name}, &updated); err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got := updated.Spec.Props[controllerpkg.PropAutoSnapshotNextID]; got != "" {
		t.Errorf("NextAutoId booked for a skipped auto-snapshot: %q", got)
	}
}

// Evacuating a node flags the node EVICTED (LINSTOR's EVACUATE); its
// replicas are on their way off it and are no snapshot source, as in
// LINSTOR's createSnapshots.
func TestAutoSnapshotLeavesOutANodeBeingEvacuated(t *testing.T) {
	t.Parallel()

	rd := makeRDWithAutoSnapshot(t, "pvc-evacuate", 15, "")

	node := func(name string, flags ...string) *blockstoriov1alpha1.Node {
		return &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       blockstoriov1alpha1.NodeSpec{Flags: flags},
		}
	}

	r, cli := autoSnapshotFixture(t, rd,
		node("n1"), node("n2", apiv1.NodeFlagEvicted), node("n3", "EVACUATE"),
		replica(rd.Name, "n1"), replica(rd.Name, "n2"), replica(rd.Name, "n3"))

	tick(t, r)

	snaps := listAutoSnapshotsByRD(t, cli, rd.Name)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snaps))
	}

	if got := snaps[0].Spec.Nodes; !slices.Equal(got, []string{"n1"}) {
		t.Errorf("auto-snapshot nodes = %v, want [n1]", got)
	}
}
