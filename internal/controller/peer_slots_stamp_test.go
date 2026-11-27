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
	"strconv"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/store"
)

// stampedPeerSlots reconciles one replica of a definition with the given
// count to steady state and returns the count stamped on the replica.
func stampedPeerSlots(t *testing.T, definition, preset *int32) *int32 {
	t.Helper()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "slots-rd"
		resourceName = rdName + ".n1"
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			PeerSlots:         definition,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1024}},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			PeerSlots:              preset,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rd, res).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, resourceName)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.DRBDNodeID == nil {
		t.Fatalf("node id not allocated")
	}

	return got.Spec.PeerSlots
}

// A replica gets its definition's peer slots, stamped with its node id,
// as LINSTOR copies the definition's count onto each replica it creates.
func TestReplicaIsStampedWithItsDefinitionPeerSlots(t *testing.T) {
	t.Parallel()

	seven := int32(7)
	if got := stampedPeerSlots(t, &seven, nil); got == nil || *got != 7 {
		t.Errorf("got %v, want 7", got)
	}
}

// A replica of a definition created before blockstor recorded a count was
// created with 15, and is stamped with 15 so a count set on the definition
// later does not change how its metadata is read.
func TestReplicaOfADefinitionWithoutACountIsStampedFifteen(t *testing.T) {
	t.Parallel()

	if got := stampedPeerSlots(t, nil, nil); got == nil || *got != 15 {
		t.Errorf("got %v, want 15", got)
	}
}

// A replica's count is settable once: a definition whose count changed
// does not change the count the replica's metadata was created with.
func TestReplicaPeerSlotsAreNotRestamped(t *testing.T) {
	t.Parallel()

	seven, twelve := int32(7), int32(12)
	if got := stampedPeerSlots(t, &twelve, &seven); got == nil || *got != 7 {
		t.Errorf("got %v, want the replica's own 7", got)
	}
}

// preexistingReplica is a diskful replica that got its identities
// before counts were recorded, with what its satellite read from its
// superblock, nil for nothing read yet.
func preexistingReplica(rdName, node string, nodeID int32, metadata *int32) *blockstoriov1alpha1.Resource {
	port, minor := int32(7000), int32(1000)
	skip := false

	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
			DRBDNodeID:             &nodeID,
			DRBDPort:               &port,
			SkipInitialSync:        &skip,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID:        &nodeID,
			DRBDPort:          &port,
			DRBDMinor:         &minor,
			MetadataPeerSlots: metadata,
		},
	}
}

// peerSlotsFixture reconciles the named replicas of a definition with
// the given count to steady state and returns the cluster.
func peerSlotsFixture(
	t *testing.T, definition *int32, replicas []*blockstoriov1alpha1.Resource, reconcile ...string,
) client.Client {
	t.Helper()

	cli, _ := peerSlotsFixtureRecorded(t, nil, definition, replicas, reconcile...)

	return cli
}

// peerSlotsFixtureRecorded is peerSlotsFixture with the reconciler's
// Events going to recorder, when it is not nil, and the reconciler
// returned to run again.
func peerSlotsFixtureRecorded(
	t *testing.T, recorder *events.FakeRecorder, definition *int32, replicas []*blockstoriov1alpha1.Resource,
	reconcile ...string,
) (client.Client, *controllerpkg.ResourceReconciler) {
	t.Helper()

	ctx := context.Background()
	scheme := newScheme(t)
	minor := int32(1000)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: replicas[0].Spec.ResourceDefinitionName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			PeerSlots: definition,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024, DRBDMinor: &minor},
			},
		},
	}

	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rd)

	for _, res := range replicas {
		builder = builder.WithObjects(res)
	}

	cli := builder.Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	if recorder != nil {
		rec.Recorder = recorder
	}

	for _, name := range reconcile {
		reconcileToSteadyState(t, ctx, rec, name)
	}

	return cli, rec
}

func getReplica(t *testing.T, cli client.Client, name string) *blockstoriov1alpha1.Resource {
	t.Helper()

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(context.Background(), client.ObjectKey{Name: name}, got); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}

	return got
}

func slotsString(v *int32) string {
	if v == nil {
		return "unset"
	}

	return strconv.Itoa(int(*v))
}

// A replica that already had a node id when the controller first sees it
// without a count predates the record. Its metadata was created by a
// blockstor that kept no count, with 15, or by LINSTOR, with its own
// count, before an older linstor-migrate adopted it: only its superblock
// tells, so it is stamped with what its satellite read there, and left
// unrecorded until then, whatever its definition says now. Its
// identities are otherwise settled, so the pass that stamps it must not
// take the settled-identities shortcut.
func TestPreexistingReplicaIsStampedFromItsSuperblock(t *testing.T) {
	t.Parallel()

	seven, fifteen := int32(7), int32(15)

	cases := []struct {
		name       string
		definition *int32
		metadata   *int32
		want       *int32
	}{
		{"adopted from LINSTOR, superblock read", nil, &seven, &seven},
		{"created by blockstor, superblock read", nil, &fifteen, &fifteen},
		{"definition lowered since, superblock read", &seven, &fifteen, &fifteen},
		{"superblock not read yet", nil, nil, nil},
		{"definition lowered since, superblock not read yet", &seven, nil, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := preexistingReplica("old-rd", "n1", 0, tc.metadata)
			cli := peerSlotsFixture(t, tc.definition, []*blockstoriov1alpha1.Resource{res}, res.Name)

			got := getReplica(t, cli, res.Name).Spec.PeerSlots
			if slotsString(got) != slotsString(tc.want) {
				t.Errorf("got %s, want %s", slotsString(got), slotsString(tc.want))
			}
		})
	}
}

// A definition adopted by a linstor-migrate that predates the count
// records none, and its replicas' superblocks hold LINSTOR's 7. Once
// every replica with a disk has told its count, the definition records
// the least of them, the count LINSTOR gave it, instead of reading as
// 15; until then it records nothing.
func TestAdoptedDefinitionRecordsItsReplicasPeerSlots(t *testing.T) {
	t.Parallel()

	seven, fifteen := int32(7), int32(15)

	cases := []struct {
		name string
		n2   *int32
		want *int32
	}{
		{"every replica told", &seven, &seven},
		{"a replica added by an older blockstor", &fifteen, &seven},
		{"a replica not told yet", nil, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n1 := preexistingReplica("adopted", "n1", 0, &seven)
			n2 := preexistingReplica("adopted", "n2", 1, tc.n2)
			cli := peerSlotsFixture(t, nil, []*blockstoriov1alpha1.Resource{n1, n2}, n1.Name, n2.Name)

			rd := &blockstoriov1alpha1.ResourceDefinition{}
			if err := cli.Get(context.Background(), client.ObjectKey{Name: "adopted"}, rd); err != nil {
				t.Fatalf("get: %v", err)
			}

			if slotsString(rd.Spec.PeerSlots) != slotsString(tc.want) {
				t.Errorf("definition: got %s, want %s", slotsString(rd.Spec.PeerSlots), slotsString(tc.want))
			}
		})
	}
}

// A new replica of an adopted definition that records no count gets the
// count its replicas' superblocks hold, as LINSTOR would give it its
// definition's 7, not 15 for good. While no replica has told its count
// yet, it gets no identities, which would have the satellite create its
// metadata with 15.
func TestNewReplicaOfAnAdoptedDefinitionGetsItsReplicasPeerSlots(t *testing.T) {
	t.Parallel()

	seven := int32(7)

	newReplica := func() *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "adopted.n3"},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "adopted", NodeName: "n3"},
		}
	}

	t.Run("a replica told its count", func(t *testing.T) {
		t.Parallel()

		cli := peerSlotsFixture(t, nil, []*blockstoriov1alpha1.Resource{
			preexistingReplica("adopted", "n1", 0, &seven), newReplica(),
		}, "adopted.n3")

		got := getReplica(t, cli, "adopted.n3")
		if got.Spec.DRBDNodeID == nil || slotsString(got.Spec.PeerSlots) != "7" {
			t.Errorf("node id %v, peer slots %s; want a node id and 7", got.Spec.DRBDNodeID, slotsString(got.Spec.PeerSlots))
		}
	})

	t.Run("no replica told its count yet", func(t *testing.T) {
		t.Parallel()

		recorder := events.NewFakeRecorder(100)
		cli, rec := peerSlotsFixtureRecorded(t, recorder, nil, []*blockstoriov1alpha1.Resource{
			preexistingReplica("adopted", "n1", 0, nil), newReplica(),
		})

		// The wait comes back on its own at the limit, so a reconcile
		// never settles; one pass is what a sibling's report triggers.
		result, err := rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Name: "adopted.n3"}})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}

		if result.RequeueAfter == 0 {
			t.Error("a waiting replica is not requeued for the limit")
		}

		got := getReplica(t, cli, "adopted.n3")
		if got.Spec.DRBDNodeID != nil || got.Spec.PeerSlots != nil {
			t.Errorf("node id %v, peer slots %s; want neither until the count is known",
				got.Spec.DRBDNodeID, slotsString(got.Spec.PeerSlots))
		}

		// The wait says what it waits for from the first pass: a
		// Warning on the new replica naming the replica whose count is
		// missing. Its status stays clean until the limit.
		if !recordedEvent(recorder, "AwaitingPeerSlots", "adopted.n1") {
			t.Error("no AwaitingPeerSlots Warning naming adopted.n1 on the waiting replica")
		}

		if c := apimeta.FindStatusCondition(got.Status.Conditions, blockstoriov1alpha1.ConditionPeerSlotsKnown); c != nil {
			t.Errorf("a replica that just started waiting carries %s=%s", c.Type, c.Status)
		}
	})

	// A wait past the limit is in the replica's status, naming the
	// replica whose count is missing, and the reconcile comes back to it
	// on its own. Once the count arrives the replica is stamped and the
	// condition turns True.
	t.Run("waited past the limit", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		waiting := newReplica()
		waiting.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))

		cli, rec := peerSlotsFixtureRecorded(t, nil, nil, []*blockstoriov1alpha1.Resource{
			preexistingReplica("adopted", "n1", 0, nil), waiting,
		})

		result, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "adopted.n3"}})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}

		if result.RequeueAfter == 0 {
			t.Error("a replica past the limit is not requeued")
		}

		got := getReplica(t, cli, "adopted.n3")

		c := apimeta.FindStatusCondition(got.Status.Conditions, blockstoriov1alpha1.ConditionPeerSlotsKnown)
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "AwaitingPeerSlots" ||
			!strings.Contains(c.Message, "adopted.n1") {
			t.Fatalf("condition %+v; want PeerSlotsKnown=False, AwaitingPeerSlots, naming adopted.n1", c)
		}

		told := getReplica(t, cli, "adopted.n1")
		told.Status.MetadataPeerSlots = &seven

		if err := cli.Status().Update(ctx, told); err != nil {
			t.Fatalf("report the count: %v", err)
		}

		reconcileToSteadyState(t, ctx, rec, "adopted.n3")

		got = getReplica(t, cli, "adopted.n3")
		if slotsString(got.Spec.PeerSlots) != "7" {
			t.Errorf("peer slots %s once the count is known, want 7", slotsString(got.Spec.PeerSlots))
		}

		if !apimeta.IsStatusConditionTrue(got.Status.Conditions, blockstoriov1alpha1.ConditionPeerSlotsKnown) {
			t.Errorf("conditions %+v; want PeerSlotsKnown=True once stamped", got.Status.Conditions)
		}
	})
}

// recordedEvent reports whether recorder holds a Warning with reason
// whose message names want.
func recordedEvent(recorder *events.FakeRecorder, reason, want string) bool {
	for {
		select {
		case event := <-recorder.Events:
			if strings.HasPrefix(event, "Warning "+reason+" ") && strings.Contains(event, want) {
				return true
			}
		default:
			return false
		}
	}
}

// A replica restored from a snapshot gets the count of the source's
// replica on its node, whose superblock its clone carries, as LINSTOR
// gives it the snapshot replica's count, not the restored definition's.
// The definition takes the source definition's current count, which a
// `rd modify --peer-slots` after the replicas were created leaves
// different from theirs. A replica on a node the source has no replica
// on gets its definition's count.
func TestRestoredReplicaGetsItsSourceReplicasPeerSlots(t *testing.T) {
	t.Parallel()

	seven, fifteen := int32(7), int32(15)

	cases := []struct {
		name     string
		node     string
		recorded *int32
		read     *int32
		want     string
	}{
		{"source replica on the node", "n1", &seven, nil, "7"},
		{"its superblock read holds more", "n1", &seven, &fifteen, "15"},
		{"no source replica on the node", "n2", &seven, nil, "15"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			scheme := newScheme(t)
			minor := int32(1000)

			restored := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "restored"},
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					PeerSlots: &fifteen,
					Props:     map[string]string{store.RestoreFromSnapshotProp: "src:snap1"},
					VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
						{VolumeNumber: 0, SizeKib: 1024, DRBDMinor: &minor},
					},
				},
			}

			source := preexistingReplica("src", "n1", 0, tc.read)
			source.Spec.PeerSlots = tc.recorded

			target := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "restored." + tc.node},
				Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "restored", NodeName: tc.node},
			}

			cli := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
				WithObjects(restored, source, target).
				Build()

			reconcileToSteadyState(t, ctx, &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}, target.Name)

			got := getReplica(t, cli, target.Name)
			if got.Spec.DRBDNodeID == nil || slotsString(got.Spec.PeerSlots) != tc.want {
				t.Errorf("node id %v, peer slots %s; want a node id and %s",
					got.Spec.DRBDNodeID, slotsString(got.Spec.PeerSlots), tc.want)
			}
		})
	}
}

// A replica stamped on a pass whose condition update never ran, as
// when the controller restarted in between, has its ids and still
// carries PeerSlotsKnown=False. Every later pass finds its identities
// settled and allocates nothing, so the condition is settled there too,
// not only on the pass that allocates.
func TestStampedReplicaSettlesAStalePeerSlotsCondition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	seven := int32(7)

	waiting := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "adopted.n3", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "adopted", NodeName: "n3"},
	}

	cli, rec := peerSlotsFixtureRecorded(t, nil, nil, []*blockstoriov1alpha1.Resource{
		preexistingReplica("adopted", "n1", 0, nil), waiting,
	})

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: waiting.Name}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if !apimeta.IsStatusConditionFalse(getReplica(t, cli, waiting.Name).Status.Conditions,
		blockstoriov1alpha1.ConditionPeerSlotsKnown) {
		t.Fatal("no PeerSlotsKnown=False on the replica waiting past the limit")
	}

	// The allocating pass lands its ids and count, and the controller
	// goes away before it settles the condition.
	told := getReplica(t, cli, "adopted.n1")
	told.Status.MetadataPeerSlots = &seven

	if err := cli.Status().Update(ctx, told); err != nil {
		t.Fatalf("report the count: %v", err)
	}

	nodeID, port, minor := int32(1), int32(7001), int32(1000)
	skip := false

	stamped := getReplica(t, cli, waiting.Name)
	stamped.Spec.DRBDNodeID, stamped.Spec.DRBDPort, stamped.Spec.SkipInitialSync = &nodeID, &port, &skip
	stamped.Spec.PeerSlots = &seven

	if err := cli.Update(ctx, stamped); err != nil {
		t.Fatalf("stamp: %v", err)
	}

	stamped.Status.DRBDNodeID, stamped.Status.DRBDPort, stamped.Status.DRBDMinor = &nodeID, &port, &minor

	if err := cli.Status().Update(ctx, stamped); err != nil {
		t.Fatalf("stamp status: %v", err)
	}

	reconcileToSteadyState(t, ctx, rec, waiting.Name)

	got := getReplica(t, cli, waiting.Name)
	if !apimeta.IsStatusConditionTrue(got.Status.Conditions, blockstoriov1alpha1.ConditionPeerSlotsKnown) {
		t.Errorf("conditions %+v; want PeerSlotsKnown=True on a replica that has its ids", got.Status.Conditions)
	}
}
