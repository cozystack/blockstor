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

package controllers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// countStatusPatches counts the observer's status applies.
func countStatusPatches(cli client.WithWatch, applies *atomic.Int64) client.Client {
	return interceptor.NewClient(cli, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			applies.Add(1)

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// The resync re-applies what the apiserver already holds only when
// something else wrote the replica since. Re-sending every replica
// every 5 s took more than the client's 20 requests a second on a node
// with about a hundred replicas, so the frames that carry a running
// resync's progress queued behind it and `SyncTarget(x%)` trailed the
// kernel by several seconds.
func TestObserverResyncSkipsAReplicaNothingChanged(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies atomic.Int64

	cli := countStatusPatches(base, &applies)
	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerConnected("n2"))

	o.resyncOnce(ctx)

	before := applies.Load()

	for range 3 {
		o.resyncOnce(ctx)
	}

	if got := applies.Load() - before; got != 0 {
		t.Fatalf("resync re-applied an unchanged replica %d times", got)
	}

	// Someone else's write puts the replica back on the next pass, so
	// a clobber is still repaired within one resync.
	stamp := &blockstoriov1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: resourceKind, APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0, StoragePool: "thin1", DevicePath: "/dev/drbd1001",
		}}},
	}

	err := base.Status().Patch(ctx, stamp,
		client.Apply, //nolint:staticcheck // SA1019: same call the reconciler makes
		client.FieldOwner(volumeStatusFieldOwner))
	if err != nil {
		t.Fatalf("restamp: %v", err)
	}

	before = applies.Load()
	o.resyncOnce(ctx)

	if got := applies.Load() - before; got != 1 {
		t.Fatalf("resync after another writer applied %d times, want 1", got)
	}

	status := getStatusReplica(t, base, name)
	if status.Volumes[0].DiskState != "UpToDate" || len(status.Connections) != 1 {
		t.Fatalf("state lost: %+v", status)
	}
}

func outOfSync(kib int64) *observation {
	return &observation{
		ResourceName: statusTestRD,
		Volumes:      []volumeObservation{{VolumeNumber: 0, OutOfSyncKib: kib, HasSync: true}},
	}
}

// A non-zero out-of-sync count that only drifts is not written on each
// statistics frame. With both sides UpToDate on an Established
// connection, DRBD moves a count the peer never cleared on every write
// to the volume, and a satellite with a few such volumes sent a status
// patch every ~10 s at idle. Going to or from zero is written at once,
// and the drifted count goes out with the next write, or once
// outOfSyncRefresh has passed.
func TestObserverCoalescesADriftingOutOfSyncCount(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies atomic.Int64

	cli := countStatusPatches(base, &applies)
	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerEstablished("n2"))
	o.handleObservation(ctx, adm, outOfSync(1380924))

	if got := getStatusReplica(t, base, name); got.Volumes[0].OutOfSyncKib != 1380924 {
		t.Fatalf("first non-zero count not written: %+v", got.Volumes)
	}

	before := applies.Load()

	for i := range int64(5) {
		o.handleObservation(ctx, adm, outOfSync(1380900-i*4))
		o.resyncOnce(ctx)
	}

	if got := applies.Load() - before; got != 0 {
		t.Fatalf("a drifting out-of-sync count was written %d times", got)
	}

	steps := []struct {
		what string
		obs  *observation
		age  time.Duration
		want int64
	}{
		{what: "drop to zero", obs: outOfSync(0), want: 0},
		{what: "rise from zero", obs: outOfSync(64), want: 64},
		{what: "drift after the refresh period", obs: outOfSync(72), age: outOfSyncRefresh, want: 72},
	}

	for _, step := range steps {
		if step.age > 0 {
			o.appliedMu.Lock()
			entry := o.applied[statusTestRD]
			entry.at = entry.at.Add(-step.age)
			o.applied[statusTestRD] = entry
			o.appliedMu.Unlock()
		}

		before = applies.Load()
		o.handleObservation(ctx, adm, step.obs)

		if got := applies.Load() - before; got != 1 {
			t.Fatalf("%s: applied %d times, want 1", step.what, got)
		}

		if got := getStatusReplica(t, base, name); got.Volumes[0].OutOfSyncKib != step.want {
			t.Fatalf("%s: outOfSyncKib = %d, want %d", step.what, got.Volumes[0].OutOfSyncKib, step.want)
		}
	}

	// Any other change carries the current count with it.
	o.handleObservation(ctx, adm, outOfSync(80))

	if got := getStatusReplica(t, base, name); got.Volumes[0].OutOfSyncKib != 72 {
		t.Fatalf("drift inside the refresh period written: %d", got.Volumes[0].OutOfSyncKib)
	}

	o.handleObservation(ctx, adm, peerConnected("n3"))

	if got := getStatusReplica(t, base, name); got.Volumes[0].OutOfSyncKib != 80 {
		t.Fatalf("a write for another change kept the old count: %d", got.Volumes[0].OutOfSyncKib)
	}
}

func peerReplicating(peer, state string) *observation {
	return &observation{
		ResourceName: statusTestRD,
		Connections: []connectionObservation{{
			PeerNodeName: peer, Connected: true, Message: "Connected", ReplicationState: state,
		}},
	}
}

// A running resync writes every change to the out-of-sync count. The
// REST view and the CLI render that count as the percentage in
// `SyncTarget(NN%)` and `Inconsistent(NN%)`; coalesced, the percentage
// froze for minutes while LINSTOR's moves live.
func TestObserverWritesEveryOutOfSyncChangeOfARunningResync(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies atomic.Int64

	cli := countStatusPatches(base, &applies)
	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	phases := []struct {
		name string
		disk string
		repl string
	}{
		{name: "sync target", disk: "Inconsistent", repl: "SyncTarget"},
		{name: "sync source", disk: "UpToDate", repl: "SyncSource"},
		{name: "inconsistent without a resync", disk: "Inconsistent", repl: "Established"},
	}

	for _, phase := range phases {
		device := deviceUpToDate()
		device.DrbdState = phase.disk
		device.Volumes[0].DiskState = phase.disk

		o.handleObservation(ctx, adm, device)
		o.handleObservation(ctx, adm, peerReplicating("n2", phase.repl))
		o.handleObservation(ctx, adm, outOfSync(1048576))

		for _, kib := range []int64{786432, 524288, 262144} {
			before := applies.Load()
			o.handleObservation(ctx, adm, outOfSync(kib))

			if got := applies.Load() - before; got != 1 {
				t.Fatalf("%s, out-of-sync %d: applied %d times, want 1", phase.name, kib, got)
			}

			if got := getStatusReplica(t, base, name); got.Volumes[0].OutOfSyncKib != kib {
				t.Fatalf("%s: outOfSyncKib = %d, want %d", phase.name, got.Volumes[0].OutOfSyncKib, kib)
			}
		}
	}
}
