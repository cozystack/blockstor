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
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// healthyDump is one replica, UpToDate and connected to n2, as a new
// stream's initial dump states it.
const healthyDump = `exists resource name:pvc-status role:Secondary suspended:no
exists device name:pvc-status volume:0 minor:1000 disk:UpToDate client:no quorum:yes
exists connection name:pvc-status peer-node-id:1 conn-name:n2 connection:Connected role:Secondary
exists peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate
exists -
`

// statusApplies records every status apply the observer sends under its
// own field manager.
type statusApplies struct {
	mu   sync.Mutex
	sent []blockstoriov1alpha1.ResourceStatus
}

func (r *statusApplies) record(status *blockstoriov1alpha1.ResourceStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sent = append(r.sent, *status.DeepCopy())
}

func (r *statusApplies) snapshot() []blockstoriov1alpha1.ResourceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]blockstoriov1alpha1.ResourceStatus(nil), r.sent...)
}

func isObserverStatusApply(opts []client.SubResourcePatchOption) bool {
	for _, opt := range opts {
		if owner, ok := opt.(client.FieldOwner); ok && string(owner) == k8s.SatelliteFieldOwner {
			return true
		}
	}

	return false
}

// recordStatusApplies wraps cli so each observer status apply is
// recorded, after waiting for gate when gate is not nil.
func recordStatusApplies(cli client.WithWatch, applies *statusApplies, gate func()) client.Client {
	return interceptor.NewClient(cli, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if gate != nil {
				gate()
			}

			if res, ok := obj.(*blockstoriov1alpha1.Resource); ok && isObserverStatusApply(opts) {
				applies.record(&res.Status)
			}

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// runObserver starts o and stops it when the test ends. feeds are the
// write ends of the streams: a stream ends only when its pipe closes,
// as drbdsetup's does when the process is killed.
func runObserver(t *testing.T, o *ObserverRunnable, feeds ...io.Closer) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- o.Start(ctx) }()

	t.Cleanup(func() {
		cancel()

		for _, feed := range feeds {
			_ = feed.Close()
		}

		<-done
	})
}

type closeOnce struct {
	once sync.Once
	ch   chan struct{}
}

func (c *closeOnce) Close() error {
	c.once.Do(func() { close(c.ch) })

	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func healthyStatus(status *blockstoriov1alpha1.ResourceStatus) bool {
	return len(status.Volumes) == 1 && status.Volumes[0].DiskState == "UpToDate" &&
		len(status.Connections) == 1 && status.Connections[0].Connected &&
		status.Connections[0].ReplicationState == "Established"
}

// The events2 reader must keep reading while the apiserver is slow. It
// used to write every frame itself, so during a resync storm it ran at
// the client's rate limit: the frames backed up into drbdsetup's
// netlink socket until the kernel dropped notifications and drbdsetup
// exited. Here every apply hangs, and the stream must still be read to
// its end; once applies go through, the frames come out as a handful of
// writes carrying the last state, not one write per frame.
func TestObserverReaderDoesNotWaitOnTheApiserver(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	release := make(chan struct{})

	var applies statusApplies

	cli := recordStatusApplies(base, &applies, func() { <-release })

	const frames = 2000

	var stream strings.Builder

	stream.WriteString(healthyDump)

	for i := frames; i > 0; i-- {
		fmt.Fprintf(&stream, "change peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 "+
			"replication:SyncSource peer-disk:Inconsistent out-of-sync:%d\n", i*4)
	}

	stream.WriteString("change peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 " +
		"replication:Established peer-disk:UpToDate out-of-sync:0\n")

	live, feed := io.Pipe()
	t.Cleanup(func() { _ = feed.Close() })

	o := &ObserverRunnable{
		Client:   cli,
		Exec:     storage.NewFakeExec(),
		NodeName: statusTestNode,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	runObserver(t, o, feed)

	fed := make(chan error, 1)

	go func() {
		_, err := io.WriteString(feed, stream.String())
		fed <- err
	}()

	select {
	case err := <-fed:
		if err != nil {
			t.Fatalf("feed: %v", err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the events2 reader stopped reading while status applies hung")
	}

	close(release)

	waitFor(t, "the last state on the replica", func() bool {
		status := getStatusReplica(t, base, name)

		return healthyStatus(&status) && status.Connections[0].PeerVolumes[0].PeerDiskState == "UpToDate"
	})

	if got := len(applies.snapshot()); got > 20 {
		t.Fatalf("%d frames took %d status applies; the writer must send each replica's view once per pass", frames, got)
	}
}

// A new stream starts with each resource line before its devices and
// connections. Writing at that point sent a status with no volumes and
// no connections, and server-side apply deleted the ones the observer
// had set: every healthy replica on the node went Unknown on each
// restart and each satellite start. LINSTOR publishes nothing until the
// dump's `exists -` line, and neither may the observer.
func TestObserverWritesNothingUntilTheDumpEnds(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies statusApplies

	cli := recordStatusApplies(base, &applies, nil)

	firstEnded := &closeOnce{ch: make(chan struct{})}
	live, feed := io.Pipe()
	t.Cleanup(func() { _ = feed.Close() })

	var starts atomic.Int64

	o := &ObserverRunnable{
		Client:       cli,
		Exec:         storage.NewFakeExec(),
		NodeName:     statusTestNode,
		restartDelay: 10 * time.Millisecond,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			if starts.Add(1) == 1 {
				first, firstFeed := io.Pipe()

				go func() {
					_, _ = io.WriteString(firstFeed, healthyDump)
					<-firstEnded.ch
					_ = firstFeed.Close()
				}()

				return drbd.NewWatcher(first), func() {}, nil
			}

			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	runObserver(t, o, firstEnded, feed)

	waitFor(t, "the first stream's state on the replica", func() bool {
		status := getStatusReplica(t, base, name)

		return healthyStatus(&status)
	})

	before := len(applies.snapshot())

	_ = firstEnded.Close()

	lines := strings.SplitAfter(healthyDump, "\n")

	// The resource line alone, then a pause long enough for the writer
	// to act on it if it were allowed to.
	_, err := io.WriteString(feed, lines[0])
	if err != nil {
		t.Fatalf("feed: %v", err)
	}

	time.Sleep(time.Second)

	if got := applies.snapshot()[before:]; len(got) != 0 {
		t.Fatalf("status written before the new dump ended: %+v", got)
	}

	_, err = io.WriteString(feed, strings.Join(lines[1:], ""))
	if err != nil {
		t.Fatalf("feed: %v", err)
	}

	waitFor(t, "a write after the new dump", func() bool { return len(applies.snapshot()) > before })

	for _, sent := range applies.snapshot()[before:] {
		if !healthyStatus(&sent) {
			t.Fatalf("a restart wrote a partial view of a healthy replica: %+v", sent)
		}
	}

	status := getStatusReplica(t, base, name)
	if !healthyStatus(&status) {
		t.Fatalf("replica not healthy after the restart: %+v", status)
	}
}

// A resource taken down while no stream ran sends no destroy frame and
// is not in the new dump. Nothing wrote it again, and it kept showing
// Connected and UpToDate with KernelLoaded=True.
func TestObserverClearsAResourceTheNewDumpLeftOut(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	var starts atomic.Int64

	live, feed := io.Pipe()
	t.Cleanup(func() { _ = feed.Close() })

	o := &ObserverRunnable{
		Client:       cli,
		Exec:         storage.NewFakeExec(),
		NodeName:     statusTestNode,
		restartDelay: 10 * time.Millisecond,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			if starts.Add(1) == 1 {
				return drbd.NewWatcher(strings.NewReader(healthyDump)), func() {}, nil
			}

			go func() { _, _ = io.WriteString(feed, "exists -\n") }()

			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	runObserver(t, o, feed)

	waitFor(t, "the departed resource cleared", func() bool {
		var res blockstoriov1alpha1.Resource

		err := cli.Get(t.Context(), client.ObjectKey{Name: name}, &res)
		if err != nil {
			t.Fatalf("get replica: %v", err)
		}

		return starts.Load() > 1 && len(res.Status.Connections) == 0 &&
			len(res.Status.Volumes) == 1 && res.Status.Volumes[0].DiskState == "" &&
			meta.IsStatusConditionFalse(res.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded)
	})

	status := getStatusReplica(t, cli, name)
	if status.Volumes[0].DevicePath != "/dev/drbd1000" {
		t.Fatalf("the reconciler's fields went with the observer's: %+v", status.Volumes)
	}
}

// Two passes writing the same replica at once could reach the
// apiserver in either order, the older view last. A pass held up in its
// apply must finish before the next one reads the caches.
func TestObserverWritesOneResourceAtATime(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	held := make(chan struct{})
	release := make(chan struct{})

	var calls atomic.Int64

	var applies statusApplies

	cli := recordStatusApplies(base, &applies, func() {
		if calls.Add(1) == 1 {
			close(held)
			<-release
		}
	})

	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.ingest(ctx, deviceUpToDate())
	o.ingest(ctx, peerConnected("n2"))
	o.ingest(ctx, &observation{
		ResourceName: statusTestRD,
		Connections: []connectionObservation{{
			PeerNodeName: "n2", ReplicationState: "SyncSource",
			PeerVolumes: []peerVolumeObservation{{VolumeNumber: 0, PeerDiskState: "Inconsistent"}},
		}},
	})

	first := make(chan struct{})

	go func() {
		o.flush(ctx, adm)
		close(first)
	}()

	<-held

	o.ingest(ctx, peerEstablished("n2"))

	second := make(chan struct{})

	go func() {
		o.flush(ctx, adm)
		close(second)
	}()

	select {
	case <-second:
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	<-first
	<-second

	status := getStatusReplica(t, base, name)
	if len(status.Connections) != 1 || status.Connections[0].ReplicationState != "Established" {
		t.Fatalf("an older view reached the apiserver after a newer one: %+v", status.Connections)
	}
}

// The kernel destroyed the resource: the observer forgets it, so the
// resync stops re-writing a state that no longer exists.
func TestObserverForgetsADestroyedResource(t *testing.T) {
	base := startStatusEnvtest(t)
	name := createStatusReplica(t, base)

	var applies statusApplies

	cli := recordStatusApplies(base, &applies, nil)
	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerConnected("n2"))
	o.handleObservation(ctx, adm, &observation{
		ResourceName: statusTestRD, HasResource: true, HasKernelLoaded: true, Destroyed: true,
	})

	if names := o.cachedResourceNames(); len(names) != 0 {
		t.Fatalf("caches still hold %v after destroy", names)
	}

	o.appliedMu.Lock()
	_, kept := o.applied[statusTestRD]
	o.appliedMu.Unlock()

	if kept {
		t.Fatal("the last applied status of a destroyed resource is kept")
	}

	before := len(applies.snapshot())
	o.resyncOnce(ctx)

	if got := len(applies.snapshot()) - before; got != 0 {
		t.Fatalf("resync wrote a destroyed resource %d times", got)
	}

	status := getStatusReplica(t, base, name)
	if len(status.Connections) != 0 || status.Volumes[0].DiskState != "" {
		t.Fatalf("destroyed resource still shows its last state: %+v", status)
	}
}

// A GI that could not be read is not published from an earlier read:
// it may belong to an older data generation, and a new replica could
// skip a sync it needed on the strength of it.
func TestObserverForgetsAGIItCouldNotRead(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	fx := statusTestExec()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != statusTestGI {
		t.Fatalf("GI not published: %q", got)
	}

	fx.Expect("drbdmeta --force - v09 "+statusTestBacking+" internal get-gi --node-id 0",
		storage.FakeResponse{Err: storage.FakeExitError{Code: 1}})

	// A peer's role reads the GI again (carriesRole).
	peer := peerConnected("n2")
	peer.Connections[0].PeerRole = "Secondary"
	o.handleObservation(ctx, adm, peer)

	status := getStatusReplica(t, cli, name)
	if status.Volumes[0].CurrentGI != "" {
		t.Fatalf("a GI read before get-gi failed is still published: %q", status.Volumes[0].CurrentGI)
	}

	if status.Volumes[0].DiskState != "UpToDate" || len(status.Connections) != 1 {
		t.Fatalf("state lost: %+v", status)
	}
}

// The GI read from disk belongs to the data the volume held then.
func TestMergeVolumeIntoForgetsTheGIOfOtherData(t *testing.T) {
	seeded := func() map[int32]volumeObservation {
		return map[int32]volumeObservation{0: {
			VolumeNumber: 0, DiskState: "UpToDate", BackingDev: "/dev/a", ProbedGI: statusTestGI,
		}}
	}

	cases := []struct {
		name     string
		incoming volumeObservation
		keep     bool
	}{
		{"statistics", volumeObservation{VolumeNumber: 0, HasSync: true, OutOfSyncKib: 8}, true},
		{"still data-bearing", volumeObservation{VolumeNumber: 0, DiskState: "Outdated"}, true},
		{"same backing device", volumeObservation{VolumeNumber: 0, BackingDev: "/dev/a"}, true},
		{"resync started", volumeObservation{VolumeNumber: 0, DiskState: "Inconsistent"}, false},
		{"detached", volumeObservation{VolumeNumber: 0, DiskState: "Diskless"}, false},
		{"other backing device", volumeObservation{VolumeNumber: 0, BackingDev: "/dev/b"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := seeded()
			mergeVolumeInto(cache, &tc.incoming)

			if kept := cache[0].ProbedGI == statusTestGI; kept != tc.keep {
				t.Fatalf("ProbedGI kept = %v, want %v (cache %+v)", kept, tc.keep, cache[0])
			}
		})
	}
}

// A change the kernel reported while drbdsetup was still writing its
// dump can precede the dump's own, older line for the same object.
// LINSTOR applies such changes after the dump ends; applied in arrival
// order, the older dump line would win and stay until the next change.
func TestObserverAppliesChangesFromTheDumpAfterIt(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	const stream = `exists resource name:pvc-status role:Secondary suspended:no
change device name:pvc-status volume:0 minor:1000 disk:UpToDate client:no quorum:yes
exists device name:pvc-status volume:0 minor:1000 disk:Inconsistent client:no quorum:yes
exists -
`

	live, feed := io.Pipe()

	o := &ObserverRunnable{
		Client:   cli,
		Exec:     storage.NewFakeExec(),
		NodeName: statusTestNode,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			go func() { _, _ = io.WriteString(feed, stream) }()

			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	runObserver(t, o, feed)

	waitFor(t, "the dump written", func() bool {
		return len(getStatusReplica(t, cli, name).Volumes[0].DiskState) > 0
	})

	// Let a pass carrying a later frame, if any, land.
	time.Sleep(300 * time.Millisecond)

	if got := getStatusReplica(t, cli, name).Volumes[0].DiskState; got != "UpToDate" {
		t.Fatalf("disk state %q: the dump's older line went over a newer change", got)
	}
}

// Writes are held until the dump's `exists -` line, and a satellite
// that starts shows none of its node's replicas until they are written
// again. A stream that never sends the line must not hide them for as
// long as it runs: once the dump has gone quiet, what it said is
// written, and the changes kept back behind it after that.
func TestObserverWritesADumpThatNeverEnds(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	dump := strings.TrimSuffix(healthyDump, "exists -\n")

	const later = "change peer-device name:pvc-status peer-node-id:1 conn-name:n2 volume:0 " +
		"replication:Established peer-disk:Outdated\n"

	live, feed := io.Pipe()

	o := &ObserverRunnable{
		Client:            cli,
		Exec:              storage.NewFakeExec(),
		NodeName:          statusTestNode,
		initialStateQuiet: 200 * time.Millisecond,
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			go func() { _, _ = io.WriteString(feed, dump+later) }()

			return drbd.NewWatcher(live), func() {}, nil
		},
	}

	runObserver(t, o, feed)

	waitFor(t, "the dump written without its end line", func() bool {
		status := getStatusReplica(t, cli, name)

		return healthyStatus(&status) && status.Connections[0].PeerVolumes[0].PeerDiskState == "Outdated"
	})
}
