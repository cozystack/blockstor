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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// suspendWatchingReader is an APIReader that records, for every read,
// whether the resource was suspended at the time.
type suspendWatchingReader struct {
	client.Reader

	kernel *fakeKernel

	mu        sync.Mutex
	reads     int
	suspended int
}

func (s *suspendWatchingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	s.note()

	return s.Reader.Get(ctx, key, obj, opts...) //nolint:wrapcheck // test double
}

func (s *suspendWatchingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	s.note()

	return s.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

func (s *suspendWatchingReader) note() {
	frozen := s.kernel.isSuspended()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.reads++

	if frozen {
		s.suspended++
	}
}

func (s *suspendWatchingReader) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.reads, s.suspended
}

// Once I/O is suspended the take is the storage command alone, as in
// LINSTOR. The check that the snapshot records this replica's LUKS
// password reads the API, and on a busy satellite each read inside the
// window held the application's writes for its whole round trip.
func TestSnapshotWindowTakeReadsNoAPIWhileSuspended(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	reader := &suspendWatchingReader{Reader: rig.cli, kernel: rig.kernel}
	rig.r.Config.APIReader = reader
	rig.startWatchdog()

	rig.reconcile()

	if !rig.kernel.isSuspended() || !rig.node().SuspendIOAcked {
		t.Fatalf("suspend phase: suspended=%t acked=%t", rig.kernel.isSuspended(), rig.node().SuspendIOAcked)
	}

	rig.controllerSets(true, true)
	rig.reconcile()
	eventually(t, "Ready", func() bool { return rig.node().Ready })

	if reads, frozen := reader.counts(); reads == 0 || frozen != 0 {
		t.Errorf("API reads %d, %d of them while suspended; want the LUKS record read before the suspend only",
			reads, frozen)
	}
}

// A node that would refuse the take refuses it without suspending: the
// whole snapshot is aborted either way, and a suspend first only froze
// the application for the round trip to the controller and back.
func TestSnapshotWindowRefusesAnUnrecordedLUKSKeyWithoutSuspending(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.create(&blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: windowTestResource + ".n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: windowTestResource, NodeName: "n1",
			Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{{VolumeNumber: 0, LUKSKey: testSnapshotKey()}},
		},
	})

	rig.reconcile()

	if !rig.node().Failed {
		t.Errorf("node status %+v, want n1 failed", rig.node())
	}

	if cmds := rig.kernel.commands(); slices.Contains(cmds, "drbdadm suspend-io "+windowTestResource) {
		t.Errorf("suspended a resource whose take is refused: %v", cmds)
	}
}

// The resume order is answered with resume-io first. A status read in
// front of it only kept the window open for one more command; the read
// after the resume still confirms it.
func TestSnapshotWindowResumesBeforeReadingTheKernel(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.startWatchdog()

	rig.reconcile()
	rig.controllerSets(true, true)
	rig.reconcile()
	eventually(t, "Ready", func() bool { return rig.node().Ready })

	issued := len(rig.kernel.commands())

	rig.controllerSets(false, false)
	rig.reconcile()
	eventually(t, "ack cleared", func() bool { return !rig.node().SuspendIOAcked })

	// The watchdog's sweep reads every resource at once on its own
	// timer; only the commands for this resource are the close's.
	after := slices.DeleteFunc(rig.kernel.commands()[issued:], func(cmd string) bool {
		return !strings.Contains(cmd, windowTestResource)
	})
	if len(after) == 0 || after[0] != "drbdadm resume-io "+windowTestResource {
		t.Errorf("commands after the resume order %v, want resume-io first", after)
	}

	if rig.kernel.isSuspended() {
		t.Errorf("still suspended after the window: %v", after)
	}
}

// hangingStatusClient is an API whose status writes hang, deaf to
// their context, while hang is set, as on an API server that is
// unreachable or a satellite starved of CPU.
type hangingStatusClient struct {
	client.Client

	hang    atomic.Bool
	release chan struct{}
	once    sync.Once
}

func (c *hangingStatusClient) Status() client.SubResourceWriter {
	return &hangingStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

func (c *hangingStatusClient) unblock() {
	c.once.Do(func() { close(c.release) })
}

type hangingStatusWriter struct {
	client.SubResourceWriter

	c *hangingStatusClient
}

func (w *hangingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.c.hang.Load() {
		<-w.c.release
	}

	return w.SubResourceWriter.Update(ctx, obj, opts...) //nolint:wrapcheck // test double
}

func (w *hangingStatusWriter) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
) error {
	if w.c.hang.Load() {
		<-w.c.release
	}

	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...) //nolint:wrapcheck // test double
}


// Between forgetting a window and removing its marker, the close is
// still settling the Snapshot. The sweep of leftover markers took such
// a marker for one an earlier process left and failed a snapshot whose
// take had already stamped Ready.
func TestSnapshotWindowCloseIsNotTakenForALeftoverMarker(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	api := &hangingStatusClient{Client: rig.cli, release: make(chan struct{})}
	rig.r.Client = api
	rig.r.timing.windowDeadline = 300 * time.Millisecond
	rig.startWatchdog()
	t.Cleanup(api.unblock)

	rig.reconcile()
	rig.controllerSets(true, true)
	rig.reconcile()
	eventually(t, "Ready", func() bool { return rig.node().Ready })

	api.hang.Store(true)

	eventually(t, "resume at the deadline", func() bool { return !rig.kernel.isSuspended() })

	// Many watchdog ticks while the close's status write hangs.
	time.Sleep(20 * rig.r.timing.watchdogTick)

	api.hang.Store(false)
	api.unblock()

	eventually(t, "ack cleared", func() bool { return !rig.node().SuspendIOAcked })
	eventually(t, "marker removed", func() bool { return !rig.markerExists() })

	if node := rig.node(); node.Failed {
		t.Errorf("node status %+v, a snapshot stamped Ready was failed as well", node)
	}
}
