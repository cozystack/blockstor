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
	"io"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Every satellite start must name a new process, a restart on the same
// kernel boot included. LINSTOR keeps a satellite's state on the
// connection to it and drops it when that connection closes, so it
// shows a node's replicas Unknown after any satellite restart until the
// new satellite reports them. A report the previous process left behind
// is told apart only by a different name; reusing one across restarts
// would also keep showing a replica that left the kernel in between.
func TestEachSatelliteStartPicksANewIncarnation(t *testing.T) {
	t.Parallel()

	first, second := &Config{}, &Config{}
	ensureWiredDefaults(first)
	ensureWiredDefaults(second)

	if first.Incarnation == "" || first.Incarnation == second.Incarnation {
		t.Fatalf("incarnations %q and %q; want two distinct non-empty ones", first.Incarnation, second.Incarnation)
	}
}

// A test or caller that names the incarnation itself keeps it.
func TestWiringKeepsAGivenIncarnation(t *testing.T) {
	t.Parallel()

	preset := &Config{Incarnation: "given"}
	ensureWiredDefaults(preset)

	if preset.Incarnation != "given" {
		t.Errorf("a given incarnation was replaced with %q", preset.Incarnation)
	}
}

// The heartbeat and the observer the manager runs carry the same
// incarnation: the heartbeat names the running process on the Node, the
// observer signs every report with it, and a read compares the two.
func TestManagerRunnablesCarryTheIncarnation(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		NodeName:    "worker-1",
		Incarnation: "inc-2",
		Apply:       satellite.NewReconciler(satellite.ReconcilerConfig{StateDir: t.TempDir()}),
	}

	if got := newHeartbeatRunnableFromConfig(nil, cfg).Incarnation; got != "inc-2" {
		t.Errorf("heartbeat incarnation = %q, want inc-2", got)
	}

	if got := newObserverRunnableFromConfig(nil, cfg).Incarnation; got != "inc-2" {
		t.Errorf("observer incarnation = %q, want inc-2", got)
	}
}

func incarnationScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	return scheme
}

func TestHeartbeatNamesTheRunningSatellite(t *testing.T) {
	t.Parallel()

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Status:     blockstoriov1alpha1.NodeStatus{SatelliteIncarnation: "inc-1"},
	}
	cli := fake.NewClientBuilder().WithScheme(incarnationScheme(t)).WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).Build()

	hb := &HeartbeatRunnable{Client: cli, NodeName: "worker-1", Incarnation: "inc-2", Period: time.Hour}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if err := hb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("get node: %v", err)
	}

	if got.Status.SatelliteIncarnation != "inc-2" {
		t.Errorf("node incarnation = %q, want the running satellite's inc-2", got.Status.SatelliteIncarnation)
	}
}

// The observer signs the state it writes with its own incarnation, so a
// replica reported since the restart reads as current again.
func TestObserverSignsTheStateItWrites(t *testing.T) {
	t.Parallel()

	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName("pvc-x", "worker-1")},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "worker-1"},
	}
	cli := fake.NewClientBuilder().WithScheme(incarnationScheme(t)).WithObjects(replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	// The satellite before the reboot, then the one after it.
	for _, run := range []struct{ incarnation, state string }{{"inc-1", "UpToDate"}, {"inc-2", "Inconsistent"}} {
		o := &ObserverRunnable{Client: cli, NodeName: "worker-1", Incarnation: run.incarnation}

		err := o.writeStatus(t.Context(), &observation{ResourceName: "pvc-x", DrbdState: run.state, Role: "Secondary"})
		if err != nil {
			t.Fatalf("writeStatus by %s: %v", run.incarnation, err)
		}
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(t.Context(), client.ObjectKey{Name: replica.Name}, &got); err != nil {
		t.Fatalf("get replica: %v", err)
	}

	if got.Status.SatelliteIncarnation != "inc-2" || got.Status.DrbdState != "Inconsistent" {
		t.Errorf("status = incarnation %q state %q; want inc-2 with the new state",
			got.Status.SatelliteIncarnation, got.Status.DrbdState)
	}
}

// A replica the kernel no longer holds is absent from the new
// satellite's initial dump. It must stay as the previous process left
// it, signed with the old incarnation, so reads keep hiding it; a write
// would sign it with the new incarnation and show the old state as
// current.
func TestObserverLeavesAReplicaTheDumpDoesNotReport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	replica := func(rd string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName(rd, "n1")},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: "n1"},
		}
	}

	base := fake.NewClientBuilder().WithScheme(incarnationScheme(t)).
		WithObjects(replica("res-a"), replica("res-gone")).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	var (
		mu     sync.Mutex
		writes = map[string]int{}
	)

	cli := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			mu.Lock()
			writes[obj.GetName()]++
			mu.Unlock()

			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})

	previous := &ObserverRunnable{Client: base, NodeName: "n1", Incarnation: "before"}
	for _, rd := range []string{"res-a", "res-gone"} {
		err := previous.writeStatus(ctx, &observation{ResourceName: rd, DrbdState: "UpToDate", Role: "Secondary"})
		if err != nil {
			t.Fatalf("seed %s: %v", rd, err)
		}
	}

	stream, feed := io.Pipe()
	defer func() { _ = feed.Close() }()

	go func() {
		_, _ = io.WriteString(feed, "exists resource name:res-a role:Secondary suspended:no\n"+
			"exists device name:res-a volume:0 minor:1000 disk:UpToDate client:no quorum:yes\n"+
			"exists -\n")
	}()

	o := &ObserverRunnable{
		Client:      cli,
		Exec:        storage.NewFakeExec(),
		NodeName:    "n1",
		Incarnation: "after",
		startEvents2: func(context.Context) (*drbd.Watcher, func(), error) {
			return drbd.NewWatcher(stream), func() {}, nil
		},
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = o.Start(ctx)
	}()

	var got blockstoriov1alpha1.Resource

	for deadline := time.Now().Add(5 * time.Second); got.Status.SatelliteIncarnation != "after"; {
		if time.Now().After(deadline) {
			t.Fatalf("res-a status = %+v; want it written by the new satellite", got.Status)
		}

		time.Sleep(10 * time.Millisecond)

		err := base.Get(ctx, client.ObjectKey{Name: k8s.ResourceCRName("res-a", "n1")}, &got)
		if err != nil {
			t.Fatalf("get res-a: %v", err)
		}
	}

	// The pass that wrote res-a has finished once writeMu is free.
	o.writeMu.Lock()
	o.writeMu.Unlock() //nolint:staticcheck // an empty critical section waits for the pass in flight

	// The watcher reads until its stream ends, as drbdsetup's does
	// when the process is stopped.
	cancel()
	_ = feed.Close()
	<-done

	var gone blockstoriov1alpha1.Resource

	err := base.Get(t.Context(), client.ObjectKey{Name: k8s.ResourceCRName("res-gone", "n1")}, &gone)
	if err != nil {
		t.Fatalf("get res-gone: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if n := writes[gone.Name]; n != 0 || gone.Status.SatelliteIncarnation != "before" {
		t.Errorf("a replica the dump did not report was written %d times and reads incarnation %q; "+
			"want it untouched and still signed \"before\" so reads hide it", n, gone.Status.SatelliteIncarnation)
	}
}
