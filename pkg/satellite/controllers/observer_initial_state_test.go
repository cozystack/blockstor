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
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// initialStateStream is what `drbdsetup events2 --full` prints when a
// satellite starts on a node holding res-a and res-b, followed by one
// live change of res-a.
const initialStateStream = `exists resource name:res-a role:Secondary suspended:no
exists connection name:res-a peer-node-id:1 conn-name:n2 connection:Connected role:Secondary
exists device name:res-a volume:0 minor:1000 disk:UpToDate client:no quorum:yes
exists peer-device name:res-a peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate
exists resource name:res-b role:Secondary suspended:no
exists connection name:res-b peer-node-id:1 conn-name:n2 connection:Connected role:Secondary
exists device name:res-b volume:0 minor:1001 disk:UpToDate client:no quorum:yes
exists peer-device name:res-b peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate
exists -
change resource name:res-a role:Primary
`

// A restarted satellite shows none of its node's replicas until it has
// written their status again, so the initial state must reach the
// apiserver in one write per replica rather than one per frame: frame
// by frame took 30 to 40 seconds on a node with ~110 replicas, all of
// them Unknown meanwhile, where LINSTOR gets them back in one batch.
// A replica the kernel no longer holds is not reported and must stay
// as the previous satellite left it, so reads keep it hidden.
func TestObserverWritesTheInitialStateOncePerReplica(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	replica := func(rd string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rd + ".n1"},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: "n1"},
		}
	}

	var (
		mu     sync.Mutex
		writes = map[string]int{}
	)

	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(replica("res-a"), replica("res-b"), replica("res-gone")).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	cli := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			o := &client.SubResourcePatchOptions{}
			o.ApplyOptions(opts)

			if o.FieldManager == k8s.SatelliteFieldOwner {
				mu.Lock()
				writes[obj.GetName()]++
				mu.Unlock()
			}

			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	})

	// The satellite before the restart reported all three replicas.
	previous := &ObserverRunnable{Client: base, NodeName: "n1", Incarnation: "before"}
	for _, rd := range []string{"res-a", "res-b", "res-gone"} {
		err := previous.writeStatus(t.Context(), &observation{ResourceName: rd, DrbdState: "UpToDate", Role: "Secondary"})
		if err != nil {
			t.Fatalf("seed %s: %v", rd, err)
		}
	}

	start, feed := openStream(initialStateStream)

	fx := storage.NewFakeExec()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: "n1", Incarnation: "after", startEvents2: start}

	runObserver(t, o, feed)

	count := func(name string) int {
		mu.Lock()
		defer mu.Unlock()

		return writes[name]
	}

	var got blockstoriov1alpha1.Resource

	waitForObserver(t, "the live change on res-a", func() bool {
		if err := base.Get(t.Context(), client.ObjectKey{Name: "res-a.n1"}, &got); err != nil {
			t.Fatalf("get res-a: %v", err)
		}

		return got.Status.Role == "Primary" && count("res-b.n1") == 1
	})

	// res-a: the initial state, then the live change, which the writer
	// may fold into the same write.
	if n := count("res-a.n1"); n < 1 || n > 2 || count("res-b.n1") != 1 {
		t.Errorf("status writes = %v; want res-a.n1 once or twice (initial state, live change) and res-b.n1 once", writes)
	}

	// res-b is left as the initial state wrote it.
	if err := base.Get(t.Context(), client.ObjectKey{Name: "res-b.n1"}, &got); err != nil {
		t.Fatalf("get res-b: %v", err)
	}

	st := got.Status
	if st.SatelliteIncarnation != "after" || st.Role != "Secondary" || len(st.Volumes) != 1 ||
		st.Volumes[0].DiskState != "UpToDate" || len(st.Connections) != 1 || !st.Connections[0].Connected {
		t.Errorf("res-b status = %+v; want the whole initial state in one write, signed by the new satellite", st)
	}

	if err := base.Get(t.Context(), client.ObjectKey{Name: "res-a.n1"}, &got); err != nil {
		t.Fatalf("get res-a: %v", err)
	}

	if got.Status.SatelliteIncarnation != "after" || got.Status.Role != "Primary" {
		t.Errorf("res-a status = %+v; want the live change written after the initial state", got.Status)
	}

	var gone blockstoriov1alpha1.Resource
	if err := base.Get(t.Context(), client.ObjectKey{Name: "res-gone.n1"}, &gone); err != nil {
		t.Fatalf("get res-gone: %v", err)
	}

	if count("res-gone.n1") != 0 || gone.Status.SatelliteIncarnation != "before" {
		t.Errorf("a replica the kernel no longer holds was written (%d times, incarnation %q); "+
			"it must keep the old one so reads hide it", count("res-gone.n1"), gone.Status.SatelliteIncarnation)
	}

	if o.isHolding() {
		t.Error("the observer still holds its writes after the initial state ended")
	}
}

// openStream starts an events2 stream that prints text and then stays
// open, as drbdsetup does, so the observer never restarts it. Closing
// the returned feed ends it.
func openStream(text string) (func(context.Context) (*drbd.Watcher, func(), error), io.Closer) {
	stream, feed := io.Pipe()

	return func(context.Context) (*drbd.Watcher, func(), error) {
		go func() { _, _ = io.WriteString(feed, text) }()

		return drbd.NewWatcher(stream), func() {}, nil
	}, feed
}

// The resync loop must not write a resource while the initial state is
// still coming in: its frames are only partly merged, and a write then
// would show the replica with half of its state.
func TestObserverResyncWaitsForTheInitialState(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	var writes atomic.Int32

	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "res-a.n1"},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "res-a", NodeName: "n1"},
		}).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()
	cli := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, _ string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			writes.Add(1)

			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	})

	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: "n1", Incarnation: "after"}
	o.mergeResource(&observation{ResourceName: "res-a", HasResource: true, Role: "Secondary"})

	o.holdWrites()
	o.resyncOnce(t.Context())

	if n := writes.Load(); n != 0 {
		t.Errorf("resync wrote %d statuses during the initial state, want none", n)
	}

	o.releaseWrites(t.Context())
	o.flush(t.Context(), drbd.NewAdm(storage.NewFakeExec()))

	if n := writes.Load(); n != 1 {
		t.Errorf("resync wrote %d statuses after the initial state, want 1", n)
	}
}
