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

package rest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheTrailsWrites plays an apiserver replica whose informer cache has
// not yet seen the objects the request it serves just wrote: until
// catchUp, the cached replica listing (with replicas set) and the cached
// volume listing (with volumes set) come back empty, while the uncached
// listings read the backend, the way the API server does. Each test
// trails one listing only, so it fails for that listing and no other.
type cacheTrailsWrites struct {
	store.Store

	catchUp  time.Time
	replicas bool
	volumes  bool
}

func trailingCache(backend store.Store, replicas, volumes bool) cacheTrailsWrites {
	return cacheTrailsWrites{
		Store:    backend,
		catchUp:  time.Now().Add(50 * time.Millisecond),
		replicas: replicas,
		volumes:  volumes,
	}
}

type trailingReplicas struct {
	store.ResourceStore

	catchUp time.Time
}

func (c trailingReplicas) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	if time.Now().Before(c.catchUp) {
		return nil, nil
	}

	return c.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (c trailingReplicas) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return c.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type trailingVolumes struct {
	store.VolumeDefinitionStore

	catchUp time.Time
}

func (c trailingVolumes) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	if time.Now().Before(c.catchUp) {
		return nil, nil
	}

	return c.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (c trailingVolumes) ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	return c.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (c cacheTrailsWrites) Resources() store.ResourceStore {
	if !c.replicas {
		return c.Store.Resources()
	}

	return trailingReplicas{ResourceStore: c.Store.Resources(), catchUp: c.catchUp}
}

func (c cacheTrailsWrites) VolumeDefinitions() store.VolumeDefinitionStore {
	if !c.volumes {
		return c.Store.VolumeDefinitions()
	}

	return trailingVolumes{VolumeDefinitionStore: c.Store.VolumeDefinitions(), catchUp: c.catchUp}
}

// TestBuildWaitSeesReplicaTheCacheHasNotSeen pins the build wait against
// the apiserver's own informer cache. The autoplace writes the replica
// and reads it back a few milliseconds later; a cache that has not
// caught up yet must not count the replica as built. Before, a replica
// missing from the cache was skipped like one deleted meanwhile, so a
// local volume, which has no ready step after the build, was answered
// at once, and linstor-csi's ControllerPublishVolume read a view
// without the device path ("could not determine device path").
func TestBuildWaitSeesReplicaTheCacheHasNotSeen(t *testing.T) {
	shortenReadyPoll(t)

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-local", "n1")
	setLayerStack(t, backend, "pvc-local", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-local", "n1", false, nil)

	// The cache catches up long before the satellite builds the
	// device, so only a wait that kept the replica in view holds the
	// answer until then.
	st := trailingCache(backend, true, false)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	standIn := deviceStandIn(t.Context(), backend, "pvc-local", 1, []string{"n1"}, 300*time.Millisecond)

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-local", []string{"n1"})

	requireDevicePaths(t, backend, "pvc-local", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a replica that was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestBuildWaitSeesVolumesTheCacheHasNotSeen pins the same against the
// volume definitions: linstor-csi writes them right before the
// autoplace, possibly through a sibling apiserver replica, and a cache
// that does not list them yet must not turn the wait into an answer
// for a definition without volumes.
func TestBuildWaitSeesVolumesTheCacheHasNotSeen(t *testing.T) {
	shortenReadyPoll(t)

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-local", "n1")
	setLayerStack(t, backend, "pvc-local", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-local", "n1", false, nil)

	st := trailingCache(backend, false, true)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	standIn := deviceStandIn(t.Context(), backend, "pvc-local", 1, []string{"n1"}, 300*time.Millisecond)

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-local", []string{"n1"})

	requireDevicePaths(t, backend, "pvc-local", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a replica that was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestReadyWaitSeesReplicasTheCacheHasNotSeen pins the ready step the
// same way: replicas of a replicated definition that the cache does not
// list yet are still awaited, rather than skipped as gone.
func TestReadyWaitSeesReplicasTheCacheHasNotSeen(t *testing.T) {
	shortenReadyPoll(t)

	nodes := []string{"n1", "n2"}

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-rep", nodes...)

	for _, n := range nodes {
		seedReplica(t, backend, "pvc-rep", n, false, nil)
	}

	st := trailingCache(backend, true, false)

	// No build step: the ready step alone has to keep the replicas in
	// view.
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second}

	standIn := satelliteStandIn(t.Context(), backend, "pvc-rep", 2, nodes, false)

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-rep", nodes)

	requireReadyReplicas(t, backend, "pvc-rep", nodes)

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for replicas that became ready: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// errAPIServerRead is what a read past the cache fails with in these
// tests: a timeout or a throttled request.
var errAPIServerRead = errors.New("the server was unable to return a response in the time allotted")

// liveReads plays the API server behind a replica store: its uncached
// listing counts each call and fails with err when one is set, and
// otherwise reads backend.
type liveReads struct {
	store.ResourceStore

	backend store.ResourceStore
	err     error
	calls   *atomic.Int32
}

func (l liveReads) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	l.calls.Add(1)

	if l.err != nil {
		return nil, l.err
	}

	return l.backend.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

// withLiveReads is st whose replica store reads the API server through
// liveReads.
type withLiveReads struct {
	store.Store

	resources liveReads
}

func (w withLiveReads) Resources() store.ResourceStore { return w.resources }

func newLiveReads(cached store.Store, backend store.Store, err error) (withLiveReads, *atomic.Int32) {
	calls := &atomic.Int32{}

	return withLiveReads{
		Store:     cached,
		resources: liveReads{ResourceStore: cached.Resources(), backend: backend.Resources(), err: err, calls: calls},
	}, calls
}

// TestBuildWaitHoldsWhenTheAPIServerReadFails pins the fallback's
// failure: a cache that does not list the replica yet and an API server
// read that fails (a timeout, a throttled request) must not end the
// wait as if there were nothing to build. Before, the error answered
// at once, before the device path existed, which is the "could not
// determine device path" refusal the fallback is there to prevent.
func TestBuildWaitHoldsWhenTheAPIServerReadFails(t *testing.T) {
	shortenReadyPoll(t)

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-local", "n1")
	setLayerStack(t, backend, "pvc-local", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-local", "n1", false, nil)

	st, _ := newLiveReads(trailingCache(backend, true, false), backend, errAPIServerRead)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	standIn := deviceStandIn(t.Context(), backend, "pvc-local", 1, []string{"n1"}, 300*time.Millisecond)

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-local", []string{"n1"})

	requireDevicePaths(t, backend, "pvc-local", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a replica that was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestBuildWaitAtTheBoundWhenTheAPIServerReadKeepsFailing pins the
// other end: when neither the cache nor the API server ever lists the
// replica, the bound decides, and the answer carries the not-ready
// warning rather than a silent success.
func TestBuildWaitAtTheBoundWhenTheAPIServerReadKeepsFailing(t *testing.T) {
	shortenReadyPoll(t)

	const deployWait = 300 * time.Millisecond

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-local", "n1")
	setLayerStack(t, backend, "pvc-local", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-local", "n1", false, nil)

	cache := cacheTrailsWrites{Store: backend, catchUp: time.Now().Add(time.Minute), replicas: true}
	st, _ := newLiveReads(cache, backend, errAPIServerRead)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: deployWait}

	start := time.Now()
	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-local", []string{"n1"})

	if elapsed := time.Since(start); elapsed < deployWait {
		t.Errorf("answered after %s, before the %s bound", elapsed, deployWait)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != maskWarn {
		t.Errorf("want the not-ready warning for n1, got %v", w)
	}
}

// TestNotConnectedWarningForReplicaTheCacheHasNotSeen pins LINSTOR's
// not-connected warning against the same lag: a replica created on a
// node that is not ONLINE is warned about even when the cache does not
// list it yet. Before, the warning read the cache alone, and the build
// step skipped the replica for its node, so the call answered clean
// where LINSTOR's updateSatellites warns.
func TestNotConnectedWarningForReplicaTheCacheHasNotSeen(t *testing.T) {
	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-offline")
	seedReadyWaitNode(t, backend, "n1", apiv1.NodeTypeOffline)
	setLayerStack(t, backend, "pvc-offline", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-offline", "n1", false, nil)

	st := cacheTrailsWrites{Store: backend, catchUp: time.Now().Add(time.Minute), replicas: true}
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-offline", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != warnNotConnected {
		t.Errorf("want the not-connected warning for n1, got %v", w)
	}
}

// TestBuildWaitAsksTheAPIServerOnceForADeletedReplica pins the load of
// the fallback: a deployed replica deleted during the wait, while
// another is still building, is confirmed gone once and not read past
// the cache again on every poll.
func TestBuildWaitAsksTheAPIServerOnceForADeletedReplica(t *testing.T) {
	shortenReadyPoll(t)

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-local", "n1", "n2")
	setLayerStack(t, backend, "pvc-local", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-local", "n1", false, nil)

	// The replica on n2 was deployed and is gone already.
	st, calls := newLiveReads(backend, backend, nil)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	standIn := deviceStandIn(t.Context(), backend, "pvc-local", 1, []string{"n1"}, 300*time.Millisecond)

	// The build wait alone: createBuildDeadline reads past the cache
	// once on its own for a create with no earlier replica.
	srv.waitReplicasReadyBy(t.Context(), "pvc-local", []string{"n1", "n2"}, time.Now().Add(10*time.Second))

	requireDevicePaths(t, backend, "pvc-local", []string{"n1"})

	if n := calls.Load(); n > 1 {
		t.Errorf("read the replicas past the cache %d times for one deleted replica, want once", n)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestReadyWaitHoldsWhenTheAPIServerReadFails pins the ready step the
// same way as the build step: replicas of a replicated definition that
// neither the cache nor a failed API server read lists are still
// awaited, rather than the wait ending on the error.
func TestReadyWaitHoldsWhenTheAPIServerReadFails(t *testing.T) {
	shortenReadyPoll(t)

	nodes := []string{"n1", "n2"}

	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-rep", nodes...)

	for _, n := range nodes {
		seedReplica(t, backend, "pvc-rep", n, false, nil)
	}

	st, _ := newLiveReads(trailingCache(backend, true, false), backend, errAPIServerRead)

	// No build step: the ready step alone has to hold the answer.
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second}

	standIn := satelliteStandIn(t.Context(), backend, "pvc-rep", 2, nodes, false)

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-rep", nodes)

	requireReadyReplicas(t, backend, "pvc-rep", nodes)

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for replicas that became ready: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestNotConnectedWarningWhenTheAPIServerReadFails pins the warning for
// a replica this call deployed on a node that is not ONLINE when the
// replica cannot be read at all: the call created it, so LINSTOR's
// not-connected warning is owed whether or not a listing shows it.
func TestNotConnectedWarningWhenTheAPIServerReadFails(t *testing.T) {
	backend := store.NewInMemory()
	seedReadyWaitDefinition(t, backend, "pvc-offline")
	seedReadyWaitNode(t, backend, "n1", apiv1.NodeTypeOffline)
	setLayerStack(t, backend, "pvc-offline", apiv1.LayerKindStorage)
	seedReplica(t, backend, "pvc-offline", "n1", false, nil)

	cache := cacheTrailsWrites{Store: backend, catchUp: time.Now().Add(time.Minute), replicas: true}
	st, _ := newLiveReads(cache, backend, errAPIServerRead)
	srv := &Server{Store: st, ResourceReadyWait: 10 * time.Second, ResourceDeployWait: 10 * time.Second}

	rcs := srv.waitDeployedReplicasReady(t.Context(), "pvc-offline", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != warnNotConnected {
		t.Errorf("want the not-connected warning for n1, got %v", w)
	}
}
