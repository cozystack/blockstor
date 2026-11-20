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

	srv.waitDeployedReplicasReady(t.Context(), "pvc-local", []string{"n1"})

	requireDevicePaths(t, backend, "pvc-local", []string{"n1"})

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
