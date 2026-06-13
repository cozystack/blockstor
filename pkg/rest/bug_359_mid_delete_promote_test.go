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
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 359, third interleaving — the SWALLOWED promote. The Bug-338
// witness collapse has set the witness CRD's deletionTimestamp (DELETE
// flag on the wire object) but the satellite finalizer is still
// pending, so the apiserver ACCEPTS spec patches against the dying
// row. A relocate `r c <ex-witness-node>` then:
//
//	Create        -> AlreadyExists (row still served)
//	Get           -> row exists, flags [DISKLESS, TIE_BREAKER, DELETE]
//	PatchResourceSpec (promote) -> SUCCEEDS against the dying row
//	finalizer strip completes   -> deletion swallows the promote
//
// Net effect: the CLI printed SUCCESS yet the RD ends with a single
// replica and no witness — the operator's create silently vanished
// (caught live: r-full Phase 3, `r d <peer>` + `r c <tb-node>`).
//
// The fix: promoteDisklessReplica refuses to patch a row carrying the
// DELETE flag (errWitnessMidDelete wraps ErrNotFound), routing the
// attempt into the existing Bug-359 retry loop; the next attempt's
// Create lands fresh once the finalizer strip completes.
//
// midDeleteTBResources simulates the interleaving deterministically:
//   - the race-key row exists with [DISKLESS, TIE_BREAKER, DELETE];
//   - the FIRST Create on the race key returns AlreadyExists;
//   - any PatchResourceSpec on the race key while the dying row is
//     present APPLIES the mutation and then completes the deletion
//     (drops the row) — the pre-fix swallow, observable as "promote
//     returned OK but the row is gone";
//   - Creates after the row is dropped pass through (fresh create).
type midDeleteTBResources struct {
	inner store.ResourceStore

	raceKey [2]string

	// dyingPresent flips false once the simulated finalizer strip
	// completes (first patch attempt or first post-AlreadyExists
	// retry tick).
	dyingPresent atomic.Bool

	createCalls atomic.Int32
	patchCalls  atomic.Int32
}

func (v *midDeleteTBResources) List(ctx context.Context) ([]apiv1.Resource, error) {
	return v.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return v.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	if [2]string{rdName, node} == v.raceKey && v.dyingPresent.Load() {
		return apiv1.Resource{
			Name:     rdName,
			NodeName: node,
			Flags: []string{
				apiv1.ResourceFlagDiskless,
				apiv1.ResourceFlagTieBreaker,
				apiv1.ResourceFlagDelete,
			},
		}, nil
	}

	return v.inner.Get(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) Create(ctx context.Context, r *apiv1.Resource) error {
	if [2]string{r.Name, r.NodeName} == v.raceKey {
		if v.createCalls.Add(1) == 1 {
			// Dying row still served by the apiserver.
			return errors.Wrapf(store.ErrAlreadyExists,
				"resource %q on node %q", r.Name, r.NodeName)
		}
		// Finalizer strip completed between attempts.
		v.dyingPresent.Store(false)
	}

	return v.inner.Create(ctx, r) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) Update(ctx context.Context, r *apiv1.Resource) error {
	return v.inner.Update(ctx, r) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) Delete(ctx context.Context, rdName, node string) error {
	return v.inner.Delete(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) DeleteIfTieBreaker(ctx context.Context, rdName, node string) (bool, error) {
	return v.inner.DeleteIfTieBreaker(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) SetState(ctx context.Context, rdName, node string,
	state apiv1.ResourceState, volumes []apiv1.VolumeObservation,
) error {
	return v.inner.SetState(ctx, rdName, node, state, volumes) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) ClearDRBDPort(ctx context.Context, rdName, node string) error {
	return v.inner.ClearDRBDPort(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *midDeleteTBResources) PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	if [2]string{rdName, node} == v.raceKey && v.dyingPresent.Load() {
		v.patchCalls.Add(1)

		live, err := v.Get(ctx, rdName, node)
		if err != nil {
			return err
		}

		// Closure error surfaces as-is, like the real store.
		if mutateErr := mutate(&live); mutateErr != nil {
			return mutateErr
		}

		// PRE-FIX swallow: the patch "succeeded" against the dying
		// row — and the pending deletion then removes it wholesale.
		v.dyingPresent.Store(false)

		return nil
	}

	return v.inner.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test helper
}

type midDeleteTBStore struct {
	inner     *store.InMemory
	resources *midDeleteTBResources
}

func newMidDeleteTBStore(rdName, node string) *midDeleteTBStore {
	inner := store.NewInMemory()

	res := &midDeleteTBResources{
		inner:   inner.Resources(),
		raceKey: [2]string{rdName, node},
	}
	res.dyingPresent.Store(true)

	return &midDeleteTBStore{inner: inner, resources: res}
}

func (s *midDeleteTBStore) Nodes() store.NodeStore               { return s.inner.Nodes() }
func (s *midDeleteTBStore) StoragePools() store.StoragePoolStore { return s.inner.StoragePools() }
func (s *midDeleteTBStore) ResourceGroups() store.ResourceGroupStore {
	return s.inner.ResourceGroups()
}

func (s *midDeleteTBStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return s.inner.ResourceDefinitions()
}

func (s *midDeleteTBStore) Resources() store.ResourceStore { return s.resources }

func (s *midDeleteTBStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return s.inner.VolumeDefinitions()
}

func (s *midDeleteTBStore) Snapshots() store.SnapshotStore { return s.inner.Snapshots() }

func (s *midDeleteTBStore) PhysicalDevices() store.PhysicalDeviceStore {
	return s.inner.PhysicalDevices()
}

func (s *midDeleteTBStore) ControllerProps() store.ControllerPropsStore {
	return s.inner.ControllerProps()
}

func (s *midDeleteTBStore) StoragePoolDefinitions() store.StoragePoolDefinitionStore {
	return s.inner.StoragePoolDefinitions()
}

// TestBug359RCPromoteRefusesMidDeleteWitness pins the swallowed-promote
// fix: a relocate `r c <ex-witness-node>` arriving while the witness
// row is mid-delete (DELETE flag, finalizer pending) must NOT promote
// the dying row — it must retry and land a FRESH diskful create after
// the deletion completes.
func TestBug359RCPromoteRefusesMidDeleteWitness(t *testing.T) {
	t.Parallel()

	const (
		rdName    = "pvc-bug-359-swallow"
		raceNode  = "worker-2"
		otherNode = "worker-1"
		pool      = "stand"
	)

	st := newMidDeleteTBStore(rdName, raceNode)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{otherNode, raceNode, "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: pool,
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// Surviving diskful sibling — also the pool-resolution source for
	// the bare `r c` toggle shape.
	if err := st.inner.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: otherNode,
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed surviving diskful: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Bare `linstor r c worker-2 <rd>` — the exact r-full Phase-3
	// shape (the TB node, no --storage-pool: pool resolves from the
	// sibling).
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: raceNode},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx (retry must converge on a fresh create)", resp.StatusCode)
	}

	// The DECISIVE assertion: the replica must actually EXIST after
	// the call — the pre-fix path returned 2xx while the promote
	// landed on the dying row and was swallowed by its deletion.
	got, getErr := st.inner.Resources().Get(ctx, rdName, raceNode)
	if getErr != nil {
		t.Fatalf("post-create Get on %s: %v (the promote was SWALLOWED by the witness deletion)",
			raceNode, getErr)
	}

	if got.Props["StorPoolName"] != pool {
		t.Errorf("post-create StorPoolName: got %q, want %q", got.Props["StorPoolName"], pool)
	}

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagTieBreaker || f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagDelete {
			t.Errorf("post-create Resource carries %s flag — promoted the dying witness instead of creating fresh: %+v",
				f, got.Flags)
		}
	}
}
