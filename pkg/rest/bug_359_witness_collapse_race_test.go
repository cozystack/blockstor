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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 359 — `r d <last-extra-diskful>` + `r c <ex-tiebreaker-node>`
// relocate races the Bug-338 carve-out's `removeWitnesses` Delete:
// the witness CRD has DeletionTimestamp set + a satellite finalizer
// pending while the relocate `r c` lands, so REST's
// `Resources().Create(<ex-witness-node>)` hits AlreadyExists but
// the follow-up `Resources().Get(...)` already sees NotFound (the
// satellite stripped its finalizer between Create and Get). Pre-fix,
// `createOrPromoteResource` surfaced that NotFound to the operator
// as a 404 "not found" envelope — confusing because they never asked
// for a promote, just to create a fresh replica.
//
// The fix retries the create-or-promote attempt up to
// `tieBreakerCollapseRetryAttempts` times with a short backoff;
// the AlreadyExists window closes within ~tens of ms (GC + finalizer
// strip) and the next Create lands cleanly as a fresh diskful.
//
// vanishingTBResources simulates the race deterministically: the
// first Create returns AlreadyExists (witness CRD still has a
// DeletionTimestamp + finalizer in the apiserver), and the first
// Get on that key returns NotFound (the satellite already stripped
// the finalizer so the apiserver garbage-collected the row, but the
// CRD admission still rejected our Create as a UID collision). The
// second Create on the same key succeeds as a fresh replica — the
// CRD is fully gone.
type vanishingTBResources struct {
	inner store.ResourceStore

	// raceKey identifies the (rdName, node) tuple we simulate the
	// race against — any other key passes straight through to inner.
	raceKey [2]string

	createCalls atomic.Int32
}

func (v *vanishingTBResources) List(ctx context.Context) ([]apiv1.Resource, error) {
	return v.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return v.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	if [2]string{rdName, node} == v.raceKey {
		// Witness already finalized — Get sees the gap.
		return apiv1.Resource{}, errors.Wrapf(store.ErrNotFound,
			"resource %q on node %q", rdName, node)
	}

	return v.inner.Get(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) Create(ctx context.Context, r *apiv1.Resource) error {
	if [2]string{r.Name, r.NodeName} == v.raceKey {
		n := v.createCalls.Add(1)
		if n == 1 {
			// First attempt: CRD still has DeletionTimestamp + a
			// pending finalizer in the apiserver.
			return errors.Wrapf(store.ErrAlreadyExists,
				"resource %q on node %q", r.Name, r.NodeName)
		}
		// Second attempt: CRD fully gone, fresh Create succeeds.
	}

	return v.inner.Create(ctx, r) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) Update(ctx context.Context, r *apiv1.Resource) error {
	return v.inner.Update(ctx, r) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) Delete(ctx context.Context, rdName, node string) error {
	return v.inner.Delete(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) SetState(ctx context.Context, rdName, node string,
	state apiv1.ResourceState, volumes []apiv1.VolumeObservation,
) error {
	return v.inner.SetState(ctx, rdName, node, state, volumes) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) ClearDRBDPort(ctx context.Context, rdName, node string) error {
	return v.inner.ClearDRBDPort(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *vanishingTBResources) PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	return v.inner.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test helper
}

type vanishingTBStore struct {
	inner     *store.InMemory
	resources store.ResourceStore
}

func newVanishingTBStore(rdName, node string) *vanishingTBStore {
	inner := store.NewInMemory()

	return &vanishingTBStore{
		inner: inner,
		resources: &vanishingTBResources{
			inner:   inner.Resources(),
			raceKey: [2]string{rdName, node},
		},
	}
}

func (s *vanishingTBStore) Nodes() store.NodeStore               { return s.inner.Nodes() }
func (s *vanishingTBStore) StoragePools() store.StoragePoolStore { return s.inner.StoragePools() }
func (s *vanishingTBStore) ResourceGroups() store.ResourceGroupStore {
	return s.inner.ResourceGroups()
}

func (s *vanishingTBStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return s.inner.ResourceDefinitions()
}

func (s *vanishingTBStore) Resources() store.ResourceStore { return s.resources }

func (s *vanishingTBStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return s.inner.VolumeDefinitions()
}

func (s *vanishingTBStore) Snapshots() store.SnapshotStore { return s.inner.Snapshots() }

func (s *vanishingTBStore) PhysicalDevices() store.PhysicalDeviceStore {
	return s.inner.PhysicalDevices()
}

func (s *vanishingTBStore) ControllerProps() store.ControllerPropsStore {
	return s.inner.ControllerProps()
}

func (s *vanishingTBStore) StoragePoolDefinitions() store.StoragePoolDefinitionStore {
	return s.inner.StoragePoolDefinitions()
}

// TestBug359RCRelocateOnVanishingTieBreakerRetriesCreate pins the
// Bug 359 retry envelope: a relocate `r c <ex-witness-node>` whose
// witness CRD is mid-finalizer-strip must NOT surface "not found"
// to the operator. The retry loop converges on a fresh Create as
// soon as the CRD GC completes.
func TestBug359RCRelocateOnVanishingTieBreakerRetriesCreate(t *testing.T) {
	t.Parallel()

	const (
		rdName    = "pvc-bug-359"
		raceNode  = "worker-3"
		otherNode = "worker-1"
		pool      = "stand"
	)

	st := newVanishingTBStore(rdName, raceNode)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{otherNode, "worker-2", raceNode} {
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

	// Surviving diskful on otherNode (worker-1) — sibling for the
	// relocate target. The witness on raceNode (worker-3) has
	// already been Bug-338-collapsed by the time the operator's
	// `r c worker-3` arrives, but the apiserver still serves the
	// pre-Bug-359 AlreadyExists/NotFound mismatch on raceNode for
	// the first attempt (vanishingTBResources contract).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: otherNode,
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed surviving diskful: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `linstor r c worker-3 pvc-bug-359 --storage-pool stand` — the
	// relocate shape from tests/e2e/tiebreaker-r-d-r-c-other-node.sh.
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: raceNode,
			Props:    map[string]string{"StorPoolName": pool},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx (Bug 359: retry must converge after the witness CRD GC)",
			resp.StatusCode)
	}

	// Surface invariant: the retry loop fired exactly twice — once
	// to hit the AlreadyExists/NotFound mismatch, once to land the
	// fresh Create. A regression to the single-attempt path would
	// stop at 1 Create call and surface the 404.
	vanishing, ok := st.resources.(*vanishingTBResources)
	if !ok {
		t.Fatalf("test setup error: store.resources is not *vanishingTBResources")
	}

	if got, want := vanishing.createCalls.Load(), int32(2); got != want {
		t.Errorf("Create call count: got %d, want %d (single attempt = regression)",
			got, want)
	}

	// The fresh Create landed: a diskful Resource on raceNode is
	// now visible through the store.
	got, getErr := st.inner.Resources().Get(ctx, rdName, raceNode)
	if getErr != nil {
		t.Fatalf("post-retry Get on raceNode: %v", getErr)
	}

	if got.Props["StorPoolName"] != pool {
		t.Errorf("post-retry StorPoolName: got %q, want %q", got.Props["StorPoolName"], pool)
	}

	// And no TIE_BREAKER flag — this is the fresh diskful relocate
	// target, not a promoted witness.
	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagTieBreaker {
			t.Errorf("post-retry Resource carries TIE_BREAKER flag: %+v", got.Flags)
		}

		if f == apiv1.ResourceFlagDiskless {
			t.Errorf("post-retry Resource carries DISKLESS flag: %+v", got.Flags)
		}
	}
}

// TestBug359RCExhaustedRetriesSurfaces503 pins the worst-case
// envelope when the witness CRD never finishes its finalizer
// strip within the retry budget. We simulate that by leaving the
// vanishingTBResources contract in place but failing every Create
// indefinitely (race never closes). The handler must NOT surface
// "not found" — that conflates an internal race with a real
// missing-RD class. Instead it surfaces 503 + a self-explanatory
// envelope the operator can retry safely.
func TestBug359RCExhaustedRetriesSurfaces503(t *testing.T) {
	t.Parallel()

	const (
		rdName   = "pvc-bug-359-exhausted"
		raceNode = "worker-3"
		pool     = "stand"
	)

	st := newVanishingTBStore(rdName, raceNode)
	// Swap in the always-vanishing variant — every Create on the
	// raceKey returns AlreadyExists, every Get returns NotFound,
	// so the retry loop drains its full budget.
	st.resources = &alwaysVanishingTBResources{
		inner:   st.inner.Resources(),
		raceKey: [2]string{rdName, raceNode},
	}

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2", raceNode} {
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

	// Seed the surviving diskful through the inner store so the
	// always-vanishing wrapper doesn't intercept it (its raceKey is
	// raceNode, not worker-1).
	if err := st.inner.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-1",
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed surviving diskful: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: raceNode,
			Props:    map[string]string{"StorPoolName": pool},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503 (Bug 359: exhausted retries must NOT surface 404)",
			resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty; want a witness-collapse description")
	}

	// The message must mention the collapse — operators reading the
	// envelope need to know this is a transient race, not a missing
	// RD or missing pool class.
	if msg := rcs[0].Message; !bug359ContainsAll(msg, "tiebreaker", "collapse", raceNode) {
		t.Errorf("envelope Message: got %q, want substrings {tiebreaker, collapse, %s}",
			msg, raceNode)
	}
}

// alwaysVanishingTBResources is the exhausted-retry mock: every
// Create against raceKey returns AlreadyExists, every Get against
// raceKey returns NotFound. The retry loop drains its budget and
// the handler surfaces 503.
type alwaysVanishingTBResources struct {
	inner store.ResourceStore

	raceKey [2]string
}

func (v *alwaysVanishingTBResources) List(ctx context.Context) ([]apiv1.Resource, error) {
	return v.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return v.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	if [2]string{rdName, node} == v.raceKey {
		return apiv1.Resource{}, errors.Wrapf(store.ErrNotFound,
			"resource %q on node %q", rdName, node)
	}

	return v.inner.Get(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) Create(ctx context.Context, r *apiv1.Resource) error {
	if [2]string{r.Name, r.NodeName} == v.raceKey {
		return errors.Wrapf(store.ErrAlreadyExists,
			"resource %q on node %q", r.Name, r.NodeName)
	}

	return v.inner.Create(ctx, r) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) Update(ctx context.Context, r *apiv1.Resource) error {
	return v.inner.Update(ctx, r) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) Delete(ctx context.Context, rdName, node string) error {
	return v.inner.Delete(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) SetState(ctx context.Context, rdName, node string,
	state apiv1.ResourceState, volumes []apiv1.VolumeObservation,
) error {
	return v.inner.SetState(ctx, rdName, node, state, volumes) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) ClearDRBDPort(ctx context.Context, rdName, node string) error {
	return v.inner.ClearDRBDPort(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (v *alwaysVanishingTBResources) PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	return v.inner.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test helper
}

// bug359ContainsAll returns true if every substr is present in s
// (case-insensitive). Kept package-private with a Bug-359-specific
// prefix so it doesn't collide with the existing test-helper
// `contains` declared in snapshot_restore_test.go.
func bug359ContainsAll(s string, subs ...string) bool {
	low := strings.ToLower(s)
	for _, sub := range subs {
		if !strings.Contains(low, strings.ToLower(sub)) {
			return false
		}
	}

	return true
}
