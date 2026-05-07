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

// Package storetest provides a shared test suite that any pkg/store.Store
// implementation must pass. It is consumed by both pkg/store (the in-memory
// implementation) and pkg/store/k8s (the CRD-backed one) so the two stay
// behaviourally identical.
package storetest

import (
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Factory builds a fresh, empty Store. Each subtest gets a new one so they
// don't share state.
type Factory func(t *testing.T) store.Store

// RunNodeStore exercises every branch of store.NodeStore.
func RunNodeStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) { testNodeListEmpty(t, newStore) })
	t.Run("CreateThenGet", func(t *testing.T) { testNodeCreateThenGet(t, newStore) })
	t.Run("CreateDuplicate", func(t *testing.T) { testNodeCreateDuplicate(t, newStore) })
	t.Run("GetMissing", func(t *testing.T) { testNodeGetMissing(t, newStore) })
	t.Run("UpdateMissing", func(t *testing.T) { testNodeUpdateMissing(t, newStore) })
	t.Run("UpdateChangesProps", func(t *testing.T) { testNodeUpdateChangesProps(t, newStore) })
	t.Run("DeleteMissing", func(t *testing.T) { testNodeDeleteMissing(t, newStore) })
	t.Run("DeleteRemoves", func(t *testing.T) { testNodeDeleteRemoves(t, newStore) })
	t.Run("ListSorted", func(t *testing.T) { testNodeListSorted(t, newStore) })
}

// RunStoragePoolStore exercises every branch of store.StoragePoolStore.
func RunStoragePoolStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) { testSPListEmpty(t, newStore) })
	t.Run("CreateRoundTrip", func(t *testing.T) { testSPCreateRoundTrip(t, newStore) })
	t.Run("CreateDuplicate", func(t *testing.T) { testSPCreateDuplicate(t, newStore) })
	t.Run("CreateSameNameDifferentNode", func(t *testing.T) { testSPCreateSameNameDifferentNode(t, newStore) })
	t.Run("GetMissing", func(t *testing.T) { testSPGetMissing(t, newStore) })
	t.Run("ListByNode", func(t *testing.T) { testSPListByNode(t, newStore) })
	t.Run("DeleteMissing", func(t *testing.T) { testSPDeleteMissing(t, newStore) })
	t.Run("DeleteRemoves", func(t *testing.T) { testSPDeleteRemoves(t, newStore) })
	t.Run("ListSorted", func(t *testing.T) { testSPListSorted(t, newStore) })
}

// --- NodeStore branches ---

func testNodeListEmpty(t *testing.T, newStore Factory) {
	got, err := newStore(t).Nodes().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil {
		t.Errorf("List returned nil, want empty slice")
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

func testNodeCreateThenGet(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(t.Context(), &n); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(t.Context(), "alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "alpha" || got.Type != apiv1.NodeTypeSatellite {
		t.Errorf("Get: got name=%q type=%q, want alpha/SATELLITE", got.Name, got.Type)
	}
}

func testNodeCreateDuplicate(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(ctx, &n); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &n)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

func testNodeGetMissing(t *testing.T, newStore Factory) {
	_, err := newStore(t).Nodes().Get(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func testNodeUpdateMissing(t *testing.T, newStore Factory) {
	err := newStore(t).Nodes().Update(t.Context(), &apiv1.Node{Name: "ghost"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Update missing: got %v, want ErrNotFound", err)
	}
}

func testNodeUpdateChangesProps(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Props: map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["foo"] != "bar" {
		t.Errorf("Props[foo]: got %q, want %q", got.Props["foo"], "bar")
	}
}

func testNodeDeleteMissing(t *testing.T, newStore Factory) {
	err := newStore(t).Nodes().Delete(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func testNodeDeleteRemoves(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(list) != 0 {
		t.Errorf("List after Delete: got %d, want 0", len(list))
	}
}

func testNodeListSorted(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := s.Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("Create %q: %v", name, err)
		}
	}

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d, want %d", len(got), len(want))
	}

	for i, w := range want {
		if got[i].Name != w {
			t.Errorf("[%d]: got %q, want %q", i, got[i].Name, w)
		}
	}
}

// --- StoragePoolStore branches ---

func testSPListEmpty(t *testing.T, newStore Factory) {
	got, err := newStore(t).StoragePools().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("List: got %v, want empty non-nil", got)
	}
}

func testSPCreateRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
	}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "n1", "pool")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.StoragePoolName != "pool" || got.NodeName != "n1" {
		t.Errorf("Get: got %s/%s, want n1/pool", got.NodeName, got.StoragePoolName)
	}

	if got.ProviderKind != apiv1.StoragePoolKindFileThin {
		t.Errorf("ProviderKind: got %q", got.ProviderKind)
	}
}

func testSPCreateDuplicate(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &sp)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

func testSPCreateSameNameDifferentNode(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Fatalf("Create n1: %v", err)
	}

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Errorf("Create n2: got %v, want nil", err)
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(all) != 2 {
		t.Errorf("List len: got %d, want 2", len(all))
	}
}

func testSPGetMissing(t *testing.T, newStore Factory) {
	_, err := newStore(t).StoragePools().Get(t.Context(), "ghost", "pool")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func testSPListByNode(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p3", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
	} {
		if err := s.Create(ctx, &sp); err != nil {
			t.Fatalf("Create %s/%s: %v", sp.NodeName, sp.StoragePoolName, err)
		}
	}

	got, err := s.ListByNode(ctx, "n1")
	if err != nil {
		t.Fatalf("ListByNode: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("ListByNode n1 len: got %d, want 2", len(got))
	}

	for _, sp := range got {
		if sp.NodeName != "n1" {
			t.Errorf("returned pool from %q (want n1)", sp.NodeName)
		}
	}

	got, err = s.ListByNode(ctx, "ghost")
	if err != nil {
		t.Fatalf("ListByNode ghost: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("ListByNode ghost: got %v, want empty", got)
	}
}

func testSPDeleteMissing(t *testing.T, newStore Factory) {
	err := newStore(t).StoragePools().Delete(t.Context(), "ghost", "pool")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func testSPDeleteRemoves(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1", "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1", "p1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("List after Delete: got %d, want 0", len(all))
	}
}

func testSPListSorted(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p2", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p1", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
	} {
		if err := s.Create(ctx, &sp); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	wantOrder := [][2]string{
		{"n1", "p1"},
		{"n1", "p2"},
		{"n2", "p1"},
		{"n2", "p2"},
	}

	if len(got) != len(wantOrder) {
		t.Fatalf("len: got %d, want %d", len(got), len(wantOrder))
	}

	for i, want := range wantOrder {
		if got[i].NodeName != want[0] || got[i].StoragePoolName != want[1] {
			t.Errorf("[%d]: got %s/%s, want %s/%s",
				i, got[i].NodeName, got[i].StoragePoolName, want[0], want[1])
		}
	}
}
