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

package store

import (
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TDD: per-pool branches the StoragePoolStore must honour. The pool is keyed
// by (node_name, pool_name); both halves matter.

// TestSPListEmpty: empty store returns an empty (non-nil) slice.
func TestSPListEmpty(t *testing.T) {
	s := NewInMemory().StoragePools()

	got, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("List: got %v, want empty non-nil slice", got)
	}
}

// TestSPCreateRoundTrip: round-trip a pool and read it back unchanged.
func TestSPCreateRoundTrip(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
		FreeCapacity:    100 * 1024 * 1024,
		TotalCapacity:   200 * 1024 * 1024,
	}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "n1", "pool")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.StoragePoolName != "pool" || got.NodeName != "n1" {
		t.Errorf("Get: got name=%q node=%q, want pool/n1", got.StoragePoolName, got.NodeName)
	}

	if got.ProviderKind != apiv1.StoragePoolKindFileThin {
		t.Errorf("ProviderKind: got %q", got.ProviderKind)
	}
}

// TestSPCreateDuplicate: same (node, name) is a conflict.
func TestSPCreateDuplicate(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{StoragePoolName: "pool", NodeName: "n1"}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &sp)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

// TestSPCreateSameNameDifferentNode: same pool name on two nodes is fine —
// the key is (node, pool).
func TestSPCreateSameNameDifferentNode(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{StoragePoolName: "pool", NodeName: "n1"}); err != nil {
		t.Fatalf("Create n1: %v", err)
	}

	if err := s.Create(ctx, &apiv1.StoragePool{StoragePoolName: "pool", NodeName: "n2"}); err != nil {
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

// TestSPGetMissing: missing key returns ErrNotFound.
func TestSPGetMissing(t *testing.T) {
	s := NewInMemory().StoragePools()

	_, err := s.Get(t.Context(), "ghost", "pool")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

// TestSPListByNode: list filtered to a single node returns only that node's
// pools (not all pools on the cluster).
func TestSPListByNode(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1"},
		{StoragePoolName: "p2", NodeName: "n1"},
		{StoragePoolName: "p3", NodeName: "n2"},
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
			t.Errorf("ListByNode n1 returned pool from %q", sp.NodeName)
		}
	}

	// Filtering for a node that has nothing returns empty, not nil.
	got, err = s.ListByNode(ctx, "ghost")
	if err != nil {
		t.Fatalf("ListByNode ghost: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("ListByNode ghost: got %v, want empty", got)
	}
}

// TestSPDeleteMissing: 404 on absent (node, pool).
func TestSPDeleteMissing(t *testing.T) {
	s := NewInMemory().StoragePools()

	err := s.Delete(t.Context(), "ghost", "pool")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

// TestSPDeleteRemoves: delete a pool, then it's gone from Get and List.
func TestSPDeleteRemoves(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{StoragePoolName: "p1", NodeName: "n1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1", "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1", "p1")
	if !errors.Is(err, ErrNotFound) {
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

// TestSPListSorted: deterministic order — by node, then by pool name. This
// is part of the contract so tests can assert without sorting.
func TestSPListSorted(t *testing.T) {
	s := NewInMemory().StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p2", NodeName: "n2"},
		{StoragePoolName: "p1", NodeName: "n1"},
		{StoragePoolName: "p2", NodeName: "n1"},
		{StoragePoolName: "p1", NodeName: "n2"},
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
