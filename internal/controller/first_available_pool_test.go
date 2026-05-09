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

package controller_test

import (
	"context"
	"testing"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestFirstAvailablePoolReturnsFirstDiskful: of the pools registered
// on `n1`, the auto-diskful selector returns the first non-DISKLESS
// one. Pins the contract: the auto-promote logic must never pick a
// diskless pseudo-pool to put a real LV on.
func TestFirstAvailablePoolReturnsFirstDiskful(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	for _, p := range []apiv1.StoragePool{
		// Diskless pseudo-pool comes first in the store — must be
		// skipped, not returned.
		{NodeName: "n1", StoragePoolName: "diskless", ProviderKind: apiv1.StoragePoolKindDiskless},
		{NodeName: "n1", StoragePoolName: "thin1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{NodeName: "n1", StoragePoolName: "zfs1", ProviderKind: "ZFS_THIN"},
	} {
		if err := st.StoragePools().Create(ctx, &p); err != nil {
			t.Fatalf("seed %s: %v", p.StoragePoolName, err)
		}
	}

	rec := &controllerpkg.ResourceReconciler{Store: st}

	got, err := rec.FirstAvailablePool(ctx, "n1")
	if err != nil {
		t.Fatalf("FirstAvailablePool: %v", err)
	}

	if got == "diskless" {
		t.Errorf("returned diskless pseudo-pool: %q", got)
	}

	if got == "" {
		t.Errorf("expected a real pool; got empty")
	}
}

// TestFirstAvailablePoolFiltersOtherNodes: pools belonging to other
// nodes must not surface in the result. This is what stops the
// auto-diskful evictor from picking n2's pool when promoting on n1.
func TestFirstAvailablePoolFiltersOtherNodes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	for _, p := range []apiv1.StoragePool{
		{NodeName: "n2", StoragePoolName: "thin-on-n2", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{NodeName: "n3", StoragePoolName: "thin-on-n3", ProviderKind: apiv1.StoragePoolKindLVMThin},
	} {
		if err := st.StoragePools().Create(ctx, &p); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := &controllerpkg.ResourceReconciler{Store: st}

	// n1 has no pools at all — must return empty without error.
	got, err := rec.FirstAvailablePool(ctx, "n1")
	if err != nil {
		t.Fatalf("FirstAvailablePool: %v", err)
	}

	if got != "" {
		t.Errorf("got %q, want empty (no pools on n1)", got)
	}
}

// TestFirstAvailablePoolEmptyStore: empty store → empty result, no
// error. The auto-promote path treats this as "no candidate, leave
// the replica as DISKLESS" rather than a hard failure.
func TestFirstAvailablePoolEmptyStore(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rec := &controllerpkg.ResourceReconciler{Store: st}

	got, err := rec.FirstAvailablePool(context.Background(), "n1")
	if err != nil {
		t.Fatalf("FirstAvailablePool: %v", err)
	}

	if got != "" {
		t.Errorf("got %q on empty store, want empty", got)
	}
}
