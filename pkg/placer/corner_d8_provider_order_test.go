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

package placer_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case campaign D8 (UG9 linstor-administration.adoc ~1227-1230):
// the `--providers LVM,LVM_THIN` list has NO priority order — it is a
// membership filter only, never a preference ranking. The placer must
// pick by composite score (default MaxFreeSpace), not by the position
// of a kind in the provider list.
//
// matchesPoolFilter uses slices.Contains(filter.ProviderList, kind) —
// pure set membership. This test proves the contract end-to-end: with
// two single-node pools of different kinds where the ZFS pool has more
// free space, ZFS wins under BOTH list orderings. If the placer ever
// honored list order, [LVM_THIN, ZFS] would pick LVM_THIN and the two
// runs would diverge.

func d8Seed(t *testing.T) store.Store {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	// ZFS pool has the larger FreeCapacity → wins on MaxFreeSpace.
	mk := func(node, pool, kind string, free int64) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: node, StoragePoolName: pool,
			ProviderKind: kind, FreeCapacity: free, TotalCapacity: 10000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", pool, err)
		}
	}

	mk("n-lvm", "lvmpool", apiv1.StoragePoolKindLVMThin, 4000)
	mk("n-zfs", "zfspool", apiv1.StoragePoolKindZFS, 9000)

	return st
}

func d8PlaceWinner(t *testing.T, providerList []string) string {
	t.Helper()

	st := d8Seed(t)

	placed, _, err := placer.New(st).Place(t.Context(), "pvc-d8", &apiv1.AutoSelectFilter{
		PlaceCount:   1,
		ProviderList: providerList,
	})
	if err != nil {
		t.Fatalf("Place(%v): %v", providerList, err)
	}

	if placed != 1 {
		t.Fatalf("Place(%v): placed %d, want 1", providerList, placed)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-d8")
	if len(got) != 1 {
		t.Fatalf("Place(%v): %d resources, want 1", providerList, len(got))
	}

	pool, err := st.StoragePools().Get(t.Context(), got[0].NodeName, got[0].Props["StorPoolName"])
	if err != nil {
		t.Fatalf("lookup chosen pool: %v", err)
	}

	return pool.ProviderKind
}

// TestCornerD8ProviderListOrderIgnored proves the provider list is a
// membership filter, not a preference order: ZFS (higher free) wins
// regardless of whether it is listed first or last.
func TestCornerD8ProviderListOrderIgnored(t *testing.T) {
	t.Parallel()

	lvmFirst := d8PlaceWinner(t, []string{apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFS})
	zfsFirst := d8PlaceWinner(t, []string{apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindLVMThin})

	if lvmFirst != zfsFirst {
		t.Errorf("provider-list ORDER changed the winner: [LVM_THIN,ZFS]->%q vs [ZFS,LVM_THIN]->%q — list must have no priority order",
			lvmFirst, zfsFirst)
	}

	// And the winner must be the higher-free pool (ZFS), proving the
	// choice is score-driven, not list-position-driven.
	if lvmFirst != apiv1.StoragePoolKindZFS {
		t.Errorf("winner: got %q, want ZFS (highest FreeCapacity) — placement must be score-driven", lvmFirst)
	}
}
