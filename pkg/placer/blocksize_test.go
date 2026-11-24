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
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedBlockSizeStore lays out a definition with one replica on n1 and
// two free nodes: n2 on a 512 pool and n3 on a 4096 pool with the most
// free space, so the scorer picks n3 first unless something filters it.
func seedBlockSizeStore(t *testing.T, vdProps map[string]string, inUse bool) store.Store {
	t.Helper()

	ctx := t.Context()
	st := store.NewInMemory()

	for i, sp := range []struct{ node, minIo string }{{"n1", "512"}, {"n2", "512"}, {"n3", "4096"}} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: sp.node, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", sp.node, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        sp.node,
			StoragePoolName: "pool",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    int64(i+1) * 100,
			TotalCapacity:   1000,
			Props:           map[string]string{blocksize.PropPoolMinIoSize: sp.minIo},
		}); err != nil {
			t.Fatalf("seed pool %s: %v", sp.node, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-bs"}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-bs", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 10, Props: vdProps}); err != nil {
		t.Fatalf("seed vd: %v", err)
	}

	replica := &apiv1.Resource{Name: "pvc-bs", NodeName: "n1", Props: map[string]string{"StorPoolName": "pool"}}
	if inUse {
		replica.State.InUse = &inUse
	}

	if err := st.Resources().Create(ctx, replica); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	return st
}

func placedNodes(t *testing.T, st store.Store) map[string]bool {
	t.Helper()

	got, err := st.Resources().ListByDefinition(t.Context(), "pvc-bs")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	on := map[string]bool{}
	for i := range got {
		on[got[i].NodeName] = true
	}

	return on
}

// TestAutoplaceSkipsPoolAboveFixedBlockSize: LINSTOR's autoplacer does
// not offer a pool whose minimum I/O size is above a definition's block
// size once that size may no longer change, frozen or under a Primary.
// Placing there anyway gave a replica larger sectors than its peers.
func TestAutoplaceSkipsPoolAboveFixedBlockSize(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		vdProps map[string]string
		inUse   bool
	}{
		{name: "frozen", vdProps: map[string]string{blocksize.PropBlockSize: "512", blocksize.PropFreeze: "True"}},
		{name: "primary", vdProps: map[string]string{blocksize.PropBlockSize: "512"}, inUse: true},
		{name: "frozen-without-size", vdProps: map[string]string{blocksize.PropFreeze: "True"}},
		{name: "auto-off-with-size", vdProps: map[string]string{blocksize.PropBlockSize: "512", blocksize.PropAuto: "false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := seedBlockSizeStore(t, tc.vdProps, tc.inUse)

			placed, _, err := placer.New(st).Place(t.Context(), "pvc-bs", &apiv1.AutoSelectFilter{PlaceCount: 2})
			if err != nil {
				t.Fatalf("Place: %v", err)
			}

			on := placedNodes(t, st)
			if placed != 2 || !on["n2"] || on["n3"] {
				t.Fatalf("placed %d on %v, want the second replica on the 512 pool of n2", placed, on)
			}
		})
	}
}

// TestAutoplaceOffersLargerPoolWhileSizeCanChange: with nothing frozen and
// nothing Primary, or with the automatic size off and no size set, the
// larger pool stays a candidate, as in LINSTOR.
func TestAutoplaceOffersLargerPoolWhileSizeCanChange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		vdProps map[string]string
	}{
		{name: "changeable", vdProps: map[string]string{blocksize.PropBlockSize: "512"}},
		{name: "auto-off-no-size", vdProps: map[string]string{blocksize.PropAuto: "false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := seedBlockSizeStore(t, tc.vdProps, false)

			if _, _, err := placer.New(st).Place(t.Context(), "pvc-bs", &apiv1.AutoSelectFilter{PlaceCount: 2}); err != nil {
				t.Fatalf("Place: %v", err)
			}

			if on := placedNodes(t, st); !on["n3"] {
				t.Fatalf("placed on %v, want the 4096 pool of n3", on)
			}
		})
	}
}
