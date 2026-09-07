// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"strconv"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A replica names its definition in whatever case it was written with, and the
// definition is stored in whatever case IT was written with. LINSTOR treats the
// two as one object; a map does not.
//
// The per-definition read hands the name to the store, which resolves it the
// way it resolves every other name. The whole-cluster read gets a map back and
// does the lookup here, so this is the one place the equality has to be
// spelled out — and keyed raw it misses silently, rendering a definition as
// though it had no volumes and dropping the sync percentage during exactly the
// resync an operator is watching.
func TestVolumeSizesFindMixedCaseDefinitionsInTheBulkRead(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	run := &runContext{Store: st}

	// Enough definitions that volumeSizesFor takes the whole-cluster read.
	definitions := volumeSizesBulkCutoff + 1
	resources := make([]apiv1.Resource, 0, definitions)

	for i := range definitions {
		// Stored mixed-case; the replica spells it lowercase, which is what
		// `resource list` holds.
		stored := "PVC-Mixed-" + strconv.Itoa(i)
		spelled := store.FoldName(stored)

		if err := st.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: stored}); err != nil {
			t.Fatalf("seed definition: %v", err)
		}

		if err := st.VolumeDefinitions().Create(ctx, stored,
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 4096}); err != nil {
			t.Fatalf("seed volume: %v", err)
		}

		resources = append(resources, apiv1.Resource{Name: spelled, NodeName: "node-1"})
	}

	sizes := volumeSizesFor(ctx, run, resources)

	for i := range resources {
		perVolume, ok := sizes[resources[i].Name]
		if !ok {
			t.Fatalf("no sizes for %q — the lookup missed the definition it was stored under",
				resources[i].Name)
		}

		if perVolume[0] != 4096 {
			t.Errorf("%q volume 0 = %d KiB, want 4096", resources[i].Name, perVolume[0])
		}
	}
}
