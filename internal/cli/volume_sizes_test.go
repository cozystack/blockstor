// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
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
		// The definition is stored one way and the replica names it another:
		// wireToCRDResourceSpec keeps Spec.ResourceDefinitionName verbatim,
		// so a replica really does carry a spelling its definition is not
		// stored under. Folding the replica's name here instead would hand
		// both sides the same key and the lookup-side fold could never be
		// the discriminator.
		stored := "pvc-fold-" + strconv.Itoa(i)
		spelled := "PVC-Fold-" + strconv.Itoa(i)

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

// failingListAll is a store whose whole-cluster read is broken and whose
// per-definition read is not — a partial outage, an RBAC gap on list, a
// request too large for the apiserver.
type failingListAll struct {
	store.VolumeDefinitionStore
}

// errBulkReadFailed stands in for whatever breaks a whole-cluster read: a
// partial outage, an RBAC gap on list, a response too large.
var errBulkReadFailed = errors.New("list every definition failed")

func (f failingListAll) ListAll(context.Context) (map[string][]apiv1.VolumeDefinition, error) {
	return nil, errBulkReadFailed
}

type failingListAllStore struct {
	store.Store
}

func (f failingListAllStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return failingListAll{f.Store.VolumeDefinitions()}
}

// One read means one failure costs every row its percentage, where the
// per-definition path loses only the definition it could not read. The two
// sides are supposed to answer the same and degrade the same, and the doc
// comment says so — so a broken bulk read falls through rather than blanking
// the column for the whole listing.
func TestVolumeSizesDegradePerDefinitionWhenTheBulkReadFails(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	run := &runContext{Store: failingListAllStore{backend}}

	definitions := volumeSizesBulkCutoff + 1
	resources := make([]apiv1.Resource, 0, definitions)

	for i := range definitions {
		name := "pvc-degrade-" + strconv.Itoa(i)

		if err := backend.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed definition: %v", err)
		}

		if err := backend.VolumeDefinitions().Create(ctx, name,
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 4096}); err != nil {
			t.Fatalf("seed volume: %v", err)
		}

		resources = append(resources, apiv1.Resource{Name: name, NodeName: "node-1"})
	}

	sizes := volumeSizesFor(ctx, run, resources)

	if len(sizes) != definitions {
		t.Fatalf("sizes for %d of %d definitions — one failed read blanked the column "+
			"for the whole listing", len(sizes), definitions)
	}
}
