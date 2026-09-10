// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

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

// countingLists records how many per-definition reads a fallback actually
// issued, so a test can assert the retry did not happen rather than assert on
// its result — which is the same either way when both reads fail.
type countingLists struct {
	store.VolumeDefinitionStore

	bulkErr error
	calls   *int
}

func (c countingLists) ListAll(context.Context) (map[string][]apiv1.VolumeDefinition, error) {
	return nil, c.bulkErr
}

func (c countingLists) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	*c.calls++

	vds, err := c.VolumeDefinitionStore.List(ctx, rdName)

	return vds, errors.Wrap(err, "counted per-definition read")
}

type countingStore struct {
	store.Store

	bulkErr error
	calls   *int
}

func (c countingStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return countingLists{
		VolumeDefinitionStore: c.Store.VolumeDefinitions(),
		bulkErr:               c.bulkErr,
		calls:                 c.calls,
	}
}

func seedDefinitionsForSizes(t *testing.T, backend store.Store, prefix string, n int) []apiv1.Resource {
	t.Helper()

	ctx := t.Context()
	resources := make([]apiv1.Resource, 0, n)

	for i := range n {
		name := prefix + strconv.Itoa(i)

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

	return resources
}

// A refusal aimed at the caller is not a statement about the read's shape, so
// answering it with one narrow request per definition spends N round trips to
// be refused N times — in the command an operator is running because something
// is already wrong. The listing still prints; the reason the column is empty
// reaches stderr instead of looking like a cluster with nothing to sync.
func TestVolumeSizesDoNotRetryARefusalPerDefinition(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	calls := 0
	warnings := &bytes.Buffer{}
	run := &runContext{
		Store: countingStore{
			Store: backend,
			bulkErr: apierrors.NewForbidden(
				schema.GroupResource{Group: "blockstor.cozystack.io", Resource: "resourcedefinitions"},
				"", errors.New("no list permission")),
			calls: &calls,
		},
		Err: warnings,
	}

	resources := seedDefinitionsForSizes(t, backend, "pvc-forbidden-", volumeSizesBulkCutoff+1)

	sizes := volumeSizesFor(t.Context(), run, resources)

	if calls != 0 {
		t.Errorf("the refused bulk read was retried as %d per-definition reads", calls)
	}

	if len(sizes) != 0 {
		t.Errorf("sizes for %d definitions after a refusal that reached no data", len(sizes))
	}

	if !strings.Contains(warnings.String(), "sync percentages unavailable") {
		t.Errorf("nothing told the operator why the column is empty; stderr = %q", warnings.String())
	}
}

// A server that answered "slow down" is the budget case by the same rule the
// refusal is. Falling through turns one rejected request into one per
// definition against the server that just asked for fewer, which is the
// opposite of what it asked for and arrives while an operator is watching a
// cluster misbehave.
func TestVolumeSizesDoNotRetryAThrottledServer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"tooManyRequests", apierrors.NewTooManyRequests("slow down", 1)},
		{"serviceUnavailable", apierrors.NewServiceUnavailable("overloaded")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			calls := 0
			warnings := &bytes.Buffer{}
			run := &runContext{
				Store: countingStore{Store: backend, bulkErr: tc.err, calls: &calls},
				Err:   warnings,
			}

			resources := seedDefinitionsForSizes(t, backend, "pvc-"+tc.name+"-", volumeSizesBulkCutoff+1)

			volumeSizesFor(t.Context(), run, resources)

			if calls != 0 {
				t.Errorf("a throttled server got %d more requests", calls)
			}

			if !strings.Contains(warnings.String(), "sync percentages unavailable") {
				t.Errorf("nothing told the operator why the column is empty; stderr = %q",
					warnings.String())
			}
		})
	}
}

// Same line, drawn on the budget rather than the permission: a cancelled or
// timed-out invocation has nothing left to spend on a larger retry, and every
// one of those reads would fail on the same expired context.
func TestVolumeSizesDoNotRetryAfterTheContextIsDone(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	calls := 0
	run := &runContext{
		Store: countingStore{Store: backend, bulkErr: context.Canceled, calls: &calls},
		Err:   &bytes.Buffer{},
	}

	resources := seedDefinitionsForSizes(t, backend, "pvc-cancelled-", volumeSizesBulkCutoff+1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	volumeSizesFor(ctx, run, resources)

	if calls != 0 {
		t.Errorf("a cancelled invocation issued %d more reads", calls)
	}
}

// The positive control for both: a bulk read that failed on its own shape,
// with the invocation still live and permitted, still falls through — and does
// so silently, because the column is filled.
func TestVolumeSizesStillFallThroughOnAnOrdinaryBulkFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	calls := 0
	warnings := &bytes.Buffer{}
	run := &runContext{
		Store: countingStore{Store: backend, bulkErr: errBulkReadFailed, calls: &calls},
		Err:   warnings,
	}

	definitions := volumeSizesBulkCutoff + 1
	resources := seedDefinitionsForSizes(t, backend, "pvc-fallthrough-", definitions)

	sizes := volumeSizesFor(t.Context(), run, resources)

	if calls != definitions {
		t.Errorf("fallback issued %d per-definition reads, want %d", calls, definitions)
	}

	if len(sizes) != definitions {
		t.Errorf("sizes for %d of %d definitions", len(sizes), definitions)
	}

	if warnings.Len() != 0 {
		t.Errorf("warned about a column it went on to fill: %q", warnings.String())
	}
}
