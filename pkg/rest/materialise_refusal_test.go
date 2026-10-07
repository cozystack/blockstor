// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rollbackStartsOnAdoption plays the attempt that created the definition
// starting its rollback at the moment this request adopts it: the adoption
// mark and the in-progress rollback mark land together, so the adopter reads
// the rollback back and refuses.
type rollbackStartsOnAdoption struct{ store.ResourceDefinitionStore }

func (r rollbackStartsOnAdoption) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, //nolint:wrapcheck // test double
		func(rd *apiv1.ResourceDefinition) error {
			if err := mutate(rd); err != nil {
				return err
			}

			if rd.Props[store.RestoreAdoptedProp] != "" {
				rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress
			}

			return nil
		})
}

type rollbackStartsOnAdoptionStore struct{ store.Store }

func (r rollbackStartsOnAdoptionStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return rollbackStartsOnAdoption{r.Store.ResourceDefinitions()}
}

// Both doors answer a creator already rolling back with the typed 409 of the
// definition it is about, and write nothing into the leftover.
func TestResumeOverACreatorRollingBackCarriesTheDefinitionBand(t *testing.T) {
	t.Parallel()

	t.Run("restore", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		seedDeployedCloneSource(t, backend, "src-rb")
		seedRestoreSnapshot(t, backend, "src-rb", "snap-rb", []string{"node-a"})
		seedRestoreLeftover(t, backend, "src-rb", "snap-rb", "dst-rb", nil)

		base, stop := startServerWithStore(t, rollbackStartsOnAdoptionStore{backend})
		defer stop()

		code, rc := restoreAnswer(t, base, "src-rb", "snap-rb", map[string]any{"to_resource": "dst-rb"})
		if code != http.StatusConflict {
			t.Fatalf("restore resume over a creator rolling back = %d %q, want 409", code, rc.Message)
		}

		if rc.RetCode&apiCallRcFailExistsRscDfn != apiCallRcFailExistsRscDfn {
			t.Errorf("refusal ret_code %#x carries no FAIL_EXISTS_RSC_DFN band", rc.RetCode)
		}

		if vds, _ := backend.VolumeDefinitions().List(t.Context(), "dst-rb"); len(vds) != 0 {
			t.Errorf("the refused resume hydrated %d volume(s)", len(vds))
		}
	})

	t.Run("clone", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		seedDeployedCloneSource(t, backend, "src-cb")

		if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
			Name:  "dst-cb",
			Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-cb", cloneSnapshotName("dst-cb"))},
		}); err != nil {
			t.Fatalf("seed the leftover: %v", err)
		}

		base, stop := startServerWithStore(t, rollbackStartsOnAdoptionStore{backend})
		defer stop()

		resp := postClone(t, base, "src-cb", map[string]any{"name": "dst-cb"})
		defer func() { _ = resp.Body.Close() }()

		rc := decodeCloneMessage(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("clone resume over a creator rolling back = %d %q, want 409", resp.StatusCode, rc.Message)
		}

		if rc.RetCode&apiCallRcFailExistsRscDfn != apiCallRcFailExistsRscDfn {
			t.Errorf("refusal ret_code %#x carries no FAIL_EXISTS_RSC_DFN band", rc.RetCode)
		}

		if vds, _ := backend.VolumeDefinitions().List(t.Context(), "dst-cb"); len(vds) != 0 {
			t.Errorf("the refused resume hydrated %d volume(s)", len(vds))
		}
	})
}

// sourceGoneAfterSnapshot answers NotFound for the source once the clone has
// taken its internal snapshot: a source deleted while the clone runs, after
// every check the handler makes on it up front.
type sourceGoneAfterSnapshot struct {
	store.ResourceDefinitionStore

	backend  store.Store
	source   string
	snapshot string
	reads    *atomic.Int32
}

func (s sourceGoneAfterSnapshot) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == s.source {
		if _, err := s.backend.Snapshots().Get(ctx, s.source, s.snapshot); err == nil {
			s.reads.Add(1)

			return apiv1.ResourceDefinition{}, store.ErrNotFound
		}
	}

	return s.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

type sourceGoneAfterSnapshotStore struct {
	store.Store

	source   string
	snapshot string
	reads    *atomic.Int32
}

func (s sourceGoneAfterSnapshotStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return sourceGoneAfterSnapshot{
		ResourceDefinitionStore: s.Store.ResourceDefinitions(),
		backend:                 s.Store, source: s.source, snapshot: s.snapshot, reads: s.reads,
	}
}

// A failure that is not this request's partial work keeps its typed store
// status on the clone door, the way it does on the restore door, rather than a
// bare 500, and leaves no target behind. The internal snapshot stays, as it
// did on every failure before: its name is the target's, so a retry takes it
// up again rather than taking another.
func TestRDCloneAnswersASourceGoneMidCloneWithItsTypedStatus(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-mid")

	var reads atomic.Int32

	base, stop := startServerWithStore(t, sourceGoneAfterSnapshotStore{
		Store: backend, source: "src-mid", snapshot: cloneSnapshotName("dst-mid"), reads: &reads,
	})
	defer stop()

	resp := postClone(t, base, "src-mid", map[string]any{"name": "dst-mid"})
	defer func() { _ = resp.Body.Close() }()

	rc := decodeCloneMessage(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("clone whose source went mid-clone = %d %q, want 404", resp.StatusCode, rc.Message)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "dst-mid"); err == nil {
		t.Error("the clone left its target behind")
	}

	if reads.Load() == 0 {
		t.Fatal("fixture: the source never went away mid-clone")
	}

	snaps, _ := backend.Snapshots().ListByDefinition(t.Context(), "src-mid")
	if len(snaps) != 1 || snaps[0].Name != cloneSnapshotName("dst-mid") {
		t.Errorf("snapshots on the source = %v, want only the internal one a retry takes up", snaps)
	}
}
