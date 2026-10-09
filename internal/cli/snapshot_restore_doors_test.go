// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// `s vd restore` into a definition being deleted is refused before anything
// is written, as `s resource restore` refuses it: the volumes would be reaped
// as they land, and the command would report a restore that is not there.
func TestSnapshotVolumeDefinitionRestoreRefusesATargetBeingDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedSnapshotSource(ctx, backend)

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-dying", Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	var errBuf bytes.Buffer

	app := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &errBuf,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	if got := app.Run(ctx, []string{
		"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-dying",
	}); got == 0 {
		t.Fatal("volume-definition restore into a definition being deleted succeeded")
	}

	if vds, _ := backend.VolumeDefinitions().List(ctx, "pvc-dying"); len(vds) != 0 {
		t.Errorf("the refused restore wrote %d volume(s)", len(vds))
	}

	if !strings.Contains(errBuf.String(), "being deleted") {
		t.Errorf("stderr %q does not say the target is being deleted", errBuf.String())
	}
}

// rollbackStartsUnderTheClaimRDs starts the creator's rollback inside the
// claim: the in-progress mark lands with the adoption mark, so the claim's
// read-back sees it.
type rollbackStartsUnderTheClaimRDs struct {
	store.ResourceDefinitionStore

	target string
}

func (r rollbackStartsUnderTheClaimRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if name != r.target {
		return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
	}

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

type rollbackStartsUnderTheClaimStore struct {
	store.Store

	target string
}

func (s rollbackStartsUnderTheClaimStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return rollbackStartsUnderTheClaimRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), target: s.target}
}

// A re-run over a finished leftover takes the adoption mark before it reports
// the restore done, as the REST replay does: the mark is what tells a creator
// still rolling back that somebody was answered for the definition. One that
// meets a creator already rolling back reports the restore as not done.
func TestSnapshotRestoreReRunOfAFinishedLeftoverTakesTheMark(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	for _, tc := range []struct {
		name      string
		rollingUp bool
	}{
		{name: "nobody rolling back"},
		{name: "the creator starts rolling back", rollingUp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedSnapshotSource(ctx, backend)
			seedLeftover(t, backend, "pvc-finished", "node-1") //nolint:contextcheck // the helper seeds on t's own context

			var st store.Store = backend
			if tc.rollingUp {
				st = rollbackStartsUnderTheClaimStore{Store: backend, target: "pvc-finished"}
			}

			var outBuf, errBuf bytes.Buffer

			app := &cli.App{
				Out:      &outBuf,
				Err:      &errBuf,
				StoreFor: func(context.Context) (store.Store, error) { return st, nil },
			}

			got := app.Run(ctx, restoreResourceArgv("pvc-finished"))

			rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-finished")
			if err != nil {
				t.Fatalf("read the leftover: %v", err)
			}

			if !tc.rollingUp {
				if got != 0 || !strings.Contains(outBuf.String(), "already restored") {
					t.Fatalf("re-run = %d, out %q, err %q: want it reported restored", got, outBuf.String(), errBuf.String())
				}

				if rd.Props[store.RestoreAdoptedProp] == "" {
					t.Error("the re-run reported the restore done without taking the adoption mark")
				}

				return
			}

			if got == 0 || strings.Contains(outBuf.String(), "already restored") {
				t.Errorf("re-run under a rollback = %d, out %q: want it refused", got, outBuf.String())
			}
		})
	}
}

// finishedUnderTheCreate plays a concurrent run that finished the restore
// between this run's read of the name and its create: the create collides
// with a finished leftover. With rollingBack, the creator of that leftover
// starts rolling it back inside this run's claim.
type finishedUnderTheCreate struct {
	store.ResourceDefinitionStore

	t           *testing.T
	backend     store.Store
	rollingBack bool
}

func (f finishedUnderTheCreate) Create(_ context.Context, rd *apiv1.ResourceDefinition) error {
	seedLeftover(f.t, f.backend, rd.Name, "node-1") //nolint:contextcheck // the helper seeds on t's own context

	return store.ErrAlreadyExists
}

func (f finishedUnderTheCreate) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if !f.rollingBack {
		return f.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
	}

	return rollbackStartsUnderTheClaimRDs{ResourceDefinitionStore: f.ResourceDefinitionStore, target: name}.
		PatchResourceDefinitionSpec(ctx, name, mutate)
}

type finishedUnderTheCreateStore struct {
	store.Store

	t           *testing.T
	rollingBack bool
}

func (s finishedUnderTheCreateStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return finishedUnderTheCreate{
		ResourceDefinitionStore: s.Store.ResourceDefinitions(), t: s.t, backend: s.Store, rollingBack: s.rollingBack,
	}
}

// The same holds when the finished leftover is met by a create that collided
// with it rather than by the read before it.
func TestSnapshotRestoreCollidingWithAFinishedRestoreTakesTheMark(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		rollingBack bool
	}{
		{name: "nobody rolling back"},
		{name: "the creator starts rolling back", rollingBack: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()
			seedSnapshotSource(ctx, backend)

			var outBuf, errBuf bytes.Buffer

			app := &cli.App{
				Out: &outBuf,
				Err: &errBuf,
				StoreFor: func(context.Context) (store.Store, error) {
					return finishedUnderTheCreateStore{Store: backend, t: t, rollingBack: tc.rollingBack}, nil
				},
			}

			got := app.Run(ctx, restoreResourceArgv("pvc-collided"))

			rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-collided")
			if err != nil {
				t.Fatalf("read the leftover: %v", err)
			}

			if !tc.rollingBack {
				if got != 0 || !strings.Contains(outBuf.String(), "already restored") {
					t.Fatalf("run = %d, out %q, err %q: want it reported restored", got, outBuf.String(), errBuf.String())
				}

				if rd.Props[store.RestoreAdoptedProp] == "" {
					t.Error("the run reported the restore done without taking the adoption mark")
				}

				return
			}

			if got == 0 || strings.Contains(outBuf.String(), "already restored") {
				t.Errorf("run under a rollback = %d, out %q: want it refused", got, outBuf.String())
			}
		})
	}
}
