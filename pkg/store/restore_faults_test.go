// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errInjected = errors.New("injected store failure")

// faults names the store calls a faultStore fails. Each is a read or a write a
// restore decision rests on; a nil field passes the call through.
type faults struct {
	rdGet, rdGetUncached, rdPatch error
	vdList                        error
	replicaList                   error
	rgGet                         error
}

// faultStore fails the calls its faults name and passes everything else to an
// in-memory backend, which has no cache, so a listing and its uncached twin
// read the same copy.
type faultStore struct {
	store.Store

	f faults
}

func (s faultStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return faultRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), f: s.f}
}

func (s faultStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return faultVDs{VolumeDefinitionStore: s.Store.VolumeDefinitions(), f: s.f}
}

func (s faultStore) Resources() store.ResourceStore {
	return faultReplicas{ResourceStore: s.Store.Resources(), f: s.f}
}

func (s faultStore) ResourceGroups() store.ResourceGroupStore {
	return faultRGs{ResourceGroupStore: s.Store.ResourceGroups(), f: s.f}
}

type faultRDs struct {
	store.ResourceDefinitionStore

	f faults
}

func (r faultRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if r.f.rdGet != nil {
		return apiv1.ResourceDefinition{}, r.f.rdGet
	}

	return r.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (r faultRDs) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if r.f.rdGetUncached != nil {
		return apiv1.ResourceDefinition{}, r.f.rdGetUncached
	}

	return r.ResourceDefinitionStore.GetUncached(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (r faultRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.f.rdPatch != nil {
		return r.f.rdPatch
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

type faultVDs struct {
	store.VolumeDefinitionStore

	f faults
}

func (v faultVDs) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	if v.f.vdList != nil {
		return nil, v.f.vdList
	}

	return v.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (v faultVDs) ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	return v.List(ctx, rdName)
}

type faultReplicas struct {
	store.ResourceStore

	f faults
}

func (r faultReplicas) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	if r.f.replicaList != nil {
		return nil, r.f.replicaList
	}

	return r.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (r faultReplicas) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return r.ListByDefinition(ctx, rdName)
}

type faultRGs struct {
	store.ResourceGroupStore

	f faults
}

func (g faultRGs) Get(ctx context.Context, name string) (apiv1.ResourceGroup, error) {
	if g.f.rgGet != nil {
		return apiv1.ResourceGroup{}, g.f.rgGet
	}

	return g.ResourceGroupStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

// assertUntouched fails when a decision that returned an error still wrote to
// the target: a mark, or a replica.
func assertUntouched(t *testing.T, backend store.Store, rdName string) {
	t.Helper()

	rd, err := backend.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("the target is gone: %v", err)
	}

	for _, key := range []string{store.RestoreAdoptedProp, store.RollbackAbandonedProp} {
		if v, ok := rd.Props[key]; ok {
			t.Errorf("a refused decision wrote %s=%q", key, v)
		}
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), rdName)
	if err != nil {
		t.Fatalf("list the replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("a refused decision placed %d replica(s)", len(replicas))
	}
}

// A prepared target is judged on four reads. Any that fails is an error, never
// a verdict: "not prepared" refuses a valid restore, and "prepared" marks a
// definition nobody looked at.
func TestPreparedRestoreTargetFailsClosedOnAFailedRead(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		f    faults
	}{
		{name: "definition", f: faults{rdGetUncached: errInjected}},
		{name: "volumes", f: faults{vdList: errInjected}},
		{name: "replicas", f: faults{replicaList: errInjected}},
		{name: "source", f: faults{rdGet: errInjected}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			snap := seedPreparedTarget(t, backend)

			rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-dst")
			if err != nil {
				t.Fatalf("read the target: %v", err)
			}

			prepared, err := store.PreparedRestoreTarget(t.Context(), faultStore{backend, tc.f}, &rd, snap)
			if prepared || !errors.Is(err, errInjected) {
				t.Errorf("judged (%v, %v) over a failed read, want (false, the read's error)", prepared, err)
			}

			assertUntouched(t, backend, "pvc-dst")
		})
	}
}

// Replicas that are all going away are refused with a reason, not passed over
// as "not prepared", which reads to the caller as somebody else's definition.
func TestPreparedRestoreTargetRefusesATargetBeingTornDown(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-dst", NodeName: "node-a", Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the terminating replica: %v", err)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	prepared, err := store.PreparedRestoreTarget(t.Context(), backend, &rd, snap)
	if prepared || !errors.Is(err, store.ErrRestoreTargetTearingDown) {
		t.Errorf("judged (%v, %v), want (false, ErrRestoreTargetTearingDown)", prepared, err)
	}
}

// markTarget puts this restore's marker, and whatever else the case needs, on
// the seeded target.
func markTarget(t *testing.T, backend store.Store, group string, flags []string) {
	t.Helper()

	if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-dst",
		func(rd *apiv1.ResourceDefinition) error {
			rd.Props = marked()
			rd.ResourceGroupName = group
			rd.Flags = flags

			return nil
		}); err != nil {
		t.Fatalf("mark the target: %v", err)
	}
}

// A leftover is judged on reads too, and a failed one is an error the door
// stops on, never a verdict it acts on.
func TestJudgeRestoreLeftoverFailsClosedOnAFailedRead(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		group string
		f     faults
	}{
		{name: "definition", f: faults{rdGetUncached: errInjected}},
		{name: "volumes", f: faults{vdList: errInjected}},
		{name: "replicas", f: faults{replicaList: errInjected}},
		{name: "group", group: "grp", f: faults{rgGet: errInjected}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			snap := seedPreparedTarget(t, backend)

			if err := backend.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "grp"}); err != nil {
				t.Fatalf("seed the group: %v", err)
			}

			markTarget(t, backend, tc.group, nil)

			_, _, err := store.JudgeRestoreLeftover(t.Context(), faultStore{backend, tc.f}, "pvc-dst", snap)
			if !errors.Is(err, errInjected) {
				t.Errorf("judged over a failed read with error %v, want the read's error", err)
			}

			assertUntouched(t, backend, "pvc-dst")
		})
	}
}

// The DELETE flag on the definition itself is a tear-down, refused before the
// leftover is assessed, and a volume the snapshot never recorded makes the
// leftover somebody else's.
func TestJudgeRestoreLeftoverRefusesADeletedOrForeignLeftover(t *testing.T) {
	t.Parallel()

	t.Run("deleting", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		snap := seedPreparedTarget(t, backend)
		markTarget(t, backend, "", []string{apiv1.ResourceFlagDelete})

		got, own, err := store.JudgeRestoreLeftover(t.Context(), backend, "pvc-dst", snap)
		if !own || got != store.LeftoverTearingDown || !errors.Is(err, store.ErrRestoreTargetTearingDown) {
			t.Errorf("judged (%v, %v, %v), want (TearingDown, true, ErrRestoreTargetTearingDown)", got, own, err)
		}
	})

	t.Run("foreign", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		snap := seedPreparedTarget(t, backend)
		markTarget(t, backend, "", nil)

		if err := backend.VolumeDefinitions().Create(t.Context(), "pvc-dst",
			&apiv1.VolumeDefinition{VolumeNumber: 5, SizeKib: 1 << 20}); err != nil {
			t.Fatalf("seed the foreign volume: %v", err)
		}

		got, own, err := store.JudgeRestoreLeftover(t.Context(), backend, "pvc-dst", snap)
		if !own || got != store.LeftoverForeign || !errors.Is(err, store.ErrRestoreTargetForeign) {
			t.Errorf("judged (%v, %v, %v), want (Foreign, true, ErrRestoreTargetForeign)", got, own, err)
		}
	})

	// The REST door refuses a foreign volume before it asks about the group
	// or a rollback mark, and the CLI asks in the same order, so the two
	// word one leftover the same way.
	t.Run("foreign-before-group-and-mark", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		snap := seedPreparedTarget(t, backend)
		markTarget(t, backend, "grp-gone", nil)

		if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-dst",
			func(rd *apiv1.ResourceDefinition) error {
				rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress

				return nil
			}); err != nil {
			t.Fatalf("mark a rollback in progress: %v", err)
		}

		if err := backend.VolumeDefinitions().Create(t.Context(), "pvc-dst",
			&apiv1.VolumeDefinition{VolumeNumber: 5, SizeKib: 1 << 20}); err != nil {
			t.Fatalf("seed the foreign volume: %v", err)
		}

		_, _, err := store.JudgeRestoreLeftover(t.Context(), backend, "pvc-dst", snap)
		if !errors.Is(err, store.ErrRestoreTargetForeign) {
			t.Errorf("judged %v, want ErrRestoreTargetForeign ahead of the group and the mark", err)
		}
	})
}

// The replica count a leftover is judged on is an error when the listing
// fails, never zero replicas.
func TestCountReplicasReportsAFailedListing(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedPreparedTarget(t, backend)

	if _, err := store.CountReplicas(t.Context(), faultStore{backend, faults{replicaList: errInjected}},
		"pvc-dst"); !errors.Is(err, errInjected) {
		t.Errorf("CountReplicas over a failed listing = %v, want the listing's error", err)
	}
}

// The adopting half of the handshake must not go on when its own mark did not
// land: the creator's rollback would not see it and would delete what the
// adopter then answers for.
func TestClaimAdoptedLeftoverFailsWhenTheMarkDoesNotLand(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedPreparedTarget(t, backend)

	if err := store.ClaimAdoptedLeftover(t.Context(), faultStore{backend, faults{rdPatch: errInjected}},
		"pvc-dst", &apiv1.Snapshot{}); !errors.Is(err, errInjected) {
		t.Errorf("claim over a refused mark = %v, want the patch's error", err)
	}
}

// A rollback mark that cannot be cleared is reported: the caller tells the
// operator the definition still carries it, and a retry would meet it.
func TestReleaseRollbackReportsAMarkItCouldNotClear(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedPreparedTarget(t, backend)

	if err := store.ReleaseRollback(t.Context(), faultStore{backend, faults{rdPatch: errInjected}},
		"pvc-dst"); !errors.Is(err, errInjected) {
		t.Errorf("release over a refused patch = %v, want the patch's error", err)
	}
}

// A rollback that stopped at the definition's snapshots touched nothing, and
// `rd d` refuses the definition over them, so the CLI names the clear as the
// way to keep it, as the REST door does, rather than only a delete that is
// refused.
func TestJudgeRestoreLeftoverOverASnapshotsMarkNamesTheClear(t *testing.T) {
	t.Parallel()

	for _, step := range []string{store.RollbackStepSnapshots, store.RollbackStepReadSnapshots} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			snap := seedPreparedTarget(t, backend)
			markTarget(t, backend, "", nil)

			if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-dst",
				func(rd *apiv1.ResourceDefinition) error {
					rd.Props[store.RollbackAbandonedProp] = step

					return nil
				}); err != nil {
				t.Fatalf("mark the rollback's step: %v", err)
			}

			_, _, err := store.JudgeRestoreLeftover(t.Context(), backend, "pvc-dst", snap)
			if !errors.Is(err, store.ErrRestoreRollbackAbandoned) ||
				!strings.Contains(err.Error(), "set-property pvc-dst "+store.RollbackAbandonedProp) {
				t.Errorf("judged %v, want the abandoned-rollback refusal naming the clear", err)
			}
		})
	}
}

// Once the snapshot is gone, a leftover the CLI refuses is not told to
// "restore again": deleted, it would meet a snapshot that is no longer there.
// It names another snapshot, as the tear-down refusal does.
func TestJudgeRestoreLeftoverWithTheSnapshotGoneNamesAnotherSnapshot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mark string
	}{
		{name: "foreign"},
		{name: "rollback-gave-up", mark: "reap-replicas"},
		{name: "rollback-in-progress", mark: store.RollbackInProgress},
		{name: "rollback-stopped-at-snapshots", mark: store.RollbackStepSnapshots},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			snap := seedPreparedTarget(t, backend)
			markTarget(t, backend, "", nil)

			// The marker as every door writes it, with the record of what
			// the snapshot held: the leftover is judged against it once the
			// snapshot is gone.
			if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-dst",
				func(rd *apiv1.ResourceDefinition) error {
					rd.Props = store.WithRestoreMarker(rd.Props, snap)

					return nil
				}); err != nil {
				t.Fatalf("record the snapshot's volumes: %v", err)
			}

			if tc.mark != "" {
				if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-dst",
					func(rd *apiv1.ResourceDefinition) error {
						rd.Props[store.RollbackAbandonedProp] = tc.mark

						return nil
					}); err != nil {
					t.Fatalf("mark the rollback's step: %v", err)
				}
			} else if err := backend.VolumeDefinitions().Create(t.Context(), "pvc-dst",
				&apiv1.VolumeDefinition{VolumeNumber: 5, SizeKib: 1 << 20}); err != nil {
				t.Fatalf("seed the foreign volume: %v", err)
			}

			_, _, err := store.JudgeRestoreLeftoverOf(t.Context(), backend, "pvc-dst", snap.ResourceName, snap.Name, nil)
			if err == nil {
				t.Fatal("judged nil, want a refusal")
			}

			if !strings.Contains(err.Error(), "another snapshot") || strings.Contains(err.Error(), "restore again") {
				t.Errorf("judged %v, want a refusal naming another snapshot and not %q", err, "restore again")
			}

			// The arms that name the clear name the definition in it, not the
			// snapshot or the way back to a restore.
			if (tc.mark == store.RollbackInProgress || tc.mark == store.RollbackStepSnapshots) &&
				!strings.Contains(err.Error(), "set-property pvc-dst "+store.RollbackAbandonedProp) {
				t.Errorf("judged %v, want the clear named for pvc-dst", err)
			}
		})
	}
}
