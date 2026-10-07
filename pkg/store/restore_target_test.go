// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheBehind answers the cached volume and replica listings from a cache
// that has not caught up: it holds no volumes and no replicas. The uncached
// listings read the backend, the way the API server does.
type cacheBehind struct{ store.Store }

type cacheBehindVolumes struct{ store.VolumeDefinitionStore }

func (c cacheBehindVolumes) List(context.Context, string) ([]apiv1.VolumeDefinition, error) {
	return nil, nil
}

func (c cacheBehindVolumes) ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	return c.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type cacheBehindReplicas struct{ store.ResourceStore }

func (c cacheBehindReplicas) ListByDefinition(context.Context, string) ([]apiv1.Resource, error) {
	return nil, nil
}

func (c cacheBehindReplicas) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return c.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (c cacheBehind) VolumeDefinitions() store.VolumeDefinitionStore {
	return cacheBehindVolumes{c.Store.VolumeDefinitions()}
}

func (c cacheBehind) Resources() store.ResourceStore {
	return cacheBehindReplicas{c.Store.Resources()}
}

func seedPreparedTarget(t *testing.T, st store.Store) *apiv1.Snapshot {
	t.Helper()

	ctx := t.Context()

	for _, name := range []string{"pvc-src", "pvc-dst"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-dst",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed the restored volume: %v", err)
	}

	return &apiv1.Snapshot{
		Name: "snap", ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	}
}

// linstor-csi's volume restore and the resource restore after it can land on
// different replicas of the server. A cache on the second that had not seen
// the first's volumes refused a target that was prepared.
func TestPreparedRestoreTargetReadsTheVolumesPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	prepared, err := store.PreparedRestoreTarget(t.Context(), cacheBehind{backend}, &rd, snap)
	if err != nil || !prepared {
		t.Errorf("prepared = %v (%v), want a prepared target", prepared, err)
	}
}

// The same lag on the replicas took a target that holds a live replica, which
// may hold somebody's data.
func TestPreparedRestoreTargetReadsTheReplicasPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	if err := backend.Resources().Create(t.Context(),
		&apiv1.Resource{Name: "pvc-dst", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the live replica: %v", err)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	prepared, err := store.PreparedRestoreTarget(t.Context(), cacheBehind{backend}, &rd, snap)
	if prepared {
		t.Errorf("a target with a live replica the cache had not seen was taken (%v)", err)
	}
}

// The definition the caller read can be the cache's too; one that has been
// marked since is no longer prepared.
func TestPreparedRestoreTargetReadsTheDefinitionPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	stale, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), backend, "pvc-dst", &apiv1.Snapshot{
		Name: "other", ResourceName: "pvc-src",
	}); err != nil {
		t.Fatalf("mark the target for another restore: %v", err)
	}

	prepared, err := store.PreparedRestoreTarget(t.Context(), backend, &stale, snap)
	if prepared || errors.Is(err, store.ErrRestoreTargetLayers) {
		t.Errorf("a target marked since the caller read it was taken (%v)", err)
	}
}

// A target another request for the same restore already marked is that
// request's: the marker stays, even over the replicas that request placed.
func TestAdoptPreparedRestoreTargetLeavesAMarkerItDidNotWrite(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	if _, err := store.AdoptPreparedRestoreTarget(ctx, backend, "pvc-dst", snap); err != nil {
		t.Fatalf("mark the target as the first request did: %v", err)
	}

	if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-dst", NodeName: "node-a"}); err != nil {
		t.Fatalf("place the first request's replica: %v", err)
	}

	adopted, err := store.AdoptPreparedRestoreTarget(ctx, backend, "pvc-dst", snap)
	if adopted || err != nil {
		t.Errorf("adopt over a marker already there = (%v, %v), want (false, nil)", adopted, err)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if rd.Props[store.RestoreFromSnapshotProp] == "" {
		t.Error("the marker the first request wrote was taken off")
	}
}

// Two restores of different snapshots into one prepared target: the second
// adoption must not write its marker over the first, or every replica placed
// from then on restores from a snapshot the first restore was not told about.
func TestAdoptPreparedRestoreTargetRefusesATargetAnotherRestoreMarked(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	if _, err := store.AdoptPreparedRestoreTarget(ctx, backend, "pvc-dst", snap); err != nil {
		t.Fatalf("mark the target for the first restore: %v", err)
	}

	other := &apiv1.Snapshot{Name: "other", ResourceName: snap.ResourceName}

	adopted, err := store.AdoptPreparedRestoreTarget(ctx, backend, "pvc-dst", other)
	if adopted || !errors.Is(err, store.ErrRestoreTargetTaken) {
		t.Errorf("adopt for another snapshot = (%v, %v), want ErrRestoreTargetTaken", adopted, err)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if want := snap.ResourceName + ":" + snap.Name; rd.Props[store.RestoreFromSnapshotProp] != want {
		t.Errorf("marker = %q, want the first restore's %q", rd.Props[store.RestoreFromSnapshotProp], want)
	}
}

// This restore's own leftover is judged the way the REST door judges it:
// unfinished without a live replica, finished with one, refused while every
// replica is going away, over a group that is gone, or over a rollback that
// gave up, and not this restore's to judge under another marker or none.
func TestJudgeRestoreLeftover(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		props    map[string]string
		group    string
		replicas []apiv1.Resource
		own      bool
		want     store.LeftoverProgress
		wantErr  error
	}{
		{name: "marked-no-replicas", props: marked(), own: true, want: store.LeftoverUnfinished},
		{name: "marked-one-live", props: marked(), own: true, want: store.LeftoverFinished, replicas: []apiv1.Resource{
			{NodeName: "node-a"},
			{NodeName: "node-b", Flags: []string{apiv1.ResourceFlagDelete}},
		}},
		{
			name: "marked-all-deleting", props: marked(), own: true, want: store.LeftoverTearingDown,
			wantErr:  store.ErrRestoreTargetTearingDown,
			replicas: []apiv1.Resource{{NodeName: "node-a", Flags: []string{apiv1.ResourceFlagDelete}}},
		},
		{
			name: "marked-group-gone", props: marked(), group: "grp-gone", own: true,
			want: store.LeftoverUnfinished, wantErr: store.ErrRestoreTargetGroupGone,
		},
		{
			name: "marked-rollback-gave-up", own: true, want: store.LeftoverUnfinished,
			props: map[string]string{
				store.RestoreFromSnapshotProp: "pvc-src:snap",
				store.RollbackAbandonedProp:   "replicas",
			},
			wantErr: store.ErrRestoreRollbackAbandoned,
		},
		{name: "another-restore", props: map[string]string{store.RestoreFromSnapshotProp: "pvc-src:other"}},
		{name: "unmarked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()
			snap := seedPreparedTarget(t, backend)

			if tc.props != nil || tc.group != "" {
				if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-dst",
					func(rd *apiv1.ResourceDefinition) error {
						rd.Props = tc.props
						rd.ResourceGroupName = tc.group

						return nil
					}); err != nil {
					t.Fatalf("mark the target: %v", err)
				}
			}

			for i := range tc.replicas {
				replica := tc.replicas[i]
				replica.Name = "pvc-dst"

				if err := backend.Resources().Create(ctx, &replica); err != nil {
					t.Fatalf("seed replica: %v", err)
				}
			}

			got, own, err := store.JudgeRestoreLeftover(ctx, backend, "pvc-dst", snap)
			if own != tc.own || (own && got != tc.want) || !errors.Is(err, tc.wantErr) {
				t.Errorf("judged = (%v, %v, %v), want (%v, %v, %v)", got, own, err, tc.want, tc.own, tc.wantErr)
			}
		})
	}
}

func marked() map[string]string {
	return map[string]string{store.RestoreFromSnapshotProp: "pvc-src:snap"}
}

// Every marker carries the record of what the snapshot held, the prepared
// door's included, so a leftover can be judged once the snapshot is gone.
func TestAdoptPreparedRestoreTargetRecordsTheSnapshotVolumes(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	snap := seedPreparedTarget(t, backend)

	if _, err := store.AdoptPreparedRestoreTarget(ctx, backend, "pvc-dst", snap); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if got, want := rd.Props[store.RestoreVolumesProp], store.EncodeRestoreVolumes(snap.VolumeDefinitions); got != want {
		t.Errorf("record = %q, want %q", got, want)
	}
}

// The record describes THIS definition's restore. Copied onto a snapshot taken
// of it, and from there onto a definition restored from that snapshot, it would
// judge the second definition against the first one's volumes.
func TestTravellingPropsDropsTheRestoreRecord(t *testing.T) {
	t.Parallel()

	out := store.TravellingProps(map[string]string{
		store.RestoreVolumesProp: "0=1024",
		"Aux/keep":               "yes",
	})

	if _, ok := out[store.RestoreVolumesProp]; ok {
		t.Error("the restore record travelled")
	}

	if out["Aux/keep"] != "yes" {
		t.Error("an ordinary prop did not travel")
	}
}

// A record that does not parse is no record, never a shape to judge against.
func TestRecordedRestoreVolumes(t *testing.T) {
	t.Parallel()

	want := []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}, {VolumeNumber: 3, SizeKib: 2048}}

	got, ok := store.RecordedRestoreVolumes(map[string]string{
		store.RestoreVolumesProp: store.EncodeRestoreVolumes(want),
	})
	if !ok || len(got) != 2 || got[1].VolumeNumber != 3 || got[1].SizeKib != 2048 {
		t.Errorf("round trip = (%v, %v), want %v", got, ok, want)
	}

	for _, bad := range []string{"0", "x=1", "0=y"} {
		if _, ok := store.RecordedRestoreVolumes(map[string]string{store.RestoreVolumesProp: bad}); ok {
			t.Errorf("%q parsed as a record", bad)
		}
	}

	if _, ok := store.RecordedRestoreVolumes(nil); ok {
		t.Error("no prop parsed as a record")
	}
}
