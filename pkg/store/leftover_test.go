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

// cacheMissesTheDelete serves a replica list from a cache that has not seen
// the DELETE stamp yet; the API server has.
type cacheMissesTheDelete struct{ store.ResourceStore }

func (c cacheMissesTheDelete) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	replicas, err := c.ResourceStore.ListByDefinition(ctx, rdName)
	for i := range replicas {
		replicas[i].Flags = nil
	}

	return replicas, err //nolint:wrapcheck // pass-through test double
}

func (c cacheMissesTheDelete) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return c.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type cacheMissesTheDeleteStore struct{ store.Store }

func (c cacheMissesTheDeleteStore) Resources() store.ResourceStore {
	return cacheMissesTheDelete{c.Store.Resources()}
}

// Every leftover decision counts replicas, and a lagging informer that still
// showed a replica being deleted as live called a restore finished over a
// definition that is going away.
func TestCountReplicasReadsPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-dst", NodeName: "node-a", Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the replica being deleted: %v", err)
	}

	count, err := store.CountReplicas(t.Context(), cacheMissesTheDeleteStore{backend}, "pvc-dst")
	if err != nil {
		t.Fatalf("count: %v", err)
	}

	if count.Live != 0 || count.Total != 1 {
		t.Errorf("live/total = %d/%d, want 0/1: the cache's view of the replica decided", count.Live, count.Total)
	}
}

// A rollback still running is not one that gave up, and the refusal says so
// rather than telling the operator to delete a definition that is about to go
// on its own.
func TestJudgeRestoreLeftoverWordsARollbackStillInProgress(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-dst", Props: map[string]string{
			store.RestoreFromSnapshotProp: "pvc-src:snap",
			store.RollbackAbandonedProp:   store.RollbackInProgress,
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	_, own, err := store.JudgeRestoreLeftover(ctx, backend, "pvc-dst", &apiv1.Snapshot{
		Name: "snap", ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	})
	if !own || !errors.Is(err, store.ErrRestoreRollbackAbandoned) {
		t.Fatalf("judge = own %v, %v; want a refusal", own, err)
	}

	if !strings.Contains(err.Error(), "being rolled back") {
		t.Errorf("refusal %q does not say a rollback is still running", err)
	}
}

// A diskless or tie-breaker replica holds no copy of the data, so a leftover
// carrying every volume and only such a replica is not finished: answered as
// done, it reported a volume restored with no data on any node. Nor is it torn
// down, since that replica is not going away.
func TestAssessLeftoverDoesNotCountAReplicaWithoutDataAsFinished(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker} {
		t.Run(flag, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
				Name: "pvc-nodata", NodeName: "node-a", Flags: []string{flag},
			}); err != nil {
				t.Fatalf("seed the replica without data: %v", err)
			}

			vds := []apiv1.VolumeDefinition{{VolumeNumber: 0, SizeKib: 1024}}
			snap := &apiv1.Snapshot{VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}}}

			progress, err := store.AssessLeftover(t.Context(), backend, "pvc-nodata", vds, snap, true)
			if err != nil {
				t.Fatalf("assess: %v", err)
			}

			if progress != store.LeftoverUnfinished {
				t.Errorf("leftover whose only replica is %s = %v, want unfinished", flag, progress)
			}
		})
	}
}

// A rollback that leaves a finished definition names the requested nodes that
// hold no copy of the data, and a diskless replica there is not one.
func TestMissingReplicasNamesANodeHoldingOnlyADisklessReplica(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()

	for _, res := range []apiv1.Resource{
		{Name: "pvc-miss", NodeName: "node-a"},
		{Name: "pvc-miss", NodeName: "node-b", Flags: []string{apiv1.ResourceFlagDiskless}},
		{Name: "pvc-miss", NodeName: "node-c", Flags: []string{apiv1.ResourceFlagDelete}},
	} {
		if err := backend.Resources().Create(t.Context(), &res); err != nil {
			t.Fatalf("seed the replica on %s: %v", res.NodeName, err)
		}
	}

	missing, err := store.MissingReplicas(t.Context(), backend, "pvc-miss", []string{"node-a", "node-b", "node-c"})
	if err != nil {
		t.Fatalf("missing replicas: %v", err)
	}

	if strings.Join(missing, ",") != "node-b,node-c" {
		t.Errorf("missing = %v, want [node-b node-c]", missing)
	}
}
