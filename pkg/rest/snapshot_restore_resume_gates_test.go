// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The first call of a restore into a target the caller prepared is that
// restore's first run. A prepared target restored without nodes needs no
// replica to be finished, so it reached the finished-replay answer, which said
// "completed on retry" to a call that retried nothing.
func TestSnapshotRestoreIntoAPreparedTargetWithoutNodesIsNotARetry(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-pbare")
	seedRestoreSnapshot(t, st, "src-pbare", "snap-pbare", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	csiPrepareRestoreTarget(t, base, "src-pbare", "snap-pbare", "pvc-pbare")

	code, rc := restoreAnswer(t, base, "src-pbare", "snap-pbare", map[string]any{"to_resource": "pvc-pbare"})
	if code != http.StatusCreated {
		t.Fatalf("restore into the prepared target = %d (%s), want 201", code, rc.Message)
	}

	if strings.Contains(rc.Message, "on retry") {
		t.Errorf("first restore into a prepared target answered %q", rc.Message)
	}

	code, rc = restoreAnswer(t, base, "src-pbare", "snap-pbare", map[string]any{"to_resource": "pvc-pbare"})
	if code != http.StatusCreated || !strings.Contains(rc.Message, "on retry") {
		t.Errorf("the replay answered %d %q, want 201 on retry", code, rc.Message)
	}
}

// A resume over a leftover parented to a group that is gone hydrated and
// placed into it first, and only the post-write guard refused it then, which
// does not roll back a definition the request adopted. The refusal has to come
// before anything is written.
func TestSnapshotRestoreResumeRefusesAnOrphanedLeftoverBeforeWriting(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-orph")
	seedRestoreSnapshot(t, st, "src-orph", "snap-orph", []string{"node-a"})

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "dst-orph",
		ResourceGroupName: "grp-gone",
		Props:             map[string]string{restoreFromSnapshotKey: restoreMarker("src-orph", "snap-orph")},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-orph", "snap-orph", map[string]any{
		"to_resource": "dst-orph", "nodes": []string{"node-a"},
	})
	if code != http.StatusConflict {
		t.Errorf("resume over a leftover whose group is gone = %d (%s), want 409", code, rc.Message)
	}

	if vds, _ := st.VolumeDefinitions().List(ctx, "dst-orph"); len(vds) > 0 {
		t.Errorf("the refused resume hydrated %d volume(s) into the leftover", len(vds))
	}

	if replicas, _ := st.Resources().ListByDefinition(ctx, "dst-orph"); len(replicas) > 0 {
		t.Errorf("the refused resume placed %d replica(s) on the leftover", len(replicas))
	}
}

// cacheMissesTheDelete serves a replica from the cache without the DELETE flag
// the API server already holds, the way an informer does right after a delete
// began, and answers GetUncached from the backend.
type cacheMissesTheDelete struct {
	store.Store
}

type cacheMissesTheDeleteResources struct {
	store.ResourceStore
}

func (c cacheMissesTheDelete) Resources() store.ResourceStore {
	return cacheMissesTheDeleteResources{c.Store.Resources()}
}

func (r cacheMissesTheDeleteResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	res, err := r.ResourceStore.Get(ctx, rdName, node)
	res.Flags = nil

	return res, err //nolint:wrapcheck // pass-through test double
}

func (r cacheMissesTheDeleteResources) GetUncached(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	return r.ResourceStore.Get(ctx, rdName, node) //nolint:wrapcheck // pass-through test double
}

// The replica a stamp collided with was read through the cache, which right
// after a delete began still served it live, and the stamp counted a replica
// that was going away as placed.
func TestRDCloneReadsACollidingReplicaPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-lagdel")

	if err := backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-lagdel", NodeName: "node-a", Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the terminating replica: %v", err)
	}

	base, stop := startServerWithStore(t, cacheMissesTheDelete{backend})
	defer stop()

	if code := cloneOnce(t, base, "src-lagdel", "dst-lagdel", nil); code == http.StatusCreated {
		t.Errorf("clone over a replica the cache still served live = 201")
	}
}
