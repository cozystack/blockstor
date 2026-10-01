// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"maps"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The restore marker records where one definition's data came from. A
// volume-less clone of a restored definition inherited it, so the first volume
// added to the shell later was restored from the source's snapshot, placed on
// that snapshot's nodes.
func TestRDCloneOfAVolumeLessRestoredDefinitionDropsItsMarker(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "shell-src11",
		Props: map[string]string{
			"BlockstorRestoreFromSnapshot": "other11:snap11",
			"Aux/keep":                     "1",
		},
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	resp := postClone(t, base, "shell-src11", map[string]any{"name": "shell-dst11"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", resp.StatusCode)
	}

	clone, err := backend.ResourceDefinitions().Get(t.Context(), "shell-dst11")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if marker, ok := clone.Props["BlockstorRestoreFromSnapshot"]; ok {
		t.Errorf("the shell inherited the source's restore marker %q", marker)
	}

	if clone.Props["Aux/keep"] != "1" {
		t.Errorf("the shell lost the source's ordinary props: %v", clone.Props)
	}
}

// markCacheTrails serves Get from a cache that has not seen the abandoned-
// rollback mark yet; GetUncached reads the backend.
type markCacheTrails struct{ store.ResourceDefinitionStore }

func (m markCacheTrails) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := m.ResourceDefinitionStore.Get(ctx, name)
	if err == nil {
		rd.Props = maps.Clone(rd.Props)
		delete(rd.Props, rollbackAbandonedKey)
	}

	return rd, err //nolint:wrapcheck // pass-through test double
}

type markCacheTrailsStore struct{ store.Store }

func (m markCacheTrailsStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return markCacheTrails{m.Store.ResourceDefinitions()}
}

// The mark is written through the API server by a rollback that gave up
// moments before linstor-csi's retry arrives, so the props the replay gate
// started from, one cache-served read taken before two other gates waited, did
// not carry it yet, and the replay answered 201 over a leftover the rollback
// had abandoned.
func TestRDCloneReplaySeesAnAbandonedRollbackTheCacheHasNotCaughtUpWith(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-lag11")

	base, stop := startServerWithStore(t, markCacheTrailsStore{backend})
	defer stop()

	first := postClone(t, base, "src-lag11", map[string]any{"name": "dst-lag11", "use_zfs_clone": true})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("fixture: first attempt = %d, want 201", first.StatusCode)
	}

	markDefinition(t, backend, "dst-lag11", "placement")

	retry := postClone(t, base, "src-lag11", map[string]any{"name": "dst-lag11", "use_zfs_clone": true})
	defer func() { _ = retry.Body.Close() }()

	if retry.StatusCode == http.StatusCreated {
		t.Fatal("the replay answered 201 over a leftover whose rollback gave up, read through a cache that trailed the mark")
	}

	if rc := decodeCloneMessage(t, retry); !strings.Contains(rc.Message, "rollback gave up") {
		t.Errorf("refusal %q does not say an earlier rollback gave up", rc.Message)
	}
}
