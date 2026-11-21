// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheMissedSnapshotDelete is a cache that has not yet seen a snapshot
// delete the API server already finished: the cached list still holds the
// snapshot as it was before the delete, with no DELETE flag.
type cacheMissedSnapshotDelete struct {
	store.SnapshotStore

	ghost apiv1.Snapshot
}

func (c cacheMissedSnapshotDelete) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	snaps, err := c.SnapshotStore.ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, err //nolint:wrapcheck // pass-through test double
	}

	if rdName == c.ghost.ResourceName {
		snaps = append(snaps, c.ghost)
	}

	return snaps, nil
}

type cacheMissedSnapshotDeleteStore struct {
	store.Store

	ghost apiv1.Snapshot
}

func (s cacheMissedSnapshotDeleteStore) Snapshots() store.SnapshotStore {
	return cacheMissedSnapshotDelete{SnapshotStore: s.Store.Snapshots(), ghost: s.ghost}
}

// linstor-csi removes the definition of a deleted volume when it deletes the
// volume's last snapshot: right after the snapshot delete answers, it lists
// the definition's snapshots and deletes the definition only when none is
// left that is not being deleted. A CSI retry of a delete that timed out
// meets the snapshot already gone and is answered at once, before this
// server's cache has seen it go. The list must not hand back the snapshot
// the delete just answered for, or linstor-csi keeps the definition for good.
func TestSnapshotListAfterDeleteDoesNotShowTheDeletedSnapshot(t *testing.T) {
	t.Parallel()

	const (
		rdName   = "pvc-deleted-volume"
		snapName = "snapshot-of-deleted-volume"
	)

	backend := store.NewInMemory()
	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	ghost := apiv1.Snapshot{
		Name:         snapName,
		ResourceName: rdName,
		Flags:        []string{apiv1.SnapshotFlagSuccessful},
	}

	base, stop := startServerWithStore(t, cacheMissedSnapshotDeleteStore{Store: backend, ghost: ghost})
	defer stop()

	del := httpDelete(t, base+"/v1/resource-definitions/"+rdName+"/snapshots/"+snapName)
	_ = del.Body.Close()

	if del.StatusCode != http.StatusOK {
		t.Fatalf("snapshot delete = %d, want 200", del.StatusCode)
	}

	resp := httpGet(t, base+"/v1/resource-definitions/"+rdName+"/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list snapshots = %d, want 200", resp.StatusCode)
	}

	var snaps []apiv1.Snapshot

	err := json.NewDecoder(resp.Body).Decode(&snaps)
	if err != nil {
		t.Fatalf("decode snapshots: %v", err)
	}

	for i := range snaps {
		if !slices.Contains(snaps[i].Flags, apiv1.ResourceFlagDelete) {
			t.Errorf("snapshot %q listed as live after its delete answered; linstor-csi keeps definition %q for it",
				snaps[i].Name, rdName)
		}
	}
}
