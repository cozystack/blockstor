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
	"errors"
	"net/http"
	"slices"
	"testing"

	lapiconsts "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedTruncateFixture seeds rdName with a volume, diskful replicas on w1 and
// w2 and a diskless one on w3, and optionally one snapshot.
func seedTruncateFixture(t *testing.T, st store.Store, rdName string, withSnapshot bool) {
	t.Helper()

	ctx := t.Context()

	for _, node := range []string{"w1", "w2", "w3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, ConnectionStatus: apiv1.NodeTypeOnline}); err != nil &&
			!errors.Is(err, store.ErrAlreadyExists) {
			t.Fatalf("seed node %s: %v", node, err)
		}
	}

	seedRDWithVD(t, st, rdName, 1024*1024)

	for _, res := range []apiv1.Resource{
		{Name: rdName, NodeName: "w1"},
		{Name: rdName, NodeName: "w2"},
		{Name: rdName, NodeName: "w3", Flags: []string{apiv1.ResourceFlagDiskless}},
	} {
		if err := st.Resources().Create(ctx, &res); err != nil {
			t.Fatalf("seed replica on %s: %v", res.NodeName, err)
		}
	}

	if withSnapshot {
		if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name:         "snap-1",
			ResourceName: rdName,
			Nodes:        []string{"w1", "w2"},
		}); err != nil {
			t.Fatalf("seed snapshot: %v", err)
		}
	}
}

func storedReplicaNodes(t *testing.T, st store.Store, rdName string) []string {
	t.Helper()

	replicas, err := st.Resources().ListByDefinition(t.Context(), rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("list replicas: %v", err)
	}

	out := make([]string, 0, len(replicas))
	for i := range replicas {
		out = append(out, replicas[i].NodeName)
	}

	slices.Sort(out)

	return out
}

func definitionExists(t *testing.T, st store.Store, rdName string) bool {
	t.Helper()

	_, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get %s: %v", rdName, err)
	}

	return err == nil
}

// TestRDTruncateFollowsDeleteVolume drives linstor-csi v1.13's DeleteVolume
// through golinstor v0.65.4: truncate with delete_empty_resource_definition.
// Without snapshots the definition goes with its replicas; with one it
// stays, with its volume definitions and its snapshot.
func TestRDTruncateFollowsDeleteVolume(t *testing.T) {
	t.Parallel()

	for _, withSnapshot := range []bool{false, true} {
		st := store.NewInMemory()
		seedTruncateFixture(t, st, "pvc-trunc", withSnapshot)

		base, stop := startServerWithStore(t, st)
		c := newClient(t, base)

		err := c.ResourceDefinitions.Truncate(t.Context(), "pvc-trunc",
			&lapi.ResourceDefinitionTruncateOpts{DeleteEmptyResourceDefinition: true})
		if err != nil {
			t.Fatalf("snapshot=%v: Truncate: %v", withSnapshot, err)
		}

		if got := storedReplicaNodes(t, st, "pvc-trunc"); len(got) != 0 {
			t.Errorf("snapshot=%v: replicas left: %v", withSnapshot, got)
		}

		if got := definitionExists(t, st, "pvc-trunc"); got != withSnapshot {
			t.Errorf("snapshot=%v: definition exists = %v, want %v", withSnapshot, got, withSnapshot)
		}

		if withSnapshot {
			vds, vdErr := st.VolumeDefinitions().List(t.Context(), "pvc-trunc")
			if vdErr != nil || len(vds) != 1 {
				t.Errorf("volume definitions after truncate: %v, %v", vds, vdErr)
			}

			rd, rdErr := st.ResourceDefinitions().Get(t.Context(), "pvc-trunc")
			if rdErr == nil && slices.Contains(rd.Flags, rdFlagDelete) {
				t.Errorf("a definition its snapshot keeps is left marked DELETE: %v", rd.Flags)
			}
		}

		stop()
	}
}

// TestRDTruncateKeepsTheDefinitionWithoutTheFlag: a plain truncate deletes
// the replicas only.
func TestRDTruncateKeepsTheDefinitionWithoutTheFlag(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTruncateFixture(t, st, "pvc-plain", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if err := newClient(t, base).ResourceDefinitions.Truncate(t.Context(), "pvc-plain"); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if got := storedReplicaNodes(t, st, "pvc-plain"); len(got) != 0 {
		t.Errorf("replicas left: %v", got)
	}

	if !definitionExists(t, st, "pvc-plain") {
		t.Error("a truncate without delete_empty_resource_definition deleted the definition")
	}
}

// TestRDTruncateRefusesAReplicaInUse: FAIL_IN_USE, nothing deleted. A
// replica on an OFFLINE node is in use nowhere.
func TestRDTruncateRefusesAReplicaInUse(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTruncateFixture(t, st, "pvc-used", false)

	inUse := true
	if err := st.Resources().SetState(t.Context(), "pvc-used", "w2", apiv1.ResourceState{InUse: &inUse}, nil); err != nil {
		t.Fatalf("mark in use: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	opts := &lapi.ResourceDefinitionTruncateOpts{DeleteEmptyResourceDefinition: true}

	err := c.ResourceDefinitions.Truncate(t.Context(), "pvc-used", opts)
	if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailInUse)) {
		t.Fatalf("Truncate of an in-use definition: %v, want FAIL_IN_USE", err)
	}

	if got := storedReplicaNodes(t, st, "pvc-used"); !slices.Equal(got, []string{"w1", "w2", "w3"}) {
		t.Errorf("a refused truncate deleted replicas: %v left", got)
	}

	if err := st.Nodes().SetConnectionStatus(t.Context(), "w2", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("mark w2 offline: %v", err)
	}

	if err := c.ResourceDefinitions.Truncate(t.Context(), "pvc-used", opts); err != nil {
		t.Fatalf("Truncate with the user on an OFFLINE node: %v", err)
	}

	if definitionExists(t, st, "pvc-used") {
		t.Error("the definition survived a truncate with delete_empty_resource_definition")
	}
}

// TestRDTruncateUnknownDefinitionWarns: 200 with WARN_NOT_FOUND, which
// golinstor reads as success.
func TestRDTruncateUnknownDefinitionWarns(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-none/resources?delete_empty_resource_definition=True")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	rcs := decodeRCs(t, resp)
	if rcs[0].RetCode != warnNotFound {
		t.Errorf("ret_code %#x, want WARN_NOT_FOUND %#x", rcs[0].RetCode, warnNotFound)
	}
}

// TestSnapshotDeleteDeletesTheEmptyDefinition drives linstor-csi v1.13's
// DeleteSnapshot through golinstor v0.65.4. The last snapshot of a deleted
// volume takes the definition with it; a definition that still has
// replicas stays; a repeated call finishes what an earlier one left.
func TestSnapshotDeleteDeletesTheEmptyDefinition(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedTruncateFixture(t, st, "pvc-live", true)
	seedTruncateFixture(t, st, "pvc-gone", true)

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	opts := &lapi.SnapshotDeleteOpts{DeleteEmptyResourceDefinition: true}

	// A volume still deployed keeps its definition.
	if err := c.Resources.DeleteSnapshot(ctx, "pvc-live", "snap-1", opts); err != nil {
		t.Fatalf("DeleteSnapshot pvc-live: %v", err)
	}

	if !definitionExists(t, st, "pvc-live") {
		t.Error("deleting a snapshot deleted a definition that has replicas")
	}

	// A volume deleted while the snapshot lived: its definition goes
	// with the snapshot.
	if err := c.ResourceDefinitions.Truncate(ctx, "pvc-gone"); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if err := c.Resources.DeleteSnapshot(ctx, "pvc-gone", "snap-1", opts); err != nil {
		t.Fatalf("DeleteSnapshot pvc-gone: %v", err)
	}

	if definitionExists(t, st, "pvc-gone") {
		t.Error("the last snapshot of a deleted volume left its definition behind")
	}

	// A retry after the snapshot went, and an earlier attempt that
	// stopped before the definition: the definition still goes.
	seedRDWithVD(t, st, "pvc-retry", 1024)

	if err := c.Resources.DeleteSnapshot(ctx, "pvc-retry", "snap-1", opts); err != nil {
		t.Fatalf("DeleteSnapshot pvc-retry: %v", err)
	}

	if definitionExists(t, st, "pvc-retry") {
		t.Error("a repeated DeleteSnapshot left the empty definition behind")
	}

	// Without the flag the definition stays, as before.
	seedRDWithVD(t, st, "pvc-keep", 1024)

	if err := c.Resources.DeleteSnapshot(ctx, "pvc-keep", "snap-1"); err != nil {
		t.Fatalf("DeleteSnapshot pvc-keep: %v", err)
	}

	if !definitionExists(t, st, "pvc-keep") {
		t.Error("a snapshot delete without delete_empty_resource_definition deleted the definition")
	}
}

// TestSnapshotDeleteOnSomeNodesIsRefused: blockstor deletes a snapshot
// everywhere, so a delete limited to some nodes is refused instead of
// widened.
func TestSnapshotDeleteOnSomeNodesIsRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTruncateFixture(t, st, "pvc-nodes", true)

	base, stop := startServerWithStore(t, st)
	defer stop()

	err := newClient(t, base).Resources.DeleteSnapshot(t.Context(), "pvc-nodes", "snap-1",
		&lapi.SnapshotDeleteOpts{Nodes: []string{"w1"}})
	if err == nil {
		t.Fatal("a snapshot delete on w1 only was accepted")
	}

	if _, getErr := st.Snapshots().Get(t.Context(), "pvc-nodes", "snap-1"); getErr != nil {
		t.Errorf("the refused delete removed the snapshot: %v", getErr)
	}
}

// TestRDTruncateMarksADefinitionOnlyItsCloneKeeps: CDI deletes the source of
// a clone while the clone runs, so linstor-csi's truncate with
// delete_empty_resource_definition finds the clone's internal snapshot on the
// source. LINSTOR has no snapshot definition there and deletes the source;
// here the snapshot must outlive the clone, so the source stays, marked for
// the definition controller to delete once the snapshot is gone. Nothing
// else asks again. A snapshot of the user keeps the definition unmarked, as
// in LINSTOR, which deletes it when linstor-csi deletes that snapshot.
func TestRDTruncateMarksADefinitionOnlyItsCloneKeeps(t *testing.T) {
	t.Parallel()

	for _, cloneOwned := range []bool{true, false} {
		st := store.NewInMemory()
		seedTruncateFixture(t, st, "pvc-src", true)

		if cloneOwned {
			if err := st.Snapshots().PatchSnapshotProps(t.Context(), "pvc-src", "snap-1", func(props map[string]string) error {
				props[store.CloneSnapshotOwnerProp] = "pvc-clone"

				return nil
			}); err != nil {
				t.Fatalf("mark the clone's snapshot: %v", err)
			}
		}

		base, stop := startServerWithStore(t, st)

		err := newClient(t, base).ResourceDefinitions.Truncate(t.Context(), "pvc-src",
			&lapi.ResourceDefinitionTruncateOpts{DeleteEmptyResourceDefinition: true})
		if err != nil {
			t.Fatalf("clone=%v: Truncate: %v", cloneOwned, err)
		}

		rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-src")
		if err != nil {
			t.Fatalf("clone=%v: the definition its snapshot keeps is gone: %v", cloneOwned, err)
		}

		if got := rd.Props[store.DeleteWhenEmptyProp] != ""; got != cloneOwned {
			t.Errorf("clone=%v: marked delete-when-empty = %v, want %v", cloneOwned, got, cloneOwned)
		}

		if slices.Contains(rd.Flags, rdFlagDelete) {
			t.Errorf("clone=%v: a definition its snapshot keeps is left marked DELETE: %v", cloneOwned, rd.Flags)
		}

		stop()
	}
}
