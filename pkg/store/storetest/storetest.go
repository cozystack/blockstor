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

// Package storetest provides a shared test suite that any pkg/store.Store
// implementation must pass. It is consumed by both pkg/store (the in-memory
// implementation) and pkg/store/k8s (the CRD-backed one) so the two stay
// behaviourally identical.
package storetest

import (
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Factory builds a fresh, empty Store. Each subtest gets a new one so they
// don't share state.
type Factory func(t *testing.T) store.Store

// RunNodeStore exercises every branch of store.NodeStore.
func RunNodeStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) { testNodeListEmpty(t, newStore) })
	t.Run("CreateThenGet", func(t *testing.T) { testNodeCreateThenGet(t, newStore) })
	t.Run("CreateDuplicate", func(t *testing.T) { testNodeCreateDuplicate(t, newStore) })
	t.Run("GetMissing", func(t *testing.T) { testNodeGetMissing(t, newStore) })
	t.Run("UpdateMissing", func(t *testing.T) { testNodeUpdateMissing(t, newStore) })
	t.Run("UpdateChangesProps", func(t *testing.T) { testNodeUpdateChangesProps(t, newStore) })
	t.Run("DeleteMissing", func(t *testing.T) { testNodeDeleteMissing(t, newStore) })
	t.Run("DeleteRemoves", func(t *testing.T) { testNodeDeleteRemoves(t, newStore) })
	t.Run("ListSorted", func(t *testing.T) { testNodeListSorted(t, newStore) })
	// SetConnectionStatus is the path the Hello RPC drives — the field
	// linstor-csi's wait-node-online initContainer polls for. Pinned
	// here so InMemory and CRD-backed stores stay identical.
	t.Run("SetConnectionStatus", func(t *testing.T) { testNodeSetConnectionStatus(t, newStore) })
	t.Run("SetConnectionStatusMissing", func(t *testing.T) { testNodeSetConnectionStatusMissing(t, newStore) })
}

func testNodeSetConnectionStatus(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.SetConnectionStatus(ctx, "n1", "ONLINE"); err != nil {
		t.Fatalf("SetConnectionStatus: %v", err)
	}

	got, err := s.Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ConnectionStatus != "ONLINE" {
		t.Errorf("ConnectionStatus: got %q, want ONLINE", got.ConnectionStatus)
	}
}

func testNodeSetConnectionStatusMissing(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	err := s.SetConnectionStatus(t.Context(), "missing", "ONLINE")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("SetConnectionStatus missing: got %v, want ErrNotFound", err)
	}
}

// RunVolumeDefinitionStore exercises every branch of
// store.VolumeDefinitionStore.
func RunVolumeDefinitionStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) {
		s := newStore(t)

		// k8s impl needs a parent RD for Get/List to find anything.
		seedRD(t, s, "pvc-1")

		got, err := s.VolumeDefinitions().List(t.Context(), "pvc-1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("CreateThenGet", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		ctx := t.Context()
		vd := apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}
		if err := s.VolumeDefinitions().Create(ctx, "pvc-1", &vd); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := s.VolumeDefinitions().Get(ctx, "pvc-1", 0)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.VolumeNumber != 0 || got.SizeKib != 1024*1024 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("CreateDuplicate", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		ctx := t.Context()
		vd := apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}
		if err := s.VolumeDefinitions().Create(ctx, "pvc-1", &vd); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := s.VolumeDefinitions().Create(ctx, "pvc-1", &vd)
		if !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("dup: got %v, want ErrAlreadyExists", err)
		}
	})
	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		_, err := s.VolumeDefinitions().Get(t.Context(), "pvc-1", 99)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("MissingRD", func(t *testing.T) {
		s := newStore(t)
		_, err := s.VolumeDefinitions().Get(t.Context(), "ghost-rd", 0)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("DeleteRemoves", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		ctx := t.Context()
		if err := s.VolumeDefinitions().Create(ctx, "pvc-1", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := s.VolumeDefinitions().Delete(ctx, "pvc-1", 0); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := s.VolumeDefinitions().Get(ctx, "pvc-1", 0)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("post-delete: got %v, want ErrNotFound", err)
		}
	})
	// Update is the resize hot-path: CSI ControllerExpandVolume →
	// REST PUT /v1/resource-definitions/{rd}/volume-definitions/{vol}
	// → VolumeDefinitions.Update. Round-trip must preserve the new
	// SizeKib so the satellite's reconciler picks up the grow on its
	// next Apply pass.
	t.Run("UpdateMissing", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		err := s.VolumeDefinitions().Update(t.Context(), "pvc-1",
			&apiv1.VolumeDefinition{VolumeNumber: 99, SizeKib: 2048})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Update missing: got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateGrowsSize", func(t *testing.T) {
		s := newStore(t)
		seedRD(t, s, "pvc-1")

		ctx := t.Context()
		if err := s.VolumeDefinitions().Create(ctx, "pvc-1",
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		err := s.VolumeDefinitions().Update(ctx, "pvc-1",
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 2 * 1024 * 1024})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := s.VolumeDefinitions().Get(ctx, "pvc-1", 0)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.SizeKib != 2*1024*1024 {
			t.Errorf("SizeKib after grow: got %d, want %d", got.SizeKib, 2*1024*1024)
		}
	})
}

// RunKeyValueStore exercises every branch of store.KeyValueStore.
func RunKeyValueStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) {
		got, err := newStore(t).KeyValueStore().ListInstances(t.Context())
		if err != nil {
			t.Fatalf("ListInstances: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("SetThenGet", func(t *testing.T) {
		s := newStore(t).KeyValueStore()
		ctx := t.Context()
		err := s.SetKeys(ctx, "csi-volumes", apiv1.GenericPropsModify{
			OverrideProps: map[string]string{"foo": "bar", "baz": "qux"},
		})
		if err != nil {
			t.Fatalf("SetKeys: %v", err)
		}
		got, err := s.GetInstance(ctx, "csi-volumes")
		if err != nil {
			t.Fatalf("GetInstance: %v", err)
		}
		if got["foo"] != "bar" || got["baz"] != "qux" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("DeleteKeys", func(t *testing.T) {
		s := newStore(t).KeyValueStore()
		ctx := t.Context()
		if err := s.SetKeys(ctx, "x", apiv1.GenericPropsModify{
			OverrideProps: map[string]string{"a": "1", "b": "2"},
		}); err != nil {
			t.Fatalf("SetKeys: %v", err)
		}
		if err := s.SetKeys(ctx, "x", apiv1.GenericPropsModify{
			DeleteProps: []string{"a"},
		}); err != nil {
			t.Fatalf("SetKeys delete: %v", err)
		}
		got, _ := s.GetInstance(ctx, "x")
		if _, ok := got["a"]; ok {
			t.Errorf("a should be deleted: %v", got)
		}
		if got["b"] != "2" {
			t.Errorf("b should remain: %v", got)
		}
	})
	t.Run("DeleteNamespace", func(t *testing.T) {
		s := newStore(t).KeyValueStore()
		ctx := t.Context()
		if err := s.SetKeys(ctx, "x", apiv1.GenericPropsModify{
			OverrideProps: map[string]string{"ns/k1": "v1", "ns/k2": "v2", "other/k": "v"},
		}); err != nil {
			t.Fatalf("SetKeys: %v", err)
		}
		if err := s.SetKeys(ctx, "x", apiv1.GenericPropsModify{
			DeleteNamespace: []string{"ns"},
		}); err != nil {
			t.Fatalf("SetKeys delete-ns: %v", err)
		}
		got, _ := s.GetInstance(ctx, "x")
		if _, ok := got["ns/k1"]; ok {
			t.Errorf("ns/k1 should be deleted: %v", got)
		}
		if got["other/k"] != "v" {
			t.Errorf("other/k should remain: %v", got)
		}
	})
	t.Run("GetMissing", func(t *testing.T) {
		_, err := newStore(t).KeyValueStore().GetInstance(t.Context(), "ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("DeleteMissing", func(t *testing.T) {
		err := newStore(t).KeyValueStore().DeleteInstance(t.Context(), "ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
}

// seedRD inserts a minimal valid ResourceDefinition the VolumeDefinition
// suite can hang volumes off of.
func seedRD(t *testing.T, s store.Store, name string) {
	t.Helper()

	err := s.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: name})
	if err != nil {
		t.Fatalf("seed RD %q: %v", name, err)
	}
}

// RunSnapshotStore exercises every branch of store.SnapshotStore.
func RunSnapshotStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) {
		got, err := newStore(t).Snapshots().List(t.Context())
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("CreateThenGet", func(t *testing.T) {
		s := newStore(t).Snapshots()
		ctx := t.Context()
		snap := apiv1.Snapshot{
			Name:         "snap-1",
			ResourceName: "pvc-1",
			Nodes:        []string{"n1", "n2"},
		}
		if err := s.Create(ctx, &snap); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1", "snap-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "snap-1" || got.ResourceName != "pvc-1" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("CreateDuplicate", func(t *testing.T) {
		s := newStore(t).Snapshots()
		ctx := t.Context()
		snap := apiv1.Snapshot{Name: "snap-1", ResourceName: "pvc-1"}
		if err := s.Create(ctx, &snap); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := s.Create(ctx, &snap)
		if !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("dup: got %v, want ErrAlreadyExists", err)
		}
	})
	t.Run("GetMissing", func(t *testing.T) {
		_, err := newStore(t).Snapshots().Get(t.Context(), "pvc-1", "ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("ListByDefinition", func(t *testing.T) {
		s := newStore(t).Snapshots()
		ctx := t.Context()
		for _, snap := range []apiv1.Snapshot{
			{Name: "s1", ResourceName: "pvc-1"},
			{Name: "s2", ResourceName: "pvc-1"},
			{Name: "s1", ResourceName: "pvc-2"},
		} {
			if err := s.Create(ctx, &snap); err != nil {
				t.Fatalf("Create %+v: %v", snap, err)
			}
		}
		got, err := s.ListByDefinition(ctx, "pvc-1")
		if err != nil {
			t.Fatalf("ListByDefinition: %v", err)
		}
		if len(got) != 2 {
			t.Errorf("len: got %d, want 2", len(got))
		}
	})
	t.Run("DeleteRemoves", func(t *testing.T) {
		s := newStore(t).Snapshots()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := s.Delete(ctx, "pvc-1", "s1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := s.Get(ctx, "pvc-1", "s1")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateMissing", func(t *testing.T) {
		err := newStore(t).Snapshots().Update(t.Context(),
			&apiv1.Snapshot{Name: "ghost", ResourceName: "pvc-1"})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Update missing: got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateChangesProps", func(t *testing.T) {
		s := newStore(t).Snapshots()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := s.Update(ctx, &apiv1.Snapshot{
			Name:         "s1",
			ResourceName: "pvc-1",
			Nodes:        []string{"n1", "n2"},
		})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1", "s1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Nodes) != 2 {
			t.Errorf("Nodes: got %v, want [n1 n2]", got.Nodes)
		}
	})
}

// RunResourceStore exercises every branch of store.ResourceStore.
func RunResourceStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) {
		got, err := newStore(t).Resources().List(t.Context())
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("CreateThenGet", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		r := apiv1.Resource{Name: "pvc-1", NodeName: "n1", Flags: []string{"DRBD_DISKLESS"}}
		if err := s.Create(ctx, &r); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1", "n1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "pvc-1" || got.NodeName != "n1" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("CreateDuplicate", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		r := apiv1.Resource{Name: "pvc-1", NodeName: "n1"}
		if err := s.Create(ctx, &r); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := s.Create(ctx, &r)
		if !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("dup: got %v, want ErrAlreadyExists", err)
		}
	})
	t.Run("ListByDefinition", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		for _, r := range []apiv1.Resource{
			{Name: "pvc-1", NodeName: "n1"},
			{Name: "pvc-1", NodeName: "n2"},
			{Name: "pvc-2", NodeName: "n1"},
		} {
			if err := s.Create(ctx, &r); err != nil {
				t.Fatalf("Create %+v: %v", r, err)
			}
		}
		got, err := s.ListByDefinition(ctx, "pvc-1")
		if err != nil {
			t.Fatalf("ListByDefinition: %v", err)
		}
		if len(got) != 2 {
			t.Errorf("len: got %d, want 2", len(got))
		}
	})
	t.Run("DeleteRemoves", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := s.Delete(ctx, "pvc-1", "n1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := s.Get(ctx, "pvc-1", "n1")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	// SetState pins the path the satellite's events2 observer drives:
	// runtime State (InUse) + DRBD-state props land on the existing
	// Resource without disturbing Spec. Tested across both InMemory
	// and CRD-backed stores so they stay behaviourally identical.
	t.Run("SetState", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := s.SetState(ctx, "pvc-1", "n1",
			apiv1.ResourceState{InUse: true},
			map[string]string{"DrbdState": "UpToDate"})
		if err != nil {
			t.Fatalf("SetState: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1", "n1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !got.State.InUse {
			t.Errorf("State.InUse: got false, want true")
		}
		if got.Props["DrbdState"] != "UpToDate" {
			t.Errorf("Props[DrbdState]: got %q, want UpToDate", got.Props["DrbdState"])
		}
	})
	t.Run("SetStateMissing", func(t *testing.T) {
		s := newStore(t).Resources()
		err := s.SetState(t.Context(), "pvc-missing", "n1",
			apiv1.ResourceState{InUse: true}, nil)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("SetState on missing: got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateMissing", func(t *testing.T) {
		err := newStore(t).Resources().Update(t.Context(),
			&apiv1.Resource{Name: "ghost", NodeName: "n1"})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Update missing: got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateChangesProps", func(t *testing.T) {
		s := newStore(t).Resources()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := s.Update(ctx, &apiv1.Resource{
			Name:     "pvc-1",
			NodeName: "n1",
			Props:    map[string]string{"StorPoolName": "thin"},
		})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1", "n1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Props["StorPoolName"] != "thin" {
			t.Errorf("Props: got %v, want StorPoolName=thin", got.Props)
		}
	})
}

// RunResourceDefinitionStore exercises every branch of
// store.ResourceDefinitionStore.
func RunResourceDefinitionStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) {
		got, err := newStore(t).ResourceDefinitions().List(t.Context())
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("List: got %v, want empty non-nil", got)
		}
	})
	t.Run("CreateThenGet", func(t *testing.T) {
		s := newStore(t).ResourceDefinitions()
		ctx := t.Context()
		rd := apiv1.ResourceDefinition{
			Name:              "pvc-1",
			ExternalName:      "pvc-1",
			ResourceGroupName: "rg-1",
			Props:             map[string]string{"foo": "bar"},
		}
		if err := s.Create(ctx, &rd); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "pvc-1" || got.ResourceGroupName != "rg-1" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("CreateDuplicate", func(t *testing.T) {
		s := newStore(t).ResourceDefinitions()
		ctx := t.Context()
		rd := apiv1.ResourceDefinition{Name: "pvc-1"}
		if err := s.Create(ctx, &rd); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := s.Create(ctx, &rd)
		if !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("dup: got %v, want ErrAlreadyExists", err)
		}
	})
	t.Run("GetMissing", func(t *testing.T) {
		_, err := newStore(t).ResourceDefinitions().Get(t.Context(), "ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("DeleteMissing", func(t *testing.T) {
		err := newStore(t).ResourceDefinitions().Delete(t.Context(), "ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("DeleteRemoves", func(t *testing.T) {
		s := newStore(t).ResourceDefinitions()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := s.Delete(ctx, "pvc-1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := s.Get(ctx, "pvc-1")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("post-delete Get: got %v, want ErrNotFound", err)
		}
	})
	// Update tests pin the props/layer-stack mutation path that the
	// REST PUT handler and the CSI layer_list pass-through depend on.
	// Both store implementations must round-trip the new spec verbatim.
	t.Run("UpdateMissing", func(t *testing.T) {
		err := newStore(t).ResourceDefinitions().
			Update(t.Context(), &apiv1.ResourceDefinition{Name: "ghost"})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Update missing: got %v, want ErrNotFound", err)
		}
	})
	t.Run("UpdateChangesProps", func(t *testing.T) {
		s := newStore(t).ResourceDefinitions()
		ctx := t.Context()
		if err := s.Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := s.Update(ctx, &apiv1.ResourceDefinition{
			Name:       "pvc-1",
			Props:      map[string]string{"k": "v"},
			LayerStack: []string{"DRBD", "STORAGE"},
		})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		got, err := s.Get(ctx, "pvc-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Props["k"] != "v" {
			t.Errorf("Props: got %v, want k=v", got.Props)
		}
		if len(got.LayerStack) != 2 || got.LayerStack[0] != "DRBD" {
			t.Errorf("LayerStack: got %v, want [DRBD STORAGE]", got.LayerStack)
		}
	})
}

// RunResourceGroupStore exercises every branch of store.ResourceGroupStore.
func RunResourceGroupStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) { testRGListEmpty(t, newStore) })
	t.Run("CreateThenGet", func(t *testing.T) { testRGCreateThenGet(t, newStore) })
	t.Run("CreateDuplicate", func(t *testing.T) { testRGCreateDuplicate(t, newStore) })
	t.Run("GetMissing", func(t *testing.T) { testRGGetMissing(t, newStore) })
	t.Run("UpdateMissing", func(t *testing.T) { testRGUpdateMissing(t, newStore) })
	t.Run("UpdateChangesProps", func(t *testing.T) { testRGUpdateChangesProps(t, newStore) })
	t.Run("DeleteMissing", func(t *testing.T) { testRGDeleteMissing(t, newStore) })
	t.Run("DeleteRemoves", func(t *testing.T) { testRGDeleteRemoves(t, newStore) })
}

func testRGListEmpty(t *testing.T, newStore Factory) {
	got, err := newStore(t).ResourceGroups().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("List: got %v, want empty non-nil", got)
	}
}

func testRGCreateThenGet(t *testing.T, newStore Factory) {
	s := newStore(t).ResourceGroups()
	ctx := t.Context()

	rg := apiv1.ResourceGroup{
		Name:        "rg-1",
		Description: "test",
		Props:       map[string]string{"DrbdOptions/auto-quorum": "io-error"},
		PeerSlots:   7,
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}
	if err := s.Create(ctx, &rg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "rg-1" || got.Description != "test" || got.PeerSlots != 7 {
		t.Errorf("Get: got %+v", got)
	}

	if got.SelectFilter.PlaceCount != 3 || got.SelectFilter.StoragePool != "pool" {
		t.Errorf("SelectFilter: got %+v", got.SelectFilter)
	}
}

func testRGCreateDuplicate(t *testing.T, newStore Factory) {
	s := newStore(t).ResourceGroups()
	ctx := t.Context()

	rg := apiv1.ResourceGroup{Name: "rg-1"}
	if err := s.Create(ctx, &rg); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &rg)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

func testRGGetMissing(t *testing.T, newStore Factory) {
	_, err := newStore(t).ResourceGroups().Get(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func testRGUpdateMissing(t *testing.T, newStore Factory) {
	err := newStore(t).ResourceGroups().Update(t.Context(), &apiv1.ResourceGroup{Name: "ghost"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Update missing: got %v, want ErrNotFound", err)
	}
}

func testRGUpdateChangesProps(t *testing.T, newStore Factory) {
	s := newStore(t).ResourceGroups()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Update(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["foo"] != "bar" {
		t.Errorf("Props[foo]: got %q, want bar", got.Props["foo"])
	}
}

func testRGDeleteMissing(t *testing.T, newStore Factory) {
	err := newStore(t).ResourceGroups().Delete(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func testRGDeleteRemoves(t *testing.T, newStore Factory) {
	s := newStore(t).ResourceGroups()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "rg-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "rg-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}
}

// RunStoragePoolStore exercises every branch of store.StoragePoolStore.
func RunStoragePoolStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("ListEmpty", func(t *testing.T) { testSPListEmpty(t, newStore) })
	t.Run("CreateRoundTrip", func(t *testing.T) { testSPCreateRoundTrip(t, newStore) })
	t.Run("CreateDuplicate", func(t *testing.T) { testSPCreateDuplicate(t, newStore) })
	t.Run("CreateSameNameDifferentNode", func(t *testing.T) { testSPCreateSameNameDifferentNode(t, newStore) })
	t.Run("GetMissing", func(t *testing.T) { testSPGetMissing(t, newStore) })
	t.Run("ListByNode", func(t *testing.T) { testSPListByNode(t, newStore) })
	t.Run("DeleteMissing", func(t *testing.T) { testSPDeleteMissing(t, newStore) })
	t.Run("DeleteRemoves", func(t *testing.T) { testSPDeleteRemoves(t, newStore) })
	t.Run("ListSorted", func(t *testing.T) { testSPListSorted(t, newStore) })
	// SetCapacity is the satellite's hot-path: ReportPoolCapacity gRPC
	// call lands here every reporting interval. Pinned across
	// implementations so InMemory and CRD stay behaviourally identical
	// when the autoplacer reads free/total figures.
	t.Run("SetCapacity", func(t *testing.T) { testSPSetCapacity(t, newStore) })
	t.Run("SetCapacityMissing", func(t *testing.T) { testSPSetCapacityMissing(t, newStore) })
	// Update is the path Hello's upsertPool uses on a re-Hello to
	// refresh provider_kind / props without losing capacity fields.
	t.Run("UpdateMissing", func(t *testing.T) { testSPUpdateMissing(t, newStore) })
	t.Run("UpdateChangesProps", func(t *testing.T) { testSPUpdateChangesProps(t, newStore) })
}

func testSPUpdateMissing(t *testing.T, newStore Factory) {
	err := newStore(t).StoragePools().Update(t.Context(), &apiv1.StoragePool{
		StoragePoolName: "ghost",
		NodeName:        "n1",
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Update missing: got %v, want ErrNotFound", err)
	}
}

func testSPUpdateChangesProps(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "thin",
		NodeName:        "n1",
		ProviderKind:    "LVM_THIN",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	err := s.Update(ctx, &apiv1.StoragePool{
		StoragePoolName: "thin",
		NodeName:        "n1",
		ProviderKind:    "LVM_THIN",
		Props:           map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(ctx, "n1", "thin")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["k"] != "v" {
		t.Errorf("Props: got %v, want k=v", got.Props)
	}
}

func testSPSetCapacity(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	pool := apiv1.StoragePool{
		StoragePoolName: "thin",
		NodeName:        "n1",
		ProviderKind:    "LVM_THIN",
	}
	if err := s.Create(ctx, &pool); err != nil {
		t.Fatalf("Create: %v", err)
	}

	err := s.SetCapacity(ctx, "n1", "thin", 500_000, 1_000_000, true)
	if err != nil {
		t.Fatalf("SetCapacity: %v", err)
	}

	got, err := s.Get(ctx, "n1", "thin")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.FreeCapacity != 500_000 || got.TotalCapacity != 1_000_000 {
		t.Errorf("capacity: got free=%d total=%d, want 500000/1000000",
			got.FreeCapacity, got.TotalCapacity)
	}

	if !got.SupportsSnapshot {
		t.Errorf("SupportsSnapshot: got false, want true")
	}
}

func testSPSetCapacityMissing(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	err := s.SetCapacity(t.Context(), "n1", "missing", 0, 0, false)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("SetCapacity on missing pool: got %v, want ErrNotFound", err)
	}
}

// --- NodeStore branches ---

func testNodeListEmpty(t *testing.T, newStore Factory) {
	got, err := newStore(t).Nodes().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil {
		t.Errorf("List returned nil, want empty slice")
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

func testNodeCreateThenGet(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(t.Context(), &n); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(t.Context(), "alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "alpha" || got.Type != apiv1.NodeTypeSatellite {
		t.Errorf("Get: got name=%q type=%q, want alpha/SATELLITE", got.Name, got.Type)
	}
}

func testNodeCreateDuplicate(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(ctx, &n); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &n)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

func testNodeGetMissing(t *testing.T, newStore Factory) {
	_, err := newStore(t).Nodes().Get(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func testNodeUpdateMissing(t *testing.T, newStore Factory) {
	err := newStore(t).Nodes().Update(t.Context(), &apiv1.Node{Name: "ghost"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Update missing: got %v, want ErrNotFound", err)
	}
}

func testNodeUpdateChangesProps(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Props: map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["foo"] != "bar" {
		t.Errorf("Props[foo]: got %q, want %q", got.Props["foo"], "bar")
	}
}

func testNodeDeleteMissing(t *testing.T, newStore Factory) {
	err := newStore(t).Nodes().Delete(t.Context(), "ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func testNodeDeleteRemoves(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(list) != 0 {
		t.Errorf("List after Delete: got %d, want 0", len(list))
	}
}

func testNodeListSorted(t *testing.T, newStore Factory) {
	s := newStore(t).Nodes()
	ctx := t.Context()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := s.Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("Create %q: %v", name, err)
		}
	}

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d, want %d", len(got), len(want))
	}

	for i, w := range want {
		if got[i].Name != w {
			t.Errorf("[%d]: got %q, want %q", i, got[i].Name, w)
		}
	}
}

// --- StoragePoolStore branches ---

func testSPListEmpty(t *testing.T, newStore Factory) {
	got, err := newStore(t).StoragePools().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("List: got %v, want empty non-nil", got)
	}
}

func testSPCreateRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
	}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "n1", "pool")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.StoragePoolName != "pool" || got.NodeName != "n1" {
		t.Errorf("Get: got %s/%s, want n1/pool", got.NodeName, got.StoragePoolName)
	}

	if got.ProviderKind != apiv1.StoragePoolKindFileThin {
		t.Errorf("ProviderKind: got %q", got.ProviderKind)
	}
}

func testSPCreateDuplicate(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	sp := apiv1.StoragePool{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile}
	if err := s.Create(ctx, &sp); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &sp)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

func testSPCreateSameNameDifferentNode(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Fatalf("Create n1: %v", err)
	}

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Errorf("Create n2: got %v, want nil", err)
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(all) != 2 {
		t.Errorf("List len: got %d, want 2", len(all))
	}
}

func testSPGetMissing(t *testing.T, newStore Factory) {
	_, err := newStore(t).StoragePools().Get(t.Context(), "ghost", "pool")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func testSPListByNode(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p3", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
	} {
		if err := s.Create(ctx, &sp); err != nil {
			t.Fatalf("Create %s/%s: %v", sp.NodeName, sp.StoragePoolName, err)
		}
	}

	got, err := s.ListByNode(ctx, "n1")
	if err != nil {
		t.Fatalf("ListByNode: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("ListByNode n1 len: got %d, want 2", len(got))
	}

	for _, sp := range got {
		if sp.NodeName != "n1" {
			t.Errorf("returned pool from %q (want n1)", sp.NodeName)
		}
	}

	got, err = s.ListByNode(ctx, "ghost")
	if err != nil {
		t.Fatalf("ListByNode ghost: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("ListByNode ghost: got %v, want empty", got)
	}
}

func testSPDeleteMissing(t *testing.T, newStore Factory) {
	err := newStore(t).StoragePools().Delete(t.Context(), "ghost", "pool")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func testSPDeleteRemoves(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1", "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1", "p1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("List after Delete: got %d, want 0", len(all))
	}
}

func testSPListSorted(t *testing.T, newStore Factory) {
	s := newStore(t).StoragePools()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p2", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFile},
		{StoragePoolName: "p1", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindFile},
	} {
		if err := s.Create(ctx, &sp); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	wantOrder := [][2]string{
		{"n1", "p1"},
		{"n1", "p2"},
		{"n2", "p1"},
		{"n2", "p2"},
	}

	if len(got) != len(wantOrder) {
		t.Fatalf("len: got %d, want %d", len(got), len(wantOrder))
	}

	for i, want := range wantOrder {
		if got[i].NodeName != want[0] || got[i].StoragePoolName != want[1] {
			t.Errorf("[%d]: got %s/%s, want %s/%s",
				i, got[i].NodeName, got[i].StoragePoolName, want[0], want[1])
		}
	}
}
