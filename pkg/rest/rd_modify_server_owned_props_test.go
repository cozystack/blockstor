// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A modify that deleted the Blockstor namespace mid-restore took the adoption
// mark with it, and the creator's rollback then reaped a definition a retry
// had already answered for. The props blockstor writes on a definition are
// refused on modify the way the clone door refuses them.
func TestRDModifyRefusesEditsOfServerOwnedProps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "override", body: map[string]any{"override_props": map[string]string{store.RestoreFromSnapshotProp: "other:snap"}}},
		{name: "delete-unfinished", body: map[string]any{"delete_props": []string{store.RestoreFromSnapshotProp}}},
		{name: "set-adoption-mark", body: map[string]any{
			"override_props": map[string]string{store.RestoreAdoptedProp: "another-claim"},
		}},
		{name: "namespace", body: map[string]any{"delete_namespaces": []string{"Blockstor"}}},
		{name: "volumes", body: map[string]any{"override_props": map[string]string{store.RestoreVolumesProp: "0:1"}}},
		{name: "set-rollback-mark", body: map[string]any{
			"override_props": map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
		}},
		// The time is what a retry takes over a live mark on: set by hand, it
		// hands a running rollback's definition to the next retry.
		{name: "set-rollback-since", body: map[string]any{
			"override_props": map[string]string{store.RollbackSinceProp: "2000-01-01T00:00:00Z"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			props := map[string]string{
				store.RestoreFromSnapshotProp: "src:snap",
				store.RestoreAdoptedProp:      "2026-10-07T00:00:00Z",
				"Aux/keep":                    "yes",
			}

			if err := st.ResourceDefinitions().Create(ctx,
				&apiv1.ResourceDefinition{Name: "rd-owned", Props: props}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPut(t, base+"/v1/resource-definitions/rd-owned", raw)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("modify touching a server-owned prop = %d, want 400", resp.StatusCode)
			}

			rd, err := st.ResourceDefinitions().Get(ctx, "rd-owned")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			for key, want := range props {
				if rd.Props[key] != want {
					t.Errorf("refused modify changed %s: %q, want %q", key, rd.Props[key], want)
				}
			}
		})
	}
}

// An adoption mark that no request answers for any more is the operator's to
// delete, by name or as an empty override: where the restore can never finish,
// every later rollback yields on it and nothing else takes it off.
func TestRDModifyClearsTheAdoptionMark(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "delete", body: map[string]any{"delete_props": []string{store.RestoreAdoptedProp}}},
		{name: "empty-override", body: map[string]any{"override_props": map[string]string{store.RestoreAdoptedProp: ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-adopted", Props: map[string]string{
				store.RestoreFromSnapshotProp: "src:snap",
				store.RestoreAdoptedProp:      "a-claims-token",
			}}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPut(t, base+"/v1/resource-definitions/rd-adopted", raw)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("modify clearing the adoption mark = %d, want 200", resp.StatusCode)
			}

			rd, err := st.ResourceDefinitions().Get(ctx, "rd-adopted")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if mark, ok := rd.Props[store.RestoreAdoptedProp]; ok {
				t.Errorf("the adoption mark survived its delete: %q", mark)
			}

			if rd.Props[store.RestoreFromSnapshotProp] != "src:snap" {
				t.Errorf("clearing the adoption mark changed the marker: %v", rd.Props)
			}
		})
	}
}

// A create carrying a server-owned prop made a definition born restoring from
// another definition's snapshot, or refused by every resume over a rollback
// that never ran. It is refused the way the modify is.
func TestRDCreateRefusesServerOwnedProps(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		store.RestoreFromSnapshotProp, store.RollbackAbandonedProp,
		store.RestoreAdoptedProp, store.RestoreVolumesProp,
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			raw, err := json.Marshal(map[string]any{"resource_definition": map[string]any{
				"name": "rd-born", "props": map[string]string{key: "x", "Aux/keep": "yes"},
			}})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/resource-definitions", raw)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("create carrying %s = %d, want 400", key, resp.StatusCode)
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), "rd-born"); err == nil {
				t.Errorf("the refused create stored the definition")
			}
		})
	}
}

// The record of what the snapshot held is written by every clone, so the
// judgement that falls back on it has something to read once the snapshot
// is gone.
func TestRDCloneRecordsTheVolumesItsSnapshotHeld(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-rec")

	if err := st.VolumeDefinitions().Create(ctx, "src-rec",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed the second volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-rec", "dst-rec", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "dst-rec")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	vols, ok := store.RecordedRestoreVolumes(rd.Props)
	if !ok || len(vols) != 2 {
		t.Errorf("clone recorded %q, want both volumes of its snapshot", rd.Props[store.RestoreVolumesProp])
	}
}

// Once a clone holds every volume and a diskful replica its satellite reports
// restored, the restore marker drives nothing, and deleting it is the only way
// to lift the placement pin it keeps to the snapshot's backend and nodes. The same delete stays refused on
// a definition that is not finished (TestRDModifyRefusesEditsOfServerOwnedProps).
func TestRDModifyDeletesTheRestoreMarkerOfAFinishedClone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "delete", body: map[string]any{"delete_props": []string{store.RestoreFromSnapshotProp}}},
		{name: "empty-override", body: map[string]any{"override_props": map[string]string{store.RestoreFromSnapshotProp: ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDeployedCloneSource(t, st, "src-unpin")

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, "src-unpin", "dst-unpin", nil); code != http.StatusCreated {
				t.Fatalf("clone = %d, want 201", code)
			}

			reportVolumesUpToDate(t, st, "dst-unpin")

			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPut(t, base+"/v1/resource-definitions/dst-unpin", raw)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("deleting the marker of a finished clone = %d, want 200", resp.StatusCode)
			}

			rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-unpin")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if mark, ok := rd.Props[store.RestoreFromSnapshotProp]; ok {
				t.Errorf("the marker survived its delete: %q", mark)
			}
		})
	}
}

// A diskful replica in the spec is one assigned, not one restored: until a
// satellite reports the volumes UpToDate the marker is still what makes it
// bring them up from the snapshot, so its delete is refused.
func TestRDModifyKeepsTheRestoreMarkerUntilASatelliteReportsTheRestore(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-early")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-early", "dst-early", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	raw, err := json.Marshal(map[string]any{"delete_props": []string{store.RestoreFromSnapshotProp}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/dst-early", raw)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("deleting the marker before any satellite reported = %d, want 400", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-early")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if _, ok := rd.Props[store.RestoreFromSnapshotProp]; !ok {
		t.Error("the refused delete took the marker off")
	}
}

// reportVolumesUpToDate stands in for the satellites of rdName: every replica
// with a disk reports every volume UpToDate.
func reportVolumesUpToDate(t *testing.T, st store.Store, rdName string) {
	t.Helper()

	ctx := t.Context()

	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("list the volumes of %q: %v", rdName, err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list the replicas of %q: %v", rdName, err)
	}

	for i := range replicas {
		if !store.HoldsData(&replicas[i]) {
			continue
		}

		replicas[i].Volumes = nil
		for _, vd := range vds {
			replicas[i].Volumes = append(replicas[i].Volumes, apiv1.Volume{
				VolumeNumber: vd.VolumeNumber,
				State:        apiv1.VolumeState{DiskState: "UpToDate"},
			})
		}

		if err := st.Resources().Update(ctx, &replicas[i]); err != nil {
			t.Fatalf("report the volumes of %q on %q: %v", rdName, replicas[i].NodeName, err)
		}
	}
}
