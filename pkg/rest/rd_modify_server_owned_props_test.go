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
		{name: "delete", body: map[string]any{"delete_props": []string{store.RestoreAdoptedProp}}},
		{name: "namespace", body: map[string]any{"delete_namespaces": []string{"Blockstor"}}},
		{name: "volumes", body: map[string]any{"override_props": map[string]string{store.RestoreVolumesProp: "0:1"}}},
		{name: "set-rollback-mark", body: map[string]any{
			"override_props": map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
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
