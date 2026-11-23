// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"errors"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// The prop edits land on the target after the clone stamped its marker, so an
// edit that removes or rewrites it would leave the satellite to bring the
// volumes up blank. Every edit shape that reaches the marker is refused before
// anything is written.
func TestRDCloneRefusesPropEditsThatReachItsMarker(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "override", body: map[string]any{
			"override_props": map[string]string{store.RestoreFromSnapshotProp: "other:snap"},
		}},
		{name: "delete", body: map[string]any{
			"delete_props": []string{store.RestoreFromSnapshotProp},
		}},
		{name: "namespace", body: map[string]any{
			"delete_namespaces": []string{"Blockstor"},
		}},
		{name: "volumes-record", body: map[string]any{
			"override_props": map[string]string{store.RestoreVolumesProp: "0:1"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDeployedCloneSource(t, st, "src-owned")

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, "src-owned", "dst-owned", tc.body); code != http.StatusBadRequest {
				t.Errorf("clone with an edit reaching a server-owned prop = %d, want 400", code)
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-owned"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the refused clone wrote its target: %v", err)
			}

			if snaps, _ := st.Snapshots().ListByDefinition(t.Context(), "src-owned"); len(snaps) > 0 {
				t.Errorf("the refused clone took %d snapshot(s) on the source", len(snaps))
			}
		})
	}
}

// linstor-csi's own edit on every clone stays accepted.
func TestRDCloneAcceptsLinstorCSIsOwnPropEdit(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-csiedit")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code := cloneOnce(t, base, "src-csiedit", "dst-csiedit", map[string]any{
		"delete_props": []string{"Aux/csi-provisioning-completed-by"},
	})
	if code != http.StatusCreated {
		t.Fatalf("clone with linstor-csi's delete_props = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-csiedit")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if rd.Props[store.RestoreFromSnapshotProp] == "" {
		t.Error("the clone lost its restore marker")
	}
}
