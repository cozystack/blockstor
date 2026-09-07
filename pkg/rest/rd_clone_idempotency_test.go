// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedCloneLeftover puts under the target name what a first clone attempt
// leaves behind when it dies after creating the definition and before
// hydrating the volumes: the definition, the marker, no volumes.
func seedCloneLeftover(t *testing.T, st store.Store, srcName, cloneName string, flags ...string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  cloneName,
		Flags: flags,
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker(srcName, cloneSnapshotName(cloneName)),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}
}

// The marker is stamped with the definition, before the volumes are hydrated
// and the replicas placed, so a target carrying it says a clone STARTED — not
// that one finished. Answering 201 on the marker alone turns the retry
// linstor-csi issues after a partial failure into a silent incomplete: CSI
// sees the volume as ready and nothing ever finishes it.
//
// The status is 201 either way, which is the point: only the volumes tell the
// resumed clone from the one that merely agreed it had already happened.
func TestRDCloneResumesAnIncompleteLeftover(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-resume")
	seedCloneLeftover(t, st, "src-resume", "dst-resume")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-resume", map[string]any{
		"name":          "dst-resume",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("retry over an incomplete leftover: got %d, want 201", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(t.Context(), "dst-resume")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("after the retry the target has %d volume(s), want 1 — the clone "+
			"reported success over a definition it never finished", len(vds))
	}

	// And it says so, rather than reading like a first run.
	var envelope cloneStartedResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode the clone envelope: %v", err)
	}

	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatal("the retry returned an empty envelope")
	}

	if !strings.Contains((*envelope.Messages)[0].Message, "on retry") {
		t.Errorf("message = %q, want it to name the retry", (*envelope.Messages)[0].Message)
	}
}

// Resuming stops at a leftover being torn down: finishing it would race the
// deletion reaping the very objects the clone writes.
func TestRDCloneRefusesALeftoverBeingDeleted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-dying")
	seedCloneLeftover(t, st, "src-dying", "dst-dying", rdFlagDelete)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-dying", map[string]any{
		"name":          "dst-dying",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — that leftover is being deleted", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(t.Context(), "dst-dying")
	if err != nil {
		t.Fatalf("list the leftover's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("the refused clone hydrated %d volume(s) into a definition being deleted", len(vds))
	}
}

// A definition under the target name that this clone did not create stays a
// refusal, and stays untouched.
func TestRDCloneStillRefusesAForeignTarget(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-foreign")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "dst-foreign",
		Props: map[string]string{"someone": "else"},
	}); err != nil {
		t.Fatalf("seed the occupant: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-foreign", map[string]any{"name": "dst-foreign"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "dst-foreign")
	if err != nil {
		t.Fatalf("get the occupant: %v", err)
	}

	if got.Props["someone"] != "else" {
		t.Error("the refused clone overwrote the definition that was already there")
	}
}

// resource_group lands on the target on both clone paths — the shallow copy
// stamps it, the data path hands it to materializeRestoredRD — and neither
// went past the validator RD-create runs. A typo produced a clone whose parent
// group does not exist: it lists fine and places badly, because the placer's
// Controller→RG→RD prop walk drops the RG tier without a word.
func TestRDCloneRefusesAnUnknownResourceGroup(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]func(t *testing.T, st store.Store){
		"volume-less source": func(t *testing.T, st store.Store) {
			t.Helper()

			if err := st.ResourceDefinitions().Create(t.Context(),
				&apiv1.ResourceDefinition{Name: "src-rg"}); err != nil {
				t.Fatalf("seed: %v", err)
			}
		},
		"source with volumes": func(t *testing.T, st store.Store) {
			t.Helper()
			seedDeployedCloneSource(t, st, "src-rg")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			src(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := postClone(t, base, "src-rg", map[string]any{
				"name":           "dst-rg",
				"resource_group": "no-such-group",
			})
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 — that resource group does not exist",
					resp.StatusCode)
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-rg"); err == nil {
				t.Error("the refused clone was created anyway, with a dangling parent group")
			}
		})
	}
}

// The positive control: the same request with the group actually there.
func TestRDCloneHonoursAKnownResourceGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(),
		&apiv1.ResourceDefinition{Name: "src-rg-ok"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.ResourceGroups().Create(t.Context(),
		&apiv1.ResourceGroup{Name: "real-group"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rg-ok", map[string]any{
		"name":           "dst-rg-ok",
		"resource_group": "real-group",
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "dst-rg-ok")
	if err != nil {
		t.Fatalf("get the clone: %v", err)
	}

	if got.ResourceGroupName != "real-group" {
		t.Errorf("resource group = %q, want the one the caller asked for", got.ResourceGroupName)
	}
}
