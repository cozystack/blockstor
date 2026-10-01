// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"net/http"
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
