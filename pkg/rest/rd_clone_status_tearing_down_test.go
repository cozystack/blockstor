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

// A status poll of an unfinished clone that is being deleted says it is being
// torn down rather than that an earlier attempt stopped: the advice to issue
// the clone again only resumes once the delete is through.
func TestRDCloneStatusOfADefinitionBeingDeletedSaysItIsTornDown(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-tear")

	if err := backend.VolumeDefinitions().Create(ctx, "src-tear",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	if code := cloneOnce(t, base, "src-tear", "dst-tear", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if err := backend.VolumeDefinitions().Delete(ctx, "dst-tear", 1); err != nil {
		t.Fatalf("leave the clone unfinished: %v", err)
	}

	if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-tear",
		func(rd *apiv1.ResourceDefinition) error {
			rd.Flags = append(rd.Flags, rdFlagDelete)

			return nil
		}); err != nil {
		t.Fatalf("flag the clone for deletion: %v", err)
	}

	resp := httpGet(t, base+"/v1/resource-definitions/src-tear/clone/dst-tear")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status poll = %d, want 404", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode: %v (%d entries)", err, len(rcs))
	}

	if !strings.Contains(rcs[0].Cause, "torn down") {
		t.Errorf("cause %q does not say the definition is being torn down", rcs[0].Cause)
	}
}
