// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedCloneSourceWithStack is seedDeployedCloneSource with a layer stack put
// on the source, which is what a clone's requested stack is compared against.
func seedCloneSourceWithStack(t *testing.T, st store.Store, rdName string, stack []string) {
	t.Helper()

	seedDeployedCloneSource(t, st, rdName)

	rd, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read the seeded source: %v", err)
	}

	rd.LayerStack = stack

	if err := st.ResourceDefinitions().Update(t.Context(), &rd); err != nil {
		t.Fatalf("put a layer stack on the source: %v", err)
	}
}

// The clone data plane restores the source's bytes and brings the layer stack
// up over them, in that order: applyStorageIfDiskful writes the data, maybeLUKS
// runs after it, and luks.Format treats a device carrying no LUKS header as one
// to format. So a layer_list that adds LUKS to a plaintext source does not hand
// back an encrypted copy — it formats away the data it just restored, and the
// clone reports COMPLETE over the wreckage.
//
// Dropping LUKS is refused for the mirror reason: the bytes stay ciphertext
// under a definition claiming they are not.
func TestRDCloneRefusesALUKSMembershipChange(t *testing.T) {
	t.Parallel()

	plain := []string{"DRBD", "STORAGE"}
	encrypted := []string{"DRBD", "LUKS", "STORAGE"}

	for name, tc := range map[string]struct {
		sourceStack []string
		requested   []string
	}{
		"adding LUKS to a plaintext source":      {sourceStack: plain, requested: encrypted},
		"dropping LUKS from an encrypted source": {sourceStack: encrypted, requested: plain},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedCloneSourceWithStack(t, st, "src-luks", tc.sourceStack)

			// The cluster passphrase is set, so the refusal under test is the
			// membership change and not the LUKS-prereq gate that runs above it.
			base, stop := startServerWithPassphrase(t, st)
			defer stop()

			resp := postClone(t, base, "src-luks", map[string]any{
				"name":       "dst-luks",
				"layer_list": tc.requested,
			})
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-luks"); err == nil {
				t.Error("the refused clone was created anyway")
			}

			// The refusal has to land before the data plane starts, not after:
			// the internal snapshot is the first thing the clone writes.
			if _, err := st.Snapshots().Get(t.Context(), "src-luks",
				cloneSnapshotName("dst-luks")); err == nil {
				t.Error("the refused clone snapshotted the source anyway")
			}
		})
	}
}

// The positive control for the refusal above. Same request shape, same
// passphrase, a layer_list that differs from the source's — but with LUKS
// membership left alone — and the clone goes through. Without this the
// refusal could be coming from validateLayerStack or from the LUKS-prereq
// gate and the test above would not know the difference.
func TestRDCloneHonoursALayerListThatKeepsLUKSMembership(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-keep", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerWithPassphrase(t, st)
	defer stop()

	resp := postClone(t, base, "src-keep", map[string]any{
		"name":       "dst-keep",
		"layer_list": []string{"LUKS", "STORAGE"},
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(t.Context(), "dst-keep")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	want := []string{"LUKS", "STORAGE"}
	if len(dst.LayerStack) != len(want) || dst.LayerStack[0] != want[0] || dst.LayerStack[1] != want[1] {
		t.Errorf("layer stack = %v, want the one the caller asked for %v", dst.LayerStack, want)
	}
}

// The empty-source path carries no bytes to lose, so it keeps taking the
// caller's stack whatever it says about LUKS. Refusing there would break the
// only reason the endpoint accepts layer_list at all.
func TestRDCloneOfAVolumelessSourceStillTakesAnyStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:       "src-empty",
		LayerStack: []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithPassphrase(t, st)
	defer stop()

	resp := postClone(t, base, "src-empty", map[string]any{
		"name":       "dst-empty",
		"layer_list": []string{"DRBD", "LUKS", "STORAGE"},
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(t.Context(), "dst-empty")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	if !apiv1.LayerInStack(dst.LayerStack, apiv1.LayerKindLUKS) {
		t.Errorf("layer stack = %v, want the LUKS stack the caller asked for", dst.LayerStack)
	}
}
