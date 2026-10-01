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
// up over them, in that order, and every layer's bring-up writes to the device
// it is handed. LUKS: luks.Format treats a device carrying no LUKS header as
// one to format, so adding it does not hand back an encrypted copy — it
// formats away the data just restored and reports COMPLETE. DRBD: create-md
// runs with --force over `meta-disk internal`, stamping metadata across the
// tail of the same bytes, and the only gate before it looks for DRBD metadata
// rather than for a filesystem.
//
// Both directions of both layers are refused: a dropped layer leaves the
// target reading data the missing layer wrote.
func TestRDCloneRefusesALayerStackChange(t *testing.T) {
	t.Parallel()

	plain := []string{"DRBD", "STORAGE"}
	encrypted := []string{"DRBD", "LUKS", "STORAGE"}
	bare := []string{"STORAGE"}

	for name, tc := range map[string]struct {
		sourceStack []string
		requested   []string
	}{
		"adding LUKS to a plaintext source":      {sourceStack: plain, requested: encrypted},
		"dropping LUKS from an encrypted source": {sourceStack: encrypted, requested: plain},
		// DRBD writes to the device as surely as LUKS does: create-md runs
		// with --force over `meta-disk internal`, stamping metadata across
		// the tail of the bytes the clone just restored, and the only gate
		// before it looks for DRBD metadata rather than for a filesystem.
		"adding DRBD to a source without it":  {sourceStack: bare, requested: plain},
		"dropping DRBD from a source with it": {sourceStack: plain, requested: bare},
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

// The positive control for the refusals above. Same request shape, same
// passphrase, a layer_list spelled in a case the source is not stored in, and
// the clone goes through, because the set is the same. (The order is the
// stack's own and validateLayerStack enforces it, so case is what is free to
// differ.) Without this the refusals could be coming from validateLayerStack or
// from the LUKS-prereq gate and the table above would not know the difference.
func TestRDCloneHonoursALayerListThatMatchesTheSource(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-keep", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerWithPassphrase(t, st)
	defer stop()

	resp := postClone(t, base, "src-keep", map[string]any{
		"name":       "dst-keep",
		"layer_list": []string{"drbd", "luks", "storage"},
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(t.Context(), "dst-keep")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	if len(dst.LayerStack) != 3 {
		t.Errorf("layer stack = %v, want the three layers the caller asked for", dst.LayerStack)
	}
}

// The CSI hot path, which is the one this gate must never touch. linstor-csi
// sends [DRBD, STORAGE] on every clone; a definition created without an
// explicit stack stores none, and reading that as "no layers" would make the
// request read as adding both and refuse every clone-from-volume — the defect
// this PR exists to fix, reintroduced by its own guard. An unset stack means
// the default, here as everywhere else that reads one.
func TestRDCloneOfASourceWithNoRecordedStackTakesTheCSIDefault(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-bare")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-bare", map[string]any{
		"name":          "dst-bare",
		"layer_list":    apiv1.DefaultLayerStack(),
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 — this is what linstor-csi sends on every clone",
			resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-bare"); err != nil {
		t.Errorf("target RD not persisted: %v", err)
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
