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

// seedCloneRG puts a resource group in the store. A clone that pins one by
// name is refused when it is not there, so every test that exercises
// resource_group has to create it first — the same order an operator works in.
func seedCloneRG(t *testing.T, st store.Store, names ...string) {
	t.Helper()

	for _, name := range names {
		if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: name}); err != nil {
			t.Fatalf("seed RG %q: %v", name, err)
		}
	}
}

// linstor-csi defaults LayerList to [DRBD, STORAGE] and never sends a clone
// without it, so a field this endpoint does not declare is a 400 on every
// clone-from-volume regardless of how the StorageClass is written. The body
// is decoded with DisallowUnknownFields, so the refusal lands before any of
// the handler runs.
func TestRDCloneAcceptsTheFullGolinstorBody(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(),
		&apiv1.ResourceDefinition{Name: "pvc-src", ResourceGroupName: "grp"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seedCloneRG(t, st, "grp")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Exactly what golinstor marshals for linstor-csi v1.11.2's clone call
	// (pkg/client/linstor.go): the name, the StorageClass's layer list and
	// group, and the provisioning-completed prop to drop. It never sends
	// use_zfs_clone.
	body := []byte(`{"name":"pvc-dst","layer_list":["DRBD","STORAGE"],` +
		`"resource_group":"grp","delete_props":["Aux/csi-provisioning-completed-by"]}`)

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/clone", body)
	defer func() { _ = resp.Body.Close() }()

	// Asserted as "created", not merely "not 400". Accepting anything
	// outside 400 lets a 500 through, and this test is the one that would
	// have to catch the endpoint accepting the body and then failing on it.
	if resp.StatusCode != http.StatusCreated {
		var raw map[string]any

		_ = json.NewDecoder(resp.Body).Decode(&raw)

		t.Fatalf("clone status = %d, want 201: %+v", resp.StatusCode, raw)
	}
}

// Declaring a field is only half of it. layer_list and resource_group are
// the caller's choice of shape, so a clone that ignores them hands back a
// definition built to the source's shape while reporting success.
func TestRDCloneHonoursLayerListAndResourceGroup(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:              "pvc-src",
		ResourceGroupName: "source-grp",
		LayerStack:        []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seedCloneRG(t, st, "chosen-grp")

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name":           "pvc-dst",
		"layer_list":     []string{"STORAGE"},
		"resource_group": "chosen-grp",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/clone", body)
	_ = resp.Body.Close()

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if err != nil {
		t.Fatalf("get the clone: %v", err)
	}

	if got.ResourceGroupName != "chosen-grp" {
		t.Errorf("resource group = %q, want the one the caller asked for", got.ResourceGroupName)
	}

	if len(got.LayerStack) != 1 || got.LayerStack[0] != "STORAGE" {
		t.Errorf("layer stack = %v, want the one the caller asked for", got.LayerStack)
	}
}

// The two fields this endpoint cannot honour are refused rather than dropped.
// Accepting external_name and returning a definition under a different name,
// or accepting volume_passphrases and materialising volumes with keys the
// caller does not hold, is the failure mode src_snap_name is already refused
// for.
func TestRDCloneRefusesWhatItCannotHonour(t *testing.T) {
	for name, tc := range map[string]struct{ body, field string }{
		"external name":      {`{"name":"pvc-dst","external_name":"something-else"}`, "external_name"},
		"volume passphrases": {`{"name":"pvc-dst","volume_passphrases":["k1"]}`, "volume_passphrases"},
		// python-linstor drops `name` from the body when it sends
		// external_name; the refusal still names the field it cannot honour.
		"external name, python-linstor body": {`{"external_name":"something-else"}`, "external_name"},
	} {
		st := store.NewInMemory()
		if err := st.ResourceDefinitions().Create(t.Context(),
			&apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
			t.Fatalf("%s: seed: %v", name, err)
		}

		base, stop := startServerWithStore(t, st)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
			base+"/v1/resource-definitions/pvc-src/clone", strings.NewReader(tc.body))
		if err != nil {
			t.Fatalf("%s: build the request: %v", name, err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "PythonLinstor/1.27.0")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: post: %v", name, err)
		}

		rc := decodeCloneMessage(t, resp)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNotImplemented {
			t.Errorf("%s: status = %d, want 501", name, resp.StatusCode)
		}

		if !strings.Contains(rc.Message, tc.field) {
			t.Errorf("%s: refusal %q does not name %s", name, rc.Message, tc.field)
		}

		if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-dst"); err == nil {
			t.Errorf("%s: the refused clone was created anyway", name)
		}

		stop()
	}
}

// The same on the path CSI actually takes. A source with volumes clones
// through the snapshot-restore machinery, which builds the target from the
// SOURCE's shape — so the caller's layer_list and resource_group have to be
// carried into it, not just applied on the volume-less shortcut.
func TestRDCloneHonoursTheCallersShapeOnTheDataPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-shape")
	seedCloneRG(t, st, "chosen-grp")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// The layer stack has to be the source's on this path — the clone
	// restores bytes and brings the stack up over them — so what is honoured
	// here is the group, and the stack is sent the way linstor-csi sends it.
	resp := postClone(t, base, "src-shape", map[string]any{
		"name":           "dst-shape",
		"layer_list":     apiv1.DefaultLayerStack(),
		"resource_group": "chosen-grp",
		"use_zfs_clone":  true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone status got %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(ctx, "dst-shape")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	if dst.ResourceGroupName != "chosen-grp" {
		t.Errorf("resource group = %q, want the one the caller asked for", dst.ResourceGroupName)
	}

	if len(dst.LayerStack) != len(apiv1.DefaultLayerStack()) {
		t.Errorf("layer stack = %v, want the one the caller asked for", dst.LayerStack)
	}
}
