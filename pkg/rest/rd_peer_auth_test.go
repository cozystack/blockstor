// SPDX-License-Identifier: Apache-2.0

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

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
)

// Every door that creates a definition gives it DRBD peer authentication,
// as LINSTOR does: a 20-character secret of its own and the sha1 digest.
// A clone and a restore get a fresh secret, not their source's. The
// failures name lengths and equality, never a secret.

// peerAuthSourceSecret is the fixture secret the sources below carry.
const peerAuthSourceSecret = "fixture-source-secret"

func requirePeerAuth(t *testing.T, st store.Store, name string) string {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), name)
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}

	secret := rd.Props[drbd.SharedSecretProp]
	if len(secret) != 20 {
		t.Errorf("%s: shared secret of length %d, want a generated 20", name, len(secret))
	}

	if got := rd.Props[drbd.CramHMACAlgProp]; got != drbd.DefaultCramHMACAlg {
		t.Errorf("%s: cram-hmac-alg %q, want %q", name, got, drbd.DefaultCramHMACAlg)
	}

	return secret
}

func postExpect(t *testing.T, url string, body any, want int) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, url, raw)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != want {
		t.Fatalf("POST %s: status %d, want %d; body=%s", url, resp.StatusCode, want, respBody)
	}
}

func TestRDCreateSeedsPeerAuthentication(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, name := range []string{"pvc-a", "pvc-b"} {
		postExpect(t, base+"/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
			ResourceDefinition: apiv1.ResourceDefinition{Name: name},
		}, http.StatusCreated)
	}

	if requirePeerAuth(t, st, "pvc-a") == requirePeerAuth(t, st, "pvc-b") {
		t.Errorf("two definitions share one secret")
	}
}

// drbd_secret was accepted and dropped; LINSTOR makes it the definition's
// secret.
func TestRDCreateTakesDrbdSecret(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	postExpect(t, base+"/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "pvc-a"},
		DrbdSecret:         peerAuthSourceSecret,
	}, http.StatusCreated)

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if rd.Props[drbd.SharedSecretProp] != peerAuthSourceSecret {
		t.Errorf("the definition does not carry the drbd_secret it was created with")
	}

	postExpect(t, base+"/v1/resource-definitions", apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "pvc-b"},
		DrbdSecret:         "two\nlines",
	}, http.StatusBadRequest)

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-b"); err == nil {
		t.Errorf("a definition with a refused drbd_secret was created")
	}
}

func TestRGSpawnSeedsPeerAuthentication(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	postExpect(t, base+"/v1/resource-groups/rg-1/spawn", apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-spawned",
		DefinitionsOnly:        true,
	}, http.StatusCreated)

	requirePeerAuth(t, st, "pvc-spawned")
}

func TestRDCloneShellGetsItsOwnSecret(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-src",
		Props: map[string]string{
			drbd.SharedSecretProp: peerAuthSourceSecret,
			drbd.CramHMACAlgProp:  drbd.DefaultCramHMACAlg,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	postExpect(t, base+"/v1/resource-definitions/pvc-src/clone", map[string]any{"name": "pvc-clone"}, http.StatusCreated)

	if requirePeerAuth(t, st, "pvc-clone") == peerAuthSourceSecret {
		t.Errorf("the clone carries its source's secret")
	}
}

func TestSnapshotRestoreGetsItsOwnSecret(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	srcProps := map[string]string{
		drbd.SharedSecretProp: peerAuthSourceSecret,
		drbd.CramHMACAlgProp:  drbd.DefaultCramHMACAlg,
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src", Props: srcProps}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		Props:        srcProps,
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	postExpect(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
	}, http.StatusCreated)

	if requirePeerAuth(t, st, "pvc-restored") == peerAuthSourceSecret {
		t.Errorf("the restored definition carries its source's secret")
	}
}

// A definition that existed before blockstor stamped authentication keeps
// running without it: turning it on under live connections fails every
// handshake between a satellite that adjusted and one that has not, so a
// later edit of the definition must not switch it on.
func TestRDModifyLeavesAnExistingDefinitionUnauthenticated(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-old"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(map[string]any{"override_props": map[string]string{"Aux/touched": "yes"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-old", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("modify: status %d", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-old")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if drbd.HasPropFold(rd.Props, drbd.SharedSecretProp) || drbd.HasPropFold(rd.Props, drbd.CramHMACAlgProp) {
		t.Errorf("an edit switched peer authentication on for an existing definition")
	}
}
