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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The cluster passphrase is the key every LUKS volume was formatted with,
// and nothing re-keys a volume, so changing it while LUKS volumes exist is
// refused and the old value keeps unlocking.
func TestPassphraseModifyIsRefusedWhileLUKSVolumesExist(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-enc", LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "old-secret"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	modifyBody, _ := json.Marshal(map[string]string{"old_passphrase": "old-secret", "new_passphrase": "new-secret"})
	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", modifyBody)
	got, _ := io.ReadAll(modifyResp.Body)
	_ = modifyResp.Body.Close()

	if modifyResp.StatusCode != http.StatusConflict || !strings.Contains(string(got), "pvc-enc") {
		t.Fatalf("modify: got %d %s, want 409 naming the LUKS volume", modifyResp.StatusCode, got)
	}

	oldEnter, _ := json.Marshal(map[string]string{"new_passphrase": "old-secret"})
	oldResp := httpPatch(t, base+"/v1/encryption/passphrase", oldEnter)
	_ = oldResp.Body.Close()

	if oldResp.StatusCode != http.StatusOK {
		t.Errorf("the old passphrase no longer unlocks after a refused rotation: %d", oldResp.StatusCode)
	}
}

// The guard asks the API server, not the cache: a LUKS definition the
// cache has not seen yet still stops the rotation.
func TestPassphraseModifyAsksTheAPIServerForLUKSVolumes(t *testing.T) {
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, testPassphraseSecret("old-secret")); err != nil {
		t.Fatalf("seed the Secret: %v", err)
	}

	if err := cli.Create(ctx, &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-new-luks"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:         pickFreeAddr(t),
		Store:        store.NewInMemory(),
		Client:       cli,
		SecretReader: cli,
		Namespace:    testRESTNamespace,
	})
	defer stop()

	body, _ := json.Marshal(map[string]string{"old_passphrase": "old-secret", "new_passphrase": "new-secret"})
	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict || !strings.Contains(string(got), "pvc-new-luks") {
		t.Fatalf("modify: got %d %s, want 409 naming the definition the cache has not seen", resp.StatusCode, got)
	}
}
