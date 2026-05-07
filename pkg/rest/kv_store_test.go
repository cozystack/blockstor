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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestKVSetThenGet: round-trip through REST.
func TestKVSetThenGet(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/key-value-store/csi-volumes", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set: got %d, want 200", resp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/key-value-store/csi-volumes")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get: got %d, want 200", getResp.StatusCode)
	}

	var kv apiv1.KV
	if jErr := json.NewDecoder(getResp.Body).Decode(&kv); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if kv.Name != "csi-volumes" || kv.Props["k"] != "v" {
		t.Errorf("got %+v", kv)
	}
}

// TestKVGetMissing: 404 on absent instance.
func TestKVGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/key-value-store/ghost")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestKVDeleteThenGet: deleted instance becomes 404.
func TestKVDeleteThenGet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.KeyValueStore().SetKeys(t.Context(), "x", apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	delResp := httpDelete(t, base+"/v1/key-value-store/x")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: got %d, want 204", delResp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/key-value-store/x")
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("post-delete: got %d, want 404", getResp.StatusCode)
	}
}
