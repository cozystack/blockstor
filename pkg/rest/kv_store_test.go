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

// TestKVList: GET /v1/key-value-store enumerates every instance with
// its full props map. piraeus-csi consumes this for the storage
// metadata catalogue, and a list that drops props would silently lose
// CSI-supplied volume parameters.
func TestKVList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, inst := range []struct {
		name string
		k, v string
	}{
		{"csi-volumes", "size", "1Gi"},
		{"snapshots", "policy", "daily"},
	} {
		err := st.KeyValueStore().SetKeys(ctx, inst.name, apiv1.GenericPropsModify{
			OverrideProps: map[string]string{inst.k: inst.v},
		})
		if err != nil {
			t.Fatalf("seed %s: %v", inst.name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/key-value-store")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.KV
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (got %+v)", len(got), got)
	}

	byName := map[string]apiv1.KV{}
	for _, kv := range got {
		byName[kv.Name] = kv
	}

	if byName["csi-volumes"].Props["size"] != "1Gi" {
		t.Errorf("csi-volumes props lost: got %+v", byName["csi-volumes"])
	}

	if byName["snapshots"].Props["policy"] != "daily" {
		t.Errorf("snapshots props lost: got %+v", byName["snapshots"])
	}
}

// TestKVListEmpty: GET /v1/key-value-store on an empty store returns
// 200 with [], not 404 (matches upstream LINSTOR's empty-list shape).
func TestKVListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/key-value-store")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.KV
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

// TestKVSetMergesWithExisting: a second POST against the same
// instance with new keys must merge with existing props rather than
// replace the whole map. The CSI driver writes individual volume
// parameters one at a time, so a non-merging set would clobber the
// metadata of every other volume on every Set call.
func TestKVSetMergesWithExisting(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.KeyValueStore().SetKeys(ctx, "csi-volumes", apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"vol1/size": "1Gi"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Add a second key via REST POST.
	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"vol2/size": "2Gi"},
	})
	resp := httpPost(t, base+"/v1/key-value-store/csi-volumes", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.KeyValueStore().GetInstance(ctx, "csi-volumes")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got["vol1/size"] != "1Gi" {
		t.Errorf("first key lost on second Set: got %+v", got)
	}

	if got["vol2/size"] != "2Gi" {
		t.Errorf("second key not added: got %+v", got)
	}
}

// TestKVSetBadJSON: malformed body → 400 before touching the store.
func TestKVSetBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/key-value-store/x", []byte("{not json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
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

// TestKVDeleteHappyPath: DELETE /v1/key-value-store/{instance} → 204
// + the instance is gone afterwards. Pins the success branch of
// handleKVDelete (was 66.7%).
func TestKVDeleteHappyPath(t *testing.T) {
	st := store.NewInMemory()
	if err := st.KeyValueStore().SetKeys(t.Context(), "csi-vols", apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/key-value-store/csi-vols")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status: got %d, want 204", resp.StatusCode)
	}

	// Subsequent GET → 404.
	getResp := httpGet(t, base+"/v1/key-value-store/csi-vols")
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("post-delete: got %d, want 404", getResp.StatusCode)
	}
}

// TestKVDeleteMissing: DELETE on a non-existent instance → 404
// from writeStoreError. Pinned because csi calls this idempotently
// on volume teardown to clear ephemeral kv state — the 404 must
// surface cleanly so csi treats it as "already gone".
func TestKVDeleteMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/key-value-store/ghost")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
