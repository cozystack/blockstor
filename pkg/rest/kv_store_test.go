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

// TestKVGetReturnsSingleElementArray pins scenario 1.9:
// GET /v1/key-value-store/{instance} must return `[]KV` (a single-element
// array), NOT a bare KV object. linstor-csi's
// KeyValueStoreService.Get decoder unmarshals into `[]KV`; a bare object
// breaks csi-sanity's "ListSnapshots check presence" with
// `cannot unmarshal object into Go value of type []client.KV`.
func TestKVGetReturnsSingleElementArray(t *testing.T) {
	// Reset process-local bag so test order doesn't leak entries.
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Seed an instance via PUT first so the GET has something to surface.
	put, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"foo": "bar"},
	})

	putResp := httpPut(t, base+"/v1/key-value-store/snap-meta", put)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", putResp.StatusCode)
	}

	resp := httpGet(t, base+"/v1/key-value-store/snap-meta")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", resp.StatusCode)
	}

	// Decoding as `[]KV` must succeed; decoding as `KV` would be wrong
	// even if it parsed (golinstor wouldn't).
	var got []apiv1.KV
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode []KV: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	if got[0].Name != "snap-meta" {
		t.Errorf("Name: got %q, want %q", got[0].Name, "snap-meta")
	}

	if got[0].Props["foo"] != "bar" {
		t.Errorf("Props[foo]: got %q, want %q", got[0].Props["foo"], "bar")
	}
}

// TestKVGetReturnsEmptyPropsArrayForUnknown pins the "no-such-instance"
// branch of 1.9: a bare `[]KV{{Name: <name>}}` envelope keeps the wire
// shape consistent so the decoder never trips, regardless of whether
// the instance was ever written.
func TestKVGetReturnsEmptyPropsArrayForUnknown(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/key-value-store/never-written")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.KV
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	if got[0].Name != "never-written" {
		t.Errorf("Name: got %q, want %q", got[0].Name, "never-written")
	}
}

// TestKVPutDeletePersistInProcessLocalBag pins scenario 1.10: the
// process-local KV bag persists writes across HTTP requests in the same
// Server instance. PUT seeds; GET reads back; DELETE drops; GET shows
// the value absent. linstor-csi's CreateSnapshot writes the snapshot's
// pvName into KV then reads it back on the next reconcile — a no-op
// PUT/DELETE breaks `CreateSnapshot from source volume` in csi-sanity.
func TestKVPutDeletePersistInProcessLocalBag(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	put, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"csi/snap-id": "vol-1",
			"keep":        "yes",
		},
	})

	putResp := httpPut(t, base+"/v1/key-value-store/csi-backup-mapping", put)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", putResp.StatusCode)
	}

	// Subsequent GET must surface the value.
	getResp := httpGet(t, base+"/v1/key-value-store/csi-backup-mapping")

	var first []apiv1.KV

	err := json.NewDecoder(getResp.Body).Decode(&first)
	_ = getResp.Body.Close()

	if err != nil {
		t.Fatalf("GET decode: %v", err)
	}

	if len(first) != 1 || first[0].Props["csi/snap-id"] != "vol-1" {
		t.Errorf("after PUT: got %+v, want csi/snap-id=vol-1", first)
	}

	// Targeted DELETE of one key via the modify endpoint (DeleteProps).
	delOne, _ := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"csi/snap-id"},
	})

	putResp2 := httpPut(t, base+"/v1/key-value-store/csi-backup-mapping", delOne)
	_ = putResp2.Body.Close()

	getResp2 := httpGet(t, base+"/v1/key-value-store/csi-backup-mapping")

	var second []apiv1.KV

	err = json.NewDecoder(getResp2.Body).Decode(&second)
	_ = getResp2.Body.Close()

	if err != nil {
		t.Fatalf("GET-2 decode: %v", err)
	}

	if len(second) != 1 {
		t.Fatalf("after delete-prop: len=%d, want 1", len(second))
	}

	if _, ok := second[0].Props["csi/snap-id"]; ok {
		t.Errorf("csi/snap-id should have been deleted: %+v", second[0].Props)
	}

	if second[0].Props["keep"] != "yes" {
		t.Errorf("untouched key dropped: %+v", second[0].Props)
	}

	// Full DELETE of the instance.
	delResp := httpDelete(t, base+"/v1/key-value-store/csi-backup-mapping")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// Post-delete GET still returns the single-element envelope, but
	// with no Props — matches "instance never written" branch.
	getResp3 := httpGet(t, base+"/v1/key-value-store/csi-backup-mapping")

	var third []apiv1.KV

	err = json.NewDecoder(getResp3.Body).Decode(&third)
	_ = getResp3.Body.Close()

	if err != nil {
		t.Fatalf("GET-3 decode: %v", err)
	}

	if len(third) != 1 {
		t.Fatalf("post-delete len: got %d, want 1", len(third))
	}

	if len(third[0].Props) != 0 {
		t.Errorf("post-delete Props: got %+v, want empty", third[0].Props)
	}
}
