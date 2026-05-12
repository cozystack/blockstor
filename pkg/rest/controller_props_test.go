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

// TestControllerPropertiesEmptyOnFreshCluster: GET on a brand-new
// controller returns 200 with an empty props map.
func TestControllerPropertiesEmptyOnFreshCluster(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]string

	err := json.NewDecoder(resp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("expected empty props; got %v", got)
	}
}

// TestControllerPropertiesSetAndGet: PUT writes, GET reads back.
func TestControllerPropertiesSetAndGet(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"DefaultDebugSslConnector": "DebugSslConnector"},
	})

	resp := httpPost(t, base+"/v1/controller/properties", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status: got %d, want 201", resp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d", getResp.StatusCode)
	}

	var got map[string]string

	err := json.NewDecoder(getResp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got["DefaultDebugSslConnector"] != "DebugSslConnector" {
		t.Errorf("expected DefaultDebugSslConnector=DebugSslConnector; got %v", got)
	}
}

// TestControllerPropertiesDelete: PUT with delete_props removes a key.
func TestControllerPropertiesDelete(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Seed two keys.
	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"KeepMe": "1", "RemoveMe": "2"},
	})
	resp := httpPost(t, base+"/v1/controller/properties", body)
	_ = resp.Body.Close()

	// Delete one.
	delBody, _ := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"RemoveMe"},
	})

	delResp := httpPost(t, base+"/v1/controller/properties", delBody)
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusCreated {
		t.Fatalf("delete status: got %d", delResp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = getResp.Body.Close() }()

	var got map[string]string
	_ = json.NewDecoder(getResp.Body).Decode(&got)

	if _, present := got["RemoveMe"]; present {
		t.Errorf("RemoveMe still in props: %v", got)
	}

	if got["KeepMe"] != "1" {
		t.Errorf("KeepMe lost: got %v", got)
	}
}

// TestControllerPropertiesModifyBadJSON: malformed body → 400 from
// the JSON decoder. Pinned because controller-prop sets are how
// satellites learn about TLS, ports, AutoBlockSize policy etc — a
// regression that surfaced decoder errors as 500 would mask
// operator typos behind golinstor's retry loop.
func TestControllerPropertiesModifyBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/controller/properties", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}
