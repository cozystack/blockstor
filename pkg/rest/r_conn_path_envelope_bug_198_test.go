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
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug198ResourceConnectionPathDeleteReturnsEnvelope pins the wire
// shape of the resource-connection path DELETE handler. Bug 198: the
// handler used to reply 204 + empty body. golinstor (and python-linstor)
// decode every write reply as `[]ApiCallRc` unconditionally — an empty
// body trips json.Unmarshal with "Unable to parse REST json data:
// Expecting value" and the Python CLI further crashes dereferencing
// `replies[0].ret_code`. Mirrors Bug 45 envelope shape.
func TestBug198ResourceConnectionPathDeleteReturnsEnvelope(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Seed a path so DELETE has something concrete to drop.
	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths",
		[]byte(`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST status: got %d, want 201", resp.StatusCode)
	}

	delResp := httpDelete(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths/path1")
	defer func() { _ = delResp.Body.Close() }()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200 (Bug 198: was 204 + empty body)", delResp.StatusCode)
	}

	var rc []apiv1.APICallRc

	if err := json.NewDecoder(delResp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — golinstor and Python CLI both crash here")
	}

	if rc[0].RetCode < 0 {
		t.Errorf("ret_code = %d, want >=0 (MASK_INFO success marker)", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}
}

// TestBug198ResourceConnectionPathDeleteIdempotent pins the no-op
// replay path: DELETE on a path-name that doesn't exist must succeed
// with a 200 + warn envelope. Mirrors the Bug 56 / 66 pattern used by
// the other "delete-of-missing" handlers (resource, RD, RG, VD,
// snapshot, ...).
func TestBug198ResourceConnectionPathDeleteIdempotent(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	delResp := httpDelete(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths/does-not-exist")
	defer func() { _ = delResp.Body.Close() }()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE-of-missing status: got %d, want 200", delResp.StatusCode)
	}

	var rc []apiv1.APICallRc

	if err := json.NewDecoder(delResp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — golinstor and Python CLI both crash here")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code = %#x, want maskWarn bit set on no-op delete", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}
}

// TestBug198PythonCLINoCrash asserts the response body is non-empty
// and JSON-decodable. The pre-fix 204 + empty body made
// `json.NewDecoder(...).Decode(&rc)` fail with io.EOF — which is the
// shape that crashes python-linstor / golinstor in production.
func TestBug198PythonCLINoCrash(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Seed and delete a real path.
	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths",
		[]byte(`{"name":"p","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`))
	_ = resp.Body.Close()

	delResp := httpDelete(t,
		base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths/p")
	defer func() { _ = delResp.Body.Close() }()

	body, err := io.ReadAll(delResp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(body) == 0 {
		t.Fatalf("response body is empty — pre-fix 204 shape; "+
			"golinstor.UnmarshalResponse crashes with 'Expecting value' "+
			"on this. status=%d", delResp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("body is not JSON-decodable as []APICallRc: %v (body=%q)", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope decoded to empty slice — replies[0].ret_code "+
			"crashes Python CLI. body=%q", body)
	}
}
