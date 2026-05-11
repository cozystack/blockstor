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
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestAPICallRcEnvelopeShape pins the response shape of every
// write-side endpoint: POST/PUT/DELETE must return a JSON array of
// `ApiCallRc` objects (upstream LINSTOR convention).
//
// Why this matters: the Python `linstor` CLI's response parser
// dereferences `replies[0].ret_code` unconditionally on every write
// reply. If a handler returns the created object (or 204 No Content
// with an empty body), the CLI crashes with `TypeError: string
// indices must be integers, not 'str'`. linstor-csi discards the
// body but golinstor still attempts an unmarshal — so the wire
// shape must be uniform across every write surface.
//
// This is a table-driven guard: adding a new write handler that
// returns the wrong shape breaks ONE assertion below. Don't paper
// over the failure — fix the handler to emit the envelope.
func TestAPICallRcEnvelopeShape(t *testing.T) {
	cases := []struct {
		name      string
		method    string
		path      string
		body      any
		seed      func(t *testing.T, st store.Store)
		wantCodes []int // acceptable status codes (200 / 201 / 200|201)
	}{
		{
			name:      "POST resource-group",
			method:    http.MethodPost,
			path:      "/v1/resource-groups",
			body:      apiv1.ResourceGroup{Name: "rg-env-1"},
			wantCodes: []int{http.StatusCreated},
		},
		{
			name:   "PUT resource-group",
			method: http.MethodPut,
			path:   "/v1/resource-groups/rg-env-2",
			body:   apiv1.ResourceGroup{Description: "modified"},
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceGroups().Create(t.Context(),
					&apiv1.ResourceGroup{Name: "rg-env-2"}); err != nil {
					t.Fatalf("seed RG: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:   "DELETE resource-group",
			method: http.MethodDelete,
			path:   "/v1/resource-groups/rg-env-3",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceGroups().Create(t.Context(),
					&apiv1.ResourceGroup{Name: "rg-env-3"}); err != nil {
					t.Fatalf("seed RG: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:   "POST resource-definition",
			method: http.MethodPost,
			path:   "/v1/resource-definitions",
			body: apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-env-1"},
			},
			wantCodes: []int{http.StatusCreated},
		},
		{
			name:   "DELETE resource-definition",
			method: http.MethodDelete,
			path:   "/v1/resource-definitions/rd-env-2",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceDefinitions().Create(t.Context(),
					&apiv1.ResourceDefinition{Name: "rd-env-2"}); err != nil {
					t.Fatalf("seed RD: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:   "DELETE volume-definition",
			method: http.MethodDelete,
			path:   "/v1/resource-definitions/rd-env-vd/volume-definitions/0",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceDefinitions().Create(t.Context(),
					&apiv1.ResourceDefinition{Name: "rd-env-vd"}); err != nil {
					t.Fatalf("seed RD: %v", err)
				}

				if err := st.VolumeDefinitions().Create(t.Context(), "rd-env-vd",
					&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
					t.Fatalf("seed VD: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:      "POST node",
			method:    http.MethodPost,
			path:      "/v1/nodes",
			body:      apiv1.Node{Name: "node-env-1", Type: "SATELLITE"},
			wantCodes: []int{http.StatusCreated},
		},
		{
			name:   "DELETE node",
			method: http.MethodDelete,
			path:   "/v1/nodes/node-env-2",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.Nodes().Create(t.Context(),
					&apiv1.Node{Name: "node-env-2", Type: "SATELLITE"}); err != nil {
					t.Fatalf("seed Node: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:   "POST resource-group/{rg}/adjust",
			method: http.MethodPost,
			path:   "/v1/resource-groups/rg-env-adjust/adjust",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceGroups().Create(t.Context(),
					&apiv1.ResourceGroup{Name: "rg-env-adjust"}); err != nil {
					t.Fatalf("seed RG: %v", err)
				}
			},
			wantCodes: []int{http.StatusOK},
		},
		{
			name:   "POST resource-group/{rg}/volume-groups",
			method: http.MethodPost,
			path:   "/v1/resource-groups/rg-env-vg/volume-groups",
			body:   apiv1.VolumeGroup{VolumeNumber: 0},
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceGroups().Create(t.Context(),
					&apiv1.ResourceGroup{Name: "rg-env-vg"}); err != nil {
					t.Fatalf("seed RG: %v", err)
				}
			},
			wantCodes: []int{http.StatusCreated},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()

			if tc.seed != nil {
				tc.seed(t, st)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			var bodyReader *bytes.Reader

			if tc.body != nil {
				raw, err := json.Marshal(tc.body)
				if err != nil {
					t.Fatalf("marshal body: %v", err)
				}

				bodyReader = bytes.NewReader(raw)
			}

			var req *http.Request

			var err error

			if bodyReader != nil {
				req, err = http.NewRequestWithContext(t.Context(),
					tc.method, base+tc.path, bodyReader)
			} else {
				req, err = http.NewRequestWithContext(t.Context(),
					tc.method, base+tc.path, http.NoBody)
			}

			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}

			defer func() { _ = resp.Body.Close() }()

			if !containsInt(tc.wantCodes, resp.StatusCode) {
				t.Errorf("status: got %d, want one of %v",
					resp.StatusCode, tc.wantCodes)
			}

			// The wire shape: must decode as `[]APICallRc` with at
			// least one entry and a non-negative ret_code (upstream
			// MASK_INFO marker for "success"). golinstor and the
			// Python CLI both call replies[0].ret_code on writes;
			// anything else breaks them.
			var rc []apiv1.APICallRc

			if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rc) == 0 {
				t.Fatalf("empty envelope — Python CLI crashes on replies[0]")
			}

			if rc[0].RetCode < 0 {
				t.Errorf("ret_code = %d, want >=0 (success marker)", rc[0].RetCode)
			}

			if rc[0].Message == "" {
				t.Errorf("empty message — operator log will be unreadable")
			}
		})
	}
}

func containsInt(haystack []int, needle int) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}

	return false
}
