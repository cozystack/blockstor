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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestVGDeleteUnknownReturns200Warning pins the Bug 66 idempotence
// contract for `DELETE /v1/resource-groups/{rg}/volume-groups/{vlmNr}`.
// Two NotFound shapes are covered:
//
//   - sub-test "missing-rg": parent RG itself is absent
//   - sub-test "missing-vg": RG exists, vlmNr is absent inside it
//
// Both fold into the same 200 + WARN + "already absent" envelope so
// the python CLI's XML decoder fallback isn't tripped and audit-log
// greppers can distinguish a no-op replay from a real drop.
func TestVGDeleteUnknownReturns200Warning(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		seed func(t *testing.T, st store.Store)
		path string
	}{
		{
			name: "missing-rg",
			seed: func(_ *testing.T, _ store.Store) {},
			path: "/v1/resource-groups/ghost-rg/volume-groups/0",
		},
		{
			name: "missing-vg",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-vg-test"}); err != nil {
					t.Fatalf("seed RG: %v", err)
				}
			},
			path: "/v1/resource-groups/rg-vg-test/volume-groups/42",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			tc.seed(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpDelete(t, base+tc.path)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			var rc []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
				t.Fatalf("decode ApiCallRc envelope: %v", err)
			}

			if len(rc) == 0 {
				t.Fatalf("ApiCallRc envelope: got empty, want one entry")
			}

			if rc[0].RetCode&maskWarn == 0 {
				t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
			}

			if !strings.Contains(rc[0].Message, "already absent") {
				t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
			}
		})
	}
}
