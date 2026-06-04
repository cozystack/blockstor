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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case D7b — `--layer-list` negative cases
// (linstor-administration.adoc ~1819-1833).
//
// Upstream LINSTOR validates the layer stack at RD-create time and
// rejects three malformed inputs. Oracle (piraeus-server 1.33.2)
// captures, taken on the dev stand 2026-06-04:
//
//   1. invalid order `luks,drbd,storage`
//      → ERROR rc=10, "The layer stack [LUKS, DRBD, STORAGE] is invalid"
//   2. duplicate layer `drbd,drbd,storage`
//      → ERROR rc=10, "The layer stack [DRBD, DRBD, STORAGE] is invalid"
//   3. unknown layer `bogus,storage`
//      → python-linstor CLI rejects CLIENT-side ("Layer name \"bogus\"
//        not valid", exit 2); the request never reaches the controller.
//
// blockstor rejects all three at the REST wire boundary with a 400 +
// the APICallRc error envelope. The OUTCOME matches upstream
// (rejection); the divergence is the error-envelope wire shape (BS
// emits a plain typed message; upstream emits an error-report id +
// obj_refs). That divergence is whitelisted in
// docs/cli-parity-known-deltas.md (row D7b). These tests pin BS's
// rejection so the gate cannot silently regress to accept-and-corrupt.

// TestCornerD7b_InvalidOrderRejected pins case 1: a LUKS-above-DRBD
// stack is rejected with ErrInvalidLayerOrder. blockstor reports the
// more specific "DRBD must be the first layer" reason rather than
// upstream's generic "is invalid"; both reject.
func TestCornerD7b_InvalidOrderRejected(t *testing.T) {
	t.Parallel()

	err := validateLayerStack([]string{"luks", "drbd", "storage"})
	if err == nil {
		t.Fatal("luks,drbd,storage must be rejected; got nil")
	}

	if !errors.Is(err, ErrInvalidLayerOrder) {
		t.Fatalf("want ErrInvalidLayerOrder; got %v", err)
	}
}

// TestCornerD7b_DuplicateReportedAsDuplicate pins case 2 AND the
// diagnostic-precision improvement: `drbd,drbd,storage` is reported as
// a duplicate, not as "DRBD must be the first layer" (the second DRBD
// at index 1 used to trip the position check first). Upstream's
// envelope is the generic "is invalid"; BS names the actual fault.
func TestCornerD7b_DuplicateReportedAsDuplicate(t *testing.T) {
	t.Parallel()

	err := validateLayerStack([]string{"drbd", "drbd", "storage"})
	if err == nil {
		t.Fatal("drbd,drbd,storage must be rejected; got nil")
	}

	if !errors.Is(err, ErrInvalidLayerOrder) {
		t.Fatalf("want ErrInvalidLayerOrder; got %v", err)
	}

	if !strings.Contains(err.Error(), "more than once") {
		t.Errorf("duplicate stack must be reported as a duplicate, "+
			"not masked by the position check; got %q", err.Error())
	}
}

// TestCornerD7b_UnknownLayerRejected pins case 3 at the REST layer.
// The python-linstor CLI rejects an unknown layer client-side so the
// controller never sees it via the CLI path — but a direct REST caller
// (or a future CLI that relaxes its client check) still can, so the
// server-side allowlist MUST hold. blockstor 400s with
// ErrUnsupportedLayer naming the offending token.
func TestCornerD7b_UnknownLayerRejected(t *testing.T) {
	t.Parallel()

	err := validateLayerStack([]string{"bogus", "storage"})
	if err == nil {
		t.Fatal("bogus,storage must be rejected; got nil")
	}

	if !errors.Is(err, ErrUnsupportedLayer) {
		t.Fatalf("want ErrUnsupportedLayer; got %v", err)
	}

	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("error must name the offending token 'bogus'; got %q", err.Error())
	}
}

// TestCornerD7b_RDCreateRejectionEnvelopes drives all three negative
// cases through the real POST /v1/resource-definitions handler and
// asserts the HTTP-400 + APICallRc envelope. The unknown-layer case
// goes through a raw JSON body (the CLI would reject it client-side,
// so the handler path is only reachable from a direct REST caller).
func TestCornerD7b_RDCreateRejectionEnvelopes(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantText string
	}{
		{
			name:     "invalid-order",
			body:     `{"resource_definition":{"name":"ccd2-ll-order","layer_data":[{"type":"luks"},{"type":"drbd"},{"type":"storage"}]}}`,
			wantText: "invalid layer order",
		},
		{
			name:     "duplicate",
			body:     `{"resource_definition":{"name":"ccd2-ll-dup","layer_data":[{"type":"drbd"},{"type":"drbd"},{"type":"storage"}]}}`,
			wantText: "more than once",
		},
		{
			name:     "unknown-layer",
			body:     `{"resource_definition":{"name":"ccd2-ll-unknown","layer_data":[{"type":"bogus"},{"type":"storage"}]}}`,
			wantText: "unsupported layer",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			resp := httpPost(t, base+"/v1/resource-definitions", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status: got %d, want 400", resp.StatusCode)
			}

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			// The envelope is a one-element APICallRc array with the
			// LINSTOR MASK_ERROR ret_code and a typed message.
			var rcs []apiv1.APICallRc
			if derr := json.NewDecoder(bytes.NewReader(raw)).Decode(&rcs); derr != nil {
				t.Fatalf("decode APICallRc envelope: %v (body=%s)", derr, raw)
			}

			if len(rcs) != 1 {
				t.Fatalf("want 1 APICallRc entry; got %d (%s)", len(rcs), raw)
			}

			if rcs[0].RetCode >= 0 {
				t.Errorf("ret_code must carry the MASK_ERROR bit (negative); got %d", rcs[0].RetCode)
			}

			if !strings.Contains(strings.ToLower(rcs[0].Message), tc.wantText) {
				t.Errorf("message should contain %q; got %q", tc.wantText, rcs[0].Message)
			}

			// The rejected create must not leak an RD into the store.
			rds, lerr := storeListRDNames(t, base)
			if lerr != nil {
				t.Fatalf("list RDs: %v", lerr)
			}

			if len(rds) != 0 {
				t.Errorf("rejected create leaked resource definitions: %v", rds)
			}
		})
	}
}

// storeListRDNames is a tiny helper that GETs the RD list and returns
// the names, so the leak-check above doesn't depend on the store
// internals.
func storeListRDNames(t *testing.T, base string) ([]string, error) {
	t.Helper()

	resp := httpGet(t, base+"/v1/resource-definitions")
	defer func() { _ = resp.Body.Close() }()

	var rds []apiv1.ResourceDefinition
	if err := json.NewDecoder(resp.Body).Decode(&rds); err != nil {
		// An empty body decodes cleanly to nil; a genuine decode error
		// is surfaced.
		if errors.Is(err, io.EOF) {
			return nil, nil
		}

		return nil, fmt.Errorf("decode RD list: %w", err)
	}

	names := make([]string, 0, len(rds))
	for i := range rds {
		names = append(names, rds[i].Name)
	}

	return names, nil
}
