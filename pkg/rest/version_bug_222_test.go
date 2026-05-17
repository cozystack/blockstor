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

	"github.com/cozystack/blockstor/pkg/version"
)

// TestRestAPIVersion127 pins Bug 222: `rest_api_version` was advertised
// as 1.23.0 on the controller-version endpoint, but the upstream Java
// LINSTOR has rolled forward to 1.27.0 several minor releases ago.
// python-linstor's `_require_version()` gates a chunk of CLI flags
// (added between 1.24 and 1.27) client-side on this value — until we
// bump it, the CLI refuses to even send the request, so blockstor
// looks like it's missing features it actually serves.
//
// Pinned at the wire layer: a future refactor of pkg/version must not
// silently regress the constant below 1.27.0 without updating the
// upstream-parity table.
func TestRestAPIVersion127(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	resp := httpGet(t, base+"/v1/controller/version")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var got struct {
		RestAPIVersion string `json:"rest_api_version"`
	}

	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v\nbody: %s", err, body)
	}

	const want = "1.27.0"
	if got.RestAPIVersion != want {
		t.Errorf("rest_api_version on the wire: got %q, want %q "+
			"(Bug 222: python-linstor's _require_version() gates "+
			"1.24-1.27 CLI flags on this string)", got.RestAPIVersion, want)
	}

	// Same expectation against the Go-side constant — the wire is
	// derived from it, but a future refactor that splits the two
	// (e.g. computing the wire string from a runtime version) must
	// still preserve the contract.
	if version.RestAPIVersion != want {
		t.Errorf("version.RestAPIVersion const: got %q, want %q",
			version.RestAPIVersion, want)
	}
}
