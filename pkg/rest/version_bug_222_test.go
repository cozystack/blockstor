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
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/version"
)

// TestRestAPIVersion pins the advertised `rest_api_version`. Bug 222:
// it was 1.23.0 while python-linstor's `_require_version()` gates CLI
// flags client-side on it, so blockstor looked like it was missing
// features it served. linstor-csi from v1.13 refuses to start below
// 1.29 in the 1.x series.
//
// Pinned at the wire layer: a future refactor of pkg/version must not
// change the constant without updating the compatibility matrix.
func TestRestAPIVersion(t *testing.T) {
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

	const want = "1.29.0"
	if got.RestAPIVersion != want {
		t.Errorf("rest_api_version on the wire: got %q, want %q "+
			"(python-linstor's _require_version() gates CLI flags on this string)",
			got.RestAPIVersion, want)
	}

	// linstor-csi v1.13's EnsureMinimumApiVersion, verbatim in substance.
	var major, minor int
	if _, err := fmt.Sscanf(got.RestAPIVersion, "%d.%d", &major, &minor); err != nil {
		t.Fatalf("linstor-csi cannot parse %q: %v", got.RestAPIVersion, err)
	}

	if major != 1 || minor < 29 {
		t.Errorf("linstor-csi v1.13 refuses REST API %s: it needs 1.29 or newer in the 1.x series",
			got.RestAPIVersion)
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
