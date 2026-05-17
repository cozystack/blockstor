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

package version

import (
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// Bug 238 (P3) — OpenAPI spec version drift.
//
// `third_party/linstor-openapi/rest_v1_openapi.yaml` is vendored from
// upstream LINSTOR and carries `info.version: 1.28.0`. The blockstor
// wire-advertised `RestAPIVersion` is `1.27.0` (Bug 222 set it
// conservatively — see the doc comment on the const). The two are
// allowed to diverge: the served version is the LOWER of (vendored
// spec, supported endpoints). What we MUST guard against is silent
// drift — a future contributor bumping the served version without
// consulting the spec, or bumping the spec without checking which
// new endpoints we actually wire.
//
// This test pins one of two valid states:
//
//  1. Served version == vendored spec version. The simple case;
//     all endpoints in the spec are wired.
//  2. Served version < vendored spec version AND the const carries
//     an explicit Bug 238 comment naming the divergence. This is
//     the current state: spec=1.28.0, served=1.27.0, with a doc
//     comment on RestAPIVersion documenting the gap.
//
// Failing this test means EITHER the served version drifted past
// what we support (bug), OR the comment naming the divergence was
// removed during a refactor (we'd lose the breadcrumb for the next
// bumper).

// TestBug238OpenAPISpecVersionDriftDocumented walks the source of
// version.go AND the vendored OpenAPI spec, compares the two
// versions, and — if they differ — asserts the divergence is
// explicitly documented in the RestAPIVersion doc comment.
func TestBug238OpenAPISpecVersionDriftDocumented(t *testing.T) {
	specVersion, err := readVendoredOpenAPIVersion()
	if err != nil {
		t.Fatalf("read vendored OpenAPI spec version: %v", err)
	}

	if specVersion == "" {
		t.Fatalf("vendored OpenAPI spec has no info.version — fixture broken")
	}

	if specVersion == RestAPIVersion {
		// Aligned — nothing to document.
		return
	}

	// Divergent — the RestAPIVersion const MUST carry an explicit
	// "Bug 238" comment naming the OpenAPI spec version drift.
	source, err := readVersionSource()
	if err != nil {
		t.Fatalf("read pkg/version/version.go: %v", err)
	}

	if !strings.Contains(source, "Bug 238") {
		t.Errorf("RestAPIVersion=%q diverges from vendored OpenAPI %q but pkg/version/version.go has no `Bug 238` comment "+
			"documenting the drift; either align the two or annotate the divergence so a future bumper consults the spec",
			RestAPIVersion, specVersion)
	}

	if !strings.Contains(source, specVersion) {
		t.Errorf("pkg/version/version.go does not mention the vendored spec version %q; the Bug 238 comment must name "+
			"the actual spec version so the divergence is obvious", specVersion)
	}
}

// readVendoredOpenAPIVersion parses the `info.version` field out of
// the vendored OpenAPI spec. We do NOT pull in a YAML dependency for
// this — the spec is large and we only need one scalar. The match is
// anchored on the YAML indentation (`  version: <semver>` at column
// 2, directly under `info:`) so a `version:` inside a schema property
// doesn't false-match.
func readVendoredOpenAPIVersion() (string, error) {
	specPath, err := vendoredOpenAPIPath()
	if err != nil {
		return "", err
	}

	content, err := os.ReadFile(specPath)
	if err != nil {
		return "", err
	}

	// `info:` block — match `  version: <semver>` immediately after
	// the `info:` marker. The vendored spec has exactly one `info:`.
	re := regexp.MustCompile(`(?m)^info:\s*\n(?:^.*\n)*?^  version:\s*(\S+)\s*$`)

	match := re.FindStringSubmatch(string(content))
	if len(match) < 2 {
		return "", nil
	}

	return match[1], nil
}

// vendoredOpenAPIPath resolves the absolute path of the vendored
// OpenAPI spec without depending on the test binary's CWD shape. We
// walk up from the file containing this test until we find the
// blockstor repo root (the directory that holds `go.mod`).
func vendoredOpenAPIPath() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}

	dir := thisFile

	for {
		dir = stripLastSegment(dir)
		if dir == "" || dir == "/" {
			return "", os.ErrNotExist
		}

		_, err := os.Stat(dir + "/go.mod")
		if err == nil {
			return dir + "/third_party/linstor-openapi/rest_v1_openapi.yaml", nil
		}
	}
}

// readVersionSource reads pkg/version/version.go itself so the test
// can scan the doc comment on RestAPIVersion. We can't introspect Go
// doc comments at runtime; the source-text read is the cheapest path.
func readVersionSource() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}

	versionFile := stripLastSegment(thisFile) + "/version.go"

	content, err := os.ReadFile(versionFile)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

// stripLastSegment trims the last `/segment` off a POSIX-style path.
// Returns "" when the input has no slash. Used for walking parent
// directories without dragging in path/filepath (which would still
// be portable but adds an indirect Windows-isolation hop the test
// doesn't need on the Linux/Darwin CI we ship).
func stripLastSegment(p string) string {
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return ""
	}

	return p[:idx]
}
