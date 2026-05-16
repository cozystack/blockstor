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
	"testing"
	"time"
)

// These tests close the plumbing gap that 169's fix left open: the
// Dockerfile declared `-ldflags -X` for `LinstorGitHash` /
// `LinstorBuildTime`, but
//
//  1. `Dockerfile` defaulted `ARG GIT_HASH=unknown`,
//  2. `.dockerignore` excluded `.git`, so the in-container
//     `git rev-parse HEAD` fallback never had a repo to read,
//  3. no build wrapper (`stand/build-images.sh`, top-level Makefile)
//     passed `--build-arg GIT_HASH=$(git rev-parse HEAD)`.
//
// Result: every deployed image still reported `git_hash: "unknown"`.
// The 169 unit test passed because it only rejected the literal
// `"blockstor"` placeholder — the new `"unknown"` value snuck past.
//
// These tests pin the END-TO-END contract: when the production image
// runs, /v1/controller/version MUST report a real commit SHA + a
// recent RFC3339 build time.
//
// The unit-test binary is built with plain `go test` (no -ldflags),
// so the symbols carry their declared defaults and the production
// assertions would fail. We gate the strict checks on
// LINSTOR_LDFLAGS_PROBE — the live-validation step sets it inside the
// running container to force the assertions to fire there.

// TestBug171GitHashIsRealCommitSHA asserts the production-build
// invariant: LinstorGitHash MUST match a real hex commit SHA (7–40
// hex chars). Rejects:
//   - the original placeholder `"blockstor"` (Bug 169 pre-fix),
//   - the new placeholder `"unknown"` (Bug 171: Dockerfile default),
//   - the empty string,
//   - any other non-SHA token.
func TestBug171GitHashIsRealCommitSHA(t *testing.T) {
	if os.Getenv("LINSTOR_LDFLAGS_PROBE") == "" {
		t.Skip("LINSTOR_LDFLAGS_PROBE not set; production-image assertion skipped on unit-test build")
	}

	re := regexp.MustCompile(`^[0-9a-f]{7,40}$`)
	if !re.MatchString(LinstorGitHash) {
		t.Errorf("LinstorGitHash = %q; expected a real commit SHA matching %q. "+
			"Either the build wrapper failed to pass --build-arg GIT_HASH=$(git rev-parse HEAD), "+
			"or the Dockerfile default leaked through.",
			LinstorGitHash, re.String())
	}
}

// TestBug171BuildTimeIsRecent asserts the production-build invariant:
// LinstorBuildTime MUST parse as RFC3339 AND be within the last year.
// The placeholder `"2026-01-01T00:00:00+00:00"` will eventually drift
// out of "recent" naturally, but rejecting it explicitly catches it
// before time passes.
func TestBug171BuildTimeIsRecent(t *testing.T) {
	if os.Getenv("LINSTOR_LDFLAGS_PROBE") == "" {
		t.Skip("LINSTOR_LDFLAGS_PROBE not set; production-image assertion skipped on unit-test build")
	}

	if LinstorBuildTime == "2026-01-01T00:00:00+00:00" {
		t.Fatalf("LinstorBuildTime is still the dev placeholder %q — -ldflags -X did not stamp it",
			LinstorBuildTime)
	}

	parsed, err := time.Parse(time.RFC3339, LinstorBuildTime)
	if err != nil {
		t.Fatalf("LinstorBuildTime %q does not parse as RFC3339: %v", LinstorBuildTime, err)
	}

	now := time.Now()
	oneYearAgo := now.AddDate(-1, 0, 0)
	oneHourAhead := now.Add(1 * time.Hour) // small clock skew tolerance

	if parsed.Before(oneYearAgo) {
		t.Errorf("LinstorBuildTime %q is more than a year old; the image was not built recently or the stamp leaked the placeholder", LinstorBuildTime)
	}
	if parsed.After(oneHourAhead) {
		t.Errorf("LinstorBuildTime %q is in the future beyond clock-skew tolerance", LinstorBuildTime)
	}
}
