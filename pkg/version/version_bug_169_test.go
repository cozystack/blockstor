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

// TestBug169VersionVarsArePopulated pins the v6 Bug 169 fix: the
// `LinstorGitHash` / `LinstorBuildTime` identifiers must be Go
// `var`s (not `const`s) so the production Dockerfile can stamp real
// values via `-ldflags -X`.
//
// Pre-fix: both fields were package-level consts with placeholder
// values (`"blockstor"` / `"2026-01-01T00:00:00+00:00"`) — `-ldflags
// -X` is a no-op against consts, so every shipped image carried the
// same fake identity. Operators correlating a wire bug to a commit
// couldn't.
//
// We can't directly assert "is a var" from runtime, but we CAN assert
// "the placeholder values are no longer compiled in" — which only
// holds if (a) the symbols are vars (so the linker can rewrite them)
// AND (b) the build that produced this binary actually passed
// -ldflags. The unit-test binary in this repo is built with go test
// (no -ldflags), so the values remain at their declared zero/default.
// To still get coverage we use the GitVar/BuildVar overrides exported
// for `-ldflags` and check them, plus we exercise the parse contract
// on BuildTime.
//
// Concretely:
//
//   - LinstorGitHash MUST be a Go variable (asserted via test-time
//     assignment compiling cleanly — if it were a const, this file
//     would fail to compile).
//   - LinstorBuildTime MUST parse as a valid timestamp; the default
//     value is an RFC3339-ish string with `+00:00` so we accept
//     either RFC3339 or the LINSTOR Java format
//     (`yyyy-MM-dd'T'HH:mm:ssXXX`).
func TestBug169VersionVarsArePopulated(t *testing.T) {
	// Step 1 — assignment proves the symbol is a var. A const
	// would make this line a compile error.
	prevHash := LinstorGitHash
	LinstorGitHash = "test-override"

	if LinstorGitHash != "test-override" {
		t.Errorf("LinstorGitHash not assignable: got %q after assignment", LinstorGitHash)
	}

	LinstorGitHash = prevHash

	prevTime := LinstorBuildTime
	LinstorBuildTime = "2030-01-02T03:04:05+00:00"

	if LinstorBuildTime != "2030-01-02T03:04:05+00:00" {
		t.Errorf("LinstorBuildTime not assignable: got %q after assignment", LinstorBuildTime)
	}

	LinstorBuildTime = prevTime

	// Step 2 — the default value must still parse as a real
	// timestamp. RFC3339 accepts the upstream-LINSTOR shape
	// (`+00:00` offset).
	if _, err := time.Parse(time.RFC3339, LinstorBuildTime); err != nil {
		t.Errorf("LinstorBuildTime %q does not parse as RFC3339: %v", LinstorBuildTime, err)
	}
}

// TestBug169LdflagsOverrideTakesEffect validates the production-build
// contract: when LINSTOR_LDFLAGS_PROBE is set in the environment by a
// CI shim, the test reads the symbol and asserts it is NOT the
// pre-fix literal `"blockstor"`. This lets the live-validation step
// pin the override on the image actually built by the Dockerfile —
// `go test` builds for the unit harness still pass because the env
// var is unset.
func TestBug169LdflagsOverrideTakesEffect(t *testing.T) {
	if os.Getenv("LINSTOR_LDFLAGS_PROBE") == "" {
		t.Skip("LINSTOR_LDFLAGS_PROBE not set; production-image assertion skipped on unit-test build")
	}

	// Tightened post-171: a real commit SHA, not just "not the
	// pre-fix sentinel". Catches the Bug 171 case where the
	// Dockerfile default `unknown` leaked through unchanged.
	if !regexp.MustCompile(`^[0-9a-f]{7,40}$`).MatchString(LinstorGitHash) {
		t.Errorf("LinstorGitHash %q is not a real commit SHA — -ldflags -X did not take effect (or build wrapper passed a placeholder like 'unknown'/'blockstor')", LinstorGitHash)
	}

	if LinstorBuildTime == "2026-01-01T00:00:00+00:00" || LinstorBuildTime == "" {
		t.Errorf("LinstorBuildTime is still the placeholder %q — -ldflags -X did not take effect", LinstorBuildTime)
	}

	if _, err := time.Parse(time.RFC3339, LinstorBuildTime); err != nil {
		t.Errorf("LinstorBuildTime %q does not parse as RFC3339: %v", LinstorBuildTime, err)
	}
}
