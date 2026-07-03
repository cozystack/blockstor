// SPDX-License-Identifier: Apache-2.0

//go:build integration

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

package harness

import (
	"testing"
	"time"
)

// TestScaledTimeoutStretchesOnCI pins the CI budget stretch: the
// per-group convergence constants are tuned for dev machines, and the
// Integration lane rotate-flaked on GitHub runners until Eventually
// budgets were scaled there (GroupI / GroupJ 30s timeouts). The scale
// is ×5 (30s → 150s): ×3 (90s) still rotate-flaked the heaviest
// autoplace-convergence cases (GroupFR / GroupJ) under full-suite
// contention while they complete in ~8s locally.
func TestScaledTimeoutStretchesOnCI(t *testing.T) {
	t.Setenv("CI", "true")

	if got := scaledTimeout(30 * time.Second); got != 150*time.Second {
		t.Fatalf("scaledTimeout on CI: got %s, want 150s", got)
	}

	t.Setenv("CI", "")

	if got := scaledTimeout(30 * time.Second); got != 30*time.Second {
		t.Fatalf("scaledTimeout off CI: got %s, want 30s", got)
	}
}
