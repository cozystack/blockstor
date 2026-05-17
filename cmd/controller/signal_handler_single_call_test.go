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

package main

import (
	"os"
	"strings"
	"testing"
)

// TestSetupSignalHandlerCalledOnce extends the Bug 219 source-text
// guard (originally landed in cmd/apiserver/) to cmd/controller. The
// underlying defect class — a second `ctrl.SetupSignalHandler()` call
// panics with "close of closed channel" because the signal channel is
// closed on first invocation — applies identically here: cmd/controller
// also calls SetupSignalHandler exactly once and reuses the returned
// context for the manager Start and any fan-out goroutines.
//
// This sibling guard is the Bug 234 preventive-hardening item: the
// apiserver-only guard left cmd/controller exposed to a copy-paste
// regression. The Bug 213 commit pattern (wiring a cache-sync
// goroutine alongside mgr.Start) could land here next; without a
// sibling guard the controller would crashloop silently and every
// reconciler-side fix shipped since the Phase 11 split would be
// invalidated.
//
// Same shape as the apiserver sibling: read main.go, count literal
// call sites, fail if not exactly 1. Wrappers that themselves call
// SetupSignalHandler will not be caught here and must be paired with
// a unit test exercising the contract directly.
func TestSetupSignalHandlerCalledOnce(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}

	const needle = "ctrl.SetupSignalHandler()"

	count := strings.Count(string(body), needle)
	if count != 1 {
		t.Fatalf("Bug 234: ctrl.SetupSignalHandler() appears %d times in cmd/controller/main.go, want exactly 1. "+
			"Each call closes the signal channel; the second one panics with 'close of closed channel' "+
			"and crashloops the controller. Capture the returned context once and reuse it across all "+
			"goroutine fan-out and mgr.Start.", count)
	}
}
