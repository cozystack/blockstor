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

// TestSetupSignalHandlerCalledOnce pins Bug 219: a second
// `ctrl.SetupSignalHandler()` call panics with "close of closed
// channel" because the underlying signal channel is closed on first
// invocation. The Bug 213 commit accidentally introduced a second
// call when wiring the cache-sync goroutine alongside the existing
// mgr.Start call site. Result: every apiserver replica
// CrashLoopBackOffs on first boot, silently invalidating the
// readyz gate (and every other apiserver-side fix shipped since
// the Phase 11 split).
//
// This source-text guard mirrors the Bug 211 anti-regression
// pattern: count the literal call sites in main.go and trip the
// test if more than one ever lands. The pattern matches the bare
// function call; if a future refactor introduces a wrapper that
// itself calls SetupSignalHandler, this guard will not catch it,
// but the wrapper must be paired with a unit test that exercises
// the contract directly.
func TestSetupSignalHandlerCalledOnce(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}

	const needle = "ctrl.SetupSignalHandler()"

	count := strings.Count(string(body), needle)
	if count != 1 {
		t.Fatalf("Bug 219: ctrl.SetupSignalHandler() appears %d times in cmd/apiserver/main.go, want exactly 1. "+
			"Each call closes the signal channel; the second one panics with 'close of closed channel' "+
			"and crashloops the apiserver. Capture the returned context once and reuse it across all "+
			"goroutine fan-out and mgr.Start.", count)
	}
}
