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

// TestSetupSignalHandlerNotUsed pins the cmd/satellite signal-handling
// contract: the satellite intentionally drives its lifecycle off
// stdlib `signal.Notify` (paired with a context that the agent loop
// awaits) rather than controller-runtime's `ctrl.SetupSignalHandler`.
// The latter is a process-wide one-shot — a second call closes the
// already-closed signal channel and panics; see the apiserver and
// controller sibling guards (Bug 219 / Bug 234).
//
// This guard is the Bug 234 zero-call companion: pin the absence so a
// future refactor that migrates the satellite onto
// ctrl.SetupSignalHandler does so deliberately. If a real need for
// the controller-runtime helper arises here, switch this test to the
// one-call shape used by cmd/apiserver and cmd/controller in the same
// patch — that way the migration carries its own regression guard
// forward.
func TestSetupSignalHandlerNotUsed(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}

	const needle = "ctrl.SetupSignalHandler()"

	count := strings.Count(string(body), needle)
	if count != 0 {
		t.Fatalf("Bug 234: ctrl.SetupSignalHandler() appears %d times in cmd/satellite/main.go, want exactly 0. "+
			"The satellite uses stdlib signal.Notify; introducing ctrl.SetupSignalHandler must be paired with "+
			"flipping this test to the one-call shape used by cmd/apiserver and cmd/controller so the same "+
			"close-of-closed-channel regression class is guarded here too.", count)
	}
}
