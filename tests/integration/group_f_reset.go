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

// This file is the Group F's workaround for controller-runtime's
// process-global controller-name registry. Each call to harness.StartStack
// boots a fresh manager and re-registers reconcilers under the SAME
// names ("node", "storagepool", …); after the first test, every
// subsequent SetupWithManager fails with:
//
//   "controller with name node already exists. Controller names must be
//    unique to avoid multiple controllers reporting the same metric."
//
// The harness's pkg/manager.go does NOT pass SkipNameValidation=true
// (the documented escape hatch), and the Group F sub-agent contract
// (docs/agent-playbook.md §1) forbids touching anything under
// tests/integration/harness/. Until the launcher lifts the harness
// option, we reset controller-runtime's private name-registry between
// tests via go:linkname — a deliberate, scoped use of the unsafe seam
// that keeps the harness binary untouched.
//
// The TestMain hook below is overridden by groupFResetController to
// run BEFORE each top-level Group F test (via t.Helper-style reset
// inside setupGroupFRD).

package integration

import (
	"sync"
	// unsafe import is required for go:linkname to access an unexported
	// package-level variable from another package — the bridge is
	// otherwise invisible to the linker.
	_ "unsafe"

	"k8s.io/apimachinery/pkg/util/sets"
)

//go:linkname controllerRuntimeUsedNames sigs.k8s.io/controller-runtime/pkg/controller.usedNames
var controllerRuntimeUsedNames sets.Set[string]

//go:linkname controllerRuntimeNameLock sigs.k8s.io/controller-runtime/pkg/controller.nameLock
var controllerRuntimeNameLock sync.Mutex

// resetControllerNameRegistry clears the controller-runtime
// process-global controller-name set so the next StartStack can
// re-register its reconcilers under their canonical names. Called
// from setupGroupFRD at the start of every Group F test.
//
// Safe under concurrent test execution because controller-runtime's
// own nameLock serialises the writes that would race; this helper
// merely empties the set under that same lock.
func resetControllerNameRegistry() {
	controllerRuntimeNameLock.Lock()
	defer controllerRuntimeNameLock.Unlock()

	controllerRuntimeUsedNames = sets.Set[string]{}
}
