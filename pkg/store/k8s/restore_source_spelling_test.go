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

package k8s_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The uncached snapshot read is scoped by the definition name in the
// snapshot's spec, which the API server compares verbatim. A restore that
// names its source in another case than the definition was registered in
// still finds the snapshot: the read asks under the stored spelling. Asked
// as given, it would answer "deleted" and withdraw every such restore.
func TestRestoreSourceIsFoundUnderAnotherSpellingOfItsDefinition(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	st := k8s.NewWithAPIReader(fixture.client, fixture.client)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "Pvc-Src"}); err != nil {
		t.Fatalf("create definition: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap", ResourceName: "Pvc-Src"}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if err := store.RestoreSourceWithdrawn(ctx, st, "PVC-SRC", "snap"); err != nil {
		t.Fatalf("a restore from PVC-SRC/snap is withdrawn: %v", err)
	}
}
