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

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func seedVolumeWithReplica(inUse bool) func(context.Context, store.Store) {
	return func(ctx context.Context, backend store.Store) {
		seedVolumeDefinition(ctx, backend)
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-1",
			State: apiv1.ResourceState{InUse: &inUse},
		})
	}
}

// With a replica, `vd d` only flags the definition: the replica's satellite
// deletes its volume, and the definition stays until it has confirmed. The
// CLI says what it is waiting for.
func TestVolumeDefinitionDeleteFlagsWhileAReplicaHoldsIt(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedVolumeWithReplica(false))

	if got := app.Run(t.Context(), []string{"vd", "d", "pvc-x", "0"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	vd, err := appStore(t, app).VolumeDefinitions().Get(t.Context(), "pvc-x", 0)
	if err != nil {
		t.Fatalf("volume definition removed before its replica confirmed: %v", err)
	}

	if !vd.Deleting() || vd.DeleteID == "" {
		t.Errorf("volume definition = %+v, want flagged DELETE with a deleteID", vd)
	}

	if !strings.Contains(errBuf.String(), "node-1") {
		t.Errorf("stderr %q does not name the node the delete waits for", errBuf.String())
	}
}

// A replica in use refuses the delete, and nothing is flagged.
func TestVolumeDefinitionDeleteRefusesInUse(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedVolumeWithReplica(true))

	if got := app.Run(t.Context(), []string{"vd", "d", "pvc-x", "0"}); got == 0 {
		t.Fatal("delete of a definition in use succeeded")
	}

	if !strings.Contains(errBuf.String(), "in use") {
		t.Errorf("stderr %q does not say the definition is in use", errBuf.String())
	}

	vd, err := appStore(t, app).VolumeDefinitions().Get(t.Context(), "pvc-x", 0)
	if err != nil || vd.Deleting() {
		t.Errorf("after a refused delete: %+v %v, want the definition unflagged", vd, err)
	}
}
