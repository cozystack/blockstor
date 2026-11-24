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
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// `s resource restore` into a name with a diskless replica and no
// definition over it is refused before anything is written, the way the
// REST door refuses it, and the replica stays.
func TestSnapshotRestoreRefusesATargetWithOrphanReplicas(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-orph", NodeName: "node-2", Flags: []string{apiv1.ResourceFlagDiskless},
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-orph")); got == 0 {
		t.Fatal("restore over an orphan diskless replica succeeded")
	}

	st := appStore(t, app)

	if _, err := st.Resources().Get(t.Context(), "pvc-orph", "node-2"); err != nil {
		t.Fatalf("the refusal took the orphan: %v", err)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-orph"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refusal left a definition behind: err=%v", err)
	}
}

// A restore into a name whose earlier definition is still draining a replica
// on a node that is down is refused as the plain create is: the definition is
// still being deleted, and the refusal names the node. Read as an existing
// target instead, it answered that the definition was not found.
func TestSnapshotRestoreIntoANameStillBeingDeletedSaysSo(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-drain", NodeName: "node-down", Flags: []string{apiv1.ResourceFlagDelete},
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-drain")); got == 0 {
		t.Fatal("restore over a draining replica succeeded")
	}

	msg := errBuf.String()
	if !strings.Contains(msg, "still being deleted") || !strings.Contains(msg, "node-down") {
		t.Fatalf("refusal = %q, want it to say the definition is still being deleted on node-down", msg)
	}

	if strings.Contains(msg, "not found") {
		t.Fatalf("refusal = %q, answered as a missing definition", msg)
	}
}
