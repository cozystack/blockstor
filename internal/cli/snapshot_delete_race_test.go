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
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A delete of a snapshot that is not there is the no-op it always was, even
// with a definition whose marker names it and whose restore can never finish.
func TestSnapshotDeleteOfAMissingSnapshotIgnoresAHangingMarker(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-hang", Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-gone"},
		})
	})

	if got := app.Run(t.Context(), []string{"s", "d", "pvc-x", "snap-gone"}); got != 0 {
		t.Fatalf("delete of a missing snapshot exit = %d (stderr: %s)", got, errBuf.String())
	}
}

// The CLI restore reads its snapshot back once the marked definition stands,
// the way the REST door does, and withdraws from one a delete has marked: the
// delete listed dependents before the definition existed and will not see it.
func TestSnapshotRestoreWithdrawsFromASnapshotBeingDeleted(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)

		snap, _ := backend.Snapshots().Get(ctx, "pvc-x", "snap-1")
		snap.Props = map[string]string{store.SnapshotDeletingProp: "tok@" + time.Now().UTC().Format(time.RFC3339)}
		_ = backend.Snapshots().Update(ctx, &snap)
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-wd")); got == 0 {
		t.Fatal("restore from a snapshot being deleted succeeded")
	}

	if !strings.Contains(errBuf.String(), "retry once it finishes") {
		t.Errorf("refusal %q does not say to retry once the delete finishes", errBuf.String())
	}

	if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-wd"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the withdrawn restore left its definition behind: %v", err)
	}
}

// A prepared target the restore marked reads the snapshot back before it
// places anything.
func TestSnapshotRestoreIntoAPreparedTargetWithdrawsFromASnapshotBeingDeleted(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	for _, argv := range [][]string{
		{"rd", "c", "pvc-prep-wd"},
		{"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-prep-wd"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v exit = %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	st := appStore(t, app)

	snap, err := st.Snapshots().Get(t.Context(), "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	snap.Props = map[string]string{store.SnapshotDeletingProp: "tok@" + time.Now().UTC().Format(time.RFC3339)}
	if err := st.Snapshots().Update(t.Context(), &snap); err != nil {
		t.Fatalf("mark the snapshot: %v", err)
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-prep-wd")); got == 0 {
		t.Fatal("restore into a prepared target from a snapshot being deleted succeeded")
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), "pvc-prep-wd")
	if err != nil || len(replicas) != 0 {
		t.Errorf("replicas placed after the refusal = %d (%v), want none", len(replicas), err)
	}
}
