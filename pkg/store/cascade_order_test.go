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

package store_test

import (
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/storetest"
)

// The cascade of a volume still in its initial sync deletes the
// Inconsistent replica before the UpToDate copies it depends on, so the
// source of the sync is never the first replica to go.
func TestCascadeDeletesIncompleteReplicasFirst(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-sync"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for node, disk := range map[string]string{
		"node-a": "UpToDate",
		"node-b": "UpToDate",
		"node-c": "Inconsistent",
	} {
		if err := backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-sync", NodeName: node,
			Volumes: []apiv1.Volume{{State: apiv1.VolumeState{DiskState: disk}}},
		}); err != nil {
			t.Fatalf("seed %s: %v", node, err)
		}
	}

	rec := storetest.NewDeleteRecorder(backend)

	if err := store.CascadeDeleteResources(ctx, rec, "pvc-sync"); err != nil {
		t.Fatalf("cascade: %v", err)
	}

	deletes := rec.Deletes()
	if len(deletes) != 3 || deletes[0].Node != "node-c" {
		t.Errorf("deletes = %+v, want the Inconsistent node-c first", deletes)
	}
}

// MarkDefinitionDeleting flags the definition once, and is a no-op on a
// definition that is already gone.
func TestMarkDefinitionDeleting(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-going"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for range 2 {
		if err := store.MarkDefinitionDeleting(ctx, backend, "pvc-going"); err != nil {
			t.Fatalf("mark: %v", err)
		}
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-going")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if n := len(slices.DeleteFunc(slices.Clone(rd.Flags), func(f string) bool {
		return f != apiv1.ResourceFlagDelete
	})); n != 1 {
		t.Errorf("flags = %v, want DELETE exactly once", rd.Flags)
	}

	if err := store.MarkDefinitionDeleting(ctx, backend, "pvc-gone"); err != nil {
		t.Errorf("mark a missing definition: %v", err)
	}
}
