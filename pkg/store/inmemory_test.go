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
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/storetest"
)

// The in-memory NodeStore must satisfy the shared suite of branches
// (created in pkg/store/storetest). This is the only place we call into
// it for in-memory; pkg/store/k8s reuses the same suite against envtest.
func TestInMemoryNodeStore(t *testing.T) {
	storetest.RunNodeStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryStoragePoolStore(t *testing.T) {
	storetest.RunStoragePoolStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryResourceGroupStore(t *testing.T) {
	storetest.RunResourceGroupStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryResourceDefinitionStore(t *testing.T) {
	storetest.RunResourceDefinitionStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryResourceStore(t *testing.T) {
	storetest.RunResourceStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryVolumeDefinitionStore(t *testing.T) {
	storetest.RunVolumeDefinitionStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemoryKeyValueStore(t *testing.T) {
	storetest.RunKeyValueStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

func TestInMemorySnapshotStore(t *testing.T) {
	storetest.RunSnapshotStore(t, func(t *testing.T) store.Store {
		t.Helper()

		return store.NewInMemory()
	})
}

// TestInMemoryNodeStoreConcurrentAccess: in-memory specific (CRD store has
// optimistic-concurrency semantics; this guarantee belongs only to the
// RAM map).
func TestInMemoryNodeStoreConcurrentAccess(t *testing.T) {
	s := store.NewInMemory().Nodes()
	ctx := t.Context()

	const writers = 8

	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			n := apiv1.Node{Name: nameFor(i)}
			_ = s.Create(ctx, &n)
			_, _ = s.Get(ctx, n.Name)
			_, _ = s.List(ctx)
		}()
	}
	wg.Wait()

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("final List: %v", err)
	}

	if len(got) != writers {
		t.Errorf("final List len: got %d, want %d", len(got), writers)
	}
}

func nameFor(i int) string {
	return string(rune('a' + i))
}
