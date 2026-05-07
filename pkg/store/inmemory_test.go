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

package store

import (
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TDD: these tests pin the contract of the in-memory NodeStore. Every branch
// the interface admits has a test before any implementation lands.

// TestNodeStoreListEmpty: an empty store returns an empty slice (never nil),
// because golinstor and downstream consumers iterate length checks blindly.
func TestNodeStoreListEmpty(t *testing.T) {
	s := NewInMemory().Nodes()

	got, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if got == nil {
		t.Errorf("List returned nil, want empty slice")
	}

	if len(got) != 0 {
		t.Errorf("List length: got %d, want 0", len(got))
	}
}

// TestNodeStoreCreateThenGet: round-trip a Node and read it back unchanged.
func TestNodeStoreCreateThenGet(t *testing.T) {
	s := NewInMemory().Nodes()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(t.Context(), &n); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(t.Context(), "alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "alpha" {
		t.Errorf("Name: got %q, want %q", got.Name, "alpha")
	}

	if got.Type != apiv1.NodeTypeSatellite {
		t.Errorf("Type: got %q, want %q", got.Type, apiv1.NodeTypeSatellite)
	}
}

// TestNodeStoreCreateDuplicate: creating with a name that already exists
// returns ErrAlreadyExists. REST will translate this to HTTP 409.
func TestNodeStoreCreateDuplicate(t *testing.T) {
	s := NewInMemory().Nodes()
	ctx := t.Context()

	n := apiv1.Node{Name: "alpha", Type: apiv1.NodeTypeSatellite}
	if err := s.Create(ctx, &n); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, &n)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("second Create: got %v, want ErrAlreadyExists", err)
	}
}

// TestNodeStoreGetNotFound: missing key returns ErrNotFound (→ HTTP 404).
func TestNodeStoreGetNotFound(t *testing.T) {
	s := NewInMemory().Nodes()

	_, err := s.Get(t.Context(), "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

// TestNodeStoreUpdateNotFound: updating a non-existent node is an error
// (not an upsert) so callers don't accidentally create with stale data.
func TestNodeStoreUpdateNotFound(t *testing.T) {
	s := NewInMemory().Nodes()

	err := s.Update(t.Context(), &apiv1.Node{Name: "ghost"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Update missing: got %v, want ErrNotFound", err)
	}
}

// TestNodeStoreUpdateChangesProps: a successful update overwrites mutable
// fields and is reflected in subsequent Gets.
func TestNodeStoreUpdateChangesProps(t *testing.T) {
	s := NewInMemory().Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Props: map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["foo"] != "bar" {
		t.Errorf("Props[foo]: got %q, want %q", got.Props["foo"], "bar")
	}
}

// TestNodeStoreDeleteNotFound: deleting an absent node is an error so
// callers can't paper over typos.
func TestNodeStoreDeleteNotFound(t *testing.T) {
	s := NewInMemory().Nodes()

	err := s.Delete(t.Context(), "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

// TestNodeStoreDeleteRemoves: a deleted node is no longer returned by Get/List.
func TestNodeStoreDeleteRemoves(t *testing.T) {
	s := NewInMemory().Nodes()
	ctx := t.Context()

	if err := s.Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, "n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, "n1")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(list) != 0 {
		t.Errorf("List after Delete: got %d items, want 0", len(list))
	}
}

// TestNodeStoreListSorted: the list contract returns nodes in deterministic
// order so REST callers and tests don't have to sort. We pick name-asc.
func TestNodeStoreListSorted(t *testing.T) {
	s := NewInMemory().Nodes()
	ctx := t.Context()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := s.Create(ctx, &apiv1.Node{Name: name}); err != nil {
			t.Fatalf("Create %q: %v", name, err)
		}
	}

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	gotNames := make([]string, len(got))
	for i, n := range got {
		gotNames[i] = n.Name
	}

	want := []string{"alpha", "bravo", "charlie"}
	if !equalStrings(gotNames, want) {
		t.Errorf("List order: got %v, want %v", gotNames, want)
	}
}

// TestNodeStoreConcurrentAccess: parallel Create+List+Get must not race or
// deadlock. We don't assert exact contents, just that the store survives.
func TestNodeStoreConcurrentAccess(t *testing.T) {
	s := NewInMemory().Nodes()
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

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	tmp := append([]string(nil), a...)
	sort.Strings(tmp)
	sortedB := append([]string(nil), b...)
	sort.Strings(sortedB)

	for i := range tmp {
		if tmp[i] != sortedB[i] {
			return false
		}
	}

	return true
}

func nameFor(i int) string {
	return string(rune('a' + i))
}
