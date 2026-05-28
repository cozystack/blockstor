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

package controllers

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// fakeDirEntry is a minimal os.DirEntry so orphanResForRemoval can be
// exercised table-driven without touching the filesystem.
type fakeDirEntry struct {
	name string
	dir  bool
}

func (f fakeDirEntry) Name() string      { return f.name }
func (f fakeDirEntry) IsDir() bool       { return f.dir }
func (f fakeDirEntry) Type() os.FileMode { return 0 }

// Info is never called by orphanResForRemoval (it only reads Name/IsDir);
// return a sentinel so the contract is satisfied without the nilnil lint.
func (f fakeDirEntry) Info() (os.FileInfo, error) { return nil, errUnusedDirEntryInfo }

var errUnusedDirEntryInfo = errors.New("fakeDirEntry.Info not implemented")

func entriesFromNames(names ...string) []os.DirEntry {
	out := make([]os.DirEntry, 0, len(names))
	for _, n := range names {
		out = append(out, fakeDirEntry{name: n})
	}

	return out
}

func ownedSet(names ...string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, n := range names {
		out[n] = struct{}{}
	}

	return out
}

// TestOrphanResForRemovalSelection is the core unit test the GC pivots
// on: given a directory listing, an owned-RD set and a grace window,
// which `.res` files must be removed. Pins every selection rule and the
// must-NOT-touch invariants (markers, global_common.conf, dirs, live
// resources).
func TestOrphanResForRemovalSelection(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	grace := 60 * time.Second

	tests := []struct {
		name    string
		entries []os.DirEntry
		owned   map[string]struct{}
		rdAges  map[string]time.Time
		want    []string
	}{
		{
			name:    "orphan res with no CRD and no grace anchor is removed",
			entries: entriesFromNames("pvc-stale.res"),
			owned:   ownedSet(),
			rdAges:  nil,
			want:    []string{"pvc-stale"},
		},
		{
			name:    "live resource res is kept",
			entries: entriesFromNames("pvc-live.res"),
			owned:   ownedSet("pvc-live"),
			rdAges:  nil,
			want:    nil,
		},
		{
			name:    "mixed: only the unowned res is removed",
			entries: entriesFromNames("pvc-live.res", "pvc-stale.res"),
			owned:   ownedSet("pvc-live"),
			rdAges:  nil,
			want:    []string{"pvc-stale"},
		},
		{
			name: "non-.res markers and global_common.conf are never touched",
			entries: entriesFromNames(
				"pvc-stale.owned",
				"pvc-stale.md-created",
				"pvc-stale.mkfs.done",
				"global_common.conf",
			),
			owned:  ownedSet(),
			rdAges: nil,
			want:   nil,
		},
		{
			name: "subdirectory ending in .res is ignored",
			entries: []os.DirEntry{
				fakeDirEntry{name: "weird.res", dir: true},
			},
			owned:  ownedSet(),
			rdAges: nil,
			want:   nil,
		},
		{
			name:    "orphan within grace window is deferred",
			entries: entriesFromNames("pvc-fresh.res"),
			owned:   ownedSet(),
			rdAges:  map[string]time.Time{"pvc-fresh": now.Add(-10 * time.Second)},
			want:    nil,
		},
		{
			name:    "orphan past grace window is removed",
			entries: entriesFromNames("pvc-old.res"),
			owned:   ownedSet(),
			rdAges:  map[string]time.Time{"pvc-old": now.Add(-2 * time.Minute)},
			want:    []string{"pvc-old"},
		},
		{
			name:    "empty basename .res is ignored",
			entries: entriesFromNames(".res"),
			owned:   ownedSet(),
			rdAges:  nil,
			want:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := orphanResForRemoval(tc.entries, tc.owned, tc.rdAges, now, grace)
			sort.Strings(got)
			sort.Strings(tc.want)

			if !slices.Equal(got, tc.want) {
				t.Errorf("orphanResForRemoval = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestOrphanResForRemovalGraceDisabled pins that a non-positive grace
// disables the window entirely (legacy behaviour): a fresh orphan is
// removed immediately.
func TestOrphanResForRemovalGraceDisabled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	got := orphanResForRemoval(
		entriesFromNames("pvc-fresh.res"),
		ownedSet(),
		map[string]time.Time{"pvc-fresh": now}, // anchor == now, inside any window
		now,
		0, // grace disabled
	)

	if !slices.Equal(got, []string{"pvc-fresh"}) {
		t.Errorf("grace<=0 should remove regardless of age; got %v", got)
	}
}

// TestSweepOrphanResFilesRemovesStaleFileNoKernel is the headline
// regression for the exit-10 collision bug: a stale `<rd>.res` left on
// disk (prior test / crash, no kernel slot) must be unlinked so a later
// `drbdadm adjust` for a new RD that reuses its device-minor no longer
// trips on the conflict.
func TestSweepOrphanResFilesRemovesStaleFileNoKernel(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	stalePath := filepath.Join(stateDir, "e2e-partition-debug.res")
	if err := os.WriteFile(stalePath, []byte("resource e2e-partition-debug {\n"+
		"  device minor 20001;\n}\n"), 0o600); err != nil {
		t.Fatalf("seed stale .res: %v", err)
	}

	// Empty kernel: the stale slot was never loaded, yet the file still
	// wedges adjust for an unrelated RD. sweepOnce must still GC it.
	sweeper, fx := sweeperFixture(t, "n1", "", nil)
	sweeper.StateDir = stateDir
	sweeper.isLoadedFn = func(_ context.Context, _ string) (bool, error) { return false, nil }

	if err := sweeper.sweepOnce(t.Context(), logr.Discard()); err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Errorf("stale .res not removed: stat err=%v", err)
	}

	// Kernel was empty → no teardown calls expected.
	for _, line := range fx.CommandLines() {
		if line == "drbdsetup down e2e-partition-debug" {
			t.Errorf("unexpected down call for never-loaded orphan: %v", fx.CommandLines())
		}
	}
}

// TestSweepOrphanResFilesDownsLoadedSlotThenRemoves pins the
// loaded-slot path: when the stale RD's kernel slot is still loaded
// (drbdadm adjust brought it up from the stale file), the GC must
// `drbdsetup down` it BEFORE unlinking so no minor is stranded.
func TestSweepOrphanResFilesDownsLoadedSlotThenRemoves(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	stalePath := filepath.Join(stateDir, "pvc-leftover.res")
	if err := os.WriteFile(stalePath, []byte("resource pvc-leftover {}\n"), 0o600); err != nil {
		t.Fatalf("seed stale .res: %v", err)
	}

	// Empty kernel status so tearDownOrphans is a no-op; the GC's own
	// per-candidate IsLoaded hook reports the slot loaded so we exercise
	// the down-then-remove branch in isolation.
	sweeper, _ := sweeperFixture(t, "n1", "", nil)
	sweeper.StateDir = stateDir

	var downCalls []string

	sweeper.isLoadedFn = func(_ context.Context, _ string) (bool, error) { return true, nil }
	sweeper.setupDownFn = func(_ context.Context, resource string) error {
		downCalls = append(downCalls, resource)

		return nil
	}

	if err := sweeper.sweepOnce(t.Context(), logr.Discard()); err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if !slices.Equal(downCalls, []string{"pvc-leftover"}) {
		t.Errorf("expected one down for loaded orphan slot; got %v", downCalls)
	}

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Errorf("stale .res not removed after slot down: stat err=%v", err)
	}
}

// TestSweepOrphanResFilesKeepsFileWhenDownFails pins the safety
// invariant: if `drbdsetup down` on the loaded orphan slot fails, the
// `.res` MUST be kept so the next cycle can retry — removing it would
// strand the kernel slot with no config to recover from.
func TestSweepOrphanResFilesKeepsFileWhenDownFails(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	stalePath := filepath.Join(stateDir, "pvc-wedged.res")
	if err := os.WriteFile(stalePath, []byte("resource pvc-wedged {}\n"), 0o600); err != nil {
		t.Fatalf("seed stale .res: %v", err)
	}

	sweeper, _ := sweeperFixture(t, "n1", "", nil)
	sweeper.StateDir = stateDir
	sweeper.isLoadedFn = func(_ context.Context, _ string) (bool, error) { return true, nil }
	sweeper.setupDownFn = func(_ context.Context, _ string) error {
		return context.DeadlineExceeded
	}

	if err := sweeper.sweepOnce(t.Context(), logr.Discard()); err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if _, err := os.Stat(stalePath); err != nil {
		t.Errorf("stale .res removed despite failed down (would strand kernel slot): %v", err)
	}
}

// TestSweepOrphanResFilesKeepsLiveResource pins that a `.res` for an RD
// this node still owns is never removed by the GC.
func TestSweepOrphanResFilesKeepsLiveResource(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	livePath := filepath.Join(stateDir, "pvc-live.res")
	if err := os.WriteFile(livePath, []byte("resource pvc-live {}\n"), 0o600); err != nil {
		t.Fatalf("seed live .res: %v", err)
	}

	// A Resource CRD on this node names pvc-live → it is owned.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-live.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n1",
			ResourceDefinitionName: "pvc-live",
		},
	}

	sweeper, _ := sweeperFixture(t, "n1", "", []*blockstoriov1alpha1.Resource{res})
	sweeper.StateDir = stateDir
	sweeper.isLoadedFn = func(_ context.Context, _ string) (bool, error) { return false, nil }

	if err := sweeper.sweepOnce(t.Context(), logr.Discard()); err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if _, err := os.Stat(livePath); err != nil {
		t.Errorf("live resource .res was removed: %v", err)
	}
}

// TestSweepOrphanResFilesDefersWithinGrace pins the create/delete-race
// guard: an orphan whose RD was just created (inside the grace window)
// is left in place; only past-grace orphans are GC'd.
func TestSweepOrphanResFilesDefersWithinGrace(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	freshPath := filepath.Join(stateDir, "pvc-fresh.res")
	if err := os.WriteFile(freshPath, []byte("resource pvc-fresh {}\n"), 0o600); err != nil {
		t.Fatalf("seed fresh .res: %v", err)
	}

	// An RD exists (just created) but no per-node Resource CRD yet — the
	// Bug 291 create-fanout window. The .res must survive.
	sweeper, _ := sweeperFixture(t, "n1", "", nil)
	sweeper.StateDir = stateDir
	sweeper.isLoadedFn = func(_ context.Context, _ string) (bool, error) { return false, nil }
	sweeper.RDGrace = 60 * time.Second

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-fresh",
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
	}
	if err := sweeper.Client.Create(t.Context(), rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := sweeper.sweepOnce(t.Context(), logr.Discard()); err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if _, err := os.Stat(freshPath); err != nil {
		t.Errorf("fresh-RD .res was GC'd inside grace window: %v", err)
	}
}
