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

package zfs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// pool is a small model of a ZFS pool: enough of `zfs list`, `zfs get`,
// `zfs set`, `zfs rename`, `zfs destroy` and `zfs clone` for the delete path
// to run against, with ZFS's own refusals and its property inheritance: a
// snapshot reads its zvol's marker unless `-s local` is asked. A recursive
// destroy fails the test outright: the provider must never issue one.
type pool struct {
	t *testing.T

	mu       sync.Mutex
	datasets map[string]*dataset
	calls    []string
	failList bool

	// exported makes the pool itself answer "does not exist", as an
	// unimported pool does.
	exported bool

	// failRename makes the next `zfs rename` fail, as a busy zvol does.
	failRename bool

	// afterSet runs once, right after the next `zfs set`, to model a
	// command another goroutine ran in between.
	afterSet func()
}

type dataset struct {
	origin string
	marked bool
}

var (
	errNoDataset     = errors.New("cannot open: dataset does not exist")
	errUnexpected    = errors.New("unexpected command")
	errSuspended     = errors.New("zfs list: pool I/O is currently suspended")
	errRecursive     = errors.New("recursive destroy refused by the test")
	errHasChildren   = errors.New("cannot destroy: volume has children")
	errHasDependents = errors.New("cannot destroy: snapshot has dependent clones")
	errBusy          = errors.New("cannot rename: dataset is busy")
	errExists        = errors.New("cannot rename to target: dataset already exists")
)

func newPool(t *testing.T, names ...string) *pool {
	t.Helper()

	p := &pool{t: t, datasets: map[string]*dataset{}}
	for _, name := range names {
		p.datasets[name] = &dataset{}
	}

	return p
}

func (p *pool) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	line := name + " " + strings.Join(args, " ")
	p.calls = append(p.calls, line)

	if name != "zfs" || len(args) == 0 {
		return nil, fmt.Errorf("%w %q", errUnexpected, line)
	}

	switch args[0] {
	case "list":
		return p.list(args[1:])
	case "get":
		return p.get(args[1:])
	case "set":
		return nil, p.set(args[1:])
	case "rename":
		return nil, p.rename(args[1], args[2])
	case "destroy":
		return nil, p.destroy(args[1:])
	}

	return nil, fmt.Errorf("%w %q", errUnexpected, line)
}

func (p *pool) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	return p.Run(ctx, name, args...)
}

// clone records target as a clone of the snapshot origin.
func (p *pool) clone(origin, target string) {
	p.datasets[target] = &dataset{origin: origin}
}

func (p *pool) mark(name string) { p.datasets[name].marked = true }

func (p *pool) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]string, 0, len(p.datasets))
	for name := range p.datasets {
		out = append(out, name)
	}

	sort.Strings(out)

	return out
}

func (p *pool) has(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.datasets[name]

	return ok
}

func (p *pool) markedFor(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	ds, ok := p.datasets[name]

	return ok && ds.marked
}

func (p *pool) snapshotsOf(name string) []string {
	var out []string

	for other := range p.datasets {
		if strings.HasPrefix(other, name+"@") {
			out = append(out, other)
		}
	}

	sort.Strings(out)

	return out
}

func (p *pool) clonesOf(name string) []string {
	var out []string

	for other, ds := range p.datasets {
		if ds.origin == name {
			out = append(out, other)
		}
	}

	sort.Strings(out)

	return out
}

func (p *pool) list(args []string) ([]byte, error) {
	if p.failList {
		return nil, errSuspended
	}

	target := args[len(args)-1]

	switch {
	case slices.Contains(args, "snapshot"):
		if _, ok := p.datasets[target]; !ok {
			return nil, errNoDataset
		}

		return []byte(strings.Join(p.snapshotsOf(target), "\n")), nil
	case slices.Contains(args, "volume"):
		var out []string

		for other := range p.datasets {
			if strings.HasPrefix(other, target+"/") && !strings.Contains(other, "@") {
				out = append(out, other)
			}
		}

		sort.Strings(out)

		return []byte(strings.Join(out, "\n")), nil
	}

	if target == "tank" {
		if p.exported {
			return nil, errNoDataset
		}

		return []byte(target + "\n"), nil
	}

	if _, ok := p.datasets[target]; !ok {
		return nil, errNoDataset
	}

	return []byte(target + "\n"), nil
}

func (p *pool) get(args []string) ([]byte, error) {
	property, target := args[len(args)-2], args[len(args)-1]

	ds, ok := p.datasets[target]
	if !ok {
		return nil, errNoDataset
	}

	switch property {
	case "origin":
		if ds.origin == "" {
			return []byte("-\n"), nil
		}

		return []byte(ds.origin + "\n"), nil
	case "clones":
		if clones := p.clonesOf(target); len(clones) > 0 {
			return []byte(strings.Join(clones, ",") + "\n"), nil
		}

		return []byte("-\n"), nil
	case "linstor:marked_for_deletion":
		if ds.marked {
			return []byte("true\n"), nil
		}

		if slices.Contains(args, "local") {
			// `-s local` leaves out a property that is only inherited.
			return nil, nil
		}

		if at := strings.LastIndex(target, "@"); at >= 0 {
			if base, ok := p.datasets[target[:at]]; ok && base.marked {
				return []byte("true\n"), nil
			}
		}

		return []byte("-\n"), nil
	}

	return nil, fmt.Errorf("%w: property %q", errUnexpected, property)
}

func (p *pool) set(args []string) error {
	if args[0] != "linstor:marked_for_deletion=true" {
		return fmt.Errorf("%w: set %v", errUnexpected, args)
	}

	ds, ok := p.datasets[args[1]]
	if !ok {
		return errNoDataset
	}

	ds.marked = true

	if hook := p.afterSet; hook != nil {
		p.afterSet = nil
		hook()
	}

	return nil
}

func (p *pool) rename(from, to string) error {
	ds, ok := p.datasets[from]
	if !ok {
		return errNoDataset
	}

	if p.failRename {
		p.failRename = false

		return errBusy
	}

	if _, taken := p.datasets[to]; taken {
		return errExists
	}

	moved := map[string]string{from: to}

	if !strings.Contains(from, "@") {
		for _, snap := range p.snapshotsOf(from) {
			moved[snap] = to + strings.TrimPrefix(snap, from)
		}
	}

	for old, renamed := range moved {
		p.datasets[renamed] = p.datasets[old]
		delete(p.datasets, old)
	}

	for _, other := range p.datasets {
		if renamed, ok := moved[other.origin]; ok {
			other.origin = renamed
		}
	}

	_ = ds

	return nil
}

func (p *pool) destroy(args []string) error {
	if len(args) != 1 {
		p.t.Errorf("zfs destroy with flags %v: the delete path must never recurse", args)

		return errRecursive
	}

	target := args[0]
	if _, ok := p.datasets[target]; !ok {
		return errNoDataset
	}

	if snaps := p.snapshotsOf(target); len(snaps) > 0 {
		return errHasChildren
	}

	if clones := p.clonesOf(target); len(clones) > 0 {
		return errHasDependents
	}

	delete(p.datasets, target)

	return nil
}

func (p *pool) destroyed() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out []string

	for _, line := range p.calls {
		if after, ok := strings.CutPrefix(line, "zfs destroy "); ok {
			out = append(out, after)
		}
	}

	return out
}

func deletedName(t *testing.T, p *pool, prefix string) string {
	t.Helper()

	for _, name := range p.names() {
		if strings.HasPrefix(name, prefix) && !strings.Contains(strings.TrimPrefix(name, prefix), "@") {
			return name
		}
	}

	t.Fatalf("no dataset named %s* in %v", prefix, p.names())

	return ""
}

func vol(rd string) storage.Volume { return storage.Volume{ResourceName: rd} }

func snap(rd, name string) storage.Snapshot {
	return storage.Snapshot{ResourceName: rd, SnapshotName: name}
}

// A zvol without snapshots is destroyed, plainly.
func TestDeleteVolumeWithoutSnapshotsDestroysIt(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool after delete: %v, want empty", got)
	}
}

// A zvol with snapshots keeps them: it is marked and renamed the way LINSTOR
// renames it, and nothing is destroyed. This is the case the issue lost data
// on.
func TestDeleteVolumeWithSnapshotsRenamesAndMarksIt(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@rollback", "tank/pvc-a_00000@daily")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.destroyed(); len(got) != 0 {
		t.Errorf("destroyed %v, want nothing", got)
	}

	renamed := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	if !p.markedFor(renamed) {
		t.Errorf("%s is not marked %s", renamed, "linstor:marked_for_deletion")
	}

	for _, s := range []string{"@rollback", "@daily"} {
		if !p.has(renamed + s) {
			t.Errorf("snapshot %s lost; pool: %v", s, p.names())
		}
	}

	if p.has("tank/pvc-a_00000") {
		t.Errorf("the zvol is still under its own name")
	}
}

// Deleting the last snapshot of a renamed zvol destroys the zvol too: its
// own delete was requested before.
func TestDeleteLastSnapshotOfDeletedVolumeCascades(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1", "tank/pvc-a_00000@s2")
	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	renamed := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	err = prov.DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("DeleteSnapshot s1: %v", err)
	}

	if !p.has(renamed) || !p.has(renamed+"@s2") {
		t.Fatalf("first snapshot delete took more than itself: %v", p.names())
	}

	err = prov.DeleteSnapshot(t.Context(), snap("pvc-a", "s2"))
	if err != nil {
		t.Fatalf("DeleteSnapshot s2: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool after the last snapshot went: %v, want empty", got)
	}
}

// Deleting the last snapshot of a live zvol leaves the zvol: nobody asked for
// it to go.
func TestDeleteSnapshotOfLiveVolumeKeepsVolume(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/pvc-a_00000"}) {
		t.Errorf("pool: %v, want only the live zvol", got)
	}
}

// A snapshot with clones is marked and renamed; the clone's delete then
// takes it, and the renamed zvol it hangs off once that is empty.
func TestSnapshotWithClonesWaitsForItsLastClone(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@base")
	p.clone("tank/pvc-a_00000@base", "tank/pvc-b_00000")

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteSnapshot(t.Context(), snap("pvc-a", "base"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	renamedSnap := deletedName(t, p, "tank/pvc-a_00000@_deleted_base_")

	if !p.markedFor(renamedSnap) {
		t.Errorf("%s not marked", renamedSnap)
	}

	err = prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume pvc-a: %v", err)
	}

	renamedVol := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	if got := p.destroyed(); len(got) != 0 {
		t.Fatalf("destroyed %v while the clone still needs them", got)
	}

	err = prov.DeleteVolume(t.Context(), vol("pvc-b"))
	if err != nil {
		t.Fatalf("DeleteVolume pvc-b: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool after the last clone went: %v, want empty (left %s)", got, renamedVol)
	}
}

// Deleting a clone whose origin snapshot nobody asked to delete leaves the
// snapshot and its zvol alone.
func TestDeleteCloneKeepsAnUnmarkedOrigin(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@base")
	p.clone("tank/pvc-a_00000@base", "tank/pvc-b_00000")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-b"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/pvc-a_00000", "tank/pvc-a_00000@base"}) {
		t.Errorf("pool: %v, want the source and its snapshot untouched", got)
	}
}

// A dataset LINSTOR left marked before the migration is finished by the same
// cascade: its last snapshot's delete takes the zvol.
func TestAdoptedLinstorLeftoverGoesWithItsLastSnapshot(t *testing.T) {
	t.Parallel()

	const left = "tank/_deleted_pvc-a_00000_2026-01-02T03-04-05-678"

	p := newPool(t, left, left+"@s1")
	p.mark(left)

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool: %v, want the LINSTOR leftover gone", got)
	}
}

// A renamed zvol whose marker is missing is not destroyed by the cascade:
// only a delete that was requested may finish.
func TestCascadeNeedsTheMarker(t *testing.T) {
	t.Parallel()

	const left = "tank/_deleted_pvc-a_00000_2026-01-02T03-04-05-678"

	p := newPool(t, left, left+"@s1")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{left}) {
		t.Errorf("pool: %v, want the unmarked zvol kept", got)
	}
}

// An older blockstor's deferred-delete snapshot counts as marked: the clone's
// delete finishes it.
func TestLegacyDeferredSnapshotGoesWithItsClone(t *testing.T) {
	t.Parallel()

	const marker = "tank/pvc-a_00000@base__DELETED__1700000000"

	p := newPool(t, "tank/pvc-a_00000", marker)
	p.clone(marker, "tank/pvc-b_00000")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-b"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/pvc-a_00000"}) {
		t.Errorf("pool: %v, want only the live source", got)
	}
}

// A marked snapshot without clones does not keep a zvol alive: the zvol's
// delete destroys it first and then the zvol.
func TestReleasedMarkedSnapshotDoesNotPinItsVolume(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@base__DELETED__1700000000")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool: %v, want empty", got)
	}
}

// A probe that fails is not "missing": the delete reports the error, so the
// satellite keeps its finalizer instead of reporting the storage gone.
func TestDeleteVolumeProbeFailureIsAnError(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000")
	p.failList = true

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-a"))
	if err == nil {
		t.Fatalf("DeleteVolume with a failing probe: got nil, want an error")
	}

	if !p.has("tank/pvc-a_00000") {
		t.Errorf("the zvol went on a failed probe")
	}
}

func TestDeleteSnapshotProbeFailureIsAnError(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")
	p.failList = true

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err == nil {
		t.Fatalf("DeleteSnapshot with a failing probe: got nil, want an error")
	}
}

// Missing stays a no-op on both paths.
func TestDeleteMissingIsNoop(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/other_00000")
	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	if err := prov.DeleteVolume(t.Context(), vol("pvc-a")); err != nil {
		t.Errorf("DeleteVolume of a missing zvol: %v", err)
	}

	if err := prov.DeleteSnapshot(t.Context(), snap("pvc-a", "s1")); err != nil {
		t.Errorf("DeleteSnapshot of a missing snapshot: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/other_00000"}) {
		t.Errorf("pool: %v", got)
	}
}

// Two renamed generations holding the same snapshot name cannot be told
// apart, so neither is touched.
func TestDeleteSnapshotRefusesAnAmbiguousMatch(t *testing.T) {
	t.Parallel()

	const (
		first  = "tank/_deleted_pvc-a_00000_2026-01-01T00-00-00-000"
		second = "tank/_deleted_pvc-a_00000_2026-02-01T00-00-00-000"
	)

	p := newPool(t, first, first+"@s1", second, second+"@s1")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err == nil {
		t.Fatalf("DeleteSnapshot: got nil, want an ambiguity error")
	}

	if got := p.destroyed(); len(got) != 0 {
		t.Errorf("destroyed %v on an ambiguous match", got)
	}
}

// A snapshot of a renamed zvol can still be restored from.
func TestRestoreFromSnapshotOfDeletedVolume(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")
	prov := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, cloningPool{p})

	err := prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	err = prov.RestoreVolumeFromSnapshot(t.Context(), vol("pvc-b"), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	renamed := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	p.mu.Lock()
	origin := p.datasets["tank/pvc-b_00000"].origin
	p.mu.Unlock()

	if origin != renamed+"@s1" {
		t.Errorf("clone origin %q, want %q", origin, renamed+"@s1")
	}
}

// cloningPool adds `zfs clone` to the model for the restore test.
type cloningPool struct{ *pool }

func (c cloningPool) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name == "zfs" && len(args) == 3 && args[0] == "clone" {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.calls = append(c.calls, "zfs "+strings.Join(args, " "))

		if _, ok := c.datasets[args[1]]; !ok {
			return nil, errNoDataset
		}

		c.datasets[args[2]] = &dataset{origin: args[1]}

		return nil, nil
	}

	return c.pool.Run(ctx, name, args...)
}

func (c cloningPool) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	return c.Run(ctx, name, args...)
}

// The recorded suffix names the renamed zvol, so its snapshots are found by
// name rather than by search.
func TestDeleteUsesTheRecordedSuffix(t *testing.T) {
	t.Parallel()

	const suffix = "2026-03-04T05-06-07-089"

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1",
		"tank/_deleted_pvc-a_00000_2026-01-01T00-00-00-000", "tank/_deleted_pvc-a_00000_2026-01-01T00-00-00-000@s1")
	p.mark("tank/_deleted_pvc-a_00000_2026-01-01T00-00-00-000")

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteVolumeWithSuffix(t.Context(), storage.Volume{ResourceName: "pvc-a"}, suffix)
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	renamed := "tank/_deleted_pvc-a_00000_" + suffix
	if !p.has(renamed + "@s1") {
		t.Fatalf("zvol not renamed with the recorded suffix: %v", p.names())
	}

	// Two renamed generations now hold an s1; the recorded suffix picks
	// the right one where a search would refuse.
	err = prov.DeleteSnapshot(t.Context(), storage.Snapshot{ResourceName: "pvc-a", SnapshotName: "s1", DeletedSuffix: suffix})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if p.has(renamed) || p.has(renamed+"@s1") {
		t.Errorf("the recorded generation is still there: %v", p.names())
	}

	if !p.has("tank/_deleted_pvc-a_00000_2026-01-01T00-00-00-000@s1") {
		t.Errorf("the other generation's snapshot went too: %v", p.names())
	}
}

// The recorded suffix stays on the Resource for the replica's life, so a
// volume deleted, created again and deleted again while snapshots keep the
// first generation finds its renamed name taken. It goes under a fresh one,
// and both generations keep their snapshots.
func TestDeleteVolumeAgainWithTheSameSuffixDoesNotCollide(t *testing.T) {
	t.Parallel()

	const suffix = "2026-03-04T05-06-07-089"

	first := "tank/_deleted_pvc-a_00000_" + suffix

	p := newPool(t, first, first+"@x", "tank/pvc-a_00000", "tank/pvc-a_00000@y")
	p.mark(first)

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteVolumeWithSuffix(t.Context(), storage.Volume{ResourceName: "pvc-a"}, suffix)
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if p.has("tank/pvc-a_00000") {
		t.Fatalf("the zvol is still under its own name: %v", p.names())
	}

	if !p.has(first + "@x") {
		t.Errorf("the first generation lost its snapshot: %v", p.names())
	}

	var second string

	for _, name := range p.names() {
		if strings.HasSuffix(name, "@y") {
			second = strings.TrimSuffix(name, "@y")
		}
	}

	if second == "" || second == first || !strings.HasPrefix(second, "tank/_deleted_pvc-a_00000_") || !p.markedFor(second) {
		t.Fatalf("second generation at %q, want a marked zvol of its own; pool: %v", second, p.names())
	}

	// Its snapshot carries the recorded suffix, which names the other
	// generation; it is still found and deleted, and that generation goes.
	err = prov.DeleteSnapshot(t.Context(), storage.Snapshot{ResourceName: "pvc-a", SnapshotName: "y", DeletedSuffix: suffix})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if p.has(second) || !p.has(first+"@x") {
		t.Errorf("pool after the snapshot delete: %v", p.names())
	}
}

// A recorded suffix whose rename never happened (a crash between recording
// it and the delete) still finds the snapshot under the live name.
func TestRecordedSuffixFallsBackToTheLiveName(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(),
		storage.Snapshot{ResourceName: "pvc-a", SnapshotName: "s1", DeletedSuffix: "2026-03-04T05-06-07-089"})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/pvc-a_00000"}) {
		t.Errorf("pool: %v", got)
	}
}

// A failed probe before a receive is not "absent": `zfs recv -F` over an
// existing dataset, and the cleanup after a failed receive, would destroy it.
func TestRecvSnapshotProbeFailureTouchesNothing(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000")
	p.failList = true

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).RecvSnapshot(t.Context(), vol("pvc-a"), strings.NewReader("stream"))
	if err == nil {
		t.Fatalf("RecvSnapshot with a failing probe: got nil, want an error")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, line := range p.calls {
		if strings.HasPrefix(line, "zfs recv") || strings.HasPrefix(line, "zfs destroy") {
			t.Errorf("ran %q over a dataset the probe could not see", line)
		}
	}
}

// A snapshot inherits its zvol's marker. Restoring from a snapshot of a
// renamed zvol and then deleting the restored replica must not read that
// inherited marker as the snapshot's own and destroy it: its Snapshot still
// exists and may be restored from again.
func TestCloneDeleteKeepsASnapshotThatOnlyInheritsTheMarker(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-x_00000", "tank/pvc-x_00000@s1", "tank/pvc-x_00000@s2")
	prov := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, cloningPool{p})

	err := prov.DeleteVolume(t.Context(), vol("pvc-x"))
	if err != nil {
		t.Fatalf("DeleteVolume pvc-x: %v", err)
	}

	renamed := deletedName(t, p, "tank/_deleted_pvc-x_00000_")

	err = prov.RestoreVolumeFromSnapshot(t.Context(), vol("pvc-y"), snap("pvc-x", "s1"))
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	err = prov.DeleteVolume(t.Context(), vol("pvc-y"))
	if err != nil {
		t.Fatalf("DeleteVolume pvc-y: %v", err)
	}

	for _, s := range []string{"@s1", "@s2"} {
		if !p.has(renamed + s) {
			t.Errorf("snapshot %s destroyed by the clone's delete; pool: %v", s, p.names())
		}
	}
}

// A delete that marked the zvol and then failed to rename it is retried. The
// retry must not read the snapshots' inherited marker as theirs and destroy
// them before renaming the zvol.
func TestRetriedDeleteAfterAFailedRenameKeepsTheSnapshots(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1", "tank/pvc-a_00000@s2")
	p.failRename = true

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err == nil {
		t.Fatalf("DeleteVolume with a failing rename: got nil, want an error")
	}

	err = prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("retried DeleteVolume: %v", err)
	}

	if got := p.destroyed(); len(got) != 0 {
		t.Errorf("destroyed %v, want nothing", got)
	}

	renamed := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	for _, s := range []string{"@s1", "@s2"} {
		if !p.has(renamed + s) {
			t.Errorf("snapshot %s lost; pool: %v", s, p.names())
		}
	}
}

// A snapshot delete that destroys the zvol's last snapshot between the
// replica delete's listing and its mark finds nothing marked to finish. The
// replica delete then finishes the renamed zvol itself, or it would stay on
// disk with nothing left to delete it.
func TestReplicaDeleteFinishesAZvolItsLastSnapshotLeftMeanwhile(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")
	p.afterSet = func() { delete(p.datasets, "tank/pvc-a_00000@s1") }

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if got := p.names(); len(got) != 0 {
		t.Errorf("pool: %v, want the renamed zvol finished", got)
	}
}

// A zvol under its live name that carries the marker (a delete stopped
// between the mark and the rename, and the replica was created over it
// again) is not destroyed when its last snapshot goes.
func TestCascadeLeavesALiveNamedZvol(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")
	p.mark("tank/pvc-a_00000")

	err := zfs.NewProvider(zfs.Config{Pool: "tank"}, p).DeleteSnapshot(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if got := p.names(); !slices.Equal(got, []string{"tank/pvc-a_00000"}) {
		t.Errorf("pool: %v, want the live zvol kept", got)
	}
}

// A pool that is not imported answers "does not exist" for every dataset in
// it. A delete must not read that as the volume being gone.
func TestDeleteOnAnUnimportedPoolIsAnError(t *testing.T) {
	t.Parallel()

	p := newPool(t)
	p.exported = true

	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	if err := prov.DeleteVolume(t.Context(), vol("pvc-a")); err == nil {
		t.Errorf("DeleteVolume on an unimported pool: got nil, want an error")
	}

	if err := prov.DeleteSnapshot(t.Context(), snap("pvc-a", "s1")); err == nil {
		t.Errorf("DeleteSnapshot on an unimported pool: got nil, want an error")
	}
}

// A snapshot whose replica was deleted on this node, and then created again,
// lives under the renamed zvol; a send ships it from there.
func TestSendFindsTheSnapshotOfARenamedZvol(t *testing.T) {
	t.Parallel()

	p := newPool(t, "tank/pvc-a_00000", "tank/pvc-a_00000@s1")
	prov := zfs.NewProvider(zfs.Config{Pool: "tank"}, p)

	err := prov.DeleteVolume(t.Context(), vol("pvc-a"))
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	p.mu.Lock()
	p.datasets["tank/pvc-a_00000"] = &dataset{}
	p.mu.Unlock()

	renamed := deletedName(t, p, "tank/_deleted_pvc-a_00000_")

	argv, err := prov.SendSnapshotArgs(t.Context(), snap("pvc-a", "s1"))
	if err != nil {
		t.Fatalf("SendSnapshotArgs: %v", err)
	}

	if want := []string{"send", "-p", renamed + "@s1"}; !slices.Equal(argv, want) {
		t.Errorf("argv %v, want %v", argv, want)
	}
}
