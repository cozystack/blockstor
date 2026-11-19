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
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Issue 264: the orphan-storage sweeper destroyed zvols that were in
// the pool before blockstor, because nothing on a zvol said who made
// it. These tests pin the ownership marker the provider now writes and
// reads, and the reap that refuses an unmarked zvol or one holding
// snapshots.

const (
	ownerDS      = "tank/pvc-1_00000"
	ownerGet     = "zfs get -H -o value,source blockstor.org:owner " + ownerDS
	ownerSet     = "zfs set blockstor.org:owner=blockstor " + ownerDS
	ownerExists  = "zfs list -H -o name " + ownerDS
	ownerSnaps   = "zfs list -H -o name -t snapshot -d 1 " + ownerDS
	ownerDestroy = "zfs destroy " + ownerDS
)

// ownerVol is the volume ownerDS holds.
func ownerVol() storage.Volume {
	return storage.Volume{ResourceName: "pvc-1", VolumeNumber: 0, SizeKib: 1024 * 1024}
}

// ownerFixture returns a thin-mode provider (no refreservation calls to
// wire) whose zvol exists and reports the given marker line.
func ownerFixture(markerLine string) (*storage.FakeExec, *zfs.Provider) {
	fx := storage.NewFakeExec()
	fx.Expect(ownerExists, storage.FakeResponse{Stdout: []byte(ownerDS + "\n")})
	fx.Expect(ownerGet, storage.FakeResponse{Stdout: []byte(markerLine + "\n")})

	return fx, zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)
}

func TestOwnerMarkerBackfilledOnExistingZvol(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		marker  string
		wantSet bool
	}{
		{name: "unset", marker: "-\t-", wantSet: true},
		{name: "inherited from the pool", marker: "blockstor\tinherited from tank", wantSet: true},
		{name: "received in a stream", marker: "blockstor\treceived", wantSet: true},
		{name: "another value", marker: "linstor\tlocal", wantSet: true},
		{name: "set locally", marker: "blockstor\tlocal", wantSet: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx, p := ownerFixture(tc.marker)

			err := p.CreateVolume(t.Context(), ownerVol())
			if err != nil {
				t.Fatalf("CreateVolume: %v", err)
			}

			gotSet := slices.Contains(fx.CommandLines(), ownerSet)
			if gotSet != tc.wantSet {
				t.Errorf("marker %q: zfs set issued=%v, want %v; calls=%v",
					tc.marker, gotSet, tc.wantSet, fx.CommandLines())
			}

			for _, line := range fx.CommandLines() {
				if strings.HasPrefix(line, "zfs create") {
					t.Errorf("existing zvol must not be re-created: %q", line)
				}
			}
		})
	}
}

// A clone of an existing restore target and a re-run recv both land on
// the existing-dataset path, which is how a volume adopted from LINSTOR
// or created by an older blockstor acquires the marker.
func TestOwnerMarkerBackfilledOnRestoreAndRecvResume(t *testing.T) {
	t.Parallel()

	fx, p := ownerFixture("-\t-")

	err := p.RestoreVolumeFromSnapshot(t.Context(), ownerVol(), storage.Snapshot{ResourceName: "pvc-src", SnapshotName: "s"})
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	err = p.RecvSnapshot(t.Context(), ownerVol(), bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	sets := 0

	for _, line := range fx.CommandLines() {
		if line == ownerSet {
			sets++
		}
	}

	if sets != 2 {
		t.Errorf("want the marker set once per path, got %d; calls=%v", sets, fx.CommandLines())
	}
}

// A fresh recv carries the sender's marker as a received property at
// best; the provider sets it locally after the stream lands.
func TestOwnerMarkerSetAfterFreshRecv(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.ExpectSequence(ownerExists,
		storage.FakeResponse{}, // absent before the recv
	)
	fx.Expect(ownerGet, storage.FakeResponse{Stdout: []byte("blockstor\treceived\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.RecvSnapshot(t.Context(), ownerVol(), bytes.NewReader([]byte("stream")))
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	lines := fx.CommandLines()

	recv := slices.Index(lines, "zfs recv -F "+ownerDS)
	set := slices.Index(lines, ownerSet)

	if recv < 0 || set < recv {
		t.Errorf("want zfs recv then the local marker set; calls=%v", lines)
	}
}

func TestInspectVolumeOwnership(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		marker    string
		snaps     string
		wantOwned bool
		wantSnaps []string
	}{
		{name: "unmarked", marker: "-\t-", wantOwned: false},
		{name: "inherited", marker: "blockstor\tinherited from tank", wantOwned: false},
		{name: "received", marker: "blockstor\treceived", wantOwned: false},
		{name: "marked", marker: "blockstor\tlocal", wantOwned: true},
		{
			name:      "marked with snapshots",
			marker:    "blockstor\tlocal",
			snaps:     ownerDS + "@pre-migration\n" + ownerDS + "@snap-1__DELETED__1700000000\n" + ownerDS + "@snap-2\n",
			wantOwned: true,
			wantSnaps: []string{"pre-migration", "snap-2"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx, p := ownerFixture(tc.marker)
			fx.Expect(ownerSnaps, storage.FakeResponse{Stdout: []byte(tc.snaps)})

			got, err := p.InspectVolume(t.Context(), ownerVol())
			if err != nil {
				t.Fatalf("InspectVolume: %v", err)
			}

			if got.Owned != tc.wantOwned {
				t.Errorf("Owned=%v, want %v", got.Owned, tc.wantOwned)
			}

			if !slices.Equal(got.Snapshots, tc.wantSnaps) {
				t.Errorf("Snapshots=%v, want %v", got.Snapshots, tc.wantSnaps)
			}
		})
	}
}

// A probe that fails is an error, never "not owned" or "no snapshots"
// by default: the sweeper must not act on a guess.
func TestInspectVolumeProbeFailuresAreErrors(t *testing.T) {
	t.Parallel()

	t.Run("marker", func(t *testing.T) {
		t.Parallel()

		fx, p := ownerFixture("")
		fx.Expect(ownerGet, storage.FakeResponse{Err: errors.New("pool suspended")})

		_, err := p.InspectVolume(t.Context(), ownerVol())
		if err == nil {
			t.Fatal("want an error when the marker cannot be read")
		}
	})

	t.Run("snapshots", func(t *testing.T) {
		t.Parallel()

		fx, p := ownerFixture("blockstor\tlocal")
		fx.Expect(ownerSnaps, storage.FakeResponse{Err: errors.New("pool suspended")})

		_, err := p.InspectVolume(t.Context(), ownerVol())
		if err == nil {
			t.Fatal("want an error when the snapshots cannot be listed")
		}
	})

	t.Run("gone", func(t *testing.T) {
		t.Parallel()

		p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, storage.NewFakeExec())

		_, err := p.InspectVolume(t.Context(), ownerVol())
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("want ErrNotFound for a zvol that is gone, got %v", err)
		}
	})
}

func TestReapVolume(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		marker      string
		snaps       string
		wantErr     error
		wantDestroy []string
	}{
		{name: "unmarked is refused", marker: "-\t-", wantErr: storage.ErrNotOwned},
		{name: "inherited marker is refused", marker: "blockstor\tinherited from tank", wantErr: storage.ErrNotOwned},
		{
			name:    "untracked snapshot is refused",
			marker:  "blockstor\tlocal",
			snaps:   ownerDS + "@pre-migration\n",
			wantErr: storage.ErrHasSnapshots,
		},
		{name: "marked without snapshots is destroyed", marker: "blockstor\tlocal", wantDestroy: []string{ownerDestroy}},
		{
			name:        "deferred-delete markers go first, one by one",
			marker:      "blockstor\tlocal",
			snaps:       ownerDS + "@s__DELETED__1700000000\n",
			wantDestroy: []string{ownerDestroy + "@s__DELETED__1700000000", ownerDestroy},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx, p := ownerFixture(tc.marker)
			fx.Expect(ownerSnaps, storage.FakeResponse{Stdout: []byte(tc.snaps)})

			err := p.ReapVolume(t.Context(), ownerVol())
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
			} else if err != nil {
				t.Fatalf("ReapVolume: %v", err)
			}

			var destroys []string

			for _, line := range fx.CommandLines() {
				if strings.HasPrefix(line, "zfs destroy") {
					destroys = append(destroys, line)
				}
			}

			if !slices.Equal(destroys, tc.wantDestroy) {
				t.Errorf("destroys=%v, want %v", destroys, tc.wantDestroy)
			}
		})
	}
}

// The listing the reap relies on can be stale by the time it runs: a
// snapshot taken in between must stop the reap, and nothing the reap
// issues is recursive, so zfs itself refuses a zvol with a snapshot.
func TestReapVolumeRechecksSnapshotsAtDeletion(t *testing.T) {
	t.Parallel()

	fx, p := ownerFixture("blockstor\tlocal")
	fx.ExpectSequence(ownerSnaps,
		storage.FakeResponse{}, // the check
		storage.FakeResponse{Stdout: []byte(ownerDS + "@taken-just-now\n")}, // right before the destroy
	)

	err := p.ReapVolume(t.Context(), ownerVol())
	if !errors.Is(err, storage.ErrHasSnapshots) {
		t.Fatalf("want ErrHasSnapshots, got %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs destroy") {
			t.Errorf("nothing may be destroyed, got %q", line)
		}
	}
}

// Promoting a clone hands it every snapshot older than its origin, so
// a zvol with a snapshot blockstor does not track must be refused
// before any clone is promoted, not only at the final destroy.
func TestReapVolumeRefusesBeforePromotingClones(t *testing.T) {
	t.Parallel()

	fx, p := ownerFixture("blockstor\tlocal")
	fx.Expect(ownerSnaps, storage.FakeResponse{Stdout: []byte(ownerDS + "@pre-migration\n")})
	fx.Expect("zfs list -H -o name,origin -t filesystem,volume",
		storage.FakeResponse{Stdout: []byte("tank/pvc-2_00000\t" + ownerDS + "@pre-migration\n")})

	err := p.ReapVolume(t.Context(), ownerVol())
	if !errors.Is(err, storage.ErrHasSnapshots) {
		t.Fatalf("want ErrHasSnapshots, got %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs promote") || strings.HasPrefix(line, "zfs destroy") {
			t.Errorf("refused reap must not change anything, got %q", line)
		}
	}
}
