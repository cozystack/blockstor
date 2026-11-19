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

package lvm_test

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Issue 264: the orphan-storage sweeper removed volumes it had no proof
// blockstor made. These tests pin the OwnerTag both LVM providers now
// write and read, and the reap that refuses an untagged LV or one with
// snapshots.

const (
	ownTagsLV  = "lvs --config " + lvm.ConfigFilter + " --noheadings -o lv_tags vg/pvc-1_00000"
	ownAddTag  = "lvchange --config " + lvm.ConfigFilter + " --addtag blockstor-owned vg/pvc-1_00000"
	ownRows    = "lvs --config " + lvm.ConfigFilter + " --noheadings --separator \t -o lv_name,origin,lv_tags vg"
	ownSnapLV  = "lvs --config " + lvm.ConfigFilter + " --noheadings -o lv_name vg/pvc-src_s_00000"
	ownRemove  = "lvremove --config " + lvm.ConfigFilter + " --force vg/pvc-1_00000"
	ownLVName  = "pvc-1_00000\n"
	ownRowsHit = "  pvc-1_00000\t\tblockstor-owned\n"
)

// ownProvider is the part of both LVM providers these tests drive.
type ownProvider interface {
	storage.Provider
	storage.OrphanReaper
}

func ownProviders(fx *storage.FakeExec) map[string]ownProvider {
	return map[string]ownProvider{
		"thin":  lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx),
		"thick": lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx),
	}
}

// existingLV wires both the per-LV probe Thin uses and the VG listing
// Thick uses so either finds pvc-1_00000 already there.
func existingLV(fx *storage.FakeExec, tags string) {
	fx.Expect(lvsFinal, storage.FakeResponse{Stdout: []byte(ownLVName)})
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte(ownLVName)})
	fx.Expect(ownTagsLV, storage.FakeResponse{Stdout: []byte(tags)})
}

func TestOwnerTagBackfilledOnExistingLV(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		tags    string
		wantAdd bool
	}{
		{name: "untagged", tags: "\n", wantAdd: true},
		{name: "other tags only", tags: "  linstor_x,blockstor-owned-v2\n", wantAdd: true},
		{name: "tagged", tags: "  linstor_x,blockstor-owned\n", wantAdd: false},
	}

	for _, tc := range cases {
		for kind := range ownProviders(storage.NewFakeExec()) {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				t.Parallel()

				fx := storage.NewFakeExec()
				existingLV(fx, tc.tags)

				err := ownProviders(fx)[kind].CreateVolume(t.Context(), thickVol())
				if err != nil {
					t.Fatalf("CreateVolume: %v", err)
				}

				if got := slices.Contains(fx.CommandLines(), ownAddTag); got != tc.wantAdd {
					t.Errorf("tags %q: addtag issued=%v, want %v; calls=%v", tc.tags, got, tc.wantAdd, fx.CommandLines())
				}

				for _, line := range fx.CommandLines() {
					if strings.HasPrefix(line, "lvcreate") {
						t.Errorf("existing LV must not be re-created: %q", line)
					}
				}
			})
		}
	}
}

// Restoring onto, and receiving into, an LV that is already there is
// the other way a pre-existing LV is found while provisioning it.
func TestOwnerTagBackfilledOnRestoreAndRecvResume(t *testing.T) {
	t.Parallel()

	src := storage.Snapshot{ResourceName: "pvc-src", SnapshotName: "s"}

	for kind := range ownProviders(storage.NewFakeExec()) {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			existingLV(fx, "\n")
			fx.Expect(ownSnapLV, storage.FakeResponse{Stdout: []byte("pvc-src_s_00000\n")})

			err := ownProviders(fx)[kind].RestoreVolumeFromSnapshot(t.Context(), thickVol(), src)
			if err != nil {
				t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
			}

			if !slices.Contains(fx.CommandLines(), ownAddTag) {
				t.Errorf("restore onto an existing LV did not tag it; calls=%v", fx.CommandLines())
			}
		})
	}

	t.Run("thin recv", func(t *testing.T) {
		t.Parallel()

		fx := storage.NewFakeExec()
		existingLV(fx, "\n")

		thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

		err := thin.RecvSnapshot(t.Context(), thickVol(), bytes.NewReader(nil))
		if err != nil {
			t.Fatalf("RecvSnapshot: %v", err)
		}

		if !slices.Contains(fx.CommandLines(), ownAddTag) {
			t.Errorf("recv onto an existing LV did not tag it; calls=%v", fx.CommandLines())
		}
	})
}

// A fresh restore or recv tags the LV in the lvcreate that allocates
// it, alongside the restore sentinel.
func TestOwnerTagOnFreshRestoreAndRecv(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	err := thin.RecvSnapshot(t.Context(), thickVol(), bytes.NewReader([]byte("stream")))
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	tagged := false

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvcreate") && strings.Contains(line, "--addtag "+lvm.RestoreIncompleteTag+" --addtag "+lvm.OwnerTag) {
			tagged = true
		}
	}

	if !tagged {
		t.Errorf("recv lvcreate must carry the owner tag; calls=%v", fx.CommandLines())
	}
}

func TestInspectLV(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		rows      string
		wantOwned bool
		wantSnaps []string
	}{
		{name: "untagged", rows: "  pvc-1_00000\t\t\n", wantOwned: false},
		{name: "untagged, trailing columns trimmed", rows: "  pvc-1_00000\n", wantOwned: false},
		{name: "tag on another LV", rows: "  pvc-1_00000\t\t\n  pvc-2_00000\t\tblockstor-owned\n", wantOwned: false},
		{name: "tagged", rows: "  pvc-1_00000\t\tlinstor_x,blockstor-owned\n", wantOwned: true},
		{
			name:      "tagged with snapshots",
			rows:      ownRowsHit + "  pvc-1_s1_00000\tpvc-1_00000\t\n  pre-migration\tpvc-1_00000\t\n  pvc-2_00000\tpvc-9_00000\tblockstor-owned\n",
			wantOwned: true,
			wantSnaps: []string{"pvc-1_s1_00000", "pre-migration"},
		},
	}

	for _, tc := range cases {
		for kind := range ownProviders(storage.NewFakeExec()) {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				t.Parallel()

				fx := storage.NewFakeExec()
				fx.Expect(ownRows, storage.FakeResponse{Stdout: []byte(tc.rows)})

				got, err := ownProviders(fx)[kind].InspectVolume(t.Context(), thickVol())
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
}

func TestInspectLVFailures(t *testing.T) {
	t.Parallel()

	for kind := range ownProviders(storage.NewFakeExec()) {
		t.Run(kind+"/lvs fails", func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect(ownRows, storage.FakeResponse{Err: errLvsLock})

			_, err := ownProviders(fx)[kind].InspectVolume(t.Context(), thickVol())
			if !errors.Is(err, errLvsLock) {
				t.Fatalf("want the lvs error, got %v", err)
			}
		})

		t.Run(kind+"/gone", func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect(ownRows, storage.FakeResponse{Stdout: []byte("  pvc-2_00000\t\tblockstor-owned\n")})

			_, err := ownProviders(fx)[kind].InspectVolume(t.Context(), thickVol())
			if !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("want ErrNotFound, got %v", err)
			}
		})
	}
}

func TestReapLV(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		rows       string
		wantErr    error
		wantRemove bool
	}{
		{name: "untagged is refused", rows: "  pvc-1_00000\t\t\n", wantErr: storage.ErrNotOwned},
		{name: "snapshot is refused", rows: ownRowsHit + "  keep-me\tpvc-1_00000\t\n", wantErr: storage.ErrHasSnapshots},
		{name: "tagged without snapshots is removed", rows: ownRowsHit, wantRemove: true},
	}

	for _, tc := range cases {
		for kind := range ownProviders(storage.NewFakeExec()) {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				t.Parallel()

				fx := storage.NewFakeExec()
				fx.Expect(ownRows, storage.FakeResponse{Stdout: []byte(tc.rows)})

				err := ownProviders(fx)[kind].ReapVolume(t.Context(), thickVol())
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("want %v, got %v", tc.wantErr, err)
					}
				} else if err != nil {
					t.Fatalf("ReapVolume: %v", err)
				}

				removes := 0

				for _, line := range fx.CommandLines() {
					if strings.HasPrefix(line, "lvremove") {
						removes++

						if line != ownRemove {
							t.Errorf("unexpected removal %q", line)
						}
					}
				}

				if (removes > 0) != tc.wantRemove {
					t.Errorf("lvremove issued=%v, want %v; calls=%v", removes > 0, tc.wantRemove, fx.CommandLines())
				}
			})
		}
	}
}
