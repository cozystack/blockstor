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

package satellite

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

const uncleanDumpMDForced = "drbdadm dump-md --force pvc-unclean/0"

// createMDLines returns the create-md command lines the fake ran.
func createMDLines(fx *storage.FakeExec) []string {
	var out []string

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "create-md") {
			out = append(out, line)
		}
	}

	return out
}

// A clone of a snapshot of an attached volume carries the source's
// superblock with its activity log open, and is initialised. Its data area
// was laid out for the source's peer slots: a volume LINSTOR created has
// metadata for 7, and metadata for 15 does not fit behind its filesystem,
// so drbdmeta refused every attempt and the restore never came up. The
// clone gets metadata in the source's slot count, on both paths.
func TestARestoredCloneKeepsThePeerSlotsOfItsSource(t *testing.T) {
	for _, path := range []string{"ensureMetadata", "ensurePerVolumeMetadata"} {
		t.Run(path, func(t *testing.T) {
			rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
			dr.Volumes[0].SourceSnapshot = "pvc-source:snap-1"

			fx.Expect(uncleanDumpMDForced, storage.FakeResponse{
				Stdout: dumpWithPeerBitmap("9F00AA11BB22CC33", "0000000000000000"),
			})
			fx.Expect("drbdadm create-md --force --max-peers=7 pvc-unclean/0", storage.FakeResponse{})

			if err := runMetadataPath(t, path, rec, dr, devices, mdMarkerPath); err != nil {
				t.Fatalf("%s: %v", path, err)
			}

			got := createMDLines(fx)
			if len(got) != 1 || got[0] != "drbdadm create-md --force --max-peers=7 pvc-unclean/0" {
				t.Errorf("create-md = %q, want the source's 7 peer slots", got)
			}
		})
	}
}

// A superblock that is there decides the layout whatever made it. A volume
// LINSTOR created carries metadata for 7 slots, and re-initialised with 15,
// drbdmeta refused it under ext4, and under a partition table or a VM disk
// --force let it overwrite the end of the data. It keeps its 7. A
// superblock with more slots than blockstor creates gets 15, which is
// smaller and fits; one blockstor made keeps its 15. An adopted replica
// with no GI recorded here is not re-initialised at all, see
// TestAnUncleanVolumeWithNoRecordedGIIsRefused.
func TestAReinitialisedSuperblockKeepsItsLayout(t *testing.T) {
	for name, tc := range map[string]struct {
		source string
		slots  int
		want   string
	}{
		"restore of LINSTOR's":   {source: "pvc-source:snap-1", slots: 7, want: "--max-peers=7"},
		"restore of 31 slots":    {source: "pvc-source:snap-1", slots: 31, want: "--max-peers=15"},
		"restore of blockstor's": {source: "pvc-source:snap-1", slots: 15, want: "--max-peers=15"},
	} {
		for _, path := range []string{"ensureMetadata", "ensurePerVolumeMetadata"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
				dr.Volumes[0].SourceSnapshot = tc.source

				fx.Expect(uncleanDumpMDForced, storage.FakeResponse{Stdout: dumpWithPeerSlots(tc.slots)})

				if err := runMetadataPath(t, path, rec, dr, devices, mdMarkerPath); err != nil {
					t.Fatalf("%s: %v", path, err)
				}

				want := "drbdadm create-md --force " + tc.want + " pvc-unclean/0"
				if got := createMDLines(fx); len(got) != 1 || got[0] != want {
					t.Errorf("create-md = %q, want %q", got, want)
				}
			})
		}
	}
}

// The activity-log layout is part of the size of the metadata, and LINSTOR
// restores it with the peer slots. A superblock with another layout is
// re-created in it, so the data area does not move.
func TestAReinitialisedSuperblockKeepsItsActivityLog(t *testing.T) {
	rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
	dr.Volumes[0].SourceSnapshot = "pvc-source:snap-1"

	dump := strings.NewReplacer("al-stripes 1;", "al-stripes 2;", "al-stripe-size-4k 8;", "al-stripe-size-4k 16;").
		Replace(string(dumpWithPeerSlots(7)))
	fx.Expect(uncleanDumpMDForced, storage.FakeResponse{Stdout: []byte(dump)})

	if err := rec.ensureMetadata(t.Context(), dr, devices, mdMarkerPath, true); err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	want := "drbdadm create-md --force --max-peers=7 --al-stripes=2 --al-stripe-size-kB=64 pvc-unclean/0"
	if got := createMDLines(fx); len(got) != 1 || got[0] != want {
		t.Errorf("create-md = %q, want %q", got, want)
	}
}

// A superblock whose layout cannot be read is not re-initialised in the
// full one. Before, a dump-md that failed for any reason, an exec error, a
// lock, EIO, a listing it could not parse, fell back to 15 slots: refused
// under ext4 and retried 5 minutes later, and under content drbdmeta does
// not recognise forced over the end of the data. It is now an error that
// is not a refusal, so the apply is retried on the short timer and nothing
// is written.
func TestAnUnreadableSuperblockIsNotSizedBlind(t *testing.T) {
	for name, dump := range map[string]storage.FakeResponse{
		"dump-md fails":   {Err: storage.FakeExitError{Code: 20}},
		"unreadable dump": {Stdout: []byte("version \"v09\";\n")},
	} {
		for _, path := range []string{"ensureMetadata", "ensurePerVolumeMetadata"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
				dr.Volumes[0].SourceSnapshot = "pvc-source:snap-1"

				fx.Expect(uncleanDumpMDForced, dump)

				err := runMetadataPath(t, path, rec, dr, devices, mdMarkerPath)
				if err == nil || errors.Is(err, drbd.ErrCreateMDRefused) {
					t.Fatalf("%s = %v, want an error that is not a refusal", path, err)
				}

				if got := createMDLines(fx); len(got) != 0 {
					t.Errorf("create-md ran over a superblock of unknown layout: %q", got)
				}
			})
		}
	}
}

// A snapshot of a volume that was detached when it was taken carries a
// clean superblock, which the probe accepted as this replica's. It carries
// the source's node id, and the kernel refused to attach it under the one
// blockstor allocated: `(119) ambiguous node id`, on every retry. A restored
// volume with no GI of its own whose superblock names another node id is
// re-initialised in its layout; anything else keeps its metadata.
func TestARestoredCleanSuperblockOfAnotherNodeIsReinitialised(t *testing.T) {
	foreign := strings.Replace(string(dumpWithPeerSlots(7)), "node-id 0;", "node-id 3;", 1)

	for name, tc := range map[string]struct {
		source, observedGI string
		forced             storage.FakeResponse
		want               []string
	}{
		"restore of another node's superblock": {
			source: "pvc-source:snap-1", forced: storage.FakeResponse{Stdout: []byte(foreign)},
			want: []string{"drbdadm create-md --force --max-peers=7 pvc-unclean/0"},
		},
		"restore of this node id": {
			source: "pvc-source:snap-1", forced: storage.FakeResponse{Stdout: dumpWithPeerSlots(7)},
		},
		"not a restore": {forced: storage.FakeResponse{Stdout: []byte(foreign)}},
		"a GI of its own": {
			source: "pvc-source:snap-1", observedGI: uncleanReportedGI,
			forced: storage.FakeResponse{Stdout: []byte(foreign)},
		},
		"layout unreadable": {
			source: "pvc-source:snap-1", forced: storage.FakeResponse{Err: storage.FakeExitError{Code: 20}},
		},
	} {
		for _, path := range []string{"ensureMetadata", "ensurePerVolumeMetadata"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
				dr.Volumes[0].SourceSnapshot = tc.source
				dr.Volumes[0].ObservedGI = tc.observedGI

				fx.Expect(uncleanDumpMD, storage.FakeResponse{Stdout: dumpWithPeerSlots(7)})
				fx.Expect(uncleanDumpMDForced, tc.forced)

				if err := runMetadataPath(t, path, rec, dr, devices, mdMarkerPath); err != nil {
					t.Fatalf("%s: %v", path, err)
				}

				if got := createMDLines(fx); !slices.Equal(got, tc.want) {
					t.Errorf("create-md = %q, want %q", got, tc.want)
				}
			})
		}
	}
}
