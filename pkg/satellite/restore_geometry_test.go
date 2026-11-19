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
	"strings"
	"testing"

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

// Only a restore keeps the old geometry. A volume of its own that carries
// some unclean superblock, a recycled extent or a volume added to a live
// resource, was sized for the full count and gets it. So does a clone
// whose superblock count cannot be read, and one whose source already had
// the full count.
func TestOtherInitialisationsUseTheFullPeerSlotCount(t *testing.T) {
	for name, tc := range map[string]struct {
		source string
		dump   storage.FakeResponse
	}{
		"not a restore":    {dump: storage.FakeResponse{Stdout: dumpV09("9F00AA11BB22CC33", "0000000000000000", "0000000000000000")}},
		"unreadable count": {source: "pvc-source:snap-1", dump: storage.FakeResponse{Stdout: []byte("version \"v09\";\n")}},
		"full count": {source: "pvc-source:snap-1", dump: storage.FakeResponse{
			Stdout: []byte(strings.Replace(string(dumpWithPeerBitmap("9F00AA11BB22CC33", "0000000000000000")), "max-peers 7;", "max-peers 15;", 1)),
		}},
	} {
		t.Run(name, func(t *testing.T) {
			rec, dr, devices, mdMarkerPath, fx := uncleanFixture(t)
			dr.Volumes[0].SourceSnapshot = tc.source

			fx.Expect(uncleanDumpMDForced, tc.dump)

			if err := rec.ensureMetadata(t.Context(), dr, devices, mdMarkerPath, true); err != nil {
				t.Fatalf("ensureMetadata: %v", err)
			}

			got := createMDLines(fx)
			if len(got) != 1 || got[0] != "drbdadm create-md --force --max-peers=15 pvc-unclean/0" {
				t.Errorf("create-md = %q, want the full 15 peer slots", got)
			}
		})
	}
}
