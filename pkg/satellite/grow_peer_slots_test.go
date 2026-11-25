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

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The sizes of the restore that split: a 64 MiB snapshot restored into
// a 256 MiB volume definition, with the 7 peer slots LINSTOR gives a new
// definition.
const (
	restoredDefinitionKb = 256 << 10
	restoredPeerSlots    = int32(7)
)

// TestSuperblockCountOfMetadataCreatedHereIsNotRead: the count of a
// superblock this satellite created is the one it wrote; reading it
// back with dump-md on every pass until the count is published is a
// read of a disk the satellite just wrote.
func TestSuperblockCountOfMetadataCreatedHereIsNotRead(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	slots := restoredPeerSlots
	dr := &intent.DesiredResource{
		Name:                  "pvc-r",
		PeerSlots:             &slots,
		ReadMetadataPeerSlots: true,
		Volumes:               []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: restoredDefinitionKb}},
	}

	r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir()})
	r.noteMetadataSlots("pvc-r/0", int(restoredPeerSlots))

	res := &intent.ResourceApplyResult{Ok: true}

	for range 2 {
		if got := r.superblockPeerSlots(t.Context(), dr, res); got != restoredPeerSlots {
			t.Fatalf("reported %d slots, want the %d create-md wrote", got, restoredPeerSlots)
		}
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "dump-md") {
			t.Errorf("read the superblock it created: %q", line)
		}
	}
}
