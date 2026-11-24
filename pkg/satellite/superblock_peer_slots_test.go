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
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// A replica adopted from LINSTOR by a linstor-migrate older than the
// count records none, and its superblock holds LINSTOR's 7 slots, not the
// 15 blockstor created its own replicas with. Asked for it, the apply
// reads the count every volume's superblock holds and reports the least;
// unasked, on a pass that grows a volume, or when a volume cannot be read,
// it reads nothing and reports nothing.
func TestSuperblockPeerSlots(t *testing.T) {
	t.Parallel()

	const (
		vol0 = "drbdadm dump-md --force pvc-adopted/0"
		vol1 = "drbdadm dump-md --force pvc-adopted/1"
	)

	volumes := func(applied int64) []*intent.DesiredVolume {
		return []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1 << 20, AppliedSizeKib: applied},
			{VolumeNumber: 1, SizeKib: 1 << 20, AppliedSizeKib: 1 << 20},
		}
	}

	cases := []struct {
		name     string
		ask      bool
		applied  int64
		vol1     storage.FakeResponse
		want     int32
		wantRead bool
	}{
		{"asked", true, 1 << 20, storage.FakeResponse{Stdout: dumpWithPeerSlots(15)}, 7, true},
		{"asked, never applied before", true, 0, storage.FakeResponse{Stdout: dumpWithPeerSlots(15)}, 7, true},
		{"not asked", false, 1 << 20, storage.FakeResponse{Stdout: dumpWithPeerSlots(15)}, 0, false},
		{"growing", true, 1 << 19, storage.FakeResponse{Stdout: dumpWithPeerSlots(15)}, 0, false},
		{"a volume unreadable", true, 1 << 20, storage.FakeResponse{Err: errors.New("exit status 10")}, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect(vol0, storage.FakeResponse{Stdout: dumpWithPeerSlots(drbd.DefaultPeerSlots)})
			fx.Expect(vol1, tc.vol1)

			r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx)})

			got := r.superblockPeerSlots(t.Context(), &intent.DesiredResource{
				Name:                  "pvc-adopted",
				Volumes:               volumes(tc.applied),
				ReadMetadataPeerSlots: tc.ask,
			})
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}

			read := slices.ContainsFunc(fx.CommandLines(), func(c string) bool { return strings.HasPrefix(c, "drbdadm dump-md") })
			if read != tc.wantRead {
				t.Errorf("superblock read: %v, want %v (%v)", read, tc.wantRead, fx.CommandLines())
			}
		})
	}
}
