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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestResolveVolumeSeedReadsSkipInitialSyncSpecFlag is the unit-level
// proof of the skip-init-sync hardening's decision-source change: the
// satellite's resolveVolumeSeed now reads the controller-authoritative,
// persisted Spec.SkipInitialSync (threaded as DesiredResource.
// SkipInitialSync) instead of relying on the live-kernel probe.
//
// The matrix covers the three tri-state values × winner/non-winner ×
// peerHasData, asserting:
//
//   - nil (unstamped / pre-upgrade) → NO skip seed (invariant 5).
//   - false (relocate / migrate / extra replica) → NO skip seed, EVEN
//     for the elected winner — the core offline-safety fix: the
//     decision came from the persisted RD latch, so an offline
//     data-holder can never make this replica come up UpToDate-empty.
//   - true (fresh initial set) → day0 skip seed (case A) and the
//     winner Consistent+UpToDate seed (case B) fire — invariant 1,
//     preserving PR #20.
//   - peerHasData always vetoes regardless of the flag.
func TestResolveVolumeSeedReadsSkipInitialSyncSpecFlag(t *testing.T) {
	bptr := func(b bool) *bool { return &b }

	cases := []struct {
		name            string
		skipInitialSync *bool
		isWinner        bool
		peerHasData     bool
		// noConnectedPeer drives the AnyConnectedPeerHasData probe: when
		// true the fake returns an empty connections list (fresh replica,
		// nothing connected) so the secondary kernel veto does not fire.
		wantOK        bool
		wantUpToDate  bool // case-B winner flag
		wantBitmapNil bool // case-A day0 skip has empty bitmap-base
	}{
		{
			name:            "nil_flag_non_winner_refuses_skip",
			skipInitialSync: nil,
			wantOK:          false,
		},
		{
			name:            "nil_flag_winner_refuses_skip",
			skipInitialSync: nil,
			isWinner:        true,
			wantOK:          false,
		},
		{
			name:            "false_flag_non_winner_refuses_skip",
			skipInitialSync: bptr(false),
			wantOK:          false,
		},
		{
			name:            "false_flag_winner_refuses_skip_offline_safe",
			skipInitialSync: bptr(false),
			isWinner:        true,
			wantOK:          false,
		},
		{
			name:            "true_flag_non_winner_takes_day0_skip",
			skipInitialSync: bptr(true),
			wantOK:          true,
			wantBitmapNil:   true,
		},
		{
			name:            "true_flag_winner_takes_uptodate_seed",
			skipInitialSync: bptr(true),
			isWinner:        true,
			wantOK:          true,
			wantUpToDate:    true,
		},
		{
			name:            "peer_has_data_vetoes_even_with_true_flag",
			skipInitialSync: bptr(true),
			isWinner:        true,
			peerHasData:     true,
			wantOK:          false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			// AnyConnectedPeerHasData secondary veto: no connected peer
			// (fresh replica) so it never fires — isolates the Spec-flag
			// decision under test.
			fx.Expect("drbdsetup status pvc-rvs --json",
				storage.FakeResponse{Stdout: []byte(`[{"node-id":0,"connections":[]}]`)})

			thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
			rec := NewReconciler(ReconcilerConfig{
				Providers: map[string]storage.Provider{"thin1": thin},
				Adm:       drbd.NewAdm(fx),
				NodeName:  "n1",
			})

			vol := &intent.DesiredVolume{
				VolumeNumber: 0,
				SizeKib:      1024 * 1024,
				StoragePool:  "thin1",
			}

			seed, ok := rec.resolveVolumeSeed(
				t.Context(), "pvc-rvs", vol, tc.peerHasData, tc.isWinner, tc.skipInitialSync)

			if ok != tc.wantOK {
				t.Fatalf("resolveVolumeSeed ok=%v, want %v (seed=%+v)", ok, tc.wantOK, seed)
			}

			if !ok {
				return
			}

			day0 := day0GiFor("pvc-rvs", 0)
			if seed.Current != day0 {
				t.Errorf("seed.Current=%q, want day0=%q", seed.Current, day0)
			}

			if seed.UpToDate != tc.wantUpToDate {
				t.Errorf("seed.UpToDate=%v, want %v", seed.UpToDate, tc.wantUpToDate)
			}

			if tc.wantBitmapNil && seed.BitmapBase != "" {
				t.Errorf("case-A day0 skip must leave bitmap-base empty, got %q", seed.BitmapBase)
			}
		})
	}
}
