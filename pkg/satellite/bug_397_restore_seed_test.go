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
	"context"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// restoreSeedFakeProvider is a minimal storage.Provider for the Bug 397
// seed-gate tests. RestoreVolumeFromSnapshot returns restoreErr (set to
// storage.ErrNotFound to force the blank-fallback branch), CreateVolume is
// counted. It does NOT implement storage.SnapshotShipper, so without a
// CrossNodeFetcher materializeVolume falls straight through to the blank
// CreateVolume fallback — exactly the Bug 397 data-loss path.
type restoreSeedFakeProvider struct {
	kind        string
	restoreErr  error
	createCalls int
}

func (p *restoreSeedFakeProvider) Kind() string { return p.kind }

func (*restoreSeedFakeProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{SupportsSnapshots: true}, nil
}

func (p *restoreSeedFakeProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	p.createCalls++

	return nil
}

func (*restoreSeedFakeProvider) DeleteVolume(_ context.Context, _ storage.Volume) error { return nil }
func (*restoreSeedFakeProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (*restoreSeedFakeProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath: "/dev/fake/" + vol.ResourceName,
		UsableKib:  vol.SizeKib,
		State:      "PROVISIONED",
	}, nil
}

func (*restoreSeedFakeProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (*restoreSeedFakeProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (p *restoreSeedFakeProvider) RestoreVolumeFromSnapshot(_ context.Context,
	_ storage.Volume, _ storage.Snapshot,
) error {
	return p.restoreErr
}

// TestResolveVolumeSeedBug397BlankFallbackRefusesSkip is the Bug 397 (P0
// DATA INTEGRITY) satellite-seed regression. A snapshot-restore-backed
// volume that fell back to a BLANK CreateVolume on this node (no local
// snapshot, no cross-node fetch) holds NONE of the snapshot's data — it
// MUST NOT take the day0 skip-init-sync seed, or it would latch UpToDate
// while empty. The gate forces SkipInitialSync=false for the blank-fallback
// replica so it comes up Inconsistent and SyncTargets the real restored
// peer.
//
// The companion case proves the LEGITIMATE all-clone restore fast-path is
// preserved: a replica that genuinely received the clone (recorded as NOT a
// blank fallback) keeps its skip, because every diskful replica is
// byte-identical to the snapshot.
func TestResolveVolumeSeedBug397BlankFallbackRefusesSkip(t *testing.T) {
	bptr := func(b bool) *bool { return &b }

	cases := []struct {
		name          string
		sourceSnap    string
		blankFallback bool // marker as recorded by materializeVolume
		isWinner      bool
		wantOK        bool // whether a skip seed is produced
	}{
		{
			name:          "blank_fallback_restore_refuses_day0_skip",
			sourceSnap:    "pvc-src:snap-1",
			blankFallback: true,
			wantOK:        false,
		},
		{
			name:          "blank_fallback_restore_refuses_winner_uptodate_seed",
			sourceSnap:    "pvc-src:snap-1",
			blankFallback: true,
			isWinner:      true,
			wantOK:        false,
		},
		{
			name:          "genuine_clone_restore_keeps_day0_skip_fastpath",
			sourceSnap:    "pvc-src:snap-1",
			blankFallback: false,
			wantOK:        true,
		},
		{
			name:          "genuine_clone_restore_winner_keeps_uptodate_seed",
			sourceSnap:    "pvc-src:snap-1",
			blankFallback: false,
			isWinner:      true,
			wantOK:        true,
		},
		{
			name:       "non_restore_volume_unaffected_keeps_skip",
			sourceSnap: "",
			wantOK:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// AnyConnectedPeerHasData secondary kernel veto: report no
			// connected peer (fresh replica) so the backstop never fires
			// and the Bug 397 / Spec-flag decision is isolated.
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-rseed --json",
				storage.FakeResponse{Stdout: []byte(`[{"node-id":0,"connections":[]}]`)})

			prov := &restoreSeedFakeProvider{kind: ProviderKindZFSThin}
			rec := NewReconciler(ReconcilerConfig{
				Providers: map[string]storage.Provider{"zfsthin1": prov},
				Adm:       drbd.NewAdm(fx),
				NodeName:  "n1",
			})

			// Reproduce what materializeVolume records for this replica.
			if tc.sourceSnap != "" {
				rec.recordRestoreBlankFallback("pvc-rseed", 0, tc.blankFallback)
			}

			vol := &intent.DesiredVolume{
				VolumeNumber:   0,
				SizeKib:        1024 * 1024,
				StoragePool:    "zfsthin1",
				SourceSnapshot: tc.sourceSnap,
			}

			// peerHasData=false reproduces the day0 ambiguity: the blank
			// replica and the restored peer both sit at day0 with clean
			// bitmaps, so the kernel/CRD probe cannot tell them apart — the
			// exact window in which the bad skip used to fire.
			seed, ok := rec.resolveVolumeSeed(
				t.Context(), "pvc-rseed", vol, false, tc.isWinner, bptr(true))

			if ok != tc.wantOK {
				t.Fatalf("resolveVolumeSeed ok=%v, want %v (seed=%+v)", ok, tc.wantOK, seed)
			}
		})
	}
}

// TestMaterializeVolumeBug397RecordsBlankFallback proves the end-to-end
// wiring: materializeVolume records the blank-fallback marker when the
// local clone misses and no cross-node fetcher is configured, and clears it
// when the clone genuinely succeeds. This is what feeds the seed gate above
// in a real reconcile pass.
func TestMaterializeVolumeBug397RecordsBlankFallback(t *testing.T) {
	t.Run("local_clone_miss_no_fetcher_marks_blank", func(t *testing.T) {
		prov := &restoreSeedFakeProvider{kind: ProviderKindZFSThin, restoreErr: storage.ErrNotFound}
		rec := NewReconciler(ReconcilerConfig{
			Providers: map[string]storage.Provider{"zfsthin1": prov},
			NodeName:  "n1",
		})

		vol := &intent.DesiredVolume{
			VolumeNumber:   0,
			SizeKib:        1024 * 1024,
			StoragePool:    "zfsthin1",
			SourceSnapshot: "pvc-src:snap-1",
		}

		if err := rec.materializeVolume(t.Context(), prov, "pvc-bf", vol); err != nil {
			t.Fatalf("materializeVolume: %v", err)
		}

		if prov.createCalls != 1 {
			t.Fatalf("expected blank CreateVolume fallback, got %d create calls", prov.createCalls)
		}

		if !rec.isRestoreBlankFallback("pvc-bf", 0) {
			t.Errorf("blank-fallback marker must be set after a missed clone with no fetcher")
		}
	})

	t.Run("local_clone_success_keeps_fastpath", func(t *testing.T) {
		// restoreErr nil → RestoreVolumeFromSnapshot succeeds locally.
		prov := &restoreSeedFakeProvider{kind: ProviderKindZFSThin}
		rec := NewReconciler(ReconcilerConfig{
			Providers: map[string]storage.Provider{"zfsthin1": prov},
			NodeName:  "n1",
		})

		vol := &intent.DesiredVolume{
			VolumeNumber:   0,
			SizeKib:        1024 * 1024,
			StoragePool:    "zfsthin1",
			SourceSnapshot: "pvc-src:snap-1",
		}

		if err := rec.materializeVolume(t.Context(), prov, "pvc-clone", vol); err != nil {
			t.Fatalf("materializeVolume: %v", err)
		}

		if prov.createCalls != 0 {
			t.Errorf("genuine clone must not fall back to CreateVolume, got %d", prov.createCalls)
		}

		if rec.isRestoreBlankFallback("pvc-clone", 0) {
			t.Errorf("genuine clone must NOT be marked blank-fallback (would break the fast-path skip)")
		}
	})
}
