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
)

// restoreSeedReconciler is a satellite on n1 with one ZFS thin pool, whose
// kernel reports no connected peer.
func restoreSeedReconciler(t *testing.T) *Reconciler {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-restored --json",
		storage.FakeResponse{Stdout: []byte(`[{"node-id":0,"connections":[]}]`)})

	return NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfsthin1": &restoreSeedFakeProvider{kind: ProviderKindZFSThin}},
		Adm:       drbd.NewAdm(fx),
		NodeName:  "n1",
	})
}

func restoredVolume() *intent.DesiredVolume {
	return &intent.DesiredVolume{
		VolumeNumber:   0,
		SizeKib:        1024 * 1024,
		StoragePool:    "zfsthin1",
		SourceSnapshot: "pvc-gone:snap",
	}
}

// uptodateSeed reports whether seed brings the volume up UpToDate on its own.
func uptodateSeed(seed drbd.GISeed, ok bool) bool {
	return ok && seed.UpToDate && seed.Consistent
}

// A replica cloned here from the snapshot comes up UpToDate without being the
// elected winner. With only the winner seeded UpToDate, a winner that never
// comes up (a replica restored without a pool) left every clone Inconsistent
// for good: none of them is a source the others could sync from.
func TestRestoredCloneSeedsItselfUpToDateWithoutTheWinner(t *testing.T) {
	t.Parallel()

	rec := restoreSeedReconciler(t)
	rec.recordRestoreBlankFallback("pvc-restored", 0, false)

	skip := true

	seed, ok := rec.resolveVolumeSeed(t.Context(), "pvc-restored", restoredVolume(), false, false, &skip)
	if !uptodateSeed(seed, ok) {
		t.Fatalf("seed of a clone that is not the winner = %+v, %v; want Consistent+UpToDate", seed, ok)
	}

	if seed.Current != day0GiFor("pvc-restored", 0) {
		t.Errorf("seed current = %q, want the day0 GI every replica of the restore shares", seed.Current)
	}
}

// A clone placed after a peer already holds data is not part of the restore
// set: the peer may have written since the snapshot, so the clone syncs from
// it rather than presenting the snapshot's data as up to date.
func TestRestoredCloneIsNotSeededUpToDateBesideAPeerWithData(t *testing.T) {
	t.Parallel()

	rec := restoreSeedReconciler(t)
	rec.recordRestoreBlankFallback("pvc-restored", 0, false)

	skip := true

	seed, ok := rec.resolveVolumeSeed(t.Context(), "pvc-restored", restoredVolume(), true, false, &skip)
	if ok {
		t.Fatalf("seed of a clone beside a peer with data = %+v; want none, so it syncs from that peer", seed)
	}
}

// The kernel veto holds for a clone too: a peer whose connection already
// shows committed data is one the clone has to sync from, whatever the
// controller's records say.
func TestRestoredCloneIsNotSeededUpToDateBesideAConnectedPeerWithData(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-restored --json",
		storage.FakeResponse{Stdout: []byte(`[{"node-id":1,"connections":[{"peer-node-id":0,` +
			`"peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]}]`)})

	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfsthin1": &restoreSeedFakeProvider{kind: ProviderKindZFSThin}},
		Adm:       drbd.NewAdm(fx),
		NodeName:  "n1",
	})
	rec.recordRestoreBlankFallback("pvc-restored", 0, false)

	skip := true

	seed, ok := rec.resolveVolumeSeed(t.Context(), "pvc-restored", restoredVolume(), false, false, &skip)
	if uptodateSeed(seed, ok) {
		t.Fatalf("seed of a clone beside a connected peer with data = %+v; want it not UpToDate", seed)
	}
}

// A blank fallback holds none of the snapshot's data and is never seeded as
// a copy of it.
func TestRestoreBlankFallbackIsNotSeededUpToDate(t *testing.T) {
	t.Parallel()

	rec := restoreSeedReconciler(t)
	rec.recordRestoreBlankFallback("pvc-restored", 0, true)

	skip := true

	for _, peerHasData := range []bool{false, true} {
		seed, ok := rec.resolveVolumeSeed(t.Context(), "pvc-restored", restoredVolume(), peerHasData, false, &skip)
		if uptodateSeed(seed, ok) {
			t.Errorf("blank fallback (peer has data: %v) seeded UpToDate: %+v", peerHasData, seed)
		}
	}
}

// A replica the controller says has to sync, or has not decided for yet, is
// not seeded UpToDate from the snapshot either: the target may have moved on
// since the restore.
func TestRestoredCloneFollowsTheControllersSyncDecision(t *testing.T) {
	t.Parallel()

	rec := restoreSeedReconciler(t)
	rec.recordRestoreBlankFallback("pvc-restored", 0, false)

	mustSync := false

	for _, skip := range []*bool{nil, &mustSync} {
		seed, ok := rec.resolveVolumeSeed(t.Context(), "pvc-restored", restoredVolume(), true, false, skip)
		if uptodateSeed(seed, ok) {
			t.Errorf("clone with SkipInitialSync %v seeded UpToDate: %+v", skip, seed)
		}
	}
}

// Once a volume fell back to a blank create it stays recorded so: the
// provider reports a restore onto the existing volume as done on the next
// pass, and that must not turn the empty volume into a copy of the snapshot.
func TestRestoreBlankFallbackStaysRecorded(t *testing.T) {
	t.Parallel()

	rec := restoreSeedReconciler(t)
	rec.recordRestoreBlankFallback("pvc-restored", 0, true)
	rec.recordRestoreBlankFallback("pvc-restored", 0, false)

	if !rec.isRestoreBlankFallback("pvc-restored", 0) || rec.isRestoredFromSnapshot("pvc-restored", 0) {
		t.Error("a blank fallback read as restored from the snapshot after a later pass")
	}
}
