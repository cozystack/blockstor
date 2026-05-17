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

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Bug 252+253+254 (P2, capacity safety): three ZFS-specific stragglers in
// the thick/thin asymmetry class, all rooted in `refreservation` being a
// secondary side-effect set AFTER the primary materialise step. Any path
// that either mutates volsize or short-circuits on dataset existence
// silently strips the thick guarantee.
//
//   - Bug 252: ResizeVolume didn't branch on cfg.Thin and never re-set
//     refreservation. `zfs set volsize=` does NOT auto-grow refreservation,
//     so any post-restore resize silently dropped the thick guarantee on
//     the grown range.
//
//   - Bug 253: RestoreVolumeFromSnapshot's idempotency check returned early
//     before the refreservation step. A crash between `zfs clone` and
//     `zfs set refreservation=` left the dataset permanently sparse-thick
//     across reconciles.
//
//   - Bug 254: RecvSnapshot had the same idempotency race as Bug 253.
//
// Fix: an idempotent `ensureRefreservation(ctx, dataset)` helper that
// no-ops on thin and on thick verifies refreservation matches volsize,
// restoring it if not. Called at the end of CreateVolume / ResizeVolume /
// RestoreVolumeFromSnapshot / RecvSnapshot — including on the
// idempotent-skip paths — so the thick guarantee is restored on every
// reconcile, not just on the happy-path first invocation.

// ---------------------------------------------------------------------------
// Bug 252 — ResizeVolume must re-set refreservation after volsize grow.
// ---------------------------------------------------------------------------

// TestThickResizeReapplyRefreservation: in thick mode, after
// `zfs set volsize=<new>`, the provider MUST re-set refreservation to the
// new volsize. Without this, the grown range is silently sparse and can
// hit ENOSPC mid-write — defeating the thick contract.
func TestThickResizeReapplyRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	// After the grow, volsize lookup returns 2 GiB.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("2147483648\n")})
	// Current refreservation is the OLD volsize (1 GiB) — needs grow.
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024, // 2 GiB
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	wantVolsize := "zfs set volsize=2048M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), wantVolsize) {
		t.Errorf("expected volsize set %q in calls; got %v",
			wantVolsize, fx.CommandLines())
	}

	wantRefres := "zfs set refreservation=2147483648 tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), wantRefres) {
		t.Errorf("Bug 252 fix: thick ResizeVolume must re-set refreservation to new "+
			"volsize (otherwise the grown range is silently sparse and may ENOSPC "+
			"mid-write); expected %q in calls; got %v",
			wantRefres, fx.CommandLines())
	}

	// Ordering: refreservation must come AFTER the volsize set.
	volsizeIdx, refresIdx := -1, -1

	for i, line := range fx.CommandLines() {
		switch line {
		case wantVolsize:
			volsizeIdx = i
		case wantRefres:
			refresIdx = i
		}
	}

	if volsizeIdx >= 0 && refresIdx >= 0 && refresIdx < volsizeIdx {
		t.Errorf("refreservation must be set AFTER volsize; got volsize at idx %d, refres at idx %d",
			volsizeIdx, refresIdx)
	}
}

// TestThinResizeDoesNotSetRefreservation: ZFS_THIN ResizeVolume MUST NOT
// set refreservation (thin = sparse oversubscription by design).
func TestThinResizeDoesNotSetRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "refreservation=") {
			t.Errorf("THIN ResizeVolume must NOT set refreservation "+
				"(thin = sparse oversubscription); got %q", line)
		}
	}
}

// TestThickResizeRefreservationAlreadyMatches: when the current
// refreservation already matches the (new) volsize, the helper is
// idempotent — no `zfs set refreservation=` is issued. This keeps the
// reconcile path quiet on steady-state.
func TestThickResizeRefreservationAlreadyMatches(t *testing.T) {
	fx := storage.NewFakeExec()

	// volsize is already 2 GiB after the set (no-op grow).
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("2147483648\n")})
	// refreservation matches exactly — no re-set needed.
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("2147483648\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs set refreservation=") {
			t.Errorf("steady-state refreservation must be a no-op when current already "+
				"matches volsize; got %q", line)
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 253 — RestoreVolumeFromSnapshot idempotency must ensure refreservation.
// ---------------------------------------------------------------------------

// TestThickRestoreIdempotentSkipStillEnsuresRefreservation: when the target
// dataset already exists (crash recovery / resumed reconcile), the provider
// MUST still ensure refreservation is set. Otherwise a crash between
// `zfs clone` and `zfs set refreservation=` would permanently leave the
// dataset sparse-thick across every subsequent reconcile.
func TestThickRestoreIdempotentSkipStillEnsuresRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target dataset ALREADY exists → idempotent-skip path.
	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-2_00000\n")})
	// volsize lookup returns 1 GiB.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	// refreservation is "none" (the post-crash sparse-thick state).
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("none\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	// `zfs clone` MUST NOT be issued on the idempotent-skip path.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs clone ") {
			t.Errorf("idempotent skip must NOT re-issue `zfs clone`; got %q", line)
		}
	}

	wantSet := "zfs set refreservation=1073741824 tank/pvc-2_00000"
	if !slices.Contains(fx.CommandLines(), wantSet) {
		t.Errorf("Bug 253 fix: even on the idempotent-skip path, thick "+
			"RestoreVolumeFromSnapshot must ensure refreservation is set "+
			"(otherwise a crash between `zfs clone` and `zfs set refreservation` "+
			"leaves the dataset permanently sparse-thick across reconciles); "+
			"expected %q in calls; got %v",
			wantSet, fx.CommandLines())
	}
}

// TestThinRestoreIdempotentSkipNoRefreservation: ZFS_THIN must never set
// refreservation, even on the idempotent-skip path.
func TestThinRestoreIdempotentSkipNoRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-2_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "refreservation=") {
			t.Errorf("THIN restore (idempotent-skip path) must NOT set refreservation; "+
				"got %q", line)
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 254 — RecvSnapshot idempotency must ensure refreservation.
// ---------------------------------------------------------------------------

// TestThickRecvIdempotentSkipStillEnsuresRefreservation: same shape as
// Bug 253, but for the cross-node RecvSnapshot path. A crash between
// `zfs recv` and the post-recv `zfs set refreservation=` would leave the
// recv'd dataset permanently sparse-thick — re-running RecvSnapshot must
// detect the missing reservation and restore it.
func TestThickRecvIdempotentSkipStillEnsuresRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target dataset ALREADY exists → idempotent-skip past `zfs recv`.
	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-2_00000\n")})
	// volsize lookup returns 1 GiB.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	// refreservation is 0 (the post-crash sparse-thick state).
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("0\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	src := bytes.NewReader([]byte("fake zfs send stream"))

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		src,
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	// `zfs recv` MUST NOT be issued on the idempotent-skip path.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs recv ") {
			t.Errorf("idempotent skip must NOT re-issue `zfs recv`; got %q", line)
		}
	}

	wantSet := "zfs set refreservation=1073741824 tank/pvc-2_00000"
	if !slices.Contains(fx.CommandLines(), wantSet) {
		t.Errorf("Bug 254 fix: even on the idempotent-skip path, thick "+
			"RecvSnapshot must ensure refreservation is set (otherwise a crash "+
			"between `zfs recv` and `zfs set refreservation` leaves the dataset "+
			"permanently sparse-thick across reconciles); expected %q in calls; got %v",
			wantSet, fx.CommandLines())
	}
}

// TestThinRecvIdempotentSkipNoRefreservation: ZFS_THIN must never set
// refreservation on the recv idempotent-skip path either.
func TestThinRecvIdempotentSkipNoRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-2_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	src := bytes.NewReader([]byte("fake zfs send stream"))

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		src,
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "refreservation=") {
			t.Errorf("THIN RecvSnapshot (idempotent-skip path) must NOT set "+
				"refreservation; got %q", line)
		}
	}
}
