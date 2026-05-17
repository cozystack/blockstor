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
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Bug 255 (P2, capacity safety): ZFS CreateVolume's idempotency check
// returned nil BEFORE calling ensureRefreservation. Same root as
// Bug 253/254 but on the Create path — a satellite that successfully
// `zfs create -V`'d a dataset but crashed before the refreservation was
// explicit on the dataset would observe the dataset on the next reconcile
// pass, short-circuit to nil, and never re-set refreservation. The
// dataset stays permanently sparse-thick across reconciles.
//
// Fix: idempotent-skip path must call ensureRefreservation. Same
// pattern as Bug 253/254 retrofit — every path that may observe an
// existing dataset must end on the same thick-correct end state.

// TestThickCreateIdempotentSkipStillEnsuresRefreservation: when the
// target dataset already exists (resumed reconcile / crash recovery),
// the provider MUST still ensure refreservation is set. Otherwise the
// dataset would stay permanently sparse-thick across every subsequent
// reconcile.
func TestThickCreateIdempotentSkipStillEnsuresRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target dataset ALREADY exists → idempotent-skip path.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})
	// volsize lookup returns 1 GiB.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	// refreservation is "none" (the post-crash sparse-thick state).
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("none\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// `zfs create` MUST NOT be issued on the idempotent-skip path.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs create ") {
			t.Errorf("idempotent skip must NOT re-issue `zfs create`; got %q", line)
		}
	}

	wantSet := "zfs set refreservation=1073741824 tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), wantSet) {
		t.Errorf("Bug 255 fix: even on the idempotent-skip path, thick "+
			"CreateVolume must ensure refreservation is set (otherwise a crash "+
			"between `zfs create` and the refreservation observability leaves the "+
			"dataset permanently sparse-thick across reconciles); "+
			"expected %q in calls; got %v",
			wantSet, fx.CommandLines())
	}
}

// TestThinCreateIdempotentSkipNoRefreservation: ZFS_THIN must never set
// refreservation, even on the idempotent-skip path.
func TestThinCreateIdempotentSkipNoRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "refreservation=") {
			t.Errorf("THIN CreateVolume (idempotent-skip path) must NOT set "+
				"refreservation; got %q", line)
		}
	}
}
