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

// Bug 251 (P2, capacity safety, cross-node): ZFS-thick RecvSnapshot used
// `zfs recv` to materialise a dataset from a peer's send stream, but
// `zfs send` on the source side was invoked WITHOUT `-p`, so the stream
// excluded `refreservation` and the receiver produced a sparse-by-
// inheritance dataset. Same blast as Bug 246 (RestoreVolumeFromSnapshot
// dropping the thick guarantee), but on the cross-node peer-stream path.
//
// Fix (two-sided, defence-in-depth):
//   - send side: invoke `zfs send -p` so props (including refreservation)
//     are part of the stream.
//   - recv side: after `zfs recv` completes, in thick mode, look up the
//     dataset's volsize and `zfs set refreservation=<bytes>` to restore
//     the space guarantee even when the sender is an old peer that
//     hasn't been patched.
//
// The recv-side fix alone is the defensive minimum; the send-side `-p`
// avoids the round-trip when both peers are patched.

// TestRecvSnapshotThickSetsRefreservation pins the recv-side fix: thick
// mode MUST set refreservation on the freshly-recv'd dataset AFTER
// `zfs recv` completes, so the dataset is fully reserved (thick)
// regardless of whether the peer sender stamped the prop into the stream.
func TestRecvSnapshotThickSetsRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target dataset does NOT exist → recv path proceeds.
	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

	// After recv, the volsize lookup on the freshly-received dataset
	// returns 1 GiB in bytes.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	src := bytes.NewReader([]byte("fake zfs send stream"))

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024, // 1 GiB
		},
		src,
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	wantSet := "zfs set refreservation=1073741824 tank/pvc-2_00000"
	if !slices.Contains(fx.CommandLines(), wantSet) {
		t.Errorf("Bug 251: thick RecvSnapshot must restore refreservation after "+
			"recv (otherwise a peer that ran the un-patched `zfs send` produces a "+
			"sparse-by-inheritance dataset and the thick space guarantee is lost); "+
			"expected %q in calls; got %v", wantSet, fx.CommandLines())
	}

	// Ordering: refreservation must come AFTER recv so the dataset
	// exists when we set the property.
	recvIdx, setIdx := -1, -1

	for i, line := range fx.CommandLines() {
		switch {
		case strings.HasPrefix(line, "zfs recv "):
			recvIdx = i
		case line == wantSet:
			setIdx = i
		}
	}

	if recvIdx < 0 {
		t.Fatalf("expected a `zfs recv` invocation in calls; got %v",
			fx.CommandLines())
	}

	if setIdx >= 0 && setIdx < recvIdx {
		t.Errorf("refreservation must be set AFTER recv; got recv at idx %d, set at idx %d",
			recvIdx, setIdx)
	}
}

// TestRecvSnapshotThinNoRefreservation is the negative-pair: ZFS_THIN
// RecvSnapshot MUST NOT set refreservation (thin = sparse oversubscription
// by design). A regression that leaked the thick refreservation into the
// thin path would defeat the whole point of ZFS_THIN.
func TestRecvSnapshotThinNoRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

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
			t.Errorf("THIN RecvSnapshot must NOT set refreservation "+
				"(thin = sparse oversubscription); got %q", line)
		}
	}
}

// TestSendSnapshotArgvIncludesDashP pins the send-side half of the fix:
// `zfs send` MUST be invoked with `-p` so the stream carries dataset
// properties (including refreservation). Without -p the receiver sees
// a stream that produces a sparse-by-inheritance dataset.
//
// SendSnapshot bypasses storage.Exec (it pipes a multi-GB stdout pipe —
// see SendSnapshot's docstring) so we can't drive it through FakeExec.
// Instead the package exposes a SendSnapshotArgs helper that returns
// the argv that SendSnapshot would have constructed for a given
// snapshot; the test asserts the shape of that argv.
func TestSendSnapshotArgvIncludesDashP(t *testing.T) {
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, storage.NewFakeExec())

	argv := p.SendSnapshotArgs(storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})

	want := []string{"send", "-p", "tank/pvc-1_00000@snap-1"}
	if !slices.Equal(argv, want) {
		t.Errorf("Bug 251: SendSnapshot must invoke `zfs send -p <snap>` so "+
			"the stream carries refreservation and other props; got argv %v, want %v",
			argv, want)
	}
}
