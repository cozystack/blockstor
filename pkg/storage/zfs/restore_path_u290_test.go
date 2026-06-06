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

package zfs_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestU290_CloneBuildsDatasetPathOnce pins the upstream U290 corner
// case: the ZFS clone/restore path must build the dataset name with
// the pool prefix applied EXACTLY ONCE — never `<pool>/<pool>/...`.
// Upstream LINSTOR shipped a regression where a config that already
// carried the pool name in the dataset got the pool prepended again,
// producing a doubled `tank/tank/<rd>` path that no `zfs clone` could
// resolve. blockstor builds the path via a single fmt.Sprintf in
// volumeDataset / snapshotDataset, so the pool appears once; this test
// guards that invariant against a future refactor.
//
// The pool name "zfs-thin" matches the stand pool so the assertion
// doubles as documentation of the real on-stand dataset shape.
func TestU290_CloneBuildsDatasetPathOnce(t *testing.T) {
	t.Parallel()

	const pool = "zfs-thin"

	fx := storage.NewFakeExec()
	// Target dataset absent → proceed to clone.
	fx.Expect("zfs list -H -o name "+pool+"/ccu3-u290-tgt_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Source snapshot present.
	fx.Expect("zfs list -H -o name "+pool+"/ccu3-u290-src_00000@snap",
		storage.FakeResponse{Stdout: []byte(pool + "/ccu3-u290-src_00000@snap\n")})

	p := zfs.NewProvider(zfs.Config{Pool: pool, Thin: true}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{ResourceName: "ccu3-u290-tgt", VolumeNumber: 0, SizeKib: 1024 * 1024},
		storage.Snapshot{ResourceName: "ccu3-u290-src", SnapshotName: "snap"},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	var cloneLine string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs clone ") {
			cloneLine = line

			break
		}
	}

	if cloneLine == "" {
		t.Fatalf("U290: no `zfs clone` command was issued; cmds=%v", fx.CommandLines())
	}

	// The exact expected single-prefix shape.
	want := "zfs clone " + pool + "/ccu3-u290-src_00000@snap " + pool + "/ccu3-u290-tgt_00000"
	if cloneLine != want {
		t.Errorf("U290: clone command shape mismatch\n got: %q\nwant: %q", cloneLine, want)
	}

	// Doubled-pool guard: the pool token must never appear back-to-back
	// (`pool/pool/`). This catches a refactor that prepends the pool to a
	// dataset that already carries it.
	if strings.Contains(cloneLine, pool+"/"+pool+"/") {
		t.Errorf("U290: doubled pool prefix in clone path: %q", cloneLine)
	}

	// Each dataset operand must carry exactly one pool-prefix token.
	for _, operand := range strings.Fields(cloneLine[len("zfs clone "):]) {
		if n := strings.Count(operand, pool+"/"); n != 1 {
			t.Errorf("U290: operand %q has %d %q prefixes, want exactly 1",
				operand, n, pool+"/")
		}
	}
}
