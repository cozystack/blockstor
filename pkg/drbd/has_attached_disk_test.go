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

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errStatusFailed is a drbdsetup status that exits non-zero.
var errStatusFailed = errors.New("exit status 10")

// HasAttachedDisk reads the local volumes' disk states only: a peer's
// disk state is not this node's, and a slot without a volume has no
// disk, which is what tells it from an attached one.
func TestHasAttachedDisk(t *testing.T) {
	t.Parallel()

	peer := "  n1 node-id:0 connection:Connected role:Secondary\n" +
		"    volume:0 replication:Established peer-disk:UpToDate\n"

	for _, tc := range []struct {
		name, out string
		want      bool
	}{
		{"attached", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:UpToDate client:no\n" + peer, true},
		{"attaching", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Attaching client:no\n" + peer, true},
		{"one of two attached", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Diskless\n" +
			"  volume:1 minor:1001 disk:Inconsistent\n" + peer, true},
		{"diskless", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Diskless client:yes\n" + peer, false},
		{"detaching", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Detaching\n" + peer, false},
		{"failed", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Failed\n" + peer, false},
		{"no volume", "r node-id:1 role:Secondary\n" + peer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status --verbose r", storage.FakeResponse{Stdout: []byte(tc.out)})

			got, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "r")
			if err != nil {
				t.Fatalf("HasAttachedDisk: %v", err)
			}

			if got != tc.want {
				t.Errorf("HasAttachedDisk = %v, want %v for\n%s", got, tc.want, tc.out)
			}
		})
	}

	t.Run("probe fails", func(t *testing.T) {
		t.Parallel()

		fx := storage.NewFakeExec()
		fx.Expect("drbdsetup status --verbose r", storage.FakeResponse{Err: errStatusFailed})

		if _, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "r"); err == nil {
			t.Error("a failed probe reported a disk state, want the error")
		}
	})
}
