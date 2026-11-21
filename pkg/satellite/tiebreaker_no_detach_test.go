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

package satellite_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// A diskless replica whose kernel slot is loaded but shows no attached
// volume, as a TieBreaker's slot did after its kernel resource was
// taken down by hand, has no disk to detach. The apply detached whenever
// it found no volume reporting Diskless, so it ran `drbdadm detach
// --force` on a resource configured without a disk, drbdadm refused,
// and the apply failed with an ApplyFailed Warning on a witness that
// never had a disk. LINSTOR detaches only a volume that has one.
func TestDisklessReplicaWithoutAnAttachedDiskIsNotDetached(t *testing.T) {
	for _, tc := range []struct {
		name    string
		verbose string
	}{
		{"no volume yet", "pvc-tb node-id:2 role:Secondary suspended:no\n" +
			"  n1 node-id:0 connection:Connecting role:Unknown\n" +
			"    volume:0 replication:Off peer-disk:UpToDate\n"},
		{"diskless volume", "pvc-tb node-id:2 role:Secondary\n" +
			"  volume:0 minor:1000 disk:Diskless client:yes quorum:yes\n" +
			"  n1 node-id:0 connection:Connected role:Secondary\n" +
			"    volume:0 replication:Established peer-disk:UpToDate\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-tb", storage.FakeResponse{Stdout: []byte("pvc-tb role:Secondary\n")})
			fx.Expect("drbdsetup status --verbose pvc-tb", storage.FakeResponse{Stdout: []byte(tc.verbose)})

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Adm:      drbd.NewAdm(fx),
				StateDir: t.TempDir(),
				NodeName: "n3",
			})

			dr := &intent.DesiredResource{
				Name:     "pvc-tb",
				NodeName: "n3",
				Flags:    []string{"DISKLESS", "TIE_BREAKER"},
				Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024}},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "2", "address": "10.0.0.3", "minor": "1000",
				},
			}

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if i := slices.IndexFunc(fx.CommandLines(), func(cmd string) bool {
				return strings.HasPrefix(cmd, "drbdadm detach")
			}); i >= 0 {
				t.Errorf("detached a replica with no disk attached: %q; commands:\n%v", fx.CommandLines()[i], fx.CommandLines())
			}
		})
	}
}
