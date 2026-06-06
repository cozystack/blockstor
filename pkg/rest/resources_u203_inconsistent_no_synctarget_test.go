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

package rest

import (
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestU203InconsistentWithoutSyncNeverShowsSyncTarget pins the upstream
// LINSTOR user report U203 ("state truth"): an Inconsistent replica that
// is NOT actively receiving data must NOT be reported as
// SyncTarget(NN%). The `SyncTarget` literal is the kernel-truth signal
// "this replica is currently pulling data from a peer". A replica that is
// merely Inconsistent (created, never seeded, no peer connected to sync
// from — e.g. a stalled / quorum-blocked / waiting-for-peer replica)
// carries NO SyncTarget/SyncSource token in its per-peer
// ReplicationStates, so annotateSyncProgress must never synthesise the
// `SyncTarget` literal for it. Doing so would lie to the operator that a
// resync is in flight when nothing is moving.
//
// annotateSyncProgress only promotes to the `SyncTarget` literal when
// activeSyncReplicationState finds a peer actually reporting
// `SyncTarget` (see resources.go). Without that token it falls through to
// the legacy disk_state path, which keeps the real DiskState
// (`Inconsistent`) — optionally with a `(NN%)` progress suffix when
// OutOfSyncKib is known, but NEVER relabelled to `SyncTarget`.
func TestU203InconsistentWithoutSyncNeverShowsSyncTarget(t *testing.T) {
	t.Parallel()

	const sizeKib = int64(1024 * 1024) // 1 GiB
	sizes := map[int32]int64{0: sizeKib}

	cases := []struct {
		name string
		in   apiv1.Volume
		want string
	}{
		{
			// Stalled Inconsistent: large out-of-sync, but NO peer in a
			// sync replication state (e.g. waiting on a peer / quorum, no
			// active resync). Must stay Inconsistent, never SyncTarget.
			name: "inconsistent_stalled_no_replication_state_large_oos",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib, // 100% out-of-sync, nothing moving
				},
			},
			// Legacy progress path renders 0% → but withSyncPercent
			// returns max(0, 100-100) = 0, so the suffix is "(0%)". The
			// CRITICAL invariant is only that it is NOT a SyncTarget
			// literal; the exact legacy suffix is asserted by the
			// substring guard below, not this equality.
			want: "Inconsistent(0%)",
		},
		{
			// Inconsistent with a peer that is merely Established (the
			// peer is up but no resync is targeting THIS replica). The
			// large OutOfSyncKib must not be dressed as a SyncTarget.
			name: "inconsistent_peer_established_not_syncing",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib / 2,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-a": {ReplicationState: "Established"},
					},
				},
			},
			want: "Inconsistent(50%)",
		},
		{
			// Inconsistent with a peer in Connecting (link down): no
			// active sync, so no SyncTarget literal.
			name: "inconsistent_peer_connecting_link_down",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib / 4,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-a": {ReplicationState: "Connecting"},
					},
				},
			},
			want: "Inconsistent(75%)",
		},
		{
			// Inconsistent with no progress signal at all (OutOfSyncKib
			// unknown / 0): stays the bare label, never SyncTarget.
			name: "inconsistent_no_progress_signal_stays_bare",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: 0,
				},
			},
			want: "Inconsistent",
		},
		{
			// Positive control: a peer ACTUALLY reporting SyncTarget DOES
			// promote to the literal — proving the guard above is not
			// just suppressing everything.
			name: "inconsistent_peer_synctarget_does_promote",
			in: apiv1.Volume{
				VolumeNumber: 0,
				State: apiv1.VolumeState{
					DiskState:    "Inconsistent",
					OutOfSyncKib: sizeKib / 4,
					ReplicationStates: map[string]apiv1.ReplicationState{
						"peer-a": {ReplicationState: "SyncTarget"},
					},
				},
			},
			want: "SyncTarget(75%)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := annotateSyncProgress([]apiv1.Volume{c.in}, sizes)
			if len(got) != 1 {
				t.Fatalf("annotateSyncProgress returned %d volumes, want 1", len(got))
			}

			state := got[0].State.DiskState

			if state != c.want {
				t.Errorf("State.DiskState = %q, want %q (in=%+v)", state, c.want, c.in)
			}

			// The U203 invariant, asserted independently of the exact
			// legacy suffix: unless a peer genuinely reports SyncTarget,
			// the rendered State must NOT carry the SyncTarget literal.
			peerSyncing := c.name == "inconsistent_peer_synctarget_does_promote"
			if !peerSyncing && strings.HasPrefix(state, "SyncTarget") {
				t.Errorf("Inconsistent-without-sync rendered as %q — must NEVER "+
					"be promoted to the SyncTarget literal (U203)", state)
			}
		})
	}
}
