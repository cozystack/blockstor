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

package v1

import "fmt"

// SyncProgress returns how far this volume's resync in the given
// replication state (`SyncTarget`, `SyncSource`) has come, from the
// peers' done percentages. ok is false when no peer in that state
// reported one.
//
// The value is DRBD's own `done`, measured against the resync's total,
// not anything derived from the volume size: a replica that resyncs
// from two peers in turn has one out-of-sync count per peer, and only
// the per-peer progress says how far the running resync is. When more
// than one peer is in the state, the lowest wins, so the figure is the
// part that is still furthest from done.
func (s VolumeState) SyncProgress(replicationState string) (float64, bool) {
	var (
		lowest float64
		found  bool
	)

	for _, peer := range s.ReplicationStates {
		if peer.ReplicationState != replicationState || peer.DonePercentage == nil {
			continue
		}

		if !found || *peer.DonePercentage < lowest {
			lowest = *peer.DonePercentage
			found = true
		}
	}

	return lowest, found
}

// SyncProgressState renders a state with its resync progress the way
// LINSTOR's satellite does for a SyncTarget (DrbdVolume.diskStateInfo):
// `SyncTarget(50.31%)`, two decimals, as drbdsetup prints `done`.
func SyncProgressState(state string, done float64) string {
	return fmt.Sprintf("%s(%.2f%%)", state, done)
}
