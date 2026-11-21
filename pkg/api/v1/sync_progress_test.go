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

package v1_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// The progress is the reported `done` of a peer in the asked state,
// the lowest when several are, and nothing at all otherwise: a peer
// in another state, or one in the state without a `done`, never
// supplies a figure.
func TestVolumeStateSyncProgress(t *testing.T) {
	t.Parallel()

	ptr := func(v float64) *float64 { return &v }

	state := apiv1.VolumeState{ReplicationStates: map[string]apiv1.ReplicationState{
		"a": {ReplicationState: "SyncTarget", DonePercentage: ptr(80)},
		"b": {ReplicationState: "SyncTarget", DonePercentage: ptr(50.31)},
		"c": {ReplicationState: "SyncTarget"},
		"d": {ReplicationState: "PausedSyncT", DonePercentage: ptr(1)},
		"e": {ReplicationState: "Established"},
	}}

	done, ok := state.SyncProgress("SyncTarget")
	if !ok || done != 50.31 {
		t.Errorf("SyncProgress(SyncTarget) = %v, %v; want 50.31, true", done, ok)
	}

	if done, ok := state.SyncProgress("SyncSource"); ok {
		t.Errorf("SyncProgress(SyncSource) = %v, true; want no progress", done)
	}

	if got := apiv1.SyncProgressState("SyncTarget", done); got != "SyncTarget(50.31%)" {
		t.Errorf("SyncProgressState = %q, want %q", got, "SyncTarget(50.31%)")
	}
}

// A replica can show more than one peer in SyncTarget: one whose
// resync started and moved to another peer within a second, or an entry
// that kept an earlier frame, next to the resync that runs. The figure
// is the one the last progress frame reported (ResyncPeer), as LINSTOR
// shows it, not the lowest: a node reset left `r l --faulty` at
// `SyncTarget(0.00%)` for 40 minutes while the kernel said 65 %.
// Once that peer is no longer in the state, the lowest is taken again.
func TestVolumeStateSyncProgressFollowsTheResyncPeer(t *testing.T) {
	t.Parallel()

	ptr := func(v float64) *float64 { return &v }

	state := apiv1.VolumeState{
		ResyncPeer: "b",
		ReplicationStates: map[string]apiv1.ReplicationState{
			"a": {ReplicationState: "SyncTarget", DonePercentage: ptr(0)},
			"b": {ReplicationState: "SyncTarget", DonePercentage: ptr(65.53)},
		},
	}

	if done, ok := state.SyncProgress("SyncTarget"); !ok || done != 65.53 {
		t.Errorf("SyncProgress(SyncTarget) = %v, %v; want 65.53, true (the resync peer's)", done, ok)
	}

	state.ReplicationStates["b"] = apiv1.ReplicationState{ReplicationState: "PausedSyncT", DonePercentage: ptr(65.53)}

	if done, ok := state.SyncProgress("SyncTarget"); !ok || done != 0 {
		t.Errorf("SyncProgress(SyncTarget) with the resync peer paused = %v, %v; want 0, true", done, ok)
	}

	state.ResyncPeer = "gone"

	if done, ok := state.SyncProgress("SyncTarget"); !ok || done != 0 {
		t.Errorf("SyncProgress(SyncTarget) with an unknown resync peer = %v, %v; want 0, true", done, ok)
	}
}
