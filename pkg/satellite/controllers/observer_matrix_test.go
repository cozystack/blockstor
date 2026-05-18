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

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestObserverStateMachineMatrix is the closed-loop recovery state-
// matrix table that pins (DiskState x ConnectionState x ReplicationState
// x SkipDisk) → wantReconnect for the observer's auto-recovery path.
//
// The matrix codifies the Bug 312 contract: the observer ONLY fires
// `drbdadm disconnect/connect` when the kernel can't self-heal AND the
// operator hasn't claimed the slot via SkipDisk. Every other cell must
// stay silent — false-positive reconnects roll resync edges back and
// re-introduce Bug 8's failure mode.
//
// Cases mirror the Bug-314 acceptance grid:
//
//   - StandAlone + UpToDate                       → reconnect
//   - StandAlone + Diskless                       → reconnect (still a
//     wedged peer; observer can't tell tiebreaker from regular diskless
//     here, but the gate is "kernel said StandAlone, operator wants it
//     up", and SkipDisk overrides if the operator disagrees)
//   - StandAlone + UpToDate + SkipDisk            → no reconnect (operator
//     intent gate fires)
//   - Connected + Established + UpToDate          → no reconnect (steady-
//     state, terminal-good)
//   - Connecting + Outdated                       → no reconnect (kernel
//     mid-handshake; closed-loop fires off StandAlone, not Connecting)
//   - BrokenPipe + UpToDate                       → no reconnect (kernel
//     auto-recovers to Connecting → Connected without operator help; only
//     StandAlone breaks that self-healing chain)
//   - Connected + Diskless (tiebreaker)           → no reconnect (terminal-
//     good for a tiebreaker replica, no disk to consult)
//
// Stuck-SyncTarget cases (a separate watchdog driven by resyncOnce) live
// in their own test below because they need clock advancement, not just
// one-shot frame translation.
func TestObserverStateMachineMatrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		diskState        string
		connectionState  string
		replicationState string
		skipDisk         bool
		wantReconnect    bool
	}{
		{
			name:            "StandAlone + UpToDate => reconnect",
			diskState:       "UpToDate",
			connectionState: drbdStateStandAlone,
			wantReconnect:   true,
		},
		{
			name:            "StandAlone + Diskless => reconnect (kernel needs verb)",
			diskState:       drbdDiskStateDiskless,
			connectionState: drbdStateStandAlone,
			wantReconnect:   true,
		},
		{
			name:            "StandAlone + UpToDate + SkipDisk => NO reconnect",
			diskState:       "UpToDate",
			connectionState: drbdStateStandAlone,
			skipDisk:        true,
			wantReconnect:   false,
		},
		{
			name:             "Connected + Established + UpToDate => steady-state, NO reconnect",
			diskState:        "UpToDate",
			connectionState:  drbdStateConnected,
			replicationState: "Established",
			wantReconnect:    false,
		},
		{
			name:            "Connecting + Outdated => kernel mid-handshake, NO reconnect",
			diskState:       "Outdated",
			connectionState: "Connecting",
			wantReconnect:   false,
		},
		{
			name:            "BrokenPipe + UpToDate => kernel self-heals, NO reconnect",
			diskState:       "UpToDate",
			connectionState: "BrokenPipe",
			wantReconnect:   false,
		},
		{
			name:             "Connected + Established + Diskless (tiebreaker) => NO reconnect",
			diskState:        drbdDiskStateDiskless,
			connectionState:  drbdStateConnected,
			replicationState: "Established",
			wantReconnect:    false,
		},
		{
			name:            "NetworkFailure + UpToDate => transient, kernel self-heals, NO reconnect",
			diskState:       "UpToDate",
			connectionState: "NetworkFailure",
			wantReconnect:   false,
		},
		{
			name:            "Timeout + UpToDate => transient, kernel self-heals, NO reconnect",
			diskState:       "UpToDate",
			connectionState: "Timeout",
			wantReconnect:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = blockstoriov1alpha1.AddToScheme(scheme)

			existing := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-m.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "pvc-m",
					NodeName:               "n1",
				},
			}

			if tc.skipDisk {
				existing.Spec.Props = map[string]string{
					skipDiskPropKey: skipDiskPropValue,
				}
			}

			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(existing).
				Build()

			fx := storage.NewFakeExec()

			o := &ObserverRunnable{
				Client:   cli,
				Exec:     fx,
				NodeName: "n1",
			}
			adm := drbd.NewAdm(fx)

			ev := &observation{
				ResourceName: "pvc-m",
				DrbdState:    tc.diskState,
				Volumes: []volumeObservation{
					{VolumeNumber: 0, DiskState: tc.diskState},
				},
				Connections: []connectionObservation{{
					PeerNodeName:     "n2",
					Connected:        tc.connectionState == drbdStateConnected,
					Message:          tc.connectionState,
					ReplicationState: tc.replicationState,
				}},
			}

			o.handleObservation(context.Background(), adm, ev)

			gotConnect := cmdSeen(fx.CommandLines(), "drbdadm connect pvc-m:n2")
			if gotConnect != tc.wantReconnect {
				t.Errorf("reconnect verb seen: got %v, want %v (cmds=%v)",
					gotConnect, tc.wantReconnect, fx.CommandLines())
			}

			// disconnect is best-effort; it always pairs with connect
			// in the reconnect cycle. If reconnect must NOT fire, then
			// disconnect must also stay silent — otherwise we'd quiesce
			// a healthy slot.
			gotDisconnect := cmdSeen(fx.CommandLines(), "drbdadm disconnect pvc-m:n2")
			if !tc.wantReconnect && gotDisconnect {
				t.Errorf("disconnect fired without reconnect intent (cmds=%v)",
					fx.CommandLines())
			}
		})
	}
}

// TestObserverStuckSyncTargetMatrix pins the resync-watchdog state matrix:
// (ReplicationState, OutOfSyncKib trajectory, time-elapsed) → reconnect.
// resyncOnce only escalates when ALL three conditions hold:
//
//  1. Replication is in a sync-receiving state (SyncTarget).
//  2. OutOfSyncKib > 0 (zero = sync complete, nothing to drive).
//  3. lastProgressAt is older than syncStallThreshold.
//
// Any other combination must stay silent — the watchdog fires from
// resyncOnce's tick, so a false-positive here would tear down healthy
// resyncs without any user-visible trigger.
func TestObserverStuckSyncTargetMatrix(t *testing.T) {
	t.Parallel()

	type seed struct {
		oos              int64
		replicationState string
	}

	cases := []struct {
		name          string
		first         seed  // initial peer-device frame state
		second        *seed // optional follow-up frame
		secondAfter   time.Duration
		probeAfter    time.Duration
		wantReconnect bool
	}{
		{
			name:          "SyncTarget + flat OoS past threshold => reconnect",
			first:         seed{oos: 524288, replicationState: drbdReplStateSyncTarget},
			probeAfter:    syncStallThreshold + time.Second,
			wantReconnect: true,
		},
		{
			name:          "SyncTarget + decreasing OoS resets watchdog => NO reconnect",
			first:         seed{oos: 524288, replicationState: drbdReplStateSyncTarget},
			second:        &seed{oos: 262144, replicationState: drbdReplStateSyncTarget},
			secondAfter:   syncStallThreshold / 2,
			probeAfter:    syncStallThreshold + time.Second,
			wantReconnect: false,
		},
		{
			name:          "Established + zero OoS => NO reconnect (sync complete)",
			first:         seed{oos: 0, replicationState: "Established"},
			probeAfter:    syncStallThreshold * 2,
			wantReconnect: false,
		},
		{
			name:          "SyncTarget + OoS zero => NO reconnect (transient sync-complete edge)",
			first:         seed{oos: 0, replicationState: drbdReplStateSyncTarget},
			probeAfter:    syncStallThreshold * 2,
			wantReconnect: false,
		},
		{
			name:          "PausedSyncT + flat OoS => NO reconnect (dependency-paused, not stuck)",
			first:         seed{oos: 524288, replicationState: "PausedSyncT"},
			probeAfter:    syncStallThreshold * 2,
			wantReconnect: false,
		},
		{
			name:          "SyncTarget + flat OoS BEFORE threshold => NO reconnect",
			first:         seed{oos: 524288, replicationState: drbdReplStateSyncTarget},
			probeAfter:    syncStallThreshold / 2,
			wantReconnect: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = blockstoriov1alpha1.AddToScheme(scheme)

			existing := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-w.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "pvc-w",
					NodeName:               "n1",
				},
			}

			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(existing).
				Build()

			fx := storage.NewFakeExec()

			frozen := time.Unix(1_700_000_000, 0)
			now := frozen
			o := &ObserverRunnable{
				Client:   cli,
				Exec:     fx,
				NodeName: "n1",
				Now:      func() time.Time { return now },
			}
			adm := drbd.NewAdm(fx)

			// Frame 1: seed cache.
			o.handleObservation(context.Background(), adm, &observation{
				ResourceName: "pvc-w",
				Connections: []connectionObservation{{
					PeerNodeName:     "n2",
					ReplicationState: tc.first.replicationState,
				}},
				Volumes: []volumeObservation{{
					VolumeNumber: 0,
					OutOfSyncKib: tc.first.oos,
					HasSync:      true,
				}},
			})

			if tc.second != nil {
				now = frozen.Add(tc.secondAfter)
				o.handleObservation(context.Background(), adm, &observation{
					ResourceName: "pvc-w",
					Connections: []connectionObservation{{
						PeerNodeName:     "n2",
						ReplicationState: tc.second.replicationState,
					}},
					Volumes: []volumeObservation{{
						VolumeNumber: 0,
						OutOfSyncKib: tc.second.oos,
						HasSync:      true,
					}},
				})
			}

			now = frozen.Add(tc.probeAfter)
			o.resyncOnce(context.Background(), adm, logr.Discard())

			got := cmdSeen(fx.CommandLines(), "drbdadm connect pvc-w:n2")
			if got != tc.wantReconnect {
				t.Errorf("watchdog reconnect: got %v, want %v (cmds=%v)",
					got, tc.wantReconnect, fx.CommandLines())
			}
		})
	}
}

// TestObserverStandAloneRecoveryFullCycle pins the closed-loop:
// StandAlone → reconnect → Connecting (suppressed by cooldown) →
// Connected (cooldown-clear by time) → second StandAlone → second
// reconnect. Mirrors a real reconnect flap on the wire: events2 emits
// many transitional frames during a single recovery, and the observer
// must fire exactly once per cooldown window.
func TestObserverStandAloneRecoveryFullCycle(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-cycle.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-cycle",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	fx := storage.NewFakeExec()

	frozen := time.Unix(1_700_000_000, 0)
	now := frozen
	o := &ObserverRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		Now:      func() time.Time { return now },
	}
	adm := drbd.NewAdm(fx)

	standAlone := func() *observation {
		return &observation{
			ResourceName: "pvc-cycle",
			Connections: []connectionObservation{{
				PeerNodeName: "n2", Message: drbdStateStandAlone,
			}},
		}
	}

	// 1. First StandAlone frame → reconnect fires.
	o.handleObservation(context.Background(), adm, standAlone())
	if countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2") != 1 {
		t.Fatalf("frame-1: reconnect count = %d, want 1",
			countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2"))
	}

	// 2. Transitional Connecting frame within cooldown → silent.
	now = frozen.Add(2 * time.Second)
	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-cycle",
		Connections: []connectionObservation{{
			PeerNodeName: "n2", Message: "Connecting",
		}},
	})
	if countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2") != 1 {
		t.Errorf("Connecting frame should not fire reconnect: count = %d",
			countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2"))
	}

	// 3. Connected frame within cooldown → silent (no StandAlone token).
	now = frozen.Add(4 * time.Second)
	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-cycle",
		Connections: []connectionObservation{{
			PeerNodeName: "n2", Connected: true, Message: drbdStateConnected,
		}},
	})
	if countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2") != 1 {
		t.Errorf("Connected frame fired reconnect: count = %d",
			countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2"))
	}

	// 4. Another StandAlone still within cooldown → silent (cooldown
	// gate, NOT because the peer is now Connected — the gate is purely
	// time-based per (resource, peer)).
	now = frozen.Add(reconnectCooldownInterval - time.Second)
	o.handleObservation(context.Background(), adm, standAlone())
	if countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2") != 1 {
		t.Errorf("StandAlone within cooldown: count = %d, want 1",
			countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2"))
	}

	// 5. Advance past the cooldown; a fresh StandAlone fires again.
	now = frozen.Add(reconnectCooldownInterval + time.Second)
	o.handleObservation(context.Background(), adm, standAlone())
	if countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2") != 2 {
		t.Errorf("post-cooldown StandAlone: count = %d, want 2 (cooldown failed to release)",
			countCommand(fx.CommandLines(), "drbdadm connect pvc-cycle:n2"))
	}
}

// TestObserverStuckSyncTargetEscalation pins the resync-watchdog's
// progress-tracking sequence: three frames with the SAME out-of-sync
// counter (no progress) MUST trip the watchdog after syncStallThreshold,
// not before. The kernel's events2 statistics tick emits ~1Hz; a flat
// counter for 20s is real-world evidence of a wedged SyncSource peer.
func TestObserverStuckSyncTargetEscalation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-esc.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-esc",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	fx := storage.NewFakeExec()

	frozen := time.Unix(1_700_000_000, 0)
	now := frozen
	o := &ObserverRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		Now:      func() time.Time { return now },
	}
	adm := drbd.NewAdm(fx)

	mkFrame := func() *observation {
		return &observation{
			ResourceName: "pvc-esc",
			Connections: []connectionObservation{{
				PeerNodeName: "n2", ReplicationState: drbdReplStateSyncTarget,
			}},
			Volumes: []volumeObservation{{
				VolumeNumber: 0, OutOfSyncKib: 524288, HasSync: true,
			}},
		}
	}

	// Three identical samples within the stall threshold — no progress,
	// but threshold hasn't elapsed yet.
	for i := range 3 {
		now = frozen.Add(time.Duration(i) * 5 * time.Second)
		o.handleObservation(context.Background(), adm, mkFrame())
	}

	// Probe at t = 15s — STILL within threshold (20s). Watchdog must
	// stay silent.
	now = frozen.Add(15 * time.Second)
	o.resyncOnce(context.Background(), adm, logr.Discard())

	if cmdSeen(fx.CommandLines(), "drbdadm connect pvc-esc:n2") {
		t.Errorf("watchdog fired prematurely at t=15s (cmds=%v)", fx.CommandLines())
	}

	// Probe at t = 21s — past threshold. Watchdog must now escalate.
	now = frozen.Add(syncStallThreshold + time.Second)
	o.resyncOnce(context.Background(), adm, logr.Discard())

	if !cmdSeen(fx.CommandLines(), "drbdadm connect pvc-esc:n2") {
		t.Errorf("watchdog failed to escalate after stall threshold (cmds=%v)",
			fx.CommandLines())
	}
}
