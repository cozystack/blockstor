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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestParseRoleRoundTrip — every Role variant parses from its wire
// string and stringifies back to the same value.
func TestParseRoleRoundTrip(t *testing.T) {
	cases := []drbd.Role{
		drbd.RoleUnknown,
		drbd.RolePrimary,
		drbd.RoleSecondary,
	}
	for _, want := range cases {
		got, err := drbd.ParseRole(string(want))
		if err != nil {
			t.Errorf("ParseRole(%q): unexpected error: %v", want, err)

			continue
		}

		if got != want {
			t.Errorf("ParseRole(%q) = %q, want %q", want, got, want)
		}

		if got.String() != string(want) {
			t.Errorf("Role(%q).String() = %q, want %q", want, got.String(), want)
		}
	}
}

// TestRolePredicates — every Role variant against every predicate.
func TestRolePredicates(t *testing.T) {
	cases := []struct {
		role        drbd.Role
		isPrimary   bool
		isSecondary bool
		isKnown     bool
	}{
		{drbd.RoleUnknown, false, false, false},
		{drbd.RolePrimary, true, false, true},
		{drbd.RoleSecondary, false, true, true},
	}
	for _, tc := range cases {
		if got := tc.role.IsPrimary(); got != tc.isPrimary {
			t.Errorf("%s.IsPrimary() = %v, want %v", tc.role, got, tc.isPrimary)
		}

		if got := tc.role.IsSecondary(); got != tc.isSecondary {
			t.Errorf("%s.IsSecondary() = %v, want %v", tc.role, got, tc.isSecondary)
		}

		if got := tc.role.IsKnown(); got != tc.isKnown {
			t.Errorf("%s.IsKnown() = %v, want %v", tc.role, got, tc.isKnown)
		}
	}
}

// TestParseDiskStateRoundTrip — every DiskState variant round-trips
// through ParseDiskState / String.
func TestParseDiskStateRoundTrip(t *testing.T) {
	cases := []drbd.DiskState{
		drbd.DiskStateDiskless,
		drbd.DiskStateAttaching,
		drbd.DiskStateDetaching,
		drbd.DiskStateFailed,
		drbd.DiskStateNegotiating,
		drbd.DiskStateInconsistent,
		drbd.DiskStateOutdated,
		drbd.DiskStateDUnknown,
		drbd.DiskStateConsistent,
		drbd.DiskStateUpToDate,
	}
	for _, want := range cases {
		got, err := drbd.ParseDiskState(string(want))
		if err != nil {
			t.Errorf("ParseDiskState(%q): unexpected error: %v", want, err)

			continue
		}

		if got != want {
			t.Errorf("ParseDiskState(%q) = %q, want %q", want, got, want)
		}

		if got.String() != string(want) {
			t.Errorf("DiskState(%q).String() = %q, want %q", want, got.String(), want)
		}
	}
}

// TestDiskStatePredicates — exhaustive truth table for DiskState
// predicates.
func TestDiskStatePredicates(t *testing.T) {
	cases := []struct {
		state        drbd.DiskState
		terminalGood bool
		transient    bool
		intervention bool
		diskless     bool
	}{
		{drbd.DiskStateDiskless, false, false, false, true},
		{drbd.DiskStateAttaching, false, true, false, false},
		{drbd.DiskStateDetaching, false, true, false, false},
		{drbd.DiskStateFailed, false, false, true, false},
		{drbd.DiskStateNegotiating, false, true, false, false},
		{drbd.DiskStateInconsistent, false, false, false, false},
		{drbd.DiskStateOutdated, false, false, false, false},
		{drbd.DiskStateDUnknown, false, false, false, false},
		{drbd.DiskStateConsistent, false, false, false, false},
		{drbd.DiskStateUpToDate, true, false, false, false},
	}
	for _, tc := range cases {
		if got := tc.state.IsTerminalGood(); got != tc.terminalGood {
			t.Errorf("%s.IsTerminalGood() = %v, want %v", tc.state, got, tc.terminalGood)
		}

		if got := tc.state.IsTransient(); got != tc.transient {
			t.Errorf("%s.IsTransient() = %v, want %v", tc.state, got, tc.transient)
		}

		if got := tc.state.RequiresIntervention(); got != tc.intervention {
			t.Errorf("%s.RequiresIntervention() = %v, want %v", tc.state, got, tc.intervention)
		}

		if got := tc.state.IsDiskless(); got != tc.diskless {
			t.Errorf("%s.IsDiskless() = %v, want %v", tc.state, got, tc.diskless)
		}
	}
}

// TestParseConnectionStateRoundTrip — every ConnectionState variant
// round-trips through ParseConnectionState / String.
func TestParseConnectionStateRoundTrip(t *testing.T) {
	cases := []drbd.ConnectionState{
		drbd.ConnectionStateStandAlone,
		drbd.ConnectionStateDisconnecting,
		drbd.ConnectionStateUnconnected,
		drbd.ConnectionStateTimeout,
		drbd.ConnectionStateBrokenPipe,
		drbd.ConnectionStateNetworkFailure,
		drbd.ConnectionStateProtocolError,
		drbd.ConnectionStateTearDown,
		drbd.ConnectionStateConnecting,
		drbd.ConnectionStateConnected,
	}
	for _, want := range cases {
		got, err := drbd.ParseConnectionState(string(want))
		if err != nil {
			t.Errorf("ParseConnectionState(%q): unexpected error: %v", want, err)

			continue
		}

		if got != want {
			t.Errorf("ParseConnectionState(%q) = %q, want %q", want, got, want)
		}

		if got.String() != string(want) {
			t.Errorf("ConnectionState(%q).String() = %q, want %q", want, got.String(), want)
		}
	}
}

// TestConnectionStatePredicates — exhaustive truth table.
func TestConnectionStatePredicates(t *testing.T) {
	cases := []struct {
		state          drbd.ConnectionState
		terminalGood   bool
		transient      bool
		intervention   bool
		transportError bool
	}{
		{drbd.ConnectionStateStandAlone, false, false, true, false},
		{drbd.ConnectionStateDisconnecting, false, true, false, false},
		{drbd.ConnectionStateUnconnected, false, true, false, false},
		{drbd.ConnectionStateTimeout, false, false, false, true},
		{drbd.ConnectionStateBrokenPipe, false, false, false, true},
		{drbd.ConnectionStateNetworkFailure, false, false, false, true},
		{drbd.ConnectionStateProtocolError, false, false, true, false},
		{drbd.ConnectionStateTearDown, false, true, false, false},
		{drbd.ConnectionStateConnecting, false, true, false, false},
		{drbd.ConnectionStateConnected, true, false, false, false},
	}
	for _, tc := range cases {
		if got := tc.state.IsTerminalGood(); got != tc.terminalGood {
			t.Errorf("%s.IsTerminalGood() = %v, want %v", tc.state, got, tc.terminalGood)
		}

		if got := tc.state.IsTransient(); got != tc.transient {
			t.Errorf("%s.IsTransient() = %v, want %v", tc.state, got, tc.transient)
		}

		if got := tc.state.RequiresIntervention(); got != tc.intervention {
			t.Errorf("%s.RequiresIntervention() = %v, want %v", tc.state, got, tc.intervention)
		}

		if got := tc.state.IsTransportError(); got != tc.transportError {
			t.Errorf("%s.IsTransportError() = %v, want %v", tc.state, got, tc.transportError)
		}
	}
}

// TestParseReplicationStateRoundTrip — every ReplicationState variant
// round-trips through ParseReplicationState / String.
func TestParseReplicationStateRoundTrip(t *testing.T) {
	cases := []drbd.ReplicationState{
		drbd.ReplicationStateOff,
		drbd.ReplicationStateEstablished,
		drbd.ReplicationStateStartingSyncS,
		drbd.ReplicationStateStartingSyncT,
		drbd.ReplicationStateWFBitMapS,
		drbd.ReplicationStateWFBitMapT,
		drbd.ReplicationStateWFSyncUUID,
		drbd.ReplicationStateSyncSource,
		drbd.ReplicationStateSyncTarget,
		drbd.ReplicationStateVerifyS,
		drbd.ReplicationStateVerifyT,
		drbd.ReplicationStatePausedSyncS,
		drbd.ReplicationStatePausedSyncT,
		drbd.ReplicationStateAhead,
		drbd.ReplicationStateBehind,
	}
	for _, want := range cases {
		got, err := drbd.ParseReplicationState(string(want))
		if err != nil {
			t.Errorf("ParseReplicationState(%q): unexpected error: %v", want, err)

			continue
		}

		if got != want {
			t.Errorf("ParseReplicationState(%q) = %q, want %q", want, got, want)
		}

		if got.String() != string(want) {
			t.Errorf("ReplicationState(%q).String() = %q, want %q", want, got.String(), want)
		}
	}
}

// TestReplicationStatePredicates — exhaustive truth table.
func TestReplicationStatePredicates(t *testing.T) {
	cases := []struct {
		state        drbd.ReplicationState
		terminalGood bool
		syncing      bool
		syncSource   bool
		syncTarget   bool
		paused       bool
		verifying    bool
		transient    bool
		intervention bool
	}{
		{drbd.ReplicationStateOff, false, false, false, false, false, false, false, true},
		{drbd.ReplicationStateEstablished, true, false, false, false, false, false, false, false},
		{drbd.ReplicationStateStartingSyncS, false, false, true, false, false, false, true, false},
		{drbd.ReplicationStateStartingSyncT, false, false, false, true, false, false, true, false},
		{drbd.ReplicationStateWFBitMapS, false, false, false, false, false, false, true, false},
		{drbd.ReplicationStateWFBitMapT, false, false, false, false, false, false, true, false},
		{drbd.ReplicationStateWFSyncUUID, false, false, false, false, false, false, true, false},
		{drbd.ReplicationStateSyncSource, false, true, true, false, false, false, true, false},
		{drbd.ReplicationStateSyncTarget, false, true, false, true, false, false, true, false},
		{drbd.ReplicationStateVerifyS, false, false, false, false, false, true, true, false},
		{drbd.ReplicationStateVerifyT, false, false, false, false, false, true, true, false},
		{drbd.ReplicationStatePausedSyncS, false, false, true, false, true, false, true, false},
		{drbd.ReplicationStatePausedSyncT, false, false, false, true, true, false, true, false},
		{drbd.ReplicationStateAhead, false, false, false, false, false, false, true, false},
		{drbd.ReplicationStateBehind, false, false, false, false, false, false, true, false},
	}
	for _, tc := range cases {
		if got := tc.state.IsTerminalGood(); got != tc.terminalGood {
			t.Errorf("%s.IsTerminalGood() = %v, want %v", tc.state, got, tc.terminalGood)
		}

		if got := tc.state.IsSyncing(); got != tc.syncing {
			t.Errorf("%s.IsSyncing() = %v, want %v", tc.state, got, tc.syncing)
		}

		if got := tc.state.IsSyncSource(); got != tc.syncSource {
			t.Errorf("%s.IsSyncSource() = %v, want %v", tc.state, got, tc.syncSource)
		}

		if got := tc.state.IsSyncTarget(); got != tc.syncTarget {
			t.Errorf("%s.IsSyncTarget() = %v, want %v", tc.state, got, tc.syncTarget)
		}

		if got := tc.state.IsPaused(); got != tc.paused {
			t.Errorf("%s.IsPaused() = %v, want %v", tc.state, got, tc.paused)
		}

		if got := tc.state.IsVerifying(); got != tc.verifying {
			t.Errorf("%s.IsVerifying() = %v, want %v", tc.state, got, tc.verifying)
		}

		if got := tc.state.IsTransient(); got != tc.transient {
			t.Errorf("%s.IsTransient() = %v, want %v", tc.state, got, tc.transient)
		}

		if got := tc.state.RequiresIntervention(); got != tc.intervention {
			t.Errorf("%s.RequiresIntervention() = %v, want %v", tc.state, got, tc.intervention)
		}
	}
}

// TestParseUnknownStateReturnsError — Parse* helpers reject unknown
// kernel strings rather than silently returning a zero value.
func TestParseUnknownStateReturnsError(t *testing.T) {
	if _, err := drbd.ParseRole("BogusRole"); err == nil {
		t.Error("ParseRole(\"BogusRole\"): want error, got nil")
	}

	if _, err := drbd.ParseDiskState("BogusDisk"); err == nil {
		t.Error("ParseDiskState(\"BogusDisk\"): want error, got nil")
	}

	if _, err := drbd.ParseConnectionState("BogusConn"); err == nil {
		t.Error("ParseConnectionState(\"BogusConn\"): want error, got nil")
	}

	if _, err := drbd.ParseReplicationState("BogusRepl"); err == nil {
		t.Error("ParseReplicationState(\"BogusRepl\"): want error, got nil")
	}

	if _, err := drbd.ParseRole(""); err == nil {
		t.Error("ParseRole(\"\"): want error, got nil")
	}
}
