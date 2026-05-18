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

package drbd

// Typed enums for DRBD-9 protocol states. Values are the public,
// human-readable strings emitted by `drbdsetup events2` / `drbdadm
// status`; we keep the wire string as the underlying value so callers
// can compare against raw kernel output without converting first.
//
// The enums exist to catch unknown-state surprises at compile time and
// to centralise predicates (transient vs. intervention-required) that
// otherwise get scattered as ad-hoc string switches across the code
// base. Variant names are public DRBD-9 protocol concepts documented
// in the upstream drbd-utils source and LINBIT user guide; only the
// public names are reused here, not any implementation structure.
//
// The predicate switches below are exhaustive over every enum variant
// on purpose (no `default:` clause) so that adding a new variant in
// the future produces an `exhaustive` lint failure on every predicate
// that needs to classify it, rather than silently defaulting to false.

import "github.com/cockroachdb/errors"

// Role is the DRBD-9 resource role on a single node.
type Role string

// Role variants emitted by drbdsetup. "Unknown" is reported by the
// kernel before the first role negotiation completes.
const (
	RoleUnknown   Role = "Unknown"
	RolePrimary   Role = "Primary"
	RoleSecondary Role = "Secondary"
)

// String returns the wire representation of the role.
func (r Role) String() string { return string(r) }

// IsPrimary reports whether the role is Primary.
func (r Role) IsPrimary() bool { return r == RolePrimary }

// IsSecondary reports whether the role is Secondary.
func (r Role) IsSecondary() bool { return r == RoleSecondary }

// IsKnown reports whether the role has been negotiated (not Unknown).
func (r Role) IsKnown() bool {
	switch r {
	case RolePrimary, RoleSecondary:
		return true
	case RoleUnknown:
		return false
	}

	return false
}

// ParseRole parses a DRBD role string into a Role. Unknown strings
// return an error rather than a zero value so callers cannot silently
// drop new kernel variants.
func ParseRole(s string) (Role, error) {
	switch Role(s) {
	case RoleUnknown, RolePrimary, RoleSecondary:
		return Role(s), nil
	}

	return "", errors.Errorf("drbd: unknown role: %q", s)
}

// DiskState is the local volume disk state.
type DiskState string

// DiskState variants. Names match the strings emitted by the kernel.
const (
	DiskStateDiskless     DiskState = "Diskless"
	DiskStateAttaching    DiskState = "Attaching"
	DiskStateDetaching    DiskState = "Detaching"
	DiskStateFailed       DiskState = "Failed"
	DiskStateNegotiating  DiskState = "Negotiating"
	DiskStateInconsistent DiskState = "Inconsistent"
	DiskStateOutdated     DiskState = "Outdated"
	DiskStateDUnknown     DiskState = "DUnknown"
	DiskStateConsistent   DiskState = "Consistent"
	DiskStateUpToDate     DiskState = "UpToDate"
)

// String returns the wire representation of the disk state.
func (d DiskState) String() string { return string(d) }

// IsTerminalGood reports whether the disk is in the steady-state OK
// condition (UpToDate). A diskless tiebreaker is operationally fine
// but has no local disk to be "good", so it does not count here.
func (d DiskState) IsTerminalGood() bool { return d == DiskStateUpToDate }

// IsTransient reports whether the kernel will resolve the state
// without operator action. Attaching / Detaching / Negotiating are
// short-lived transitions; the resync states are reported via
// ReplicationState, not DiskState.
func (d DiskState) IsTransient() bool {
	switch d {
	case DiskStateAttaching, DiskStateDetaching, DiskStateNegotiating:
		return true
	case DiskStateDiskless, DiskStateFailed, DiskStateInconsistent,
		DiskStateOutdated, DiskStateDUnknown, DiskStateConsistent,
		DiskStateUpToDate:
		return false
	}

	return false
}

// RequiresIntervention reports whether the disk state indicates a
// condition the operator must clear (Failed local backing device).
// Inconsistent / Outdated are recoverable via resync once a peer
// reconnects, so they are *not* intervention-required.
func (d DiskState) RequiresIntervention() bool {
	switch d {
	case DiskStateFailed:
		return true
	case DiskStateDiskless, DiskStateAttaching, DiskStateDetaching,
		DiskStateNegotiating, DiskStateInconsistent, DiskStateOutdated,
		DiskStateDUnknown, DiskStateConsistent, DiskStateUpToDate:
		return false
	}

	return false
}

// IsDiskless reports whether this node has no local backing storage
// for the volume (e.g. a tiebreaker or a diskful-to-diskless toggle).
func (d DiskState) IsDiskless() bool { return d == DiskStateDiskless }

// ParseDiskState parses a DRBD disk state string. Unknown strings
// return an error.
func ParseDiskState(s string) (DiskState, error) {
	switch DiskState(s) {
	case DiskStateDiskless, DiskStateAttaching, DiskStateDetaching,
		DiskStateFailed, DiskStateNegotiating, DiskStateInconsistent,
		DiskStateOutdated, DiskStateDUnknown, DiskStateConsistent,
		DiskStateUpToDate:
		return DiskState(s), nil
	}

	return "", errors.Errorf("drbd: unknown disk state: %q", s)
}

// ConnectionState is the state of a connection to a single peer.
type ConnectionState string

// ConnectionState variants.
const (
	ConnectionStateStandAlone     ConnectionState = "StandAlone"
	ConnectionStateDisconnecting  ConnectionState = "Disconnecting"
	ConnectionStateUnconnected    ConnectionState = "Unconnected"
	ConnectionStateTimeout        ConnectionState = "Timeout"
	ConnectionStateBrokenPipe     ConnectionState = "BrokenPipe"
	ConnectionStateNetworkFailure ConnectionState = "NetworkFailure"
	ConnectionStateProtocolError  ConnectionState = "ProtocolError"
	ConnectionStateTearDown       ConnectionState = "TearDown"
	ConnectionStateConnecting     ConnectionState = "Connecting"
	ConnectionStateConnected      ConnectionState = "Connected"
)

// String returns the wire representation of the connection state.
func (c ConnectionState) String() string { return string(c) }

// IsTerminalGood reports steady-state OK (Connected to peer).
func (c ConnectionState) IsTerminalGood() bool { return c == ConnectionStateConnected }

// IsTransient reports whether the kernel will resolve the state on
// its own. Unconnected / Connecting are the normal "looking for peer"
// states; Disconnecting / TearDown are short-lived shutdown phases.
func (c ConnectionState) IsTransient() bool {
	switch c {
	case ConnectionStateUnconnected, ConnectionStateConnecting,
		ConnectionStateDisconnecting, ConnectionStateTearDown:
		return true
	case ConnectionStateStandAlone, ConnectionStateTimeout,
		ConnectionStateBrokenPipe, ConnectionStateNetworkFailure,
		ConnectionStateProtocolError, ConnectionStateConnected:
		return false
	}

	return false
}

// RequiresIntervention reports whether the connection is stuck in a
// state that needs operator action — typically a `drbdadm connect` or
// a split-brain resolution. StandAlone is the canonical example: the
// kernel will not retry on its own. ProtocolError likewise indicates
// a version / configuration mismatch the kernel cannot fix.
func (c ConnectionState) RequiresIntervention() bool {
	switch c {
	case ConnectionStateStandAlone, ConnectionStateProtocolError:
		return true
	case ConnectionStateDisconnecting, ConnectionStateUnconnected,
		ConnectionStateTimeout, ConnectionStateBrokenPipe,
		ConnectionStateNetworkFailure, ConnectionStateTearDown,
		ConnectionStateConnecting, ConnectionStateConnected:
		return false
	}

	return false
}

// IsTransportError reports whether the connection failed because of a
// transport-level error (broken socket, timeout, network failure).
// These are usually transient at the cluster level but the local DRBD
// will sit in this state until the network recovers.
func (c ConnectionState) IsTransportError() bool {
	switch c {
	case ConnectionStateTimeout, ConnectionStateBrokenPipe,
		ConnectionStateNetworkFailure:
		return true
	case ConnectionStateStandAlone, ConnectionStateDisconnecting,
		ConnectionStateUnconnected, ConnectionStateProtocolError,
		ConnectionStateTearDown, ConnectionStateConnecting,
		ConnectionStateConnected:
		return false
	}

	return false
}

// ParseConnectionState parses a DRBD connection state string. Unknown
// strings return an error.
func ParseConnectionState(s string) (ConnectionState, error) {
	switch ConnectionState(s) {
	case ConnectionStateStandAlone, ConnectionStateDisconnecting,
		ConnectionStateUnconnected, ConnectionStateTimeout,
		ConnectionStateBrokenPipe, ConnectionStateNetworkFailure,
		ConnectionStateProtocolError, ConnectionStateTearDown,
		ConnectionStateConnecting, ConnectionStateConnected:
		return ConnectionState(s), nil
	}

	return "", errors.Errorf("drbd: unknown connection state: %q", s)
}

// ReplicationState is the per-peer-device replication state.
type ReplicationState string

// ReplicationState variants.
const (
	ReplicationStateOff           ReplicationState = "Off"
	ReplicationStateEstablished   ReplicationState = "Established"
	ReplicationStateStartingSyncS ReplicationState = "StartingSyncS"
	ReplicationStateStartingSyncT ReplicationState = "StartingSyncT"
	ReplicationStateWFBitMapS     ReplicationState = "WFBitMapS"
	ReplicationStateWFBitMapT     ReplicationState = "WFBitMapT"
	ReplicationStateWFSyncUUID    ReplicationState = "WFSyncUUID"
	ReplicationStateSyncSource    ReplicationState = "SyncSource"
	ReplicationStateSyncTarget    ReplicationState = "SyncTarget"
	ReplicationStateVerifyS       ReplicationState = "VerifyS"
	ReplicationStateVerifyT       ReplicationState = "VerifyT"
	ReplicationStatePausedSyncS   ReplicationState = "PausedSyncS"
	ReplicationStatePausedSyncT   ReplicationState = "PausedSyncT"
	ReplicationStateAhead         ReplicationState = "Ahead"
	ReplicationStateBehind        ReplicationState = "Behind"
)

// String returns the wire representation of the replication state.
func (r ReplicationState) String() string { return string(r) }

// IsTerminalGood reports steady-state OK (Established with peer, no
// resync in progress).
func (r ReplicationState) IsTerminalGood() bool { return r == ReplicationStateEstablished }

// IsSyncing reports whether a resync is actively running in either
// direction (excludes the WF* / Starting* preparation phases).
func (r ReplicationState) IsSyncing() bool {
	switch r {
	case ReplicationStateSyncSource, ReplicationStateSyncTarget:
		return true
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncS, ReplicationStateStartingSyncT,
		ReplicationStateWFBitMapS, ReplicationStateWFBitMapT,
		ReplicationStateWFSyncUUID, ReplicationStateVerifyS,
		ReplicationStateVerifyT, ReplicationStatePausedSyncS,
		ReplicationStatePausedSyncT, ReplicationStateAhead,
		ReplicationStateBehind:
		return false
	}

	return false
}

// IsSyncSource reports whether this node is sending resync data.
func (r ReplicationState) IsSyncSource() bool {
	switch r {
	case ReplicationStateSyncSource, ReplicationStateStartingSyncS,
		ReplicationStatePausedSyncS:
		return true
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncT, ReplicationStateWFBitMapS,
		ReplicationStateWFBitMapT, ReplicationStateWFSyncUUID,
		ReplicationStateSyncTarget, ReplicationStateVerifyS,
		ReplicationStateVerifyT, ReplicationStatePausedSyncT,
		ReplicationStateAhead, ReplicationStateBehind:
		return false
	}

	return false
}

// IsSyncTarget reports whether this node is receiving resync data.
func (r ReplicationState) IsSyncTarget() bool {
	switch r {
	case ReplicationStateSyncTarget, ReplicationStateStartingSyncT,
		ReplicationStatePausedSyncT:
		return true
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncS, ReplicationStateWFBitMapS,
		ReplicationStateWFBitMapT, ReplicationStateWFSyncUUID,
		ReplicationStateSyncSource, ReplicationStateVerifyS,
		ReplicationStateVerifyT, ReplicationStatePausedSyncS,
		ReplicationStateAhead, ReplicationStateBehind:
		return false
	}

	return false
}

// IsPaused reports whether a resync is paused (typically because
// another resync on a shared device has priority).
func (r ReplicationState) IsPaused() bool {
	switch r {
	case ReplicationStatePausedSyncS, ReplicationStatePausedSyncT:
		return true
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncS, ReplicationStateStartingSyncT,
		ReplicationStateWFBitMapS, ReplicationStateWFBitMapT,
		ReplicationStateWFSyncUUID, ReplicationStateSyncSource,
		ReplicationStateSyncTarget, ReplicationStateVerifyS,
		ReplicationStateVerifyT, ReplicationStateAhead,
		ReplicationStateBehind:
		return false
	}

	return false
}

// IsVerifying reports whether an online verify is running.
func (r ReplicationState) IsVerifying() bool {
	switch r {
	case ReplicationStateVerifyS, ReplicationStateVerifyT:
		return true
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncS, ReplicationStateStartingSyncT,
		ReplicationStateWFBitMapS, ReplicationStateWFBitMapT,
		ReplicationStateWFSyncUUID, ReplicationStateSyncSource,
		ReplicationStateSyncTarget, ReplicationStatePausedSyncS,
		ReplicationStatePausedSyncT, ReplicationStateAhead,
		ReplicationStateBehind:
		return false
	}

	return false
}

// IsTransient reports whether the kernel will progress on its own —
// every resync / verify / bitmap-exchange phase is transient in the
// sense that no operator action is required for it to complete.
func (r ReplicationState) IsTransient() bool {
	switch r {
	case ReplicationStateStartingSyncS, ReplicationStateStartingSyncT,
		ReplicationStateWFBitMapS, ReplicationStateWFBitMapT,
		ReplicationStateWFSyncUUID,
		ReplicationStateSyncSource, ReplicationStateSyncTarget,
		ReplicationStateVerifyS, ReplicationStateVerifyT,
		ReplicationStatePausedSyncS, ReplicationStatePausedSyncT,
		ReplicationStateAhead, ReplicationStateBehind:
		return true
	case ReplicationStateOff, ReplicationStateEstablished:
		return false
	}

	return false
}

// RequiresIntervention reports whether the replication state needs
// operator action. "Off" means replication is not running and only
// resumes after a (re)connect; the kernel will not start replication
// on its own from this state.
func (r ReplicationState) RequiresIntervention() bool {
	switch r {
	case ReplicationStateOff:
		return true
	case ReplicationStateEstablished, ReplicationStateStartingSyncS,
		ReplicationStateStartingSyncT, ReplicationStateWFBitMapS,
		ReplicationStateWFBitMapT, ReplicationStateWFSyncUUID,
		ReplicationStateSyncSource, ReplicationStateSyncTarget,
		ReplicationStateVerifyS, ReplicationStateVerifyT,
		ReplicationStatePausedSyncS, ReplicationStatePausedSyncT,
		ReplicationStateAhead, ReplicationStateBehind:
		return false
	}

	return false
}

// ParseReplicationState parses a DRBD replication state string.
// Unknown strings return an error.
func ParseReplicationState(s string) (ReplicationState, error) {
	switch ReplicationState(s) {
	case ReplicationStateOff, ReplicationStateEstablished,
		ReplicationStateStartingSyncS, ReplicationStateStartingSyncT,
		ReplicationStateWFBitMapS, ReplicationStateWFBitMapT,
		ReplicationStateWFSyncUUID,
		ReplicationStateSyncSource, ReplicationStateSyncTarget,
		ReplicationStateVerifyS, ReplicationStateVerifyT,
		ReplicationStatePausedSyncS, ReplicationStatePausedSyncT,
		ReplicationStateAhead, ReplicationStateBehind:
		return ReplicationState(s), nil
	}

	return "", errors.Errorf("drbd: unknown replication state: %q", s)
}
