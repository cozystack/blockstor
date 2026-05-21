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

// This file defines an explicit finite-state machine for the DRBD
// satellite reconciler. The current reconciler.go uses implicit
// branching; this file establishes vocabulary (phases, observations,
// transitions) as data, so the reconciler can be rewritten as a
// table-driven dispatcher in a follow-up (Phase 11.2.b).
//
// Nothing here imports reconciler.go; the symbols are unused by
// production code until 11.2.b wires them in.

package satellite

// DRBDPhase is the satellite-observed phase of a single Resource on
// the local node. Phase is a coarse partitioning of (kernel state,
// on-disk state, spec intent) into actionable buckets — each phase
// has a clear set of next-actions defined by the transition table
// below.
type DRBDPhase string

const (
	// PhaseUnprovisioned — no .res file, no metadata, no kernel
	// slot. Spec exists. First action: render .res.
	PhaseUnprovisioned DRBDPhase = "Unprovisioned"

	// PhaseMetadataPending — .res exists, but on-disk DRBD metadata
	// is absent. First action: drbdmeta create-md.
	PhaseMetadataPending DRBDPhase = "MetadataPending"

	// PhaseMetadataReady — metadata exists, kernel slot absent.
	// First action: drbdadm up.
	PhaseMetadataReady DRBDPhase = "MetadataReady"

	// PhaseRunning — kernel slot loaded. State (UpToDate/Diskless/
	// Inconsistent/etc.) is in observer Status; transitions handle
	// intra-Running shape changes (adjust, diskful flip, etc.).
	PhaseRunning DRBDPhase = "Running"

	// PhaseSkipDisk — operator-pinned (DrbdOptions/SkipDisk=True).
	// No-op until operator clears the prop.
	PhaseSkipDisk DRBDPhase = "SkipDisk"

	// PhaseDecommissioning — Resource has DeletionTimestamp. Final
	// actions: down, del-peer, forget-peer, remove .res.
	PhaseDecommissioning DRBDPhase = "Decommissioning"
)

// PeerDeletionIntent (342-v10) carries one peer's identity into
// ActionForgetPeer dispatch. Name is the peer node name; NodeID is
// the DRBD-9 node-id resolved from (in priority order)
// peer.Status.DRBDNodeID, kernel `drbdsetup show`, or the rendered
// .res file. NodeID is negative when no resolver succeeded — the
// FSM transition skips such peers this reconcile (next reconcile
// retries; node-id 0 is a valid DRBD slot id, so a separate
// sentinel is required).
type PeerDeletionIntent struct {
	Name   string
	NodeID int32
}

// Logical action names returned by Transition.Action. Kept as
// constants so consumers (and tests) can refer to them without
// string-literal duplication. Real wiring of action→func lives in
// Phase 11.2.b.
const (
	ActionRenderRes    = "renderRes"
	ActionCreateMd     = "createMd"
	ActionUp           = "up"
	ActionAdjust       = "adjust"
	ActionDecommission = "decommission"
	ActionNoop         = "noop"
	// ActionForgetPeer (342-v10) runs `drbdadm del-peer` +
	// `drbdmeta forget-peer` against every peer in
	// Observation.PeersDeleting and stamps a peer-forget ACK
	// annotation on the local Resource so the REST handler's
	// waitForPeerDeletionAcks loop can unblock. Triggered by the
	// peer carrying Spec.Flags=[DELETING] (REST 2-phase delete
	// Phase 1). Self-loop on PhaseRunning — kernel slot stays
	// loaded, only the per-peer state changes.
	ActionForgetPeer = "forgetPeer"
	// ActionAdjustSkipDisk runs `drbdadm adjust --skip-disk` for
	// operator-pinned resources (DrbdOptions/SkipDisk=True or
	// satellite-observer SkipDisk-stamp). The pin tells DRBD not to
	// attach the lower disk on this node, but net/peer config can
	// still drift and must be re-applied — a literal noop here would
	// freeze convergence inside the SkipDisk phase. Matches the
	// legacy `runAdjust` → `AdjustSkipDisk` branch (reconciler.go
	// L1661-1662).
	ActionAdjustSkipDisk = "adjustSkipDisk"
)

// Observation is the union of inputs to FSM trigger evaluation.
// Kept opaque here; consumers fill it in from kernel probe + Spec
// + Status reads. The FSM doesn't access apiserver — triggers are
// pure functions of Observation.
type Observation struct {
	// SpecHasResource — Resource object exists in apiserver.
	SpecHasResource bool
	// SpecFlagsHasDiskless — Spec carries the Diskless flag (this
	// node is intentionally a diskless peer).
	SpecFlagsHasDiskless bool
	// SpecHasDeletionTS — Resource has DeletionTimestamp set.
	SpecHasDeletionTS bool
	// ResFileExists — /etc/drbd.d/<res>.res is on disk.
	ResFileExists bool
	// KernelLoaded — drbdsetup reports a slot for this resource.
	KernelLoaded bool
	// KernelHasDiskless — kernel slot is currently Diskless (volume
	// has no backing disk). Bug 319 lives in this dimension.
	KernelHasDiskless bool
	// MetadataExists — on-disk DRBD metadata is present on the
	// backing device.
	MetadataExists bool
	// SkipDiskProp — operator pinned DrbdOptions/SkipDisk=True.
	SkipDiskProp bool
	// StatusDiskState — observer's last seen disk state
	// (UpToDate/Inconsistent/Diskless/…). Reserved for future
	// triggers; not consulted by current transitions.
	StatusDiskState string
	// PeersDeleting (342-v10) — peer Resources of the same RD
	// whose Spec.Flags contains DELETING (REST 2-phase delete
	// Phase 1 stamp). ActionForgetPeer iterates this list to run
	// del-peer + forget-peer per peer and stamps the per-peer ACK
	// annotation the REST handler waits for. Empty in steady
	// state; non-empty only during the few-second window between
	// REST's flag stamp and the physical Resources().Delete.
	PeersDeleting []PeerDeletionIntent
}

// Transition is one edge of the FSM. Triggers are pure functions of
// Observation. Action is the idempotent operation to apply when the
// edge fires; real implementation wiring lands in 11.2.b.
type Transition struct {
	// From — the source phase this transition applies to.
	From DRBDPhase
	// To — the destination phase after Action succeeds.
	To DRBDPhase
	// Trigger — pure predicate on Observation. No I/O.
	Trigger func(obs Observation) bool
	// Action — logical action name, e.g. ActionRenderRes,
	// ActionCreateMd, ActionUp, ActionAdjust, ActionDecommission,
	// ActionNoop.
	Action string
}

// fsm is the declarative transition table. The reconciler iterates
// this in order; the first transition whose From matches the
// current phase AND whose Trigger returns true fires its Action.
//
// Ordering matters: earlier rows shadow later ones. Decommission
// must come first so DeletionTimestamp wins over everything else.
//
//nolint:gochecknoglobals // declarative FSM table; intentionally a package-level fixture.
var fsm = []Transition{
	// Decommission supersedes everything when DeletionTimestamp is set.
	{From: PhaseRunning, To: PhaseDecommissioning, Trigger: func(obs Observation) bool { return obs.SpecHasDeletionTS }, Action: ActionDecommission},
	{From: PhaseMetadataReady, To: PhaseDecommissioning, Trigger: func(obs Observation) bool { return obs.SpecHasDeletionTS }, Action: ActionDecommission},
	{From: PhaseMetadataPending, To: PhaseDecommissioning, Trigger: func(obs Observation) bool { return obs.SpecHasDeletionTS }, Action: ActionDecommission},
	{From: PhaseUnprovisioned, To: PhaseDecommissioning, Trigger: func(obs Observation) bool { return obs.SpecHasDeletionTS }, Action: ActionDecommission},
	{From: PhaseSkipDisk, To: PhaseDecommissioning, Trigger: func(obs Observation) bool { return obs.SpecHasDeletionTS }, Action: ActionDecommission},

	// SkipDisk operator pin overrides Running's adjust. Placed
	// before the Running→Running adjust row so the pin wins.
	//
	// Action is ActionAdjustSkipDisk, NOT ActionNoop: the pin tells
	// DRBD not to attach the lower disk on this node, but net/peer
	// config still has to converge — drbdadm adjust --skip-disk is
	// the exact verb production runs for SkipDisk-pinned resources.
	// A literal no-op here would freeze drift convergence inside the
	// SkipDisk phase, matching the legacy `runAdjust` (reconciler.go
	// L1642-1665) which always dispatches AdjustSkipDisk under the
	// pin.
	{From: PhaseRunning, To: PhaseSkipDisk, Trigger: func(obs Observation) bool { return obs.SkipDiskProp }, Action: ActionAdjustSkipDisk},
	{From: PhaseSkipDisk, To: PhaseRunning, Trigger: func(obs Observation) bool { return !obs.SkipDiskProp }, Action: ActionAdjust},
	// SkipDisk self-loop: pin still set, kernel still loaded. Same
	// `adjust --skip-disk` verb as the entering-edge — keeps net/peer
	// drift converging while the disk stays detached.
	{From: PhaseSkipDisk, To: PhaseSkipDisk, Trigger: func(obs Observation) bool { return obs.SkipDiskProp && obs.KernelLoaded }, Action: ActionAdjustSkipDisk},

	// Initial provisioning chain.
	{From: PhaseUnprovisioned, To: PhaseMetadataPending, Trigger: func(obs Observation) bool { return obs.SpecHasResource && !obs.ResFileExists }, Action: ActionRenderRes},
	{From: PhaseMetadataPending, To: PhaseMetadataReady, Trigger: func(obs Observation) bool {
		return obs.ResFileExists && !obs.MetadataExists && !obs.SpecFlagsHasDiskless
	}, Action: ActionCreateMd},
	{From: PhaseMetadataReady, To: PhaseRunning, Trigger: func(obs Observation) bool { return obs.MetadataExists && !obs.KernelLoaded }, Action: ActionUp},

	// Within-Running transitions.
	//
	// Diskless→diskful flag flip (Bug 319): kernel currently has a
	// Diskless volume, but Spec is now diskful and metadata is
	// missing. We must drop back to MetadataPending to lay down
	// fresh metadata before adjusting.
	{From: PhaseRunning, To: PhaseMetadataPending, Trigger: func(obs Observation) bool {
		return obs.KernelLoaded && obs.KernelHasDiskless && !obs.SpecFlagsHasDiskless && !obs.MetadataExists
	}, Action: ActionCreateMd},
	// 342-v10 ForgetPeer (PhaseRunning self-loop with side-effect):
	// peer Resources of the same RD carry Spec.Flags=[DELETING] —
	// REST 2-phase delete Phase 1 has stamped them but not yet
	// physically removed. Satellite must run del-peer +
	// forget-peer per peer and stamp the peer-forget ACK
	// annotation BEFORE the new incarnation lands and attempts a
	// DRBD handshake against the stale kernel slot.
	//
	// Placed BEFORE the PhaseRunning → PhaseRunning ActionAdjust
	// self-loop because NextTransition returns the first match.
	// Guard: KernelLoaded — del-peer / forget-peer are no-ops if
	// the kernel slot isn't even loaded (no peer to forget).
	{From: PhaseRunning, To: PhaseRunning, Trigger: func(obs Observation) bool {
		return obs.KernelLoaded && len(obs.PeersDeleting) > 0
	}, Action: ActionForgetPeer},
	// Default Running self-loop: kernel is loaded and no other row
	// matched, so issue a routine adjust to converge config drift.
	{From: PhaseRunning, To: PhaseRunning, Trigger: func(obs Observation) bool { return obs.KernelLoaded }, Action: ActionAdjust},
}

// ObservePhase derives the current Phase from Observation by
// inspecting the same fields the transitions inspect. Useful for
// tests, logging, and status reporting.
func ObservePhase(obs Observation) DRBDPhase {
	switch {
	case !obs.SpecHasResource:
		return "" // no resource at all; FSM doesn't apply
	case obs.SpecHasDeletionTS:
		return PhaseDecommissioning
	case obs.SkipDiskProp:
		return PhaseSkipDisk
	case !obs.ResFileExists:
		return PhaseUnprovisioned
	case !obs.MetadataExists && !obs.SpecFlagsHasDiskless:
		return PhaseMetadataPending
	case !obs.KernelLoaded:
		return PhaseMetadataReady
	default:
		return PhaseRunning
	}
}

// NextTransition returns a pointer to the first Transition in the
// table whose From matches `from` and whose Trigger fires for `obs`,
// or nil if no transition applies (terminal-good state).
func NextTransition(from DRBDPhase, obs Observation) *Transition {
	for i := range fsm {
		if fsm[i].From == from && fsm[i].Trigger(obs) {
			return &fsm[i]
		}
	}

	return nil
}
