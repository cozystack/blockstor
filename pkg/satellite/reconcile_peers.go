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

package satellite

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// zombieGraceDefault is the minimum age a Connecting / StandAlone
// kernel slot must reach before the Pass-3 zombie probe is allowed
// to tear it down. Tunable via BSTOR_ZOMBIE_GRACE_S so operators on
// slow-handshake networks can extend the debounce without recompile.
// Default 30s mirrors the design's reviewer-recommended floor: shorter
// than the handshake retry interval would race healthy reconnects.
const zombieGraceDefault = 30 * time.Second

// AppliedPeerUIDsStamper writes the Resource.Status.AppliedPeerUIDs
// map after a successful `drbdadm adjust`. Mirrors the existing
// MetadataCreatedStamper / FilesystemFormattedStamper pattern — the
// K8s SSA call lives in pkg/satellite/controllers (cached client
// owns the apiserver wire); the satellite's apply chain stays free
// of a controller-runtime client dependency.
type AppliedPeerUIDsStamper interface {
	// StampAppliedPeerUIDs SSA-patches the AppliedPeerUIDs map onto
	// Resource <resourceName>.Status. resourceName MUST be the
	// per-replica CRD name (`<rd>.<node>`, the Bug 344 lesson) —
	// passing the RD-only name 404s on every apply, polluting
	// satellite logs forever.
	//
	// Idempotent — a repeat call with the same UIDs is a no-op at
	// the apiserver level. Conflict-retry is the stamper's
	// responsibility (Status writers race the observer here).
	StampAppliedPeerUIDs(ctx context.Context, resourceName string, uids map[string]string) error
}

// reconcilePeers is the Bug 342 three-pass diff. Replaces the old
// .res-file-based tearDownRemovedPeers primitive with a stateless
// three-source diff over K8s desired (dr.Peers, carrying ResourceUID
// per peer), the last-applied UID map (dr.AppliedPeerUIDs, sourced
// from local Resource.Status.AppliedPeerUIDs), and the kernel actual
// state from `drbdsetup show -j` (drbd.Adm.Show).
//
// Pass 1: kernel-not-in-K8s — peer slots present in the kernel but
// no longer in desired → del-peer + forget-peer.
//
// Pass 2: UID mismatch (Bug 342 core) — peer name present on both
// sides but the K8s UID differs from the last-applied UID → same
// peer name, new identity. Force del-peer + forget-peer; the
// subsequent adjust pass re-registers under the new UID. forget-peer
// is keyed on the KERNEL-OBSERVED node-id (not the K8s-desired one)
// because the allocator may have reissued the new UID a fresh
// node-id (Bug 87 follow-up); the zombie slot is bound to the OLD id.
//
// Pass 3: zombie probe — DEBOUNCED via zombieGraceSeconds + multi-vol
// HasAnyPeerDeviceConfigured cross-check. Catches the Bug 342 wedge
// even when AppliedPeerUIDs is empty (rollout window / adoption-mode
// pre-stamp) by spotting Connecting / StandAlone slots with no
// peer-device registered for any volume after the grace period.
//
// `drbdsetup show` errors degrade gracefully — the diff falls through
// to UID-only mode (passes 1 and 3 become no-ops; pass 2 still acts
// when AppliedPeerUIDs is populated). A logged warning makes the
// degradation visible without wedging the reconcile.
//
// adoption-mode gate (separate function): when AppliedPeerUIDs is
// empty and the kernel is configured, trust the kernel state as the
// baseline and stamp current UIDs without touching connections —
// disaster recovery / LINSTOR-takeover path. See reconcilePeersAdopt.
func (r *Reconciler) reconcilePeers(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	if r.cfg.Adm == nil {
		// No DRBD wrapper wired (LayerStack without DRBD) — no
		// kernel slots to reconcile. Caller already guarded but
		// keep belt-and-braces.
		return nil
	}

	expected := indexExpectedPeers(dr.GetPeers())
	applied := dr.GetAppliedPeerUIDs()
	logger := log.FromContext(ctx)

	actual, showErr := r.cfg.Adm.Show(ctx, dr.GetName())
	if showErr != nil {
		logger.Info("drbdsetup show failed; falling back to UID-only diff",
			"rd", dr.GetName(), "err", showErr.Error())

		actual = nil
	}

	if r.maybeAdoptPeers(ctx, dr, expected, applied, actual) {
		// Adoption-mode handled this reconcile — the satellite has
		// stamped the baseline UIDs from observed kernel state and
		// declined to touch connections. Subsequent reconciles will
		// run the normal three-pass diff (applied is now non-empty).
		return nil
	}

	volNums := volumeNumbersOf(dr)

	if err := r.diffKernelNotInK8s(ctx, dr, expected, actual, devices); err != nil {
		return err
	}

	if err := r.diffUIDMismatch(ctx, dr, expected, applied, actual, devices); err != nil {
		return err
	}

	if err := r.diffZombieSlots(ctx, dr, actual, devices, volNums); err != nil {
		return err
	}

	return nil
}

// indexExpectedPeers builds a name → DesiredPeer map for O(1) lookup
// inside the three passes. The reconciler iterates kernel actual,
// K8s desired, and last-applied independently — a name-keyed map is
// the natural pivot.
func indexExpectedPeers(peers []intent.DesiredPeer) map[string]intent.DesiredPeer {
	out := make(map[string]intent.DesiredPeer, len(peers))
	for _, p := range peers {
		out[p.Name] = p
	}

	return out
}

// volumeNumbersOf collects the volume numbers from a DesiredResource,
// used by the multi-vol HasAnyPeerDeviceConfigured probe in pass 3
// and the per-vol forget-peer fan-out in passes 1 and 2.
func volumeNumbersOf(dr *intent.DesiredResource) []int32 {
	vols := dr.GetVolumes()
	if len(vols) == 0 {
		return nil
	}

	out := make([]int32, 0, len(vols))

	for _, v := range vols {
		out = append(out, v.GetVolumeNumber())
	}

	return out
}

// diffKernelNotInK8s is Pass 1: drop kernel slots that K8s no longer
// names in dr.Peers. del-peer severs the live kernel connection;
// forget-peer (per-volume, keyed on the slot's kernel-observed
// node-id) clears the on-disk GI slot so the MaxPeers-1 budget can
// be reused.
//
// forget-peer errors are non-fatal — a stale slot is a recoverable
// leak; the next reconcile retries. del-peer errors bubble: a live
// kernel connection that survives spec removal is a faster
// correctness issue (writes still replicate to a ghost peer).
func (r *Reconciler) diffKernelNotInK8s(ctx context.Context, dr *intent.DesiredResource,
	expected map[string]intent.DesiredPeer, actual map[string]drbd.KernelSlot, devices map[int32]string,
) error {
	for name, slot := range actual {
		if _, want := expected[name]; want {
			continue
		}

		if err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), name); err != nil {
			return errors.Wrapf(err, "pass1 del-peer %s from %s", name, dr.GetName())
		}

		r.forgetPeerAllVols(ctx, dr.GetName(), devices, slot.NodeID)
	}

	return nil
}

// diffUIDMismatch is Pass 2 (Bug 342 core). For every peer K8s still
// names: if AppliedPeerUIDs disagrees with the current ResourceUID
// (peer was re-created in K8s — same name, new identity), force
// del-peer + forget-peer. forget-peer is keyed on the KERNEL-observed
// node-id (slot.NodeID from `actual`) rather than the K8s-expected
// id because the allocator may have reissued the new UID a fresh
// id; the zombie is bound to the OLD id.
//
// Empty AppliedPeerUIDs entry (rollout / fresh-pod / adoption-mode
// pre-stamp) is treated as "no known identity" — fall through to
// adjust, which will register fresh. Pass 3's zombie probe is the
// safety net for the Bug 342 wedge when applied is empty.
func (r *Reconciler) diffUIDMismatch(ctx context.Context, dr *intent.DesiredResource,
	expected map[string]intent.DesiredPeer, applied map[string]string,
	actual map[string]drbd.KernelSlot, devices map[int32]string,
) error {
	logger := log.FromContext(ctx)

	for name, peer := range expected {
		last := applied[name]
		if last == "" || last == peer.ResourceUID {
			continue
		}

		slot, ok := actual[name]
		if !ok {
			// Kernel doesn't know this peer (yet) — adjust will
			// add it fresh under the new UID. Bug 342 zombie can
			// only manifest when a kernel slot exists.
			continue
		}

		logger.Info("UID mismatch, tearing down zombie slot",
			"rd", dr.GetName(), "peer", name,
			"appliedUID", last, "expectedUID", peer.ResourceUID,
			"kernelNodeID", slot.NodeID)

		if err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), name); err != nil {
			return errors.Wrapf(err, "pass2 del-peer %s from %s", name, dr.GetName())
		}

		r.forgetPeerAllVols(ctx, dr.GetName(), devices, slot.NodeID)
	}

	return nil
}

// diffZombieSlots is Pass 3. Catches Bug 342 even when AppliedPeerUIDs
// is empty (adoption-mode pre-stamp / rollout / fresh-pod) by
// inspecting kernel state directly:
//
//   - connection in {Connecting, StandAlone} (off-limits otherwise)
//   - state-change age >= zombieGraceSeconds (handshake retry window)
//   - no peer-device registered for ANY of the resource's volumes
//     (HasAnyPeerDeviceConfigured == false; multi-vol cross-check
//     so a partial handshake mid-flight isn't false-tripped)
//
// All three conditions together signal a wedged slot worth force-
// killing. zombieGraceSeconds is read from BSTOR_ZOMBIE_GRACE_S
// (seconds, integer) when set; falls back to zombieGraceDefault.
func (r *Reconciler) diffZombieSlots(ctx context.Context, dr *intent.DesiredResource,
	actual map[string]drbd.KernelSlot, devices map[int32]string, volNums []int32,
) error {
	grace := zombieGrace()
	logger := log.FromContext(ctx)
	now := time.Now()

	for name, slot := range actual {
		if !slot.IsConnectingOrStandalone() {
			continue
		}

		age := now.Sub(slot.LastStateChangeTime)
		if !slot.LastStateChangeTime.IsZero() && age < grace {
			// Within grace window — let the handshake retry path
			// finish before tearing down. Open-fail (timestamp
			// zero) treats the slot as "old enough" because we
			// have no better signal; the multi-vol HasAny check
			// below still requires no peer-device for ANY vol.
			continue
		}

		if slot.HasAnyPeerDeviceConfigured(volNums) {
			// Partial handshake — at least one vol has registered
			// a peer-device. DRBD is mid-flight; don't tear down.
			continue
		}

		logger.Info("zombie slot detected, forcing cleanup",
			"rd", dr.GetName(), "peer", name,
			"state", slot.ConnectionState, "ageSeconds", age.Seconds())

		if err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), name); err != nil {
			return errors.Wrapf(err, "pass3 del-peer %s from %s", name, dr.GetName())
		}

		r.forgetPeerAllVols(ctx, dr.GetName(), devices, slot.NodeID)
	}

	return nil
}

// forgetPeerAllVols fans forget-peer across every diskful volume of
// the resource. Empty device paths (diskless replica) and non-fatal
// errors are tolerated — a slot leak is recoverable; wedging the
// reconcile here would block convergent steady-state.
func (r *Reconciler) forgetPeerAllVols(ctx context.Context, rd string, devices map[int32]string, peerNodeID int32) {
	for volNum, device := range devices {
		if device == "" {
			continue
		}

		_ = r.cfg.Adm.ForgetPeer(ctx, rd, volNum, device, peerNodeID)
	}
}

// zombieGrace returns the Pass-3 debounce window, reading the
// BSTOR_ZOMBIE_GRACE_S environment variable (seconds, integer) when
// set and falling back to zombieGraceDefault otherwise. Malformed
// values (non-numeric, negative) silently revert to the default —
// operators see the standard 30s behaviour rather than a wedged
// reconcile or an over-eager teardown.
func zombieGrace() time.Duration {
	raw := os.Getenv("BSTOR_ZOMBIE_GRACE_S")
	if raw == "" {
		return zombieGraceDefault
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return zombieGraceDefault
	}

	return time.Duration(n) * time.Second
}

// maybeAdoptPeers is the adoption-mode gate. Returns true when this
// reconcile pass was satisfied by adoption (stamping baseline UIDs
// from observed kernel state without disrupting connections); false
// when adoption was declined and the normal three-pass diff must run.
//
// Gate conditions (from design rev 2):
//
//  1. applied is empty (fresh restore, fresh pod, LINSTOR takeover)
//  2. kernel has at least one configured peer slot (something to adopt)
//  3. peerSetsAgree(expected, actual) returns ok — name set + node-id
//     parity + per-volume peer-device presence all match (PSK
//     equivalence skipped while .res rendering parity is still in
//     flight; mismatch is logged but does not block adoption)
//
// On agreement: stamp Status.AppliedPeerUIDs and return true. The
// first reconcile after pod boot adds randomised jitter to avoid
// thundering-herd Status writes on N-RD clusters at startup
// (firstReconcileAfterBoot flag).
//
// On disagreement: return false. The caller falls through to the
// normal three-pass diff (safer default — del-peer + adjust will
// converge correctly even at the cost of a brief disconnect).
func (r *Reconciler) maybeAdoptPeers(ctx context.Context, dr *intent.DesiredResource,
	expected map[string]intent.DesiredPeer, applied map[string]string,
	actual map[string]drbd.KernelSlot,
) bool {
	if len(applied) != 0 || len(actual) == 0 {
		return false
	}

	agree, reason := peerSetsAgree(expected, actual, volumeNumbersOf(dr))
	logger := log.FromContext(ctx)

	if !agree {
		logger.Info("adoption refused, falling through to normal diff",
			"rd", dr.GetName(), "reason", reason)

		return false
	}

	if r.firstReconcileAfterBoot(dr.GetName(), dr.GetNodeName()) {
		// Stagger Status patches: rand[0, 30s) per Resource on
		// boot to avoid 6000 patches racing apiserver in lockstep
		// after a cluster-wide restart. Subsequent reconciles
		// skip this sleep (the flag flips after first pass).
		jitter := time.Duration(rand.Intn(int(zombieGraceDefault))) //nolint:gosec // adoption-mode jitter is not security-sensitive

		select {
		case <-ctx.Done():
			return false
		case <-time.After(jitter):
		}
	}

	logger.Info("adoption mode: stamping baseline UIDs from observed kernel",
		"rd", dr.GetName(), "peers", len(actual))

	if err := r.stampAppliedPeerUIDs(ctx, dr, expected); err != nil {
		logger.Error(err, "adoption mode: stamp failed; will retry next reconcile",
			"rd", dr.GetName())

		return false
	}

	return true
}

// peerSetsAgree returns (true, "") when expected (K8s desired) and
// actual (kernel observed) agree closely enough for adoption-mode to
// stamp baseline UIDs without disrupting connections.
//
// Checks (in order):
//
//  1. Name set equality — every K8s-desired peer is in the kernel,
//     and the kernel has no extra peers K8s doesn't know about.
//  2. Node-id parity — each peer's K8s-desired NodeID matches the
//     kernel-observed one. A mismatch means the allocator reissued
//     a different id and the kernel has the OLD one — adoption
//     can't bridge that without a del-peer first.
//  3. Per-volume peer-device presence — for every peer the kernel
//     has at least one peer-device registered across the resource's
//     volumes. Catches the Bug 342 zombie shape (no peer-device for
//     any vol → don't stamp UIDs; the zombie-probe will tear down
//     in the normal diff path).
//
// PSK equivalence (Spec.NetSecret vs kernel shared-secret) is part
// of the design but deferred to a follow-up — the .res rendering
// parity workstream hasn't surfaced Spec.NetSecret yet, so the
// kernel's shared-secret is the only source of truth. Skipping the
// PSK check is safe: a PSK mismatch surfaces as Connecting state
// (the kernel's handshake fails) → pass 3 zombie probe tears down
// after grace. The recovery is slower than the design's
// AdjustWithoutTearDown shortcut but correct.
func peerSetsAgree(expected map[string]intent.DesiredPeer, actual map[string]drbd.KernelSlot, vols []int32) (bool, string) {
	if len(expected) != len(actual) {
		return false, "name_set_size_mismatch"
	}

	for name, peer := range expected {
		slot, ok := actual[name]
		if !ok {
			return false, "name_set_mismatch:" + name
		}

		if peer.NodeID != 0 && slot.NodeID != 0 && peer.NodeID != slot.NodeID {
			return false, "node_id_mismatch:" + name
		}

		if len(vols) > 0 && !slot.HasAnyPeerDeviceConfigured(vols) {
			return false, "peer_device_absent:" + name
		}

		// Bug 342 v2: a Connecting/StandAlone slot is the exact wedge
		// shape this reconciler exists to clean up — adopting it would
		// stamp the stale-incarnation UID as baseline and block both
		// Pass 2 (UID-mismatch teardown) and Pass 3 (zombie probe)
		// from ever firing. Decline adoption so the caller falls
		// through to the normal three-pass diff where Pass 3 tears
		// the slot down after zombieGrace.
		if slot.IsConnectingOrStandalone() {
			return false, "peer_not_established:" + name + "=" + slot.ConnectionState
		}
	}

	return true, ""
}

// firstReconcileAfterBoot reports whether this is the first time
// the satellite has reconciled (rd, node) since the process started.
// True triggers the adoption-mode jitter sleep; subsequent reconciles
// (same process lifetime) skip it.
//
// In-memory only — no persistence across pod restarts (each restart
// IS a fresh boot; the jitter applies again, which is correct: pod
// restart = all RDs hit adoption-mode in lockstep again).
func (r *Reconciler) firstReconcileAfterBoot(rd, node string) bool {
	r.adoptOnceMu.Lock()
	defer r.adoptOnceMu.Unlock()

	if r.adoptOnce == nil {
		r.adoptOnce = make(map[string]struct{})
	}

	key := rd + "." + node
	if _, seen := r.adoptOnce[key]; seen {
		return false
	}

	r.adoptOnce[key] = struct{}{}

	return true
}

// stampAppliedPeerUIDs calls the configured stamper with the per-
// replica CRD name (Bug 344 lesson — passing the RD-only name 404s
// on every patch). expected is the current K8s desired peer set;
// each peer's ResourceUID becomes the value in the new map.
//
// nil stamper → no-op + nil error (unit-test friendly; the legacy
// code path tolerated stamper-absent configurations).
func (r *Reconciler) stampAppliedPeerUIDs(ctx context.Context, dr *intent.DesiredResource,
	expected map[string]intent.DesiredPeer,
) error {
	if r.cfg.AppliedPeerUIDsStamper == nil {
		return nil
	}

	next := make(map[string]string, len(expected))
	for name, peer := range expected {
		next[name] = peer.ResourceUID
	}

	// Bug 344 (regression of Bug 311 followup #501): SSA-patches a
	// Resource object whose Name is the CRD object name. Real
	// Resource CRDs are named `<rd>.<node>` (per-node sharding);
	// passing the RD-only name made the apiserver return 404 on
	// every stamp attempt, polluting ERROR logs since Phase 11.3
	// Stage 1 (#489). Mirror the Bug 344 fix here verbatim.
	resourceCRDName := dr.GetName() + "." + dr.GetNodeName()

	return r.cfg.AppliedPeerUIDsStamper.StampAppliedPeerUIDs(ctx, resourceCRDName, next)
}
