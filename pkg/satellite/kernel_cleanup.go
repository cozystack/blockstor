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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// defaultStuckSlotGrace is the in-memory debounce window the Bug 342
// v3 Pass-3 stuck-slot probe applies before tearing down a Connecting
// / StandAlone kernel slot with no peer-device. 30s is the upstream
// LINSTOR window for the same scenario; long enough that a transient
// network blip or in-flight handshake clears on its own, short enough
// that the catcher's 240s budget on tests/e2e/cli-matrix/r-full-
// lifecycle.sh covers the recovery on a relocate.
//
// Override via BSTOR_ZOMBIE_GRACE_S environment variable (seconds,
// integer). Invalid / negative / zero values fall back to the default.
const defaultStuckSlotGrace = 30 * time.Second

// stuckSlotGraceEnv is the env-var name that overrides
// defaultStuckSlotGrace. Documented separately so the stand-side iter
// can crank the window down for fast e2e validation without touching
// code.
const stuckSlotGraceEnv = "BSTOR_ZOMBIE_GRACE_S"

// Pass-number tags for structured-log key/value pairs. The two-pass
// taxonomy mirrors the original Bug 342 v1 design (pass 2 — UID-
// reconciliation — was revert-rolled-back in 2f0c61c62; the numbering
// is preserved so log-grep recipes from the design doc keep working).
const (
	pruneLogPass1 = 1
	pruneLogPass3 = 3
)

// PruneStaleKernelSlots is the Bug 342 v3 hook. It runs unconditionally
// on every Resource reconcile, BEFORE the controller-side allocation
// gate (waitForControllerAllocation) — kernel-state cleanup doesn't
// need peer Status.DRBDNodeID to be stamped, only the current
// expected peer-name set. Running before the gate is what unblocks
// the Bug 342 catcher scenario: the relocated peer's
// Status.DRBDNodeID is still nil mid-allocation, the gate would
// short-circuit the entire Apply.Apply path, and the kernel-side
// zombie slot from the previous incarnation would survive forever.
//
// Two passes, both state-only (no UID tracking — the v1/v2 fix
// attempts that added per-peer UIDs were rolled back as
// overengineered; this implementation aligns with upstream LINSTOR's
// state+timing approach):
//
//	Pass 1 — kernel slot for a peer name not in `expectedPeerNames`
//	  → drbdadm del-peer + drbdmeta forget-peer (every volume that has
//	  a device path). Equivalent to tearDownRemovedPeers but driven by
//	  kernel state instead of the previously-rendered .res file —
//	  which may not exist on a fresh satellite-pod for a relocated
//	  replica, which is precisely the Bug 342 root scenario.
//
//	Pass 3 — kernel slot in Connecting / StandAlone with no
//	  peer-device configured for any volume, observed in that state
//	  for at least `grace` seconds. → drbdadm del-peer + drbdmeta
//	  forget-peer. The subsequent `drbdadm adjust` in the regular
//	  apply path re-registers the peer-device cleanly.
//
//	  Debounce is in-memory (Reconciler.seenStuckAt). First sighting
//	  records the timestamp; subsequent reconciles compare against
//	  `grace`. Any reconcile where the slot is no longer stuck clears
//	  the entry so a flapping connection doesn't accumulate false
//	  debounce credits.
//
// Returns nil on best-effort success:
//   - Pass 1 del-peer errors bubble (a live kernel connection for a
//     peer K8s no longer expects is a correctness issue that should
//     surface in Apply results).
//   - Pass 3 errors are logged and not bubbled — the v3 prune is a
//     best-effort safety net; worst case the next reconcile retries.
//
// Show probe failure (kernel queryable but JSON malformed / drbdsetup
// missing) returns nil immediately — no kernel state observed means
// no cleanup to do, fall through to the normal apply path. devices
// may be nil; forget-peer is skipped for volumes without a device path.
func (r *Reconciler) PruneStaleKernelSlots(
	ctx context.Context,
	rdName string,
	expectedPeerNames []string,
	vols []int32,
	devices map[int32]string,
) error {
	if r.cfg.Adm == nil {
		// DRBD half disabled (storage-only tests / RDs). Nothing
		// to prune in kernel.
		return nil
	}

	logger := log.FromContext(ctx).WithValues("rd", rdName, "v3prune", true)

	slots, err := r.cfg.Adm.Show(ctx, rdName)
	if err != nil {
		// Show returns nil on the "resource not loaded" branch
		// — that's never an error. A bubbled error here means the
		// JSON itself was unreadable; degrade to no-op so the
		// reconcile path proceeds.
		logger.Info("PruneStaleKernelSlots: Show failed, skipping prune", "err", err.Error())

		return nil
	}

	if len(slots) == 0 {
		// No kernel-resident slots → nothing to prune. Clear any
		// stale debounce entries for this RD so a future
		// re-bring-up starts with a clean timer.
		r.clearDebounceForResource(rdName)

		return nil
	}

	expectedSet := make(map[string]struct{}, len(expectedPeerNames))
	for _, n := range expectedPeerNames {
		expectedSet[n] = struct{}{}
	}

	err = r.prunePass1Unexpected(ctx, rdName, slots, expectedSet, vols, devices, logger)
	if err != nil {
		return err
	}

	r.prunePass3Stuck(ctx, rdName, slots, expectedSet, vols, devices, logger)

	return nil
}

// prunePass1Unexpected fires `drbdadm del-peer` + `drbdmeta forget-peer`
// for every kernel-resident slot whose peer name is not in the
// K8s-supplied expected set. Bubbles del-peer errors because a live
// kernel connection for an unexpected peer is a correctness issue,
// not a slow-leak forget-peer slot. forget-peer errors stay inside
// teardownKernelSlot and don't bubble.
func (r *Reconciler) prunePass1Unexpected(
	ctx context.Context,
	rdName string,
	slots map[string]drbd.KernelSlot,
	expectedSet map[string]struct{},
	vols []int32,
	devices map[int32]string,
	logger logr.Logger,
) error {
	for name, slot := range slots {
		if _, expected := expectedSet[name]; expected {
			continue
		}

		teardownErr := r.teardownKernelSlot(ctx, rdName, &slot, vols, devices, logger.WithValues("pass", pruneLogPass1, "peer", name))
		if teardownErr != nil {
			return errors.Wrapf(teardownErr, "PruneStaleKernelSlots pass 1 (unexpected peer %s)", name)
		}
	}

	return nil
}

// prunePass3Stuck handles the Bug 342 zombie signature: a slot
// for an expected peer that's stuck in Connecting / StandAlone
// state with no peer-device configured, observed across at least
// `grace` seconds. Debounced via the Reconciler's seenStuckAt map;
// any healthy observation clears the entry so the timer resets.
// Errors are logged and not bubbled — the v3 prune is a best-effort
// safety net.
func (r *Reconciler) prunePass3Stuck(
	ctx context.Context,
	rdName string,
	slots map[string]drbd.KernelSlot,
	expectedSet map[string]struct{},
	vols []int32,
	devices map[int32]string,
	logger logr.Logger,
) {
	now := time.Now()
	grace := stuckSlotGrace()

	for name, slot := range slots {
		if _, expected := expectedSet[name]; !expected {
			// Already handled in Pass 1.
			continue
		}

		key := rdName + "/" + name

		if !r.shouldPruneStuckSlot(&slot, vols) {
			r.clearDebounceEntry(key)

			continue
		}

		first, ready := r.observeStuck(key, now, grace)
		if !ready {
			logger.V(1).Info("Pass 3: slot stuck but inside grace window",
				"peer", name,
				"state", slot.ConnectionState,
				"firstSeen", first,
				"grace", grace)

			continue
		}

		teardownErr := r.teardownKernelSlot(ctx, rdName, &slot, vols, devices, logger.WithValues("pass", pruneLogPass3, "peer", name))
		if teardownErr != nil {
			logger.Info("Pass 3: teardown of stuck slot failed; will retry next reconcile",
				"peer", name,
				"err", teardownErr.Error())

			continue
		}

		// Drop the debounce entry after a successful teardown so a
		// re-incarnated peer with the same name starts a fresh
		// timer.
		r.clearDebounceEntry(key)
	}
}

// teardownKernelSlot is the per-slot del-peer + per-volume forget-peer
// fan-out shared between Pass 1 and Pass 3. del-peer is mandatory;
// forget-peer is per-volume best-effort because v09 metadata lives
// in the per-volume block and a missing device path (DISKLESS local
// replica) means there's no metadata to clean up for that volume.
//
// Mirrors tearDownRemovedPeers' del-peer-then-forget-peer ordering
// without reusing it directly — that function is .res-file-driven and
// the v3 prune runs before the apply path's .res render.
func (r *Reconciler) teardownKernelSlot(
	ctx context.Context,
	rdName string,
	slot *drbd.KernelSlot,
	vols []int32,
	devices map[int32]string,
	logger logr.Logger,
) error {
	logger.Info("PruneStaleKernelSlots: tearing down kernel slot",
		"state", slot.ConnectionState,
		"nodeID", slot.NodeID)

	err := r.cfg.Adm.DelPeer(ctx, rdName, slot.Name)
	if err != nil {
		return errors.Wrapf(err, "drbdadm del-peer %s:%s", slot.Name, rdName)
	}

	// forget-peer is per-volume. Skip volumes without a resolved
	// device path — that's the diskless-local case where no metadata
	// exists to clean. Errors are non-fatal: a leaked metadata slot
	// is a slow correctness drag (eats one of MaxPeers-1 budget
	// entries) that the next reconcile will retry, while del-peer
	// success above already closed the immediate kernel-connection
	// leak.
	for _, vol := range vols {
		device, ok := devices[vol]
		if !ok || device == "" {
			continue
		}

		forgetErr := r.cfg.Adm.ForgetPeer(ctx, rdName, vol, device, slot.NodeID)
		if forgetErr != nil {
			logger.Info("PruneStaleKernelSlots: forget-peer failed (non-fatal)",
				"volume", vol,
				"device", device,
				"err", forgetErr.Error())
		}
	}

	return nil
}

// shouldPruneStuckSlot returns true when `slot` matches the Pass-3
// zombie signature: Connecting / StandAlone connection state AND no
// peer-device registered for ANY of the named volumes. Empty volumes
// list → false (no volumes = nothing to register = no zombie).
func (r *Reconciler) shouldPruneStuckSlot(slot *drbd.KernelSlot, vols []int32) bool {
	if !slot.IsConnectingOrStandalone() {
		return false
	}

	if len(vols) == 0 {
		return false
	}

	return !slot.HasAnyPeerDeviceConfigured(vols)
}

// observeStuck records (or consults) the first-seen timestamp for a
// stuck slot. Returns the recorded timestamp and whether the grace
// window has elapsed. Under-lock to keep concurrent reconciles for
// different RDs serialised correctly.
func (r *Reconciler) observeStuck(key string, now time.Time, grace time.Duration) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	first, ok := r.seenStuckAt[key]
	if !ok {
		r.seenStuckAt[key] = now

		return now, false
	}

	return first, now.Sub(first) >= grace
}

// clearDebounceEntry drops a single (rd, peer) debounce entry. Used
// after a successful teardown and on healthy state observation.
func (r *Reconciler) clearDebounceEntry(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.seenStuckAt, key)
}

// clearDebounceForResource drops every debounce entry under the
// "<rd>/" prefix. Invoked when Show returns no slots — the kernel
// has no view of the resource at all, so any stale debounce state
// is irrelevant and would only cause false-fast teardown on a future
// re-bring-up.
func (r *Reconciler) clearDebounceForResource(rdName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prefix := rdName + "/"

	for k := range r.seenStuckAt {
		if strings.HasPrefix(k, prefix) {
			delete(r.seenStuckAt, k)
		}
	}
}

// stuckSlotGrace reads BSTOR_ZOMBIE_GRACE_S (seconds, integer) and
// falls back to defaultStuckSlotGrace on missing / invalid / non-
// positive values. The env-var override is intended for stand-side
// e2e validation where the operator cranks the window down to a
// couple of seconds to fit inside the catcher's timeout.
func stuckSlotGrace() time.Duration {
	raw := os.Getenv(stuckSlotGraceEnv)
	if raw == "" {
		return defaultStuckSlotGrace
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultStuckSlotGrace
	}

	return time.Duration(n) * time.Second
}
