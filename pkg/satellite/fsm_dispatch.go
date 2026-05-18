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

// Phase 11.2.c Stage 3d: the shadow-dispatch router. Maps each
// FSM-recommended Action onto its corresponding extracted helper
// (renderResFile / createMetadata / bringUpResource / adjustResource).
// The legacy chain in applyDRBD still runs after this dispatch fires
// — every helper is content-idempotent, so the second call is a
// near-no-op stat-and-skip path. Once dashboards confirm every
// transition has been FSM-dispatched in steady state over a full
// burnin window, Stage 4 retires the legacy gate chain entirely.

package satellite

import (
	"context"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// dispatchFsmAction routes a FSM-recommended Action to its
// corresponding extracted helper. Returns nil for unknown actions
// (the legacy chain handles them), for the explicit "no-op" action
// (FSM says nothing to do), and for actions the FSM can't safely
// double-fire on (ActionDecommission — delete-path territory the
// legacy chain still owns through tearDownRemovedPeers and the
// dedicated DeleteResource pipeline).
//
// Phase 11.2.c Stage 3d: shadow-dispatches every action. The helpers
// are content-idempotent so re-running the same work via the legacy
// chain below is safe. ActionCreateMd is gated inside the dispatch
// to mirror the legacy `firstActivation && !diskless && !MdExists`
// invariant — defense-in-depth in case the FSM lookup ever drifts
// from the legacy gate ordering.
func (r *Reconciler) dispatchFsmAction(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, action string, obs Observation) error {
	switch action {
	case ActionRenderRes:
		return r.renderResFile(ctx, dr, devices)
	case ActionCreateMd:
		// Mirror the legacy gates here as defense-in-depth: never
		// fire create-md on a Diskless replica (no lower disk to
		// stamp) and never on metadata that already exists
		// (`create-md --force` would wipe operator-stamped GI +
		// bitmap state). The legacy `firstActivation` gate stays at
		// its current call site — this re-check belts-and-braces it.
		if !obs.SpecHasResource || obs.MetadataExists || obs.SpecFlagsHasDiskless {
			return nil
		}

		return r.createMetadata(ctx, dr, devices)
	case ActionUp:
		return r.bringUpResource(ctx, dr)
	case ActionAdjust, ActionAdjustSkipDisk:
		// adjustResource computes the SkipDisk variant internally
		// from operator prop + kernel state. Pass diskfulFlip=false —
		// the Bug 319 flip is gate-level state the legacy chain still
		// owns (it has the .res-file stat + .md-created marker reads
		// the FSM Observation can't reproduce on its own).
		return r.adjustResource(ctx, dr, false)
	case ActionDecommission:
		// Decommission is delete-path territory; the legacy chain
		// owns tearDownRemovedPeers + storage cleanup via the
		// dedicated DeleteResource pipeline. Skip in shadow.
		return nil
	case ActionNoop:
		return nil
	default:
		return nil
	}
}
