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

// Phase 11.2.b shadow mode: integrate the FSM lookup (defined in
// fsm.go) into applyDRBD as observability-only. Both the existing
// implicit-gate logic and the FSM run side by side; the FSM is
// READ-ONLY and only logs the action it would have chosen. After a
// few production reconciles we can compare expected-action against
// the actual code path and confidently flip the switchover
// (Phase 11.2.c).
//
// Constraints:
//   - No behaviour change. Observation builders are pure reads
//     (file stat + kernel probe); they never mutate state.
//   - No retries, no sleeps. A failing probe falls through to a
//     best-effort Observation and the shadow log notes the partial
//     reading. The old reconciler logic is unaffected.
//   - No coupling to the reconciler's branching structure: the
//     shadow lives in a single observation block at the top of
//     applyDRBD; everything below it is the unchanged historical
//     path.

package satellite

import (
	"context"
	"os"
	"path/filepath"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// observeForFsm builds an Observation snapshot for FSM shadow-mode
// evaluation. Pure reads only: file stat on the .res / .md-created
// markers plus the kernel probe via drbdadm. No mutation, no
// retries.
//
// `diskless` is the same flag applyDRBD computes from
// `isDiskless(dr.GetFlags())` — passed in to keep the observation
// helper free of slice-walking duplication.
//
// On any individual probe error (e.g. the kernel probe times out)
// the field stays at its zero value and the caller proceeds with a
// best-effort snapshot. The shadow path is observability only, so
// a partially-filled Observation is preferable to dropping the log
// entry entirely.
//
// SpecHasDeletionTS is always false here: the satellite-side
// DesiredResource carries no DeletionTimestamp (deletion is routed
// through the separate DeleteResource path), and applyDRBD only
// runs on the apply branch. The Decommissioning transitions in the
// FSM table remain for future symmetry with the delete reconciler.
func (r *Reconciler) observeForFsm(ctx context.Context, dr *intent.DesiredResource, diskless bool) Observation {
	obs := Observation{
		SpecHasResource:      dr != nil && dr.GetName() != "",
		SpecFlagsHasDiskless: diskless,
		SpecHasDeletionTS:    false,
		SkipDiskProp:         isSkipDiskEnabled(dr),
	}

	if !obs.SpecHasResource {
		return obs
	}

	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	_, resErr := os.Stat(resPath)
	obs.ResFileExists = resErr == nil

	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")
	_, mdErr := os.Stat(mdMarkerPath)
	obs.MetadataExists = mdErr == nil

	if r.cfg.Adm != nil {
		loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
		if err == nil {
			obs.KernelLoaded = loaded
		}

		if obs.KernelLoaded {
			disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
			if err == nil {
				obs.KernelHasDiskless = disklessVol
			}
		}
	}

	return obs
}

// logFsmShadow runs the FSM against the current Observation and
// emits a single V(1) log line with the expected phase + action.
// Pure observability: no return value, no side effects on the
// reconciler. Safe to call before, after, or in parallel with the
// historical apply path.
//
// Logged at V(1) so production deployments can opt into the noise
// (one line per apply) without touching the default log level. The
// "FSM shadow" prefix makes the lines greppable for divergence
// triage in Phase 11.2.c.
func (r *Reconciler) logFsmShadow(ctx context.Context, dr *intent.DesiredResource, diskless bool) {
	obs := r.observeForFsm(ctx, dr, diskless)

	phase := ObservePhase(obs)
	logger := log.FromContext(ctx).WithName("fsm-shadow").V(1)

	if next := NextTransition(phase, obs); next != nil {
		logger.Info("expected transition",
			"resource", dr.GetName(),
			"phase", string(phase),
			"action", next.Action,
			"to", string(next.To),
		)

		return
	}

	logger.Info("terminal phase (no transition)",
		"resource", dr.GetName(),
		"phase", string(phase),
	)
}
