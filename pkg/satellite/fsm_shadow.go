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

// Phase 11.2.b shadow mode + Phase 11.2.c Stage 1 agreement counter:
// integrate the FSM lookup (defined in fsm.go) into applyDRBD as
// observability-only. Both the existing implicit-gate logic and the
// FSM run side by side; the FSM is READ-ONLY and only logs the action
// it would have chosen, plus an agree/diverge counter for each call.
// After a multi-day production soak we can compare expected-action
// against the OLD applyDRBD path's deterministic action choice and
// confidently flip the switchover (Phase 11.2.c).
//
// Constraints:
//   - No behaviour change. Observation builders are pure reads
//     (file stat + kernel probe); they never mutate state.
//   - No retries, no sleeps. A failing probe falls through to a
//     best-effort Observation and the shadow log notes the partial
//     reading. The old reconciler logic is unaffected.
//   - The agreement counter is cheap: a single expvar.Map.Add call
//     per reconcile. expvar is concurrent-safe and lock-free for
//     reads from /debug/vars.
//   - computeLegacyAction is a pure function — same Observation in,
//     same legacy-action out. Tests pin this so the counter never
//     produces spurious "diverge" entries from non-determinism on
//     the shadow side.

package satellite

import (
	"context"
	"expvar"
	"os"
	"path/filepath"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// fsmShadowAgreeCount tracks per-reconcile agreement between the FSM-
// suggested action and the OLD applyDRBD path's deterministic action
// choice. Keys are `<expectedAction>:<agreement>` where agreement is
// either "agree" or "diverge". One increment per logFsmShadow call.
//
// Exposed automatically under /debug/vars when the binary imports
// net/http/pprof or net/http's DefaultServeMux is served. The
// satellite binary currently exposes neither — wire-up for
// /debug/vars on the c-r manager's metrics endpoint is a Phase
// 11.2.c Stage 2 follow-up (see commit body). Tests read the map
// directly via Get / expvar.Map.Do.
//
//nolint:gochecknoglobals // expvar maps are conventionally package-level.
var fsmShadowAgreeCount = expvar.NewMap("blockstor_fsm_shadow")

// recordFsmShadowAgreement bumps the agree/diverge counter for one
// (expected, legacy) pair. Cheap: a single expvar.Map.Add — no
// allocations on the hot path other than the key concat (small
// stack-friendly strings).
func recordFsmShadowAgreement(expectedAction, legacyAction string) {
	agreement := "diverge"
	if expectedAction == legacyAction {
		agreement = "agree"
	}

	fsmShadowAgreeCount.Add(expectedAction+":"+agreement, 1)
}

// computeLegacyAction returns the logical action name the OLD
// applyDRBD path would dispatch for `obs`. Deterministic: same
// Observation → same action. Mirrors the gate chain in reconciler.go
// applyDRBD (L1055-1158) → runApplyDRBDVerb (L1547-1553) →
// runBringUpOrAdjust (L1578-1594) → runAdjust (L1642-1665).
//
// Branches in order:
//
//  1. Empty volumes (`len(dr.GetVolumes()) == 0`) → "noop". The
//     legacy path returns early before any drbdadm verb runs.
//     Observation alone cannot express "empty volumes" (the FSM
//     never sees that DR), so this branch lives on Observation's
//     SpecHasResource=false sentinel for the equivalent shape.
//     Documented divergence: when SpecHasResource=true but volumes
//     would be empty, the FSM still suggests renderRes — gate 1 of
//     the 11.2.c plan, classified as KNOWN.
//
//  2. firstActivation && diskless && !diskfulFlip → "adjust"
//     (skipMetadata, runApplyDRBDVerb→runAdjust); diskless replicas
//     never run create-md. Reconciler L1146 gate inverts this.
//
//  3. firstActivation && !diskless → "createMd" (ensureMetadata
//     fires before runApplyDRBDVerb). Reconciler L1146-1151.
//
//  4. diskfulFlip (Bug 319: kernel diskless, spec diskful, metadata
//     absent) → "createMd". Reconciler L1134, L1146.
//
//  5. !firstActivation && !diskfulFlip → runBringUpOrAdjust:
//     a. !isLoaded → "up" (Reconciler L1584-1590)
//     b. isLoaded && skipDisk (prop or kernel-diskless coercion) →
//     "adjustSkipDisk" (Reconciler L1661-1662)
//     c. isLoaded && !skipDisk → "adjust" (Reconciler L1663-1665)
//
// firstActivation is approximated by `!MetadataExists && !diskless`
// at this layer: the legacy `.md-created` marker file is the same
// stat the Observation already does (observeForFsm L83-85). diskful
// flip is approximated by `KernelLoaded && KernelHasDiskless &&
// !SpecFlagsHasDiskless`.
func computeLegacyAction(obs Observation) string {
	// Gate 1: no resource → no work.
	if !obs.SpecHasResource {
		return ActionNoop
	}

	diskless := obs.SpecFlagsHasDiskless

	// Approximation of `firstActivation := os.IsNotExist(.md-created
	// marker)`: the marker is absent precisely when MetadataExists is
	// false. For diskless replicas the legacy path skips the entire
	// ensureMetadata branch (Reconciler L1146 `!diskless`), so
	// firstActivation only matters when diskless=false.
	firstActivation := !obs.MetadataExists && !diskless

	// Diskful-flip recovery (the Bug-319 root-cause path): kernel
	// loaded + currently diskless + spec flipped to diskful +
	// metadata absent. Forces create-md re-entry regardless of the
	// .md-created marker.
	diskfulFlip := obs.KernelLoaded &&
		obs.KernelHasDiskless &&
		!obs.SpecFlagsHasDiskless &&
		!obs.MetadataExists

	// Gate 3 / 4: create-md fires when (firstActivation || flip) on
	// a diskful spec.
	if (firstActivation || diskfulFlip) && !diskless {
		return ActionCreateMd
	}

	// Gate 5a: kernel slot absent on a not-first-activation pass →
	// drbdadm up (Reconciler L1584-1590). Diskless replicas hit this
	// same gate on cold start (firstActivation=true but the !diskless
	// gate above skipped create-md, so we fall through here).
	if !obs.KernelLoaded {
		return ActionUp
	}

	// Gate 5b: kernel loaded + skip-disk pin (prop or kernel-diskless
	// coercion from Bug 280). The legacy path coerces skipDisk=true
	// whenever the kernel reports a Diskless volume on a loaded slot
	// (Reconciler L1652-1657) — match that here so the counter
	// agrees with production behaviour.
	skipDisk := obs.SkipDiskProp
	if !skipDisk && obs.KernelHasDiskless && !obs.SpecFlagsHasDiskless {
		skipDisk = true
	}

	if skipDisk {
		return ActionAdjustSkipDisk
	}

	// Gate 5c: kernel loaded, no skip-disk → plain drbdadm adjust.
	return ActionAdjust
}

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

	// Phase 11.3 Stage 1: derive MetadataExists from the
	// `MetadataCreated=True` Status Condition on the parent
	// Resource CRD (carried in via dr.MetadataCreated). The
	// on-disk `.md-created` marker is the migration-window
	// fallback: a cluster upgraded from a pre-11.3 build may have
	// the marker file but no Condition yet, and the FSM should
	// still see MetadataExists=true so it doesn't suggest
	// PhaseUnprovisioned → createMd against an already-created
	// metadata block. Once the satellite's startup backfill
	// stamps the Condition, the file fallback becomes redundant.
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")
	_, mdErr := os.Stat(mdMarkerPath)
	obs.MetadataExists = dr.GetMetadataCreated() || mdErr == nil

	// Phase 11.3 Stage 3: prefer the cached KernelLoaded Condition
	// over the kernel probe. The observer is the authoritative
	// pusher for the Condition — it stamps True on every events2
	// `exists resource` and False on `destroy resource`, so the
	// Condition tracks the kernel slot lifecycle without a syscall
	// on the reconciler hot path. Fall back to the kernel-direct
	// probe when the Condition is absent (cluster just upgraded
	// from a pre-11.3 satellite, observer restarting and yet to
	// re-stamp). Stage 4 / Phase 11.4 phases out the fallback after
	// production burnin.
	loaded := dr.GetKernelLoaded()
	if !loaded && r.cfg.Adm != nil {
		probe, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
		if err == nil {
			loaded = probe
		}
	}

	obs.KernelLoaded = loaded

	if obs.KernelLoaded && r.cfg.Adm != nil {
		disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
		if err == nil {
			obs.KernelHasDiskless = disklessVol
		}
	}

	return obs
}

// logFsmShadow runs the FSM against the current Observation and
// emits a single V(1) log line with the expected phase + action,
// the legacy action the OLD applyDRBD path would dispatch, and
// whether the two agree. Increments the fsmShadowAgreeCount expvar
// counter exactly once per call.
//
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

	expectedAction := ActionNoop
	nextPhase := phase

	if next := NextTransition(phase, obs); next != nil {
		expectedAction = next.Action
		nextPhase = next.To
	}

	legacyAction := computeLegacyAction(obs)
	recordFsmShadowAgreement(expectedAction, legacyAction)

	logger.Info("FSM shadow",
		"resource", dr.GetName(),
		"phase", string(phase),
		"expected", expectedAction,
		"legacy", legacyAction,
		"agreement", expectedAction == legacyAction,
		"to", string(nextPhase),
	)
}
