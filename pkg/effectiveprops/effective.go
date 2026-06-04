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

// Package effectiveprops resolves the DRBD-options bag for one
// Resource by walking the upstream-LINSTOR override hierarchy:
// Controller → ResourceGroup → ResourceDefinition → Resource.
//
// Lower scopes override upper, per non-nil field. Each scope is
// best-effort — a missing ControllerConfig / missing RG /
// missing RD degrades to "empty" rather than blocking the
// dispatch, so a partially-migrated cluster still produces a
// usable .res file.
//
// Lifted out of `internal/controller.ResourceReconciler` in
// Phase 10.1 so both the controller-side dispatcher AND the new
// satellite-side `pkg/satellite/controllers.ResourceReconciler`
// share one implementation.
package effectiveprops

import (
	"context"
	"maps"
	"strings"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// Resolve walks the four scopes and returns the merged Props
// map. The `c` reader is typically a controller-runtime
// `manager.GetClient()` — both controller and satellite hold
// one. `target` is the Resource whose effective props we want;
// `rd` is the parent ResourceDefinition (may be nil; the
// reconciler usually fetched it already).
//
// Phase 10.1 step.
func Resolve(ctx context.Context, c client.Reader, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (map[string]string, error) {
	if target == nil {
		return map[string]string{}, nil
	}

	ctrlCfg, err := controllerConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	var (
		ctrlTyped  *blockstoriov1alpha1.DRBDOptions
		ctrlExtras map[string]string
	)

	if ctrlCfg != nil {
		ctrlTyped = ctrlCfg.Spec.DRBDOptions
		ctrlExtras = ctrlCfg.Spec.ExtraProps
	}

	rgInfo, rdInfo, err := scopeInputs(ctx, c, rd)
	if err != nil {
		return nil, err
	}

	// Build one flat DRBD-props map per scope — typed fields emitted
	// back to their `DrbdOptions/...` wire keys, unioned with that
	// scope's untyped ExtraProps. A scope's typed/extras keysets are
	// disjoint by construction (the store transcoder routes each
	// recognised key into the typed struct and everything else into
	// ExtraProps), so the union is unambiguous within a scope.
	//
	// The controller scope is special: `controller drbd-options`
	// persists into ControllerConfig.Spec.ExtraProps (raw, untyped —
	// see pkg/rest/controller_props.go), so ctrlTyped is normally nil
	// and ctrlExtras carries the cluster-wide knobs. We still feed
	// ctrlTyped through emitScopeProps for completeness.
	ctrlProps := emitScopeProps(ctrlTyped, ctrlExtras)
	rgProps := emitScopeProps(rgInfo.Typed, rgInfo.Extras)
	rdProps := emitScopeProps(rdInfo.Typed, rdInfo.Extras)
	resProps := emitScopeProps(target.Spec.DRBDOptions, target.Spec.ExtraProps)

	// Merge the DRBD knobs in upstream-LINSTOR precedence order:
	// Controller → ResourceGroup → ResourceDefinition → Resource, each
	// lower (closer-to-the-resource) scope overriding the one above.
	// ResolveOptions also threads through the non-DRBD raw Spec.Props
	// (StorPoolName, Aux/*) from each scope. Because every scope's DRBD
	// knobs are funneled through the *same* precedence walk here, a
	// closer scope's explicit override always wins regardless of
	// whether the value was stored typed or as an ExtraProp — the
	// "closer to the resource wins" rule (C2) holds across the whole
	// chain, including the controller tier (C1).
	ctrlMerged := mergeScopeProps(ctrlProps, nil)
	rgMerged := mergeScopeProps(rgProps, rgInfo.Props)
	rdMerged := mergeScopeProps(rdProps, rdInfo.Props)
	resMerged := mergeScopeProps(resProps, target.Spec.Props)

	out := drbd.ResolveOptions(ctrlMerged, rgMerged, rdMerged, resMerged)

	// ResolveOptions only threads through NON-DRBD props from the
	// most-specific (resource) scope — by design for keys like
	// StorPoolName that only make sense on the resource. But the RG and
	// RD scopes carry load-bearing NON-DRBD ExtraProps that the
	// satellite reads off the dispatched resource: most importantly
	// `FileSystem/Type` (+ `FileSystem/MkfsParams`), which the
	// `linstor rd set-property … FileSystem/Type ext4` CLI and the
	// linstor-csi CreateVolume path stamp on the RD and which gate the
	// satellite's mkfs / `primary --force` seed path
	// (pkg/satellite/reconciler.go hasFileSystemConfigured). Dropping
	// them silently disables mkfs → a fresh replica never writes → the
	// DRBD current-UUID never rotates past the day0 GI → the
	// controller's RD.Spec.Initialized latch never flips.
	//
	// Re-overlay the non-DRBD ExtraProps from the RG then RD scope
	// (closer scope last, so RD wins over RG; the resource scope already
	// won via ResolveOptions). DRBD keys are intentionally skipped here —
	// the precedence walk above already resolved them. The controller
	// scope is deliberately NOT re-overlaid: cluster-wide non-DRBD knobs
	// like `Aux/zone` must not leak onto every resource (C1 contract).
	overlayNonDRBDExtras(out, rgInfo.Extras)
	overlayNonDRBDExtras(out, rdInfo.Extras)

	return out, nil
}

// overlayNonDRBDExtras copies the non-`DrbdOptions/...` entries of a
// scope's ExtraProps into out (overriding on key collision). DRBD keys
// are skipped — those are resolved by the precedence walk in
// ResolveOptions and must not be re-applied here (that would let an
// upper scope's DRBD ExtraProp clobber a closer scope's resolved
// value). nil src is a no-op.
func overlayNonDRBDExtras(out, src map[string]string) {
	for key, value := range src {
		if strings.HasPrefix(key, drbd.PropPrefix) {
			continue
		}

		out[key] = value
	}
}

// emitScopeProps flattens one scope's typed DRBDOptions plus its
// untyped ExtraProps into a single `DrbdOptions/...` wire-key map.
// Typed fields win over an ExtraProp of the same key within the scope
// (the transcoder keeps the two keysets disjoint, so this only ever
// matters for hand-crafted inputs). Returns a fresh map.
func emitScopeProps(typed *blockstoriov1alpha1.DRBDOptions, extras map[string]string) map[string]string {
	out := map[string]string{}

	maps.Copy(out, extras)
	maps.Copy(out, drbd.TypedDRBDOptionsToProps(typed))

	return out
}

// mergeScopeProps unions a scope's flattened DRBD knobs (drbdProps)
// with its raw, non-DRBD Spec.Props (StorPoolName, Aux/*). The DRBD
// knobs take precedence on key collision — a `DrbdOptions/...` key
// only ever appears in drbdProps, so in practice the two are disjoint.
// Returns a fresh map; nil inputs are treated as empty.
func mergeScopeProps(drbdProps, rawProps map[string]string) map[string]string {
	out := map[string]string{}

	maps.Copy(out, rawProps)
	maps.Copy(out, drbdProps)

	return out
}

// scopeInputs gathers the RG + RD scope inputs the hierarchy
// resolver needs. Returns zero-valued info structs for missing
// scopes — a missing RG / RD softly degrades to "no input at
// this level" rather than blocking dispatch.
type scopeInfo struct {
	Props  map[string]string
	Typed  *blockstoriov1alpha1.DRBDOptions
	Extras map[string]string
}

func scopeInputs(ctx context.Context, c client.Reader, rd *blockstoriov1alpha1.ResourceDefinition) (scopeInfo, scopeInfo, error) {
	var (
		rgInfo scopeInfo
		rdInfo scopeInfo
	)

	if rd == nil {
		return rgInfo, rdInfo, nil
	}

	rdInfo = scopeInfo{
		Props:  rd.Spec.Props,
		Typed:  rd.Spec.DRBDOptions,
		Extras: rd.Spec.ExtraProps,
	}

	if rd.Spec.ResourceGroupName == "" {
		return rgInfo, rdInfo, nil
	}

	var rgObj blockstoriov1alpha1.ResourceGroup

	getErr := c.Get(ctx, client.ObjectKey{Name: rd.Spec.ResourceGroupName}, &rgObj)
	switch {
	case getErr == nil:
		rgInfo = scopeInfo{
			Props:  rgObj.Spec.Props,
			Typed:  rgObj.Spec.DRBDOptions,
			Extras: rgObj.Spec.ExtraProps,
		}
	case apierrors.IsNotFound(getErr):
		// Soft-fail; see package doc.
	default:
		return rgInfo, rdInfo, errors.Wrapf(getErr, "get ResourceGroup %q", rd.Spec.ResourceGroupName)
	}

	return rgInfo, rdInfo, nil
}

// controllerConfig fetches the singleton ControllerConfig CRD.
// Returns (nil, nil) when missing — caller falls through to
// the legacy KVEntry path. Phase 10.4 step 1.
func controllerConfig(ctx context.Context, c client.Reader) (*blockstoriov1alpha1.ControllerConfig, error) {
	var cfg blockstoriov1alpha1.ControllerConfig

	err := c.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // optional singleton
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	return &cfg, nil
}
