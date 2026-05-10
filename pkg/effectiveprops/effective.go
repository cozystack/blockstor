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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// LegacyControllerPropsInstance is the KVEntry-instance name
// the pre-Phase-10.4 `ControllerProps` used. Read-only fallback
// — once every cluster has migrated to `ControllerConfig` this
// path stops firing and the KVEntry CRD can go.
const LegacyControllerPropsInstance = "ControllerProps"

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

	controllerProps, err := LegacyControllerProps(ctx, c)
	if err != nil {
		return nil, err
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

	typed := drbd.ResolveDRBDOptions(ctrlTyped, rgInfo.Typed, rdInfo.Typed, target.Spec.DRBDOptions)

	out := drbd.ResolveOptions(controllerProps, rgInfo.Props, rdInfo.Props, target.Spec.Props)

	maps.Copy(out, drbd.TypedDRBDOptionsToProps(typed))
	maps.Copy(out, ctrlExtras)
	maps.Copy(out, rgInfo.Extras)
	maps.Copy(out, rdInfo.Extras)
	maps.Copy(out, target.Spec.ExtraProps)

	return out, nil
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

// LegacyControllerProps reads the KVEntry-shaped ControllerProps
// instance and returns its flat `{key: value}` view. Drained as
// more keys migrate to ControllerConfig's typed fields;
// eventually disappears with the KVEntry CRD. Exported so the
// controller_props test can pin the instance-filter invariant
// directly against the package.
func LegacyControllerProps(ctx context.Context, c client.Reader) (map[string]string, error) {
	var list blockstoriov1alpha1.KVEntryList

	err := c.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list KVEntries")
	}

	out := map[string]string{}

	for i := range list.Items {
		if list.Items[i].Spec.Instance != LegacyControllerPropsInstance {
			continue
		}

		out[list.Items[i].Spec.Key] = list.Items[i].Spec.Value
	}

	return out, nil
}
